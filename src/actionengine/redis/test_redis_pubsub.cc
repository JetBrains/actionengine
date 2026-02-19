// Copyright 2026 The Action Engine Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <absl/status/status_matchers.h>
#include <absl/strings/str_cat.h>
#include <absl/time/time.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest.h>

#include "actionengine/concurrency/concurrency.h"  // act::Mutex/CondVar
#include "actionengine/redis/pubsub.h"
#include "actionengine/redis/redis.h"
#include "actionengine/redis/test_helpers.h"  // Factored common helpers

namespace act::redis {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;

TEST(RedisPubSubTest, SubscribePublishQueueDelivery) {
  auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }

  const std::string channel = redis->GetKey("pubsub:test:queue");

  auto sub = redis->Subscribe(channel);
  ASSERT_TRUE(sub.ok()) << sub.status();

  // Wait for the subscribe ACK to arrive and be processed by the client.
  thread::Select({(*sub)->OnSubscribe()});

  // Publish two messages and read them back through the queue reader.
  test::PublishOrDie(redis.get(), channel, "m1");
  test::PublishOrDie(redis.get(), channel, "m2");

  Reply r1, r2;
  ASSERT_TRUE((*sub)->GetReader()->Read(&r1));
  ASSERT_TRUE((*sub)->GetReader()->Read(&r2));

  EXPECT_THAT(ConvertTo<std::string>(std::move(r1)), IsOkAndHolds("m1"));
  EXPECT_THAT(ConvertTo<std::string>(std::move(r2)), IsOkAndHolds("m2"));

  // Now unsubscribe and ensure OnUnsubscribe is triggered.
  ASSERT_TRUE(redis->Unsubscribe(channel).ok());
  thread::Select({(*sub)->OnUnsubscribe()});

  // After unsubscribe, further publishes should not be delivered to this sub.
  test::PublishOrDie(redis.get(), channel, "m3");

  Reply r3;
  // Reader should be closed after unsubscribe; Read should return false.
  EXPECT_FALSE((*sub)->GetReader()->Read(&r3));
}

TEST(RedisPubSubTest, MultipleSubscribersReceiveAll) {
  auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }
  const std::string channel = redis->GetKey("pubsub:test:multi");

  auto sub1 = redis->Subscribe(channel);
  ASSERT_TRUE(sub1.ok()) << sub1.status();
  auto sub2 = redis->Subscribe(channel);
  ASSERT_TRUE(sub2.ok()) << sub2.status();

  // Wait for subscribe notifications for both.
  thread::Select({(*sub1)->OnSubscribe()});
  thread::Select({(*sub2)->OnSubscribe()});

  test::PublishOrDie(redis.get(), channel, "hello");

  Reply a, b;
  ASSERT_TRUE((*sub1)->GetReader()->Read(&a));
  ASSERT_TRUE((*sub2)->GetReader()->Read(&b));
  EXPECT_THAT(ConvertTo<std::string>(std::move(a)), IsOkAndHolds("hello"));
  EXPECT_THAT(ConvertTo<std::string>(std::move(b)), IsOkAndHolds("hello"));

  ASSERT_TRUE(redis->Unsubscribe(channel).ok());
  thread::Select({(*sub1)->OnUnsubscribe()});
  thread::Select({(*sub2)->OnUnsubscribe()});
}

TEST(RedisPubSubTest, CallbackSubscriberReceivesMessages) {
  auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }
  const std::string channel = redis->GetKey("pubsub:test:cb");

  act::Mutex mu;
  std::vector<std::string> got;
  act::CondVar cv;

  auto sub = redis->Subscribe(channel, [&](Reply r) {
    absl::StatusOr<std::string> s = ConvertTo<std::string>(std::move(r));
    ASSERT_THAT(s, IsOk());
    act::MutexLock lg(&mu);
    got.push_back(*std::move(s));
    cv.SignalAll();
  });
  ASSERT_TRUE(sub.ok()) << sub.status();

  // Even with callback-based subscription, OnSubscribe should fire.
  thread::Select({(*sub)->OnSubscribe()});

  test::PublishOrDie(redis.get(), channel, "x");
  test::PublishOrDie(redis.get(), channel, "y");

  // Wait until both messages observed.
  {
    act::MutexLock lk(&mu);
    while (got.size() < 2) {
      cv.Wait(&mu);
    }
  }
  EXPECT_EQ(got[0], "x");
  EXPECT_EQ(got[1], "y");

  ASSERT_TRUE(redis->Unsubscribe(channel).ok());
  thread::Select({(*sub)->OnUnsubscribe()});
}

TEST(RedisPubSubTest, Resp3PushDelivery_AfterHello3) {
  auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }
  // Switch to RESP3 to validate PUSH handling.
  auto hello = redis->Hello(3);
  ASSERT_TRUE(hello.ok()) << hello.status();

  const std::string channel = redis->GetKey("pubsub:test:resp3");
  auto sub = redis->Subscribe(channel);
  ASSERT_TRUE(sub.ok()) << sub.status();

  // Wait for subscribe ack.
  thread::Select({(*sub)->OnSubscribe()});

  test::PublishOrDie(redis.get(), channel, "resp3-msg");

  Reply msg;
  // Expect delivery. If OnPushReply is not implemented, this will hang.
  ASSERT_TRUE((*sub)->GetReader()->Read(&msg))
      << "Expected message via RESP3 PUSH delivery";
  EXPECT_THAT(ConvertTo<std::string>(std::move(msg)),
              IsOkAndHolds("resp3-msg"));

  ASSERT_TRUE(redis->Unsubscribe(channel).ok());
  thread::Select({(*sub)->OnUnsubscribe()});
}

}  // namespace
}  // namespace act::redis
