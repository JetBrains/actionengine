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

#include <memory>
#include <string>

#include <absl/status/statusor.h>
#include <absl/strings/numbers.h>
#include <absl/strings/string_view.h>
#include <absl/time/time.h>
#include <gtest/gtest.h>

#include "actionengine/redis/redis.h"
#include "actionengine/redis/test_helpers.h"  // Factored common helpers

namespace act::redis {

namespace {

// Common TryConnect now lives in test_helpers.h

TEST(RedisBasicTest, ConnectAndHelloOrSkip) {
  const auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }

  auto hello = redis->Hello();
  ASSERT_TRUE(hello.ok()) << hello.status();
  // Version is a string; require it to be non-empty and numeric >= 2 if parsable.
  EXPECT_FALSE(hello->version.empty());
  int v = 0;
  if (absl::SimpleAtoi(hello->version, &v)) {
    EXPECT_GE(v, 2);
  }
}

TEST(RedisBasicTest, SetGetRoundtrip) {
  const auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }

  redis->SetKeyPrefix("unittest:");

  const std::string key = "redis_basic_roundtrip";
  const std::string value = "hello";

  const auto st = redis->Set(key, value);
  ASSERT_TRUE(st.ok()) << st;

  auto got = redis->Get<std::string>(key);
  ASSERT_TRUE(got.ok()) << got.status();
  EXPECT_EQ(*got, value);
}

}  // namespace

}  // namespace act::redis
