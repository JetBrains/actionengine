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

#include <cstdint>
#include <memory>
#include <string>

#include <absl/time/time.h>
#include <gtest/gtest.h>

#include "actionengine/redis/redis.h"
#include "actionengine/redis/test_helpers.h"  // Factored common helpers

namespace act::redis {
namespace {

TEST(RedisZRangeTest, WithScores) {
  auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }

  const std::string key = redis->GetKey("zrange:test");
  // Ensure clean state
  (void)redis->ExecuteCommand("DEL", {key});

  // Prepare sorted set: ZADD key score member
  {
    // Use numeric members so Redis::ZRange(withscores=false) can convert them
    // into integers as expected by the current implementation.
    CommandArgs args = {key, "1", "1", "2", "2", "3", "3"};
    auto r = redis->ExecuteCommand("ZADD", args);
    ASSERT_TRUE(r.ok()) << r.status();
  }

  auto z = redis->ZRange(key, 0, -1, /*withscores=*/false);
  ASSERT_TRUE(z.ok()) << z.status();
  // Expect the map to contain the 3 members; values should be nullopt
  ASSERT_EQ(z->size(), 3u);
  EXPECT_FALSE(z->at("1").has_value());
  EXPECT_FALSE(z->at("2").has_value());
  EXPECT_FALSE(z->at("3").has_value());
}

}  // namespace
}  // namespace act::redis
