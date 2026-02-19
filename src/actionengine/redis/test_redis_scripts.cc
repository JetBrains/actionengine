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
#include <vector>

#include <absl/time/time.h>
#include <gtest/gtest.h>

#include "actionengine/redis/redis.h"
#include "actionengine/redis/test_helpers.h"  // Factored common helpers

namespace act::redis {
namespace {

TEST(RedisScriptsTest, RegisterAndExecute) {
  auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }

  // Simple script: returns KEYS[1] .. returns argument unchanged
  const std::string script_name = "echo_key";
  const std::string script_code = "return KEYS[1]";
  auto sha =
      redis->RegisterScript(script_name, script_code, /*overwrite=*/true);
  ASSERT_TRUE(sha.ok()) << sha.status();

  CommandArgs keys = {"scripts:test:key"};
  auto reply = redis->ExecuteScript(script_name, keys, {});
  ASSERT_TRUE(reply.ok()) << reply.status();
  absl::StatusOr<std::string> str_reply = reply->ConsumeStringContent();
  ASSERT_TRUE(str_reply.ok()) << str_reply.status();
  EXPECT_EQ(str_reply.value(), keys[0]);
}

}  // namespace
}  // namespace act::redis
