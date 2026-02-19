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

#ifndef ACTIONENGINE_REDIS_TEST_HELPERS_H_
#define ACTIONENGINE_REDIS_TEST_HELPERS_H_

#include <memory>
#include <string>

#include <absl/status/statusor.h>
#include <absl/strings/numbers.h>
#include <absl/strings/string_view.h>
#include <absl/time/time.h>
#include <gtest/gtest.h>

#include "actionengine/redis/redis.h"

namespace act::redis::test {

// Tries to connect to localhost Redis quickly; returns status for diagnostics.
inline absl::StatusOr<std::unique_ptr<Redis>> ConnectOrStatus(
    absl::string_view host = "127.0.0.1", int port = 6379,
    absl::Duration timeout = absl::Seconds(1)) {
  return Redis::Connect(host, port, timeout);
}

// Helper that tries to connect and returns nullptr if not available.
inline std::unique_ptr<Redis> TryConnect(
    absl::string_view host = "127.0.0.1", int port = 6379,
    absl::Duration timeout = absl::Seconds(1)) {
  absl::StatusOr<std::unique_ptr<Redis>> r =
      ConnectOrStatus(host, port, timeout);
  if (!r.ok())
    return nullptr;
  return std::move(r).value();
}

// Publishes a message and ASSERTs on failure.
inline void PublishOrDie(Redis* redis, std::string_view channel,
                         std::string_view payload) {
  ASSERT_NE(redis, nullptr);
  auto r = redis->ExecuteCommand("PUBLISH", {channel, payload});
  ASSERT_TRUE(r.ok()) << r.status();
}

// Validates that HELLO version string parses to an integer >= min_version if
// numeric; otherwise simply ensures it is non-empty.
inline void ExpectHelloVersionAtLeast(Redis* redis, int min_version) {
  ASSERT_NE(redis, nullptr);
  auto hello = redis->Hello();
  ASSERT_TRUE(hello.ok()) << hello.status();
  EXPECT_FALSE(hello->version.empty());
  int parsed = 0;
  if (absl::SimpleAtoi(hello->version, &parsed)) {
    EXPECT_GE(parsed, min_version);
  }
}

}  // namespace act::redis::test

#endif  // ACTIONENGINE_REDIS_TEST_HELPERS_H_
