// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/subprocess/server.h"

#include <gtest/gtest.h>

#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace subprocess {

class SubprocessServerTest : public KuduTest {
};

//TEST_F(SubprocessServerTest, TestImmediateShutdown) {
//  SubprocessServer server;
//  ASSERT_OK(server.Init());
//  server.Shutdown();
//
//  SubprocessRequestPB request;
//  SubprocessServer::ResponseCallback callback;
//  int64_t id;
//  Status s = server.SendRequest(&request, &callback, &id);
//  ASSERT_TRUE(s.IsServiceUnavailable());
//}
//
} // namespace subprocess
} // namespace kudu
