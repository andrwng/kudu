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

#include "kudu/subprocess/client.h"

#include <gflags/gflags_declare.h>
#include <google/protobuf/any.h>
#include <gtest/gtest.h>

#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(subprocess_rpc_timeout_ms);

using google::protobuf::Any;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace subprocess {

class SubprocessClientTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    server_ = make_shared<SubprocessServer>();
    ASSERT_OK(server_->Init());
    client_.reset(new SubprocessClient(server_));
    ASSERT_OK(client_->Start());
  }

  void TearDown() override {
    if (client_) {
      client_->Stop();
    }
    server_->Shutdown();
    KuduTest::TearDown();
  }

 protected:
  shared_ptr<SubprocessServer> server_;
  unique_ptr<SubprocessClient> client_;
};

TEST_F(SubprocessClientTest, TestBasicCommmunication) {
  const string kEchoData = "hello";
  SubprocessRequestPB request;
  EchoRequestPB echo_request;
  echo_request.set_data(kEchoData);
  unique_ptr<Any> any(new Any);
  any->PackFrom(echo_request);
  request.set_allocated_request(any.release());

  SubprocessResponsePB response;
  ASSERT_OK(client_->Execute(&request, &response));
  EchoResponsePB echo_response;
  ASSERT_TRUE(response.response().UnpackTo(&echo_response));
  ASSERT_EQ(echo_response.data(), kEchoData);
}

} // namespace subprocess
} // namespace kudu
