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

#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags_declare.h>
#include <google/protobuf/any.h>
#include <gtest/gtest.h>

#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(subprocess_rpc_timeout_ms);

using google::protobuf::Any;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

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

TEST_F(SubprocessClientTest, TestMultithreadedCommunication) {
  constexpr int kNumThreads = 20;
  constexpr int kNumPerThread = 10000;
  const string kEchoData = "this is a long long long payload";
  vector<vector<SubprocessRequestPB>> requests(kNumThreads,
      vector<SubprocessRequestPB>(kNumPerThread));
  vector<vector<SubprocessResponsePB>> responses(kNumThreads,
      vector<SubprocessResponsePB>(kNumPerThread));
  for (int t = 0; t < kNumThreads; t++) {
    for (int i = 0; i < kNumPerThread; i++) {
      unique_ptr<Any> any(new Any);
      EchoRequestPB echo_request;
      echo_request.set_data(kEchoData);
      any->PackFrom(echo_request);
      requests[t][i].set_allocated_request(any.release());
    }
  }
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
    vector<thread> threads;
    for (int t = 0; t < kNumThreads; t++) {
      threads.emplace_back([&, t] {
        for (int i = 0; i < kNumPerThread; i++) {
          ASSERT_OK(client_->Execute(&requests[t][i], &responses[t][i]));
        }
      });
    }
    for (auto& t : threads) {
      t.join();
    }
  sw.stop();
  double reqs_sent = kNumThreads * kNumPerThread;
  double elapsed_seconds = sw.elapsed().wall_seconds();
  LOG(INFO) << Substitute("Sent $0 requests in $1 seconds: $2 rps",
      reqs_sent, elapsed_seconds, reqs_sent / elapsed_seconds);
  for (int t = 0; t < kNumThreads; t++) {
    for (int i = 0; i < kNumPerThread; i++) {
      EchoResponsePB echo_resp;
      ASSERT_TRUE(responses[t][i].response().UnpackTo(&echo_resp));
      ASSERT_EQ(echo_resp.data(), kEchoData);

    }
  }
}

} // namespace subprocess
} // namespace kudu
