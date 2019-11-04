// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/subprocess/client.h"

#include <gflags/gflags.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/async_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/status.h"

DEFINE_int32(subprocess_rpc_timeout_ms, 15 * 1000, // 15 sec
             "Timeout used for the subprocess async rpc calls.");
TAG_FLAG(subprocess_rpc_timeout_ms, advanced);
TAG_FLAG(subprocess_rpc_timeout_ms, runtime);

using std::shared_ptr;

namespace kudu {
namespace subprocess {

const char* const SubprocessClient::kSubprocessName = "subprocess";

SubprocessClient::SubprocessClient(shared_ptr<SubprocessServer> server)
    : server_(std::move(server)),
      timer_(1) {
}

SubprocessClient::~SubprocessClient() {
  Stop();
}

Status SubprocessClient::Start() {
  if (threadpool_) {
    return Status::IllegalState("Subprocess client is already started");
  }
  return ThreadPoolBuilder(kSubprocessName)
      .set_min_threads(1)
      .set_max_threads(1)
      .Build(&threadpool_);
}

void SubprocessClient::Stop() {
  if (threadpool_) {
    threadpool_->Shutdown();
  }
}

Status SubprocessClient::Execute(SubprocessRequestPB* req,
                                 SubprocessResponsePB* res) {
  Synchronizer synchronizer;
  auto callback = synchronizer.AsStdStatusCallback();

  RETURN_NOT_OK(threadpool_->SubmitFunc([=] {
    // TODO(hao): add retry logic

    Status s;
    SubprocessServer::ResponseCallback cb =
      [&](const Status& status, SubprocessResponsePB response) {
        if (status.ok()) {
          *res = response;
        } else {
          s = status;
        }
        timer_.Reset(0);
      };
    int64_t id;
    timer_.Reset(1);
    s = server_->SendRequest(req, &cb, &id);
    // Timeout the request if takes too long.
    if (!timer_.WaitFor(MonoDelta::FromMilliseconds(FLAGS_subprocess_rpc_timeout_ms))) {
      server_->UnregisterCallbackById(id);
      return callback(
          Status::TimedOut("unable to get the response from subprocess before time out "));
    }
    return callback(s);
  }));

  return synchronizer.Wait();
}

} // namespace subprocess
} // namespace kudu
