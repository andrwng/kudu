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
#include "kudu/subprocess/call.h"
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

// XXX(awong): templatize the API with some kind of SubprocessProxy.
SubprocessClient::SubprocessClient(shared_ptr<SubprocessServer> server)
    : server_(std::move(server)),
      next_id_(1) {
}

SubprocessClient::~SubprocessClient() {
  Stop();
}

Status SubprocessClient::Start() {
  return Status::OK();
}

void SubprocessClient::Stop() {
}

Status SubprocessClient::Execute(SubprocessRequestPB* req,
                                 SubprocessResponsePB* resp) {
  req->set_id(next_id_.Increment());
  Synchronizer synchronizer;
  auto callback = synchronizer.AsStdStatusCallback();
  shared_ptr<SubprocessCall> call(new SubprocessCall(req, resp, &callback));
  RETURN_NOT_OK(server_->QueueCall(call));

  // XXX(awong): wrap the callback, request, and response in some OutboundCall
  // object and put that on the queue
  // - The writer thread will pull calls from the queue, put the callback
  //   somewhere, and serialize the request bytes to the pipe. The deadline
  //   tracking starts when the call is pulled from the queue.
  // - The reader thread will serialize request bytes from the pipe and put it
  //   onto the inbound response queue.
  // - The deadline checker thread will look at the earliest deadlines and
  //   check if any Calls need to be terminated, removing the callback for the
  //   given call from the list, and calling it with a TimedOut error.
  //   - The callback will be done in the responder threadpool.
  // - Responder threads will take from the queue of responses, look for the
  //   call (may be removed if the deadline passed), and call the callback.
  //   These will live in a threadpool.
  return synchronizer.Wait();
}

} // namespace subprocess
} // namespace kudu
