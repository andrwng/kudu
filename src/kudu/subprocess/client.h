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

#pragma once

#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/threadpool.h"

namespace kudu {
namespace subprocess {

class SubprocessClient {
 public:
  explicit SubprocessClient(std::shared_ptr<SubprocessServer> server);
  ~SubprocessClient();

  // Starts the subprocesss client instance.
  Status Start() WARN_UNUSED_RESULT;

  // Stops the subprocesss client instance.
  void Stop();

  // Synchronous call to submit a request and fills the 'response' from the
  // subprocess server.
  //
  // Each call can time out if there isn't any responses in an expected
  // period of time.
  Status Execute(SubprocessRequestPB* request,
                 SubprocessResponsePB* response) WARN_UNUSED_RESULT;

 private:
  static const char* const kSubprocessName;
  std::shared_ptr<SubprocessServer> server_;

  // ID to assign to the next request.
  AtomicInt<int64_t> next_id_;

};

} // namespace subprocess
} // namespace kudu
