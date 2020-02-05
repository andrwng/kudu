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

#include "kudu/util/status_callback.h" 

namespace kudu {
namespace subprocess {

class SubprocessRequestPB;
class SubprocessResponsePB;

// Encapsulates the state tracked by a single call made to to a subprocess.
class SubprocessCall {
 public:
  SubprocessCall(const SubprocessRequestPB* req,
                 SubprocessResponsePB* resp,
                 const StdStatusCallback* cb)
      : req_(req), resp_(resp), cb_(cb),
        callback_taken_(false), start_time_(MonoTime::Now()) {}

  // Returns whether the callback has been scheduled yet.
  // Only the thread that this returns 'true' to should schedule the callback.
  bool TakeCallback() {
    return true;
  }

  const StdStatusCallback* cb() const {
    return cb_;
  }

  const MonoTime& start_time() {
    return start_time_;
  }

  const SubprocessRequestPB* req() const {
    return req_;
  }

  void SetResponse(SubprocessResponsePB&& resp) {
    *resp_ = std::move(resp);
  }

  const SubprocessRequestPB* req_;
  SubprocessResponsePB* resp_;
  const StdStatusCallback* cb_;
  bool callback_taken_;
  const MonoTime start_time_;
};

} // namespace subprocess
} // namespace kudu
