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

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/util/atomic.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/threadpool.h"

namespace kudu {

class Thread;

namespace subprocess {

// Used by 'SubprocessServer::request_queue_' to determine logical size of a request.
struct RequestLogicalSize {
  static size_t logical_size(const SubprocessRequestPB* request) {
    return request->ByteSizeLong();
  }
};

typedef BlockingQueue<SubprocessRequestPB*, RequestLogicalSize> RequestQueue;

class SubprocessServer {
 public:
  static const Status kShutdownStatus;

  SubprocessServer();
  ~SubprocessServer();

  typedef std::function<void(const Status& status, SubprocessResponsePB response)>
          ResponseCallback;

  // Initializes the subprocess.
  Status Init() WARN_UNUSED_RESULT;

  void Shutdown();

  // Insert the request into incoming queue and register callback by
  // request ID.
  Status SendRequest(SubprocessRequestPB* request,
                     ResponseCallback* callback,
                     int64_t* id);

  void UnregisterCallbackById(int64_t id);

 private:
  Status StartProcess() WARN_UNUSED_RESULT;
  Status StopProcess() WARN_UNUSED_RESULT;

  // Retrieve a message from the queue and write to the subprocess stdin.
  void SendMessages();

  // Read from the subprocess stdout and notify the caller the response
  // with the matching message ID.
  void ReceiveMessages();

  // Creates a configuration file for the subprocess.
  Status CreateConfigs(const std::string& tmp_dir) const WARN_UNUSED_RESULT;

  RequestQueue* request_queue() {
    return &request_queue_;
  }

  RequestQueue request_queue_;
  std::unique_ptr<Subprocess> process_;
  std::unique_ptr<SubprocessProtocol> proto_;
  std::string data_root_;

  // The task threads for sending and recieving message.
  std::vector<scoped_refptr<kudu::Thread>> threads_;

  AtomicInt<int64_t> id_;

  // Protects access to fields below.
  mutable Mutex lock_;

  // Set to true if the task is in the process of shutting down.
  //
  // Protected by lock_.
  bool closing_;

  // Map from a call ID to the callbacks to run upon receiving the call's response.
  //
  // Protected by 'lock_'.
  std::unordered_map<int64_t, ResponseCallback*> callbacks_;
};

} // namespace subprocess
} // namespace kudu
