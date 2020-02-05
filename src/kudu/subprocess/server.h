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

#include <atomic>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <map>
#include <utility>
#include <vector>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/util/atomic.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/locks.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/thread.h"

namespace kudu {
namespace subprocess {

class SubprocessCall;

// Used by BlockingQueue to determing the size of requests.
struct RequestLogicalSize {
  static size_t logical_size(const SubprocessRequestPB* request) {
    return request->ByteSizeLong();
  }
};
struct ResponseLogicalSize {
  static size_t logical_size(const SubprocessResponsePB* response) {
    return response->ByteSizeLong();
  }
};

typedef BlockingQueue<const SubprocessRequestPB*, RequestLogicalSize> RequestQueue;
typedef BlockingQueue<SubprocessResponsePB> ResponseQueue;

class SubprocessServer {
 public:
  SubprocessServer();
  ~SubprocessServer();

  // Initializes the subprocess.
  Status Init() WARN_UNUSED_RESULT;

  // Insert the request into outbound queue and register the callback:
  // - put it on the timeout queue (ordered by timeout, fifo)
  // - put it in the callback map (keyed by call ID)
  // - put it on the request queue
  // It is expected that the inputs stay valid until the callback is called.
  Status QueueCall(const std::shared_ptr<SubprocessCall>& call);


 private:
  void Shutdown();

  // Retrieve a message from the queue and write to the subprocess stdin.
  void CheckDeadlines();

  // Pulls responses from the responses queue and calls the associated
  // callbacks.
  void Respond();

  // Pulls messages fromt he request queue and sends them to the pipe.
  void SendMessages();

  // Read from the subprocess stdout and put the responses onto the response
  // queue.
  void ReceiveMessages();

  // The subprocess.
  std::unique_ptr<Subprocess> process_;

  // Protocol with which to send and receive bytes to and from 'process_'.
  std::unique_ptr<SubprocessProtocol> message_protocol_;

  // Pulls requests off the request queue and serializes them to via the
  // message protocol.
  scoped_refptr<Thread> write_thread_;

  // Reads from the message protocol, constructs the response, and puts it on
  // the response queue.
  scoped_refptr<Thread> read_thread_;

  // Looks at the front of the queue for calls that are past their deadlines
  // and triggers their callbacks.
  scoped_refptr<Thread> deadline_checker_;

  // Pull work off the response queue and trigger the associated callbacks if
  // appropriate.
  std::vector<scoped_refptr<Thread>> responder_threads_;

  // Set to true if the task is in the process of shutting down.
  std::atomic<bool> closing_;

  // Outbound queue of requests to send to the subprocess.
  RequestQueue request_queue_;

  // Inbound queue of responses sent by the subprocess.
  ResponseQueue response_queue_;

  // Calls that are currently in-flight, ordered by ID. This allows for lookup
  // by ID, and gives us a rough way to get the calls with earliest start
  // times which is useful for deadline checking.
  //
  // Only a single thread may remove a call; that thread must run the call's
  // callback.
  mutable simple_spinlock call_lock_;
  std::map<int64_t, std::shared_ptr<SubprocessCall>> call_by_id_;

};

} // namespace subprocess
} // namespace kudu
