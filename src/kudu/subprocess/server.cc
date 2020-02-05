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

#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/call.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DEFINE_int32(subprocess_request_queue_size_bytes, 4 * 1024 * 1024,
             "Maximum size of the group commit queue in bytes");
TAG_FLAG(subprocess_request_queue_size_bytes, advanced);

DEFINE_int32(subprocess_thread_idle_threshold_ms, 1000,
             "Number of milliseconds after which the log append thread decides that a "
             "log is idle, and considers shutting down. Used by tests.");
TAG_FLAG(subprocess_thread_idle_threshold_ms, hidden);

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace subprocess {

SubprocessServer::SubprocessServer()
    : closing_(false),
      request_queue_(FLAGS_subprocess_request_queue_size_bytes),
      response_queue_(FLAGS_subprocess_request_queue_size_bytes) {
}

SubprocessServer::~SubprocessServer() {
  Shutdown();
}

Status SubprocessServer::Init() {
  Env* env = Env::Default();

  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  const string bin_dir = DirName(exe);

  string subprocess_home;
  string java_home;
  RETURN_NOT_OK(FindHomeDir("subprocess", bin_dir, &subprocess_home));
  RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home));

  vector<string> argv;
  process_.reset(new Subprocess({
      Substitute("$0/bin/java", java_home),
      "-jar", Substitute("$0/kudu-subprocess.jar", subprocess_home),
  }));

  process_->ShareParentStdin(false);
  process_->ShareParentStdout(false);
  VLOG(2) << "Starting the subprocess";
  RETURN_NOT_OK_PREPEND(process_->Start(), "Failed to start subprocess");

  // Start the message protocol.
  CHECK(!message_protocol_);
  message_protocol_.reset(new SubprocessProtocol(SubprocessProtocol::SerializationMode::PB,
                                                 SubprocessProtocol::CloseMode::CLOSE_ON_DESTROY,
                                                 process_->ReleaseChildStdoutFd(),
                                                 process_->ReleaseChildStdinFd()));
  int num_threads = 20;
  responder_threads_.resize(num_threads);
  for (int i = 0; i < num_threads; i++) {
    RETURN_NOT_OK(Thread::Create("subprocess", "responder", &SubprocessServer::Respond,
                                 this, &responder_threads_[i]));
  }
  RETURN_NOT_OK(Thread::Create("subprocess", "read", &SubprocessServer::ReceiveMessages,
                               this, &read_thread_));
  RETURN_NOT_OK(Thread::Create("subprocess", "write", &SubprocessServer::SendMessages,
                               this, &write_thread_));
  RETURN_NOT_OK(Thread::Create("subprocess", "deadline-checker", &SubprocessServer::CheckDeadlines,
                               this, &deadline_checker_));
  return Status::OK();
}

void SubprocessServer::Shutdown() {
  // Stop further work from happening by killing the subprocess and shutting
  // down the queues.
  closing_.store(true);
  WARN_NOT_OK(process_->KillAndWait(SIGTERM), "failed to stop subprocess");
  response_queue_.Shutdown();
  request_queue_.Shutdown();

  // Clean up our threads.
  write_thread_->Join();
  read_thread_->Join();
  deadline_checker_->Join();
  for (auto t : responder_threads_) {
    t->Join();
  }
}

// Read from the subprocess stdout and put it on the queue.
void SubprocessServer::ReceiveMessages() {
  DCHECK(message_protocol_) << "Subprocess protocol is not initialized";

  while (!closing_.load()) {
    // Receive a new request, blocking until one is received.
    SubprocessResponsePB response;
    Status s = message_protocol_->ReceiveMessage(&response);
    if (s.IsEndOfFile()) {
      // The underlying pipe was closed. We're likely shutting down.
      return;
    }
    WARN_NOT_OK(s, "failed to receive message from the subprocess");
    if (s.ok() &&
        !response_queue_.BlockingPut(std::move(response))) {
      // The queue is shut down and we should too.
      VLOG(2) << "put failed, inbound queue shut down";
      return;
    }
  }
}

void SubprocessServer::Respond() {
  while (!closing_.load()) {
    SubprocessResponsePB resp;
    if (!response_queue_.BlockingGet(&resp)) {
      VLOG(2) << "get failed, inbound queue shut down";
      return;
    }
    if (!resp.has_id()) {
      LOG(WARNING) << Substitute("Received invalid response: $0",
                                 pb_util::SecureDebugString(resp));
      continue;
    }
    shared_ptr<SubprocessCall> call;
    {
      std::lock_guard<simple_spinlock> l(call_lock_);
      call = EraseKeyReturnValuePtr(&call_by_id_, resp.id());
    }
    if (call) {
      call->SetResponse(std::move(resp));
      const auto& cb = *call->cb();
      cb(Status::OK());
    }
  }
}

void SubprocessServer::CheckDeadlines() {
  while (!closing_.load()) {
    MonoTime now = MonoTime::Now();
    shared_ptr<SubprocessCall> timed_out_call;
    {
      std::lock_guard<simple_spinlock> l(call_lock_);
      if (!call_by_id_.empty()) {
        const auto& id_and_call = call_by_id_.begin();
        const auto& oldest_call = id_and_call->second;
        if (now > oldest_call->start_time() + MonoDelta::FromSeconds(15)) {
          call_by_id_.erase(id_and_call);
          timed_out_call = oldest_call;
        }
      }
    }
    if (timed_out_call) {
      const auto& cb = *timed_out_call->cb();
      cb(Status::TimedOut("Timed out while in flight"));
    }
  }
}

void SubprocessServer::SendMessages() {
  while (!closing_.load()) {
    const SubprocessRequestPB* req;
    if (!request_queue_.BlockingGet(&req)) {
      VLOG(2) << "outbound queue shut down";
      return;
    }
    WARN_NOT_OK(message_protocol_->SendMessage(*req), "failed to send message");
  }
}

Status SubprocessServer::QueueCall(const shared_ptr<SubprocessCall>& call) {
  if (MonoTime::Now() > call->start_time() + MonoDelta::FromSeconds(15)) {
    return Status::TimedOut("timed out before queueing call");
  }

  // XXX(awong):
  // Start tracking this call. It's possible that the callback will be called
  // before even putting the request on the queue.
  //
  // That's bad -- we might reference deleted state.
  //
  // Alternatively, we could Put() first, but then we run into the possibility
  // of finishing really quickly and our callback not being registered yet. If
  // that happens, we'll look for our callback and assume the deadline-checker
  // killed it; but when we _do_ add the call to the queue, it'll time out even
  // though we may have had a valid response.
  {
    std::lock_guard<simple_spinlock> l(call_lock_);
    EmplaceOrDie(&call_by_id_, call->req()->id(), call);
  }
  do {
    QueueStatus queue_status = request_queue_.Put(call->req());
    switch (queue_status) {
      case QUEUE_SUCCESS: return Status::OK();
      case QUEUE_SHUTDOWN: return Status::ServiceUnavailable("outbound queue shutting down");
      case QUEUE_FULL: {
        // If we still have more time allotted for this call, wait for a bit
        // and try again; otherwise, time out.
        if (MonoTime::Now() > call->start_time() + MonoDelta::FromSeconds(15)) {
          return Status::TimedOut("outbound queue is full");
        }
        SleepFor(MonoDelta::FromMilliseconds(50));
      }
    }
  } while (true);

  return Status::OK();
}

} // namespace subprocess
} // namespace kudu
