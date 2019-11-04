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

#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/path_util.h"
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

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace subprocess {

const Status SubprocessServer::kShutdownStatus(
    Status::ServiceUnavailable("Subprocess is shutting down", "", ESHUTDOWN));

SubprocessServer::SubprocessServer()
    : request_queue_(FLAGS_subprocess_request_queue_size_bytes),
      id_(0),
      closing_(false) {
}

SubprocessServer::~SubprocessServer() {
  if (!threads_.empty()) {
    Shutdown();
  }
}

Status SubprocessServer::Init() {
  RETURN_NOT_OK(StartProcess());

  // Start the protocol interface.
  CHECK(!proto_);
  proto_.reset(new SubprocessProtocol(SubprocessProtocol::SerializationMode::PB,
                                      SubprocessProtocol::CloseMode::CLOSE_ON_DESTROY,
                                      process_->ReleaseChildStdoutFd(),
                                      process_->ReleaseChildStdinFd()));
  scoped_refptr<Thread> new_thread;
  Status s = kudu::Thread::Create("subprocess", "subprocess-send-msg",
                                  &SubprocessServer::SendMessages, this, &new_thread);
  if (s.ok()) {
    threads_.push_back(new_thread);
    s = kudu::Thread::Create("subprocess", "subprocess-receive-msg",
                             &SubprocessServer::ReceiveMessages, this, &new_thread);
  }
  if (!s.ok()) {
    Shutdown();
    return s;
  }
  threads_.push_back(new_thread);
  return Status::OK();
}

void SubprocessServer::Shutdown() {
  VLOG(2) << "Shutting down the subprocess";

  // Shutdown the request queue.
  request_queue()->Shutdown();

  // Shutdown the subprocess.
  ignore_result(StopProcess());

  {
    std::lock_guard<Mutex> l(lock_);
    DCHECK(!closing_);
    closing_ = true;

    // Signal all the waiting requests.
    for (const auto& entry : callbacks_) {
      SubprocessResponsePB res;
        (*entry.second)(kShutdownStatus, res);
    }
  }

  for (const scoped_refptr<kudu::Thread>& thread : threads_) {
    CHECK_OK(ThreadJoiner(thread.get()).Join());
  }

  if (proto_) {
    proto_.reset();
  }
  threads_.clear();
}

Status SubprocessServer::StartProcess() {
  CHECK(!process_);

  Env* env = Env::Default();

  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  const string bin_dir = DirName(exe);

  string subprocess_home;
  string java_home;
  RETURN_NOT_OK(FindHomeDir("subprocess", bin_dir, &subprocess_home));
  RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home));

  if (data_root_.empty()) {
    data_root_ = GetTestDataDirectory();
  }

  RETURN_NOT_OK(CreateConfigs(data_root_));

  vector<string> argv;
  process_.reset(new Subprocess({
      Substitute("$0/bin/java", java_home),
      "-jar",
      Substitute("$0/kudu-subprocess.jar", subprocess_home),
      "--conffile", JoinPathSegments(data_root_, "subprocess-site.xml"),
  }));

  process_->ShareParentStdin(false);
  process_->ShareParentStdout(false);
  VLOG(2) << "Starting the subprocess";
  RETURN_NOT_OK_PREPEND(process_->Start(), "Failed to start subprocess");
  return Status::OK();
}

Status SubprocessServer::StopProcess() {
  if (process_) {
    VLOG(2) << "Shutting down the subprocess";
    unique_ptr<Subprocess> proc = std::move(process_);
    RETURN_NOT_OK_PREPEND(proc->KillAndWait(SIGTERM), "failed to stop the subprocess");
  }
  return Status::OK();
}

// Retrieve the message from the queue and write to the subprocess stdin
void SubprocessServer::SendMessages() {
  CHECK(proto_) << "Subprocess protocol is not initialized";

  while (true) {
    SubprocessRequestPB* request;
    // Block on getting a request.
    bool isAvailable = request_queue()->BlockingGet(&request);
    if (isAvailable) {
      Status s = proto_->SendMessage(*request);
      if (!s.IsEndOfFile()) {
        WARN_NOT_OK(s, "failed to send message to the subprocess");
      }
    } else {
      // The request queue is shutdown.
      break;
    }
  }
}

// Read from the subprocess stdout and put it on the queue.
void SubprocessServer::ReceiveMessages() {
  CHECK(proto_) << "Subprocess protocol is not initialized";

  while (true) {
    // Receive a new request, blocking until one is received.
    SubprocessResponsePB response;
    Status s = proto_->ReceiveMessage(&response);
    if (!s.IsEndOfFile()) {
      WARN_NOT_OK(s, "failed to receive message from the subprocess");
    }

    if (s.ok()) {
      // Get the id from the response and notify the callback based
      // on the id. If the response has no ID, skip it.
      if (response.has_id()) {
        // Process error response.
        if (response.has_error()) {
          s = StatusFromPB(response.error());
        }
        int64_t id = response.id();
        {
          std::lock_guard<Mutex> l(lock_);
          if (!closing_) {
            auto cb = EraseKeyReturnValuePtr(&callbacks_, id);
            if (cb) {
              (*cb)(s, std::move(response));
            }
          }
        }
      }
    }

    // Check if shutdown was signaled while in the loop.
    {
      std::lock_guard<Mutex> l(lock_);
      if (closing_) {
        break;
      }
    }
  }
}

Status SubprocessServer::SendRequest(SubprocessRequestPB* request,
                                     ResponseCallback* callback,
                                     int64_t* id) {
  DCHECK(request);
  DCHECK(callback);
  DCHECK(id);

  *id = id_.Increment();
  request->set_id(*id);

  QueueStatus queue_status = request_queue()->Put(request);
  if (queue_status == QUEUE_SHUTDOWN) {
    return kShutdownStatus;
  }

  if (queue_status == QUEUE_FULL) {
    return Status::ServiceUnavailable("Subprocess queue is full");
  }

  // Register callback by request ID.
  {
    std::lock_guard<Mutex> l(lock_);
    if (!closing_) {
      EmplaceOrDie(&callbacks_, *id, callback);
    } else {
      return kShutdownStatus;
    }
  }

  return Status::OK();
}

void SubprocessServer::UnregisterCallbackById(int64_t id) {
  std::lock_guard<Mutex> l(lock_);
  EraseKeyReturnValuePtr(&callbacks_, id);
}

Status SubprocessServer::CreateConfigs(const string& tmp_dir) const {

  static const string kFileTemplate = R"(
<configuration>

  <property>
    <name>kudu.subprocess.message.type</name>
    <value>$0</value>
  </property>

</configuration>
  )";

  string file_contents = Substitute(
      kFileTemplate,
      "echo");
  RETURN_NOT_OK(WriteStringToFile(Env::Default(),
                file_contents,
                JoinPathSegments(tmp_dir, "subprocess-site.xml")));
  return Status::OK();
}

} // namespace subprocess
} // namespace kudu
