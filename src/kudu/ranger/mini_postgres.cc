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

#include "kudu/ranger/mini_postgres.h"

#include <string>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/path_util.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unique_ptr;
using strings::Substitute;

static constexpr int kPgStartTimeoutMs = 60000;

namespace kudu {

namespace {

Status GetRandomPort(uint16_t* port) {
  Sockaddr address;
  address.ParseString("127.0.0.1", 0);
  Socket listener;
  RETURN_NOT_OK(listener.Init(0));
  RETURN_NOT_OK(listener.BindAndListen(address, 0));
  Sockaddr listen_address;
  RETURN_NOT_OK(listener.GetSocketAddress(&listen_address));
  listener.Close();
  *port = listen_address.port();
  return Status::OK();
}

} // anonymous namespace

Status MiniPostgres::Start() {
  CHECK(!pg_process_);

  VLOG(1) << "Starting Postgres";

  Env* env = Env::Default();
  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  bin_dir_ = DirName(exe);

  if (data_root_.empty()) {
    data_root_ = GetTestDataDirectory();
  }
  string pg_root = this->pg_root();
  if (!env->FileExists(pg_root)) {
    // This is our first time running. Set up our directories, config files,
    // and port.
    LOG(INFO) << "Running initdb...";
    unique_ptr<Subprocess> initdb(new Subprocess({
      JoinPathSegments(bin_dir_, "postgres/initdb"),
      "-D", pg_root,
    }));
    RETURN_NOT_OK_PREPEND(initdb->Start(), "failed to start initdb");
    RETURN_NOT_OK_PREPEND(initdb->Wait(), "failed to wait on initdb");

    RETURN_NOT_OK(GetRandomPort(&port_));
    RETURN_NOT_OK(CreateConfigs(pg_root));
  }

  pg_process_.reset(new Subprocess({
        JoinPathSegments(bin_dir_, "postgres/postgres"),
        "-D", pg_root}));
  RETURN_NOT_OK(pg_process_->Start());
  // Postgres doesn't support binding to port 0, so generate a port now.
  // TODO(awong): keep trying until we succeed.
  const string ip = "127.0.0.1";
  Status wait = WaitForTcpBind(pg_process_->pid(), &port_, ip,
                               MonoDelta::FromMilliseconds(kPgStartTimeoutMs));
  if (!wait.ok()) {
    WARN_NOT_OK(pg_process_->Kill(SIGINT), "failed to send SIGQUIT to Postgres");
  }
  LOG(INFO) << "Postgres bound to " << port_;
  return wait;
}

Status MiniPostgres::Stop() {
  RETURN_NOT_OK_PREPEND(pg_process_->KillAndWait(SIGTERM), "failed to stop Postgres");
  return Status::OK();
}

Status MiniPostgres::AddUser(const string& user, bool super) {
  Subprocess add({
    JoinPathSegments(bin_dir_, "postgres/createuser"),
    user,
    Substitute("--$0superuser", super ? "" : "no"),
    "-p", Substitute("$0", port_),
  });
  RETURN_NOT_OK(add.Start());
  return add.Wait();
}

Status MiniPostgres::CreateDb(const string& db, const string& owner) {
  Subprocess createdb({
    JoinPathSegments(bin_dir_, "postgres/createdb"),
    "-O", owner, db,
    "-p", Substitute("$0", port_),
  });
  RETURN_NOT_OK(createdb.Start());
  return createdb.Wait();

}

Status MiniPostgres::CreateConfigs(const string& pg_root) {
  Env* env = Env::Default();
  static const string kFileTemplate = R"(
max_connections = 100                   # (change requires restart)
shared_buffers = 128MB                  # min 128kB
dynamic_shared_memory_type = posix      # the default is the first option
max_wal_size = 1GB
min_wal_size = 80MB
datestyle = 'iso, mdy'
lc_messages = 'en_US.UTF-8'                     # locale for system error message
lc_monetary = 'en_US.UTF-8'                     # locale for monetary formatting
lc_numeric = 'en_US.UTF-8'                      # locale for number formatting
lc_time = 'en_US.UTF-8'                         # locale for time formatting
default_text_search_config = 'pg_catalog.english'
port=$0
  )";
  std::unique_ptr<WritableFile> file;
  RETURN_NOT_OK(env->NewWritableFile(JoinPathSegments(pg_root, "postgresql.conf"), &file));
  RETURN_NOT_OK(file->Append(Substitute(kFileTemplate, port_)));
  RETURN_NOT_OK(file->Flush(WritableFile::FlushMode::FLUSH_SYNC));
  return file->Close();
}

} // namespace kudu
