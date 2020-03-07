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
#pragma once

#include <memory>
#include <string>

#include "kudu/util/status.h"
#include "kudu/util/path_util.h"

namespace kudu {

class Subprocess;

class MiniPostgres {
 public:
  Status Start();
  Status Stop();

  Status AddUser(const std::string& user, bool super);
  Status CreateDb(const std::string& db, const std::string& owner);

  uint16_t bound_port() const {
    CHECK_NE(0, port_);
    return port_;
  }
  std::string pg_root() const {
    DCHECK(!data_root_.empty());
    return JoinPathSegments(data_root_, "postgres");
  }

  std::string pg_bin_dir() const {
    DCHECK(!bin_dir_.empty());
    return JoinPathSegments(bin_dir_, "postgres");
  }
 private:
  // 'pg_root' is the subdirectory in which Postgres will live.
  Status CreateConfigs(const std::string& pg_root);

  // Directory in which to put all our stuff.
  std::string data_root_;

  // Directory that has the Postgres binary.
  // This may be in the thirdparty build, or may be shared across tests. As
  // suhc, its contents should be treated as read-only.
  std::string bin_dir_;

  std::unique_ptr<Subprocess> pg_process_;
  uint16_t port_ = 0;
};

} // namespace kudu
