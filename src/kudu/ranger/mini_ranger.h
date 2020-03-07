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

#include "kudu/util/status.h"
#include "kudu/ranger/mini_postgres.h"
#include "kudu/util/path_util.h"
#include "kudu/util/subprocess.h"

namespace kudu {

class MiniPostgres;

namespace ranger {

class MiniRanger {
 public:
  Status Start() WARN_UNUSED_RESULT;

  Status Stop() WARN_UNUSED_RESULT;

 private:
  Status StartRanger() WARN_UNUSED_RESULT;

  Status StopRanger() WARN_UNUSED_RESULT;

  Status CreateRangerConfigs(const std::string& config_string,
                             const std::string& file_path);

  std::string ranger_admin_home() const {
    DCHECK(!data_root_.empty());
    return JoinPathSegments(data_root_, "ranger-admin");
  }

  int GetRandomPort();

  MiniPostgres mini_pg_;
  std::unique_ptr<Subprocess> ranger_process_;

  // Directory in which to put all our stuff.
  std::string data_root_;

  // Locations in which to find Hadoop, Ranger, and Java.
  // These may be in the thirdparty build, or may be shared across tests. As
  // such, their contents should be treated as read-only.
  std::string hadoop_home_;
  std::string ranger_home_;
  std::string java_home_;

  uint16_t ranger_port_;
};

} // namespace ranger
} // namespace kudu
