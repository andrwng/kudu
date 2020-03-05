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

#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

namespace kudu {

namespace ranger {

class MiniRanger {
 public:
  Status Start() WARN_UNUSED_RESULT;

  Status Stop() WARN_UNUSED_RESULT;

 private:
  Status StartPostgres() WARN_UNUSED_RESULT;

  Status StopPostgres() WARN_UNUSED_RESULT;

  Status StartRanger() WARN_UNUSED_RESULT;

  Status StopRanger() WARN_UNUSED_RESULT;

  Status CreatePostgresConfigs(std::string data_root);

  Status CreateRangerConfigs(std::string data_root);

  int GetRandomPort();

  std::unique_ptr<Subprocess> postgres_process_;

  std::unique_ptr<Subprocess> ranger_process_;

  std::string data_root_;

  std::string hadoop_home_;
  std::string ranger_home_;
  std::string java_home_;

  uint16_t postgres_port_;

  uint16_t ranger_port_;
};

} // namespace ranger
} // namespace kudu
