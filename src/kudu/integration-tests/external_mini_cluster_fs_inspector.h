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

#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/env.h"

namespace kudu {
namespace itest {

class ExternalMiniClusterFsInspector : public MiniClusterFsInspector {
 public:
  explicit ExternalMiniClusterFsInspector(cluster::ExternalMiniCluster* cluster)
      : env_(Env::Default()),
        cluster_(cluster) {}

  ~ExternalMiniClusterFsInspector() override {}

  std::string WalRootForTS(int idx) const override {
    return cluster_->tablet_server(idx)->wal_dir();
  }

  std::string UUIDforTS(int idx) const override {
    return cluster_->tablet_server(idx)->uuid();
  }

  Env* env() const override { return env_; }
  int num_tablet_servers() const override { return cluster_->num_tablet_servers(); }

 private:
  Env* const env_;
  cluster::ExternalMiniCluster* const cluster_;

  DISALLOW_COPY_AND_ASSIGN(ExternalMiniClusterFsInspector);
};
} // namespace itest
} // namespace kudu
