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

#include <vector>

#include <glog/logging.h>

#include "kudu/fs/fs.pb.h"

using std::vector;
using std::move;

namespace kudu {
namespace fs {

class DataDirGroup {
public:
  static DataDirGroup CreateEmptyGroup() {
    return DataDirGroup(vector<uint16_t>());
  }

  static DataDirGroup FromPB(const DataDirGroupPB& pb) {
    vector<uint16_t> uuids;
    for (uint16_t uuid : pb.uuids()) {
      uuids.push_back(uuid);
    }
    return DataDirGroup(uuids);
  }

  void CopyToPB(DataDirGroupPB* pb) {
    CHECK(pb != nullptr);
    DataDirGroupPB group;
    for (uint16_t uuid : uuids_) {
      *group.mutable_uuids()->Add() = uuid;
    }
    *pb = group;
  }

  vector<uint16_t>* uuids() { return &uuids_; }

private:
  DataDirGroup(vector<uint16_t> uuids) : uuids_(move(uuids)) {}

  // UUIDs corresponding to the data directories within the group.
  vector<uint16_t> uuids_;
};

}  // namespace fs
}  // namespace kudu
