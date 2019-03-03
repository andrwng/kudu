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

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;

namespace tablet {

class SuperBlockUpdatePB;
class TabletSuperBlockPB;
class TabletMetadata;

class TabletMetadataManager : public RefCountedThreadSafe<TabletMetadataManager> {
 public:
  explicit TabletMetadataManager(FsManager* fs_manager);
  Status Load(const std::string& tablet_id, scoped_refptr<TabletMetadata>* tmeta);
  Status Delete(const std::string& tablet_id);
  Status Flush(const TabletSuperBlockPB& pb) const;
  Status FlushUpdate(const std::string& tablet_id, const SuperBlockUpdatePB& pb) const;
  bool Exists(const std::string& tablet_id) const;
  Status GetSize(const std::string& tablet_id, uint64_t* size) const;
  bool SupportsIncrementalUpdates() const {
    return false;
  }
 private:
  friend class RefCountedThreadSafe<TabletMetadataManager>;

  FsManager* const fs_manager_;
  DISALLOW_COPY_AND_ASSIGN(TabletMetadataManager);
};

}  // namespace tablet
}  // namespace kudu
