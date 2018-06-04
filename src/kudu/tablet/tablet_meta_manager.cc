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
#include "kudu/tablet/tablet_meta_manager.h"

#include <string>

#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/pb_util.h"

namespace kudu {
namespace tablet {

using std::string;
using strings::Substitute;

TabletMetadataManager::TabletMetadataManager(FsManager* fs_manager)
  : fs_manager_(fs_manager) {}

Status TabletMetadataManager::Load(const string& tablet_id,
                                   scoped_refptr<TabletMetadata>* tmeta) {
  TabletSuperBlockPB superblock;
  const string& path = fs_manager_->GetTabletMetadataPath(tablet_id);
  RETURN_NOT_OK_PREPEND(
      pb_util::ReadPBContainerFromPath(fs_manager_->env(), path, &superblock),
      Substitute("Could not load tablet metadata from $0", path));
  scoped_refptr<TabletMetadata> metadata (new TabletMetadata(fs_manager_, this, tablet_id));
  metadata->LoadFromSuperBlock(superblock);
  metadata->UpdateOnDiskSize();
  tmeta->swap(metadata);
  return Status::OK();
}

// TODO(awong): should this take TabletMetadata instead of SuperBlockPB?
Status TabletMetadataManager::Flush(const TabletSuperBlockPB& pb) const {
  const string& path = fs_manager_->GetTabletMetadataPath(pb.tablet_id());
  RETURN_NOT_OK_PREPEND(pb_util::WritePBContainerToPath(
      fs_manager_->env(), path, pb, pb_util::OVERWRITE, pb_util::SYNC),
      Substitute("Failed to write tablet metadata $0", pb.tablet_id()));
  return Status::OK();
}

Status TabletMetadataManager::Delete(const string& tablet_id) {
  const string& path = fs_manager_->GetTabletMetadataPath(tablet_id);
  RETURN_NOT_OK_PREPEND(fs_manager_->env()->DeleteFile(path),
      "Unable to delete superblock for tablet " + tablet_id);
  return Status::OK();
}

bool TabletMetadataManager::Exists(const string& tablet_id) const {
  return fs_manager_->env()->FileExists(tablet_id);
}

Status TabletMetadataManager::GetSize(const string& tablet_id, uint64_t* size) const {
  const string& path = fs_manager_->GetTabletMetadataPath(tablet_id);
  return fs_manager_->env()->GetFileSize(path, size);
}

}  // namespace tablet
}  // namespace kudu
