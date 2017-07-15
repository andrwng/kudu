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

#include "kudu/tablet/tablet_metadata.h"

#include <algorithm>
#include <boost/optional.hpp>
#include <gflags/gflags.h>
#include <mutex>
#include <string>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

DECLARE_bool(fs_stripe_metadata);

DEFINE_bool(enable_tablet_orphaned_block_deletion, true,
            "Whether to enable deletion of orphaned blocks from disk. "
            "Note: This is only exposed for debugging purposes!");
TAG_FLAG(enable_tablet_orphaned_block_deletion, advanced);
TAG_FLAG(enable_tablet_orphaned_block_deletion, hidden);
TAG_FLAG(enable_tablet_orphaned_block_deletion, runtime);

using std::memory_order_relaxed;
using std::shared_ptr;

using base::subtle::Barrier_AtomicIncrement;
using strings::Substitute;

namespace kudu {

using consensus::MinimumOpId;
using consensus::OpId;
using fs::DataDir;

namespace tablet {

const int64 kNoDurableMemStore = -1;

// ============================================================================
//  Tablet Metadata
// ============================================================================

Status TabletMetadata::CreateNew(FsManager* fs_manager,
                                 const string& tablet_id,
                                 const string& table_name,
                                 const string& table_id,
                                 const Schema& schema,
                                 const PartitionSchema& partition_schema,
                                 const Partition& partition,
                                 const TabletDataState& initial_tablet_data_state,
                                 scoped_refptr<TabletMetadata>* metadata) {

  // Verify that no existing tablet exists with the same ID.
  if (fs_manager->env()->FileExists(fs_manager->GetTabletMetadataPath(tablet_id))) {
    return Status::AlreadyPresent("Tablet already exists", tablet_id);
  }

  RETURN_NOT_OK_PREPEND(fs_manager->dd_manager()->CreateDataDirGroup(tablet_id),
      "Failed to create TabletMetadata");
  if (FLAGS_fs_stripe_metadata) {
    DataDir* metadata_dir;
    RETURN_NOT_OK_PREPEND(fs_manager->dd_manager()->GetNextDataDir({ tablet_id }, &metadata_dir),
        Substitute("Failed to select a metadata directory for tablet $0", tablet_id));
    fs_manager->dd_manager()->SetTabletMetadataDir(tablet_id, metadata_dir);
  }

  auto dir_group_cleanup = MakeScopedCleanup([&]() {
    fs_manager->dd_manager()->DeleteDataDirGroup(tablet_id);
  });
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager,
                                                       tablet_id,
                                                       table_name,
                                                       table_id,
                                                       schema,
                                                       partition_schema,
                                                       partition,
                                                       initial_tablet_data_state));
  RETURN_NOT_OK(ret->Flush());
  dir_group_cleanup.cancel();

  metadata->swap(ret);
  return Status::OK();
}

Status TabletMetadata::Load(FsManager* fs_manager,
                            const string& tablet_id,
                            const string& metadata_dir,
                            scoped_refptr<TabletMetadata>* metadata) {
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager, tablet_id));
  RETURN_NOT_OK(ret->LoadFromDisk(metadata_dir));
  metadata->swap(ret);
  return Status::OK();
}

Status TabletMetadata::LoadOrCreate(FsManager* fs_manager,
                                    const string& tablet_id,
                                    const string& table_name,
                                    const string& table_id,
                                    const Schema& schema,
                                    const PartitionSchema& partition_schema,
                                    const Partition& partition,
                                    const TabletDataState& initial_tablet_data_state,
                                    scoped_refptr<TabletMetadata>* metadata) {
  Status s = Load(fs_manager, tablet_id, "", metadata);
  if (s.ok()) {
    if (!(*metadata)->schema().Equals(schema)) {
      return Status::Corruption(Substitute("Schema on disk ($0) does not "
        "match expected schema ($1)", (*metadata)->schema().ToString(),
        schema.ToString()));
    }
    return Status::OK();
  } else if (s.IsNotFound()) {
    return CreateNew(fs_manager, tablet_id, table_name, table_id, schema,
                     partition_schema, partition, initial_tablet_data_state,
                     metadata);
  } else {
    return s;
  }
}

vector<BlockIdPB> TabletMetadata::CollectBlockIdPBs(const TabletSuperBlockPB& superblock) {
  vector<BlockIdPB> block_ids;
  for (const RowSetDataPB& rowset : superblock.rowsets()) {
    for (const ColumnDataPB& column : rowset.columns()) {
      block_ids.push_back(column.block());
    }
    for (const DeltaDataPB& redo : rowset.redo_deltas()) {
      block_ids.push_back(redo.block());
    }
    for (const DeltaDataPB& undo : rowset.undo_deltas()) {
      block_ids.push_back(undo.block());
    }
    if (rowset.has_bloom_block()) {
      block_ids.push_back(rowset.bloom_block());
    }
    if (rowset.has_adhoc_index_block()) {
      block_ids.push_back(rowset.adhoc_index_block());
    }
  }
  return block_ids;
}

vector<BlockId> TabletMetadata::CollectBlockIds() {
  vector<BlockId> block_ids;
  for (const auto& r : rowsets_) {
    vector<BlockId> rowset_block_ids = r->GetAllBlocks();
    block_ids.insert(block_ids.begin(),
                     rowset_block_ids.begin(),
                     rowset_block_ids.end());
  }
  return block_ids;
}

Status TabletMetadata::DeleteTabletData(TabletDataState delete_type,
                                        const boost::optional<OpId>& last_logged_opid) {
  CHECK(delete_type == TABLET_DATA_DELETED ||
        delete_type == TABLET_DATA_TOMBSTONED ||
        delete_type == TABLET_DATA_COPYING)
      << "DeleteTabletData() called with unsupported delete_type on tablet "
      << tablet_id_ << ": " << TabletDataState_Name(delete_type)
      << " (" << delete_type << ")";

  // First add all of our blocks to the orphan list
  // and clear our rowsets. This serves to erase all the data.
  //
  // We also set the state in our persisted metadata to indicate that
  // we have been deleted.
  {
    std::lock_guard<LockType> l(data_lock_);
    for (const shared_ptr<RowSetMetadata>& rsmd : rowsets_) {
      AddOrphanedBlocksUnlocked(rsmd->GetAllBlocks());
    }
    rowsets_.clear();
    tablet_data_state_ = delete_type;
    if (last_logged_opid) {
      tombstone_last_logged_opid_ = *last_logged_opid;
    }
  }

  // Keep a copy of the old data dir group in case of flush failure.
  DataDirGroupPB pb;
  bool old_group_exists = fs_manager_->dd_manager()->GetDataDirGroupPB(tablet_id_, &pb);

  // Remove the tablet's data dir group tracked by the DataDirManager.
  fs_manager_->dd_manager()->DeleteDataDirGroup(tablet_id_);
  auto revert_group_cleanup = MakeScopedCleanup([&]() {
    if (old_group_exists) {
      fs_manager_->dd_manager()->LoadDataDirGroupFromPB(tablet_id_, pb);
    }
  });

  // Flushing will sync the new tablet_data_state_ to disk and will now also
  // delete all the data.
  RETURN_NOT_OK(Flush());
  revert_group_cleanup.cancel();

  // Re-sync to disk one more time.
  // This call will typically re-sync with an empty orphaned blocks list
  // (unless deleting any orphans failed during the last Flush()), so that we
  // don't try to re-delete the deleted orphaned blocks on every startup.
  return Flush();
}

bool TabletMetadata::IsTombstonedWithNoBlocks() const {
  std::lock_guard<LockType> l(data_lock_);
  return tablet_data_state_ == TABLET_DATA_TOMBSTONED &&
      rowsets_.empty() &&
      orphaned_blocks_.empty();
}

Status TabletMetadata::DeleteSuperBlock() {
  std::lock_guard<LockType> l(data_lock_);
  if (!orphaned_blocks_.empty()) {
    return Status::InvalidArgument("The metadata for tablet " + tablet_id_ +
                                   " still references orphaned blocks. "
                                   "Call DeleteTabletData() first");
  }
  if (tablet_data_state_ != TABLET_DATA_DELETED) {
    return Status::IllegalState(
        Substitute("Tablet $0 is not in TABLET_DATA_DELETED state. "
                   "Call DeleteTabletData(TABLET_DATA_DELETED) first. "
                   "Tablet data state: $1 ($2)",
                   tablet_id_,
                   TabletDataState_Name(tablet_data_state_),
                   tablet_data_state_));
  }

  // TODO(awong) store this in the metadata itself
  string path = fs_manager_->GetTabletMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(fs_manager_->env()->DeleteFile(path),
                        "Unable to delete superblock for tablet " + tablet_id_);
  return Status::OK();
}

TabletMetadata::TabletMetadata(FsManager* fs_manager, string tablet_id,
                               string table_name, string table_id,
                               const Schema& schema, PartitionSchema partition_schema,
                               Partition partition,
                               const TabletDataState& tablet_data_state)
    : state_(kNotWrittenYet),
      tablet_id_(std::move(tablet_id)),
      table_id_(std::move(table_id)),
      partition_(std::move(partition)),
      fs_manager_(fs_manager),
      next_rowset_idx_(0),
      last_durable_mrs_id_(kNoDurableMemStore),
      schema_(new Schema(schema)),
      schema_version_(0),
      table_name_(std::move(table_name)),
      partition_schema_(std::move(partition_schema)),
      tablet_data_state_(tablet_data_state),
      tombstone_last_logged_opid_(MinimumOpId()),
      num_flush_pins_(0),
      needs_flush_(false),
      pre_flush_callback_(Bind(DoNothingStatusClosure)) {
  CHECK(schema_->has_column_ids());
  CHECK_GT(schema_->num_key_columns(), 0);
}

TabletMetadata::~TabletMetadata() {
  STLDeleteElements(&old_schemas_);
  delete schema_;
}

TabletMetadata::TabletMetadata(FsManager* fs_manager, string tablet_id)
    : state_(kNotLoadedYet),
      tablet_id_(std::move(tablet_id)),
      fs_manager_(fs_manager),
      next_rowset_idx_(0),
      schema_(nullptr),
      tombstone_last_logged_opid_(MinimumOpId()),
      num_flush_pins_(0),
      needs_flush_(false),
      pre_flush_callback_(Bind(DoNothingStatusClosure)) {}

Status TabletMetadata::LoadFromDisk(const string& metadata_dir) {
  TRACE_EVENT1("tablet", "TabletMetadata::LoadFromDisk",
               "tablet_id", tablet_id_);

  CHECK_EQ(state_, kNotLoadedYet);

  TabletSuperBlockPB superblock;
  RETURN_NOT_OK(ReadSuperBlockFromDisk(metadata_dir, &superblock));
  RETURN_NOT_OK_PREPEND(LoadFromSuperBlock(superblock),
                        "Failed to load data from superblock protobuf");
  state_ = kInitialized;
  return Status::OK();
}

Status TabletMetadata::LoadFromSuperBlock(const TabletSuperBlockPB& superblock) {
  vector<BlockId> orphaned_blocks;

  VLOG(2) << "Loading TabletMetadata from SuperBlockPB:" << std::endl
          << SecureDebugString(superblock);

  {
    std::lock_guard<LockType> l(data_lock_);

    // Verify that the tablet id matches with the one in the protobuf
    if (superblock.tablet_id() != tablet_id_) {
      return Status::Corruption("Expected id=" + tablet_id_ +
                                " found " + superblock.tablet_id(),
                                SecureDebugString(superblock));
    }

    last_durable_mrs_id_ = superblock.last_durable_mrs_id();

    table_name_ = superblock.table_name();

    uint32_t schema_version = superblock.schema_version();
    gscoped_ptr<Schema> schema(new Schema());
    RETURN_NOT_OK_PREPEND(SchemaFromPB(superblock.schema(), schema.get()),
                          "Failed to parse Schema from superblock " +
                          SecureShortDebugString(superblock));
    SetSchemaUnlocked(std::move(schema), schema_version);

    if (!superblock.has_partition()) {
      // KUDU-818: Possible backward compatibility issue with tables created
      // with version <= 0.5, throw warning.
      LOG_WITH_PREFIX(WARNING) << "Upgrading from Kudu 0.5.0 directly to this"
          << " version is not supported. Please upgrade to 0.6.0 before"
          << " moving to a higher version.";
      return Status::NotFound("Missing partition in superblock "+
                              SecureDebugString(superblock));
    }

    // Some metadata fields are assumed to be immutable and thus are
    // only read from the protobuf when the tablet metadata is loaded
    // for the very first time. See KUDU-1500 for more details.
    if (state_ == kNotLoadedYet) {
      table_id_ = superblock.table_id();
      RETURN_NOT_OK(PartitionSchema::FromPB(superblock.partition_schema(),
                                            *schema_, &partition_schema_));
      Partition::FromPB(superblock.partition(), &partition_);
    } else {
      CHECK_EQ(table_id_, superblock.table_id());
      PartitionSchema partition_schema;
      RETURN_NOT_OK(PartitionSchema::FromPB(superblock.partition_schema(),
                                            *schema_, &partition_schema));
      CHECK(partition_schema_.Equals(partition_schema));

      Partition partition;
      Partition::FromPB(superblock.partition(), &partition);
      CHECK(partition_.Equals(partition));
    }

    tablet_data_state_ = superblock.tablet_data_state();

    rowsets_.clear();
    for (const RowSetDataPB& rowset_pb : superblock.rowsets()) {
      gscoped_ptr<RowSetMetadata> rowset_meta;
      RETURN_NOT_OK(RowSetMetadata::Load(this, rowset_pb, &rowset_meta));
      next_rowset_idx_ = std::max(next_rowset_idx_, rowset_meta->id() + 1);
      rowsets_.push_back(shared_ptr<RowSetMetadata>(rowset_meta.release()));
    }

    for (const BlockIdPB& block_pb : superblock.orphaned_blocks()) {
      orphaned_blocks.push_back(BlockId::FromPB(block_pb));
    }
    AddOrphanedBlocksUnlocked(orphaned_blocks);

    if (superblock.has_data_dir_group()) {
      RETURN_NOT_OK_PREPEND(fs_manager_->dd_manager()->LoadDataDirGroupFromPB(
          tablet_id_, superblock.data_dir_group()), "Failed to load DataDirGroup from superblock");
      // If any of the data dirs in the tablet's directory group have failed,
      // do not open the tablet, as some of the tablet's data may be unusable.
      if (PREDICT_FALSE(!fs_manager_->dd_manager()->GetFailedDataDirs().empty())) {
        for (const string& uuid : superblock.data_dir_group().uuids()) {
          uint16_t uuid_idx;
          CHECK(fs_manager_->dd_manager()->FindUuidIndexByUuid(uuid, &uuid_idx));
          if (fs_manager_->dd_manager()->IsDataDirFailed(uuid_idx)) {
            return Status::IOError("Tablet has data in a failed directory", "", EIO);
          }
        }
      }
    } else if (tablet_data_state_ == TABLET_DATA_READY) {
      // If any data dir has failed, do not open the tablet.
      if (PREDICT_FALSE(!fs_manager_->dd_manager()->GetFailedDataDirs().empty())) {
        return Status::IOError("Tablet has data in a failed directory", "", EIO);
      }
      // If the superblock does not contain a DataDirGroup, this server has
      // likely been upgraded from before 1.5.0. Create a new DataDirGroup for
      // the tablet. If the data is not TABLET_DATA_READY, group creation is
      // pointless, as the tablet metadata will be deleted anyway.
      //
      // Since we don't know what directories the existing blocks are in, we
      // should assume the data is spread across all disks.
      RETURN_NOT_OK(fs_manager_->dd_manager()->CreateDataDirGroup(tablet_id_,
          fs::DataDirManager::DirDistributionMode::ACROSS_ALL_DIRS));
    }

    if (superblock.has_tombstone_last_logged_opid()) {
      tombstone_last_logged_opid_ = superblock.tombstone_last_logged_opid();
    } else {
      tombstone_last_logged_opid_ = MinimumOpId();
    }
  }

  // Now is a good time to clean up any orphaned blocks that may have been
  // left behind from a crash just after replacing the superblock.
  if (!fs_manager()->read_only()) {
    DeleteOrphanedBlocks(orphaned_blocks);
  }

  on_disk_size_.store(superblock.ByteSizeLong(), memory_order_relaxed);
  return Status::OK();
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add,
                                      int64_t last_durable_mrs_id) {
  {
    std::lock_guard<LockType> l(data_lock_);
    RETURN_NOT_OK(UpdateUnlocked(to_remove, to_add, last_durable_mrs_id));
  }
  return Flush();
}

void TabletMetadata::AddOrphanedBlocks(const vector<BlockId>& blocks) {
  std::lock_guard<LockType> l(data_lock_);
  AddOrphanedBlocksUnlocked(blocks);
}

void TabletMetadata::AddOrphanedBlocksUnlocked(const vector<BlockId>& blocks) {
  DCHECK(data_lock_.is_locked());
  orphaned_blocks_.insert(blocks.begin(), blocks.end());
}

void TabletMetadata::DeleteOrphanedBlocks(const vector<BlockId>& blocks) {
  if (PREDICT_FALSE(!FLAGS_enable_tablet_orphaned_block_deletion)) {
    LOG_WITH_PREFIX(WARNING) << "Not deleting " << blocks.size()
        << " block(s) from disk. Block deletion disabled via "
        << "--enable_tablet_orphaned_block_deletion=false";
    return;
  }

  vector<BlockId> deleted;
  for (const BlockId& b : blocks) {
    Status s = fs_manager()->DeleteBlock(b);
    // If we get NotFound, then the block was actually successfully
    // deleted before. So, we can remove it from our orphaned block list
    // as if it was a success.
    if (!s.ok() && !s.IsNotFound()) {
      WARN_NOT_OK(s, Substitute("Could not delete block $0", b.ToString()));
      continue;
    }

    deleted.push_back(b);
  }

  // Remove the successfully-deleted blocks from the set.
  {
    std::lock_guard<LockType> l(data_lock_);
    for (const BlockId& b : deleted) {
      orphaned_blocks_.erase(b);
    }
  }
}

void TabletMetadata::PinFlush() {
  std::lock_guard<LockType> l(data_lock_);
  CHECK_GE(num_flush_pins_, 0);
  num_flush_pins_++;
  VLOG(1) << "Number of flush pins: " << num_flush_pins_;
}

Status TabletMetadata::UnPinFlush() {
  std::unique_lock<LockType> l(data_lock_);
  CHECK_GT(num_flush_pins_, 0);
  num_flush_pins_--;
  if (needs_flush_) {
    l.unlock();
    RETURN_NOT_OK(Flush());
  }
  return Status::OK();
}

Status TabletMetadata::Flush() {
  TRACE_EVENT1("tablet", "TabletMetadata::Flush",
               "tablet_id", tablet_id_);

  MutexLock l_flush(flush_lock_);
  vector<BlockId> orphaned;
  TabletSuperBlockPB pb;
  {
    std::lock_guard<LockType> l(data_lock_);
    CHECK_GE(num_flush_pins_, 0);
    if (num_flush_pins_ > 0) {
      needs_flush_ = true;
      LOG(INFO) << "Not flushing: waiting for " << num_flush_pins_ << " pins to be released.";
      return Status::OK();
    }
    needs_flush_ = false;

    RETURN_NOT_OK(ToSuperBlockUnlocked(&pb, rowsets_));

    // Make a copy of the orphaned blocks list which corresponds to the superblock
    // that we're writing. It's important to take this local copy to avoid a race
    // in which another thread may add new orphaned blocks to the 'orphaned_blocks_'
    // set while we're in the process of writing the new superblock to disk. We don't
    // want to accidentally delete those blocks before that next metadata update
    // is persisted. See KUDU-701 for details.
    orphaned.assign(orphaned_blocks_.begin(), orphaned_blocks_.end());
  }
  pre_flush_callback_.Run();
  RETURN_NOT_OK(ReplaceSuperBlockUnlocked(pb));
  TRACE("Metadata flushed");
  l_flush.Unlock();

  // Now that the superblock is written, try to delete the orphaned blocks.
  //
  // If we crash just before the deletion, we'll retry when reloading from
  // disk; the orphaned blocks were persisted as part of the superblock.
  DeleteOrphanedBlocks(orphaned);

  return Status::OK();
}

Status TabletMetadata::UpdateUnlocked(
    const RowSetMetadataIds& to_remove,
    const RowSetMetadataVector& to_add,
    int64_t last_durable_mrs_id) {
  DCHECK(data_lock_.is_locked());
  CHECK_NE(state_, kNotLoadedYet);
  if (last_durable_mrs_id != kNoMrsFlushed) {
    DCHECK_GE(last_durable_mrs_id, last_durable_mrs_id_);
    last_durable_mrs_id_ = last_durable_mrs_id;
  }

  RowSetMetadataVector new_rowsets = rowsets_;
  auto it = new_rowsets.begin();
  while (it != new_rowsets.end()) {
    if (ContainsKey(to_remove, (*it)->id())) {
      AddOrphanedBlocksUnlocked((*it)->GetAllBlocks());
      it = new_rowsets.erase(it);
    } else {
      it++;
    }
  }

  for (const shared_ptr<RowSetMetadata>& meta : to_add) {
    new_rowsets.push_back(meta);
  }
  rowsets_ = new_rowsets;

  TRACE("TabletMetadata updated");
  return Status::OK();
}

Status TabletMetadata::ReplaceSuperBlock(const TabletSuperBlockPB &pb) {
  {
    MutexLock l(flush_lock_);
    RETURN_NOT_OK_PREPEND(ReplaceSuperBlockUnlocked(pb), "Unable to replace superblock");
    fs_manager_->dd_manager()->DeleteDataDirGroup(tablet_id_);
  }

  RETURN_NOT_OK_PREPEND(LoadFromSuperBlock(pb),
                        "Failed to load data from superblock protobuf");

  return Status::OK();
}

Status TabletMetadata::ReplaceSuperBlockUnlocked(const TabletSuperBlockPB &pb) {
  flush_lock_.AssertAcquired();

  string path = fs_manager_->GetTabletMetadataPath(tablet_id_);
  // TODO(awong): if this is called with a brand new DataDirGroup, move the path.
  // This may happen if there's a tablet copy.
  //
  // This path may not reflect any existing metadata.
  RETURN_NOT_OK_PREPEND(pb_util::WritePBContainerToPath(
                            fs_manager_->env(), path, pb,
                            pb_util::OVERWRITE, pb_util::SYNC),
                        Substitute("Failed to write tablet metadata $0", tablet_id_));
  on_disk_size_.store(pb.ByteSizeLong(), memory_order_relaxed);

  return Status::OK();
}

Status TabletMetadata::ReadSuperBlockFromDisk(const string& metadata_dir,
                                              TabletSuperBlockPB* superblock) const {
  string path = JoinPathSegments(metadata_dir, tablet_id_);
  // TODO(awong): handle failures here by making the DataDirManager aware of
  // which directories contain tablet metadata.
  RETURN_NOT_OK_PREPEND(
      pb_util::ReadPBContainerFromPath(fs_manager_->env(), path, superblock),
      Substitute("Could not load tablet metadata from $0", path));
  return Status::OK();
}

Status TabletMetadata::ToSuperBlock(TabletSuperBlockPB* super_block) const {
  // acquire the lock so that rowsets_ doesn't get changed until we're finished.
  std::lock_guard<LockType> l(data_lock_);
  return ToSuperBlockUnlocked(super_block, rowsets_);
}

Status TabletMetadata::ToSuperBlockUnlocked(TabletSuperBlockPB* super_block,
                                            const RowSetMetadataVector& rowsets) const {
  DCHECK(data_lock_.is_locked());
  // Convert to protobuf
  TabletSuperBlockPB pb;
  pb.set_table_id(table_id_);
  pb.set_tablet_id(tablet_id_);
  partition_.ToPB(pb.mutable_partition());
  pb.set_last_durable_mrs_id(last_durable_mrs_id_);
  pb.set_schema_version(schema_version_);
  partition_schema_.ToPB(pb.mutable_partition_schema());
  pb.set_table_name(table_name_);

  for (const shared_ptr<RowSetMetadata>& meta : rowsets) {
    meta->ToProtobuf(pb.add_rowsets());
  }

  DCHECK(schema_->has_column_ids());
  RETURN_NOT_OK_PREPEND(SchemaToPB(*schema_, pb.mutable_schema()),
                        "Couldn't serialize schema into superblock");

  pb.set_tablet_data_state(tablet_data_state_);
  if (!OpIdEquals(tombstone_last_logged_opid_, MinimumOpId())) {
    *pb.mutable_tombstone_last_logged_opid() = tombstone_last_logged_opid_;
  }

  for (const BlockId& block_id : orphaned_blocks_) {
    block_id.CopyToPB(pb.mutable_orphaned_blocks()->Add());
  }

  // Serialize the tablet's DataDirGroupPB if one exists. One may not exist if
  // this is called during a tablet deletion.
  DataDirGroupPB group_pb;
  if (fs_manager_->dd_manager()->GetDataDirGroupPB(tablet_id_, &group_pb)) {
    pb.mutable_data_dir_group()->Swap(&group_pb);
  }

  super_block->Swap(&pb);
  return Status::OK();
}

Status TabletMetadata::CreateRowSet(shared_ptr<RowSetMetadata> *rowset,
                                    const Schema& schema) {
  AtomicWord rowset_idx = Barrier_AtomicIncrement(&next_rowset_idx_, 1) - 1;
  gscoped_ptr<RowSetMetadata> scoped_rsm;
  RETURN_NOT_OK(RowSetMetadata::CreateNew(this, rowset_idx, &scoped_rsm));
  rowset->reset(DCHECK_NOTNULL(scoped_rsm.release()));
  return Status::OK();
}

const RowSetMetadata *TabletMetadata::GetRowSetForTests(int64_t id) const {
  for (const shared_ptr<RowSetMetadata>& rowset_meta : rowsets_) {
    if (rowset_meta->id() == id) {
      return rowset_meta.get();
    }
  }
  return nullptr;
}

RowSetMetadata *TabletMetadata::GetRowSetForTests(int64_t id) {
  std::lock_guard<LockType> l(data_lock_);
  for (const shared_ptr<RowSetMetadata>& rowset_meta : rowsets_) {
    if (rowset_meta->id() == id) {
      return rowset_meta.get();
    }
  }
  return nullptr;
}

void TabletMetadata::SetSchema(const Schema& schema, uint32_t version) {
  gscoped_ptr<Schema> new_schema(new Schema(schema));
  std::lock_guard<LockType> l(data_lock_);
  SetSchemaUnlocked(std::move(new_schema), version);
}

void TabletMetadata::SetSchemaUnlocked(gscoped_ptr<Schema> new_schema, uint32_t version) {
  DCHECK(new_schema->has_column_ids());

  Schema* old_schema = schema_;
  // "Release" barrier ensures that, when we publish the new Schema object,
  // all of its initialization is also visible.
  base::subtle::Release_Store(reinterpret_cast<AtomicWord*>(&schema_),
                              reinterpret_cast<AtomicWord>(new_schema.release()));
  if (PREDICT_TRUE(old_schema)) {
    old_schemas_.push_back(old_schema);
  }
  schema_version_ = version;
}

void TabletMetadata::SetTableName(const string& table_name) {
  std::lock_guard<LockType> l(data_lock_);
  table_name_ = table_name;
}

string TabletMetadata::table_name() const {
  std::lock_guard<LockType> l(data_lock_);
  DCHECK_NE(state_, kNotLoadedYet);
  return table_name_;
}

uint32_t TabletMetadata::schema_version() const {
  std::lock_guard<LockType> l(data_lock_);
  DCHECK_NE(state_, kNotLoadedYet);
  return schema_version_;
}

void TabletMetadata::set_tablet_data_state(TabletDataState state) {
  std::lock_guard<LockType> l(data_lock_);
  tablet_data_state_ = state;
}

string TabletMetadata::LogPrefix() const {
  return Substitute("T $0 P $1: ", tablet_id_, fs_manager_->uuid());
}

TabletDataState TabletMetadata::tablet_data_state() const {
  std::lock_guard<LockType> l(data_lock_);
  return tablet_data_state_;
}

} // namespace tablet
} // namespace kudu
