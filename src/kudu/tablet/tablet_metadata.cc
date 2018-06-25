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
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet_metadata_manager.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

DEFINE_bool(enable_tablet_orphaned_block_deletion, true,
            "Whether to enable deletion of orphaned blocks from disk. "
            "Note: This is only exposed for debugging purposes!");
TAG_FLAG(enable_tablet_orphaned_block_deletion, advanced);
TAG_FLAG(enable_tablet_orphaned_block_deletion, hidden);
TAG_FLAG(enable_tablet_orphaned_block_deletion, runtime);

using base::subtle::Barrier_AtomicIncrement;
using kudu::consensus::MinimumOpId;
using kudu::consensus::OpId;
using kudu::fs::BlockManager;
using kudu::fs::BlockDeletionTransaction;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::memory_order_relaxed;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

const int64_t kNoDurableMemStore = -1;

// ============================================================================
//  Tablet Metadata
// ============================================================================

Status TabletMetadata::CreateNew(FsManager* fs_manager,
                                 TabletMetadataManager* tmeta_manager,
                                 const string& tablet_id,
                                 const string& table_name,
                                 const string& table_id,
                                 const Schema& schema,
                                 const PartitionSchema& partition_schema,
                                 const Partition& partition,
                                 const TabletDataState& initial_tablet_data_state,
                                 boost::optional<OpId> tombstone_last_logged_opid,
                                 scoped_refptr<TabletMetadata>* metadata) {

  // Verify that no existing tablet exists with the same ID.
  if (tmeta_manager->Exists(tablet_id)) {
    return Status::AlreadyPresent("Tablet already exists", tablet_id);
  }

  RETURN_NOT_OK_PREPEND(fs_manager->dd_manager()->CreateDataDirGroup(tablet_id),
      "Failed to create TabletMetadata");
  auto dir_group_cleanup = MakeScopedCleanup([&]() {
    fs_manager->dd_manager()->DeleteDataDirGroup(tablet_id);
  });
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager,
                                                       tmeta_manager,
                                                       tablet_id,
                                                       table_name,
                                                       table_id,
                                                       schema,
                                                       partition_schema,
                                                       partition,
                                                       initial_tablet_data_state,
                                                       std::move(tombstone_last_logged_opid)));
  RETURN_NOT_OK(ret->Flush());
  dir_group_cleanup.cancel();

  metadata->swap(ret);
  return Status::OK();
}

Status TabletMetadata::Load(FsManager* fs_manager,
                            TabletMetadataManager* tmeta_manager,
                            const string& tablet_id,
                            scoped_refptr<TabletMetadata>* metadata) {
  return tmeta_manager->Load(tablet_id, metadata, nullptr);
}

Status TabletMetadata::LoadOrCreate(FsManager* fs_manager,
                                    TabletMetadataManager* tmeta_manager,
                                    const string& tablet_id,
                                    const string& table_name,
                                    const string& table_id,
                                    const Schema& schema,
                                    const PartitionSchema& partition_schema,
                                    const Partition& partition,
                                    const TabletDataState& initial_tablet_data_state,
                                    boost::optional<OpId> tombstone_last_logged_opid,
                                    scoped_refptr<TabletMetadata>* metadata) {
  Status s = Load(fs_manager, tmeta_manager, tablet_id, metadata);
  if (s.ok()) {
    if (!(*metadata)->schema().Equals(schema)) {
      return Status::Corruption(Substitute("Schema on disk ($0) does not "
        "match expected schema ($1)", (*metadata)->schema().ToString(),
        schema.ToString()));
    }
    return Status::OK();
  }
  if (s.IsNotFound()) {
    return CreateNew(fs_manager, tmeta_manager, tablet_id, table_name, table_id, schema,
                     partition_schema, partition, initial_tablet_data_state,
                     std::move(tombstone_last_logged_opid), metadata);
  }
  return s;
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

void TabletMetadata::ClearRowSets(TabletDataState delete_type,
                  const boost::optional<OpId>& last_logged_opid) {
  DCHECK(!last_logged_opid || last_logged_opid->IsInitialized());
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
      tombstone_last_logged_opid_ = last_logged_opid;
    }
  }
}

bool TabletMetadata::IsTombstonedWithNoBlocks() const {
  std::lock_guard<LockType> l(data_lock_);
  return tablet_data_state_ == TABLET_DATA_TOMBSTONED &&
      rowsets_.empty() &&
      orphaned_blocks_.empty();
}

// XXX(awong): think about whether this needs the flush lock
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

  RETURN_NOT_OK(tmeta_manager_->Delete(tablet_id_));
  return Status::OK();
}

TabletMetadata::TabletMetadata(FsManager* fs_manager,
                               TabletMetadataManager* tmeta_manager,
                               string tablet_id, string table_name, string table_id,
                               const Schema& schema, PartitionSchema partition_schema,
                               Partition partition,
                               const TabletDataState& tablet_data_state,
                               boost::optional<OpId> tombstone_last_logged_opid)
    : tablet_id_(std::move(tablet_id)),
      table_id_(std::move(table_id)),
      partition_(std::move(partition)),
      fs_manager_(fs_manager),
      tmeta_manager_(tmeta_manager),
      next_rowset_idx_(0),
      last_durable_mrs_id_(kNoDurableMemStore),
      schema_(new Schema(schema)),
      schema_version_(0),
      table_name_(std::move(table_name)),
      partition_schema_(std::move(partition_schema)),
      tablet_data_state_(tablet_data_state),
      tombstone_last_logged_opid_(std::move(tombstone_last_logged_opid)),
      num_flush_pins_(0),
      needs_flush_(false),
      flush_count_for_tests_(0),
      pre_flush_callback_(Bind(DoNothingStatusClosure)) {
  CHECK(schema_->has_column_ids());
  CHECK_GT(schema_->num_key_columns(), 0);
}

TabletMetadata::~TabletMetadata() {
}

TabletMetadata::TabletMetadata(FsManager* fs_manager,
                               TabletMetadataManager* tmeta_manager,
                               string tablet_id)
    : tablet_id_(std::move(tablet_id)),
      fs_manager_(fs_manager),
      tmeta_manager_(tmeta_manager),
      next_rowset_idx_(0),
      schema_(nullptr),
      num_flush_pins_(0),
      needs_flush_(false),
      flush_count_for_tests_(0),
      pre_flush_callback_(Bind(DoNothingStatusClosure)) {}

Status TabletMetadata::UpdateOnDiskSize() {
  uint64_t on_disk_size;
  RETURN_NOT_OK(tmeta_manager_->GetSize(tablet_id_, &on_disk_size));
  on_disk_size_.store(on_disk_size, memory_order_relaxed);
  return Status::OK();
}

Status TabletMetadata::LoadFromSuperBlock(const TabletSuperBlockPB& superblock,
                                          vector<BlockId>* orphaned_blocks_list) {
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
    unique_ptr<Schema> schema(new Schema());
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
    if (table_id_.empty()) {
      // XXX(awong): DCHECK on the things.
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
      unique_ptr<RowSetMetadata> rowset_meta;
      RETURN_NOT_OK(RowSetMetadata::Load(this, rowset_pb, &rowset_meta));
      next_rowset_idx_ = std::max(next_rowset_idx_, rowset_meta->id() + 1);
      rowsets_.push_back(shared_ptr<RowSetMetadata>(rowset_meta.release()));
    }

    // Determine the largest block ID known to the tablet metadata so we can
    // notify the block manager of blocks it may have missed (e.g. if a data
    // directory failed and the blocks on it were not read).
    BlockId max_block_id;
    const auto& block_ids = CollectBlockIds();
    for (BlockId block_id : block_ids) {
      max_block_id = std::max(max_block_id, block_id);
    }

    for (const BlockIdPB& block_pb : superblock.orphaned_blocks()) {
      BlockId orphaned_block_id = BlockId::FromPB(block_pb);
      max_block_id = std::max(max_block_id, orphaned_block_id);
      orphaned_blocks.push_back(orphaned_block_id);
    }
    AddOrphanedBlocksUnlocked(orphaned_blocks);

    // Notify the block manager of the highest block ID seen.
    fs_manager()->block_manager()->NotifyBlockId(max_block_id);

    if (superblock.has_data_dir_group()) {
      // An error loading the data dir group is non-fatal, it just means the
      // tablet will fail to bootstrap later.
      WARN_NOT_OK(fs_manager_->dd_manager()->LoadDataDirGroupFromPB(
          tablet_id_, superblock.data_dir_group()),
          "failed to load DataDirGroup from superblock");
    } else if (tablet_data_state_ == TABLET_DATA_READY) {
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

    // Note: Previous versions of Kudu used MinimumOpId() as a "null" value on
    // disk for the last-logged opid, so we special-case it at load time and
    // consider it equal to "not present".
    if (superblock.has_tombstone_last_logged_opid() &&
        superblock.tombstone_last_logged_opid().IsInitialized() &&
        !OpIdEquals(MinimumOpId(), superblock.tombstone_last_logged_opid())) {
      tombstone_last_logged_opid_ = superblock.tombstone_last_logged_opid();
    } else {
      tombstone_last_logged_opid_ = boost::none;
    }
  }

  // Now is a good time to clean up any orphaned blocks that may have been
  // left behind from a crash just after replacing the superblock.
  // if (!fs_manager()->read_only()) {
  //   DeleteOrphanedBlocks(orphaned_blocks);
  // }
  if (orphaned_blocks_list) {
    orphaned_blocks_list->swap(orphaned_blocks);
  }

  return Status::OK();
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add,
                                      int64_t last_durable_mrs_id) {
  {
    std::lock_guard<LockType> l(data_lock_);
    UpdateUnlocked(to_remove, to_add, last_durable_mrs_id);
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

  BlockManager* bm = fs_manager()->block_manager();
  shared_ptr<BlockDeletionTransaction> transaction = bm->NewDeletionTransaction();
  for (const BlockId& b : blocks) {
    transaction->AddDeletedBlock(b);
  }
  vector<BlockId> deleted;
  WARN_NOT_OK(transaction->CommitDeletedBlocks(&deleted),
              "not all orphaned blocks were deleted");

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

    RETURN_NOT_OK(ToSuperBlockUnlocked(&pb));

    // Make a copy of the orphaned blocks list which corresponds to the superblock
    // that we're writing. It's important to take this local copy to avoid a race
    // in which another thread may add new orphaned blocks to the 'orphaned_blocks_'
    // set while we're in the process of writing the new superblock to disk. We don't
    // want to accidentally delete those blocks before that next metadata update
    // is persisted. See KUDU-701 for details.
    orphaned.assign(orphaned_blocks_.begin(), orphaned_blocks_.end());
  }
  pre_flush_callback_.Run();

  // XXX(awong):
  // tmeta_manager_->Write()
  // tmeta_manager_->WriteUpdate()
  RETURN_NOT_OK(tmeta_manager_->Flush(pb));
  flush_count_for_tests_++;
  TRACE("Metadata flushed");
  l_flush.Unlock();

  RETURN_NOT_OK(UpdateOnDiskSize());

  // Now that the superblock is written, try to delete the orphaned blocks.
  //
  // If we crash just before the deletion, we'll retry when reloading from
  // disk; the orphaned blocks were persisted as part of the superblock.
  DeleteOrphanedBlocks(orphaned);

  return Status::OK();
}

void TabletMetadata::UpdateUnlocked(
    const RowSetMetadataIds& to_remove,
    const RowSetMetadataVector& to_add,
    int64_t last_durable_mrs_id) {
  DCHECK(data_lock_.is_locked());
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
}

void TabletMetadata::SetPreFlushCallback(StatusClosure callback) {
  MutexLock l_flush(flush_lock_);
  pre_flush_callback_ = std::move(callback);
}

boost::optional<consensus::OpId> TabletMetadata::tombstone_last_logged_opid() const {
  std::lock_guard<LockType> l(data_lock_);
  return tombstone_last_logged_opid_;
}

Status TabletMetadata::ToSuperBlock(TabletSuperBlockPB* super_block) const {
  // acquire the lock so that rowsets_ doesn't get changed until we're finished.
  std::lock_guard<LockType> l(data_lock_);
  return ToSuperBlockUnlocked(super_block);
}

Status TabletMetadata::ToSuperBlockUnlocked(TabletSuperBlockPB* super_block) const {
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

  for (const shared_ptr<RowSetMetadata>& meta : rowsets_) {
    meta->ToProtobuf(pb.add_rowsets());
  }

  DCHECK(schema_->has_column_ids());
  RETURN_NOT_OK_PREPEND(SchemaToPB(*schema_, pb.mutable_schema()),
                        "Couldn't serialize schema into superblock");

  pb.set_tablet_data_state(tablet_data_state_);
  if (tombstone_last_logged_opid_ &&
      !OpIdEquals(MinimumOpId(), *tombstone_last_logged_opid_)) {
    *pb.mutable_tombstone_last_logged_opid() = *tombstone_last_logged_opid_;
  }

  for (const BlockId& block_id : orphaned_blocks_) {
    block_id.CopyToPB(pb.mutable_orphaned_blocks()->Add());
  }

  // Serialize the tablet's DataDirGroupPB if one exists. One may not exist if
  // this is called during a tablet deletion.
  DataDirGroupPB group_pb;
  if (fs_manager_->dd_manager()->GetDataDirGroupPB(tablet_id_, &group_pb).ok()) {
    pb.mutable_data_dir_group()->Swap(&group_pb);
  }

  super_block->Swap(&pb);
  return Status::OK();
}

Status TabletMetadata::CreateRowSet(shared_ptr<RowSetMetadata>* rowset) {
  AtomicWord rowset_idx = Barrier_AtomicIncrement(&next_rowset_idx_, 1) - 1;
  unique_ptr<RowSetMetadata> scoped_rsm;
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
  unique_ptr<Schema> new_schema(new Schema(schema));
  std::lock_guard<LockType> l(data_lock_);
  SetSchemaUnlocked(std::move(new_schema), version);
}

void TabletMetadata::SetSchemaUnlocked(unique_ptr<Schema> new_schema, uint32_t version) {
  DCHECK(new_schema->has_column_ids());

  if (PREDICT_TRUE(schema_)) {
    old_schemas_.push_back(std::move(schema_));
  }
  schema_ = std::move(new_schema);
  schema_version_ = version;
}

void TabletMetadata::SetTableName(const string& table_name) {
  std::lock_guard<LockType> l(data_lock_);
  table_name_ = table_name;
}

string TabletMetadata::table_name() const {
  std::lock_guard<LockType> l(data_lock_);
  return table_name_;
}

uint32_t TabletMetadata::schema_version() const {
  std::lock_guard<LockType> l(data_lock_);
  return schema_version_;
}

void TabletMetadata::set_tablet_data_state(TabletDataState state) {
  std::lock_guard<LockType> l(data_lock_);
  if (state == TABLET_DATA_READY) {
    tombstone_last_logged_opid_ = boost::none;
  }
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
