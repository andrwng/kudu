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

#include "kudu/tablet/deltamemstore.h"

#include <algorithm>
#include <memory>
#include <ostream>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/row_changelist.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/rowset.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memcmpable_varint.h"
#include "kudu/util/memory/memory.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tablet {

using fs::IOContext;
using log::LogAnchorRegistry;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

////////////////////////////////////////////////////////////
// DeltaMemStore implementation
////////////////////////////////////////////////////////////

static const int kInitialArenaSize = 16;

Status DeltaMemStore::Create(int64_t id,
                             int64_t rs_id,
                             LogAnchorRegistry* log_anchor_registry,
                             shared_ptr<MemTracker> parent_tracker,
                             shared_ptr<DeltaMemStore>* dms) {
  auto local_dms(DeltaMemStore::make_shared(
      id, rs_id, log_anchor_registry, std::move(parent_tracker)));
  *dms = std::move(local_dms);
  return Status::OK();
}

DeltaMemStore::DeltaMemStore(int64_t id,
                             int64_t rs_id,
                             LogAnchorRegistry* log_anchor_registry,
                             shared_ptr<MemTracker> parent_tracker)
  : id_(id),
    rs_id_(rs_id),
    creation_time_(MonoTime::Now()),
    highest_timestamp_(Timestamp::kMin),
    allocator_(new MemoryTrackingBufferAllocator(
        HeapBufferAllocator::Get(), std::move(parent_tracker))),
    arena_(new ThreadSafeMemoryTrackingArena(kInitialArenaSize, allocator_)),
    tree_(arena_),
    anchorer_(log_anchor_registry,
              Substitute("Rowset-$0/DeltaMemStore-$1", rs_id_, id_)),
    disambiguator_sequence_number_(0),
    deleted_row_count_(0) {
}

Status DeltaMemStore::Init(const IOContext* /*io_context*/) {
  return Status::OK();
}

Status DeltaMemStore::Update(Timestamp timestamp,
                             rowid_t row_idx,
                             const RowChangeList &update,
                             const consensus::OpId& op_id) {
  DeltaKey key(row_idx, timestamp);

  faststring buf;

  key.EncodeTo(&buf);

  Slice key_slice(buf);
  btree::PreparedMutation<DMSTreeTraits> mutation(key_slice);
  mutation.Prepare(&tree_);
  if (PREDICT_FALSE(mutation.exists())) {
    // We already have a delta for this row at the same timestamp.
    // Try again with a disambiguating sequence number appended to the key.
    int seq = disambiguator_sequence_number_.Increment();
    PutMemcmpableVarint64(&buf, seq);
    key_slice = Slice(buf);
    mutation.Reset(key_slice);
    mutation.Prepare(&tree_);
    CHECK(!mutation.exists())
      << "Appended a sequence number but still hit a duplicate "
      << "for rowid " << row_idx << " at timestamp " << timestamp;
  }
  if (PREDICT_FALSE(!mutation.Insert(update.slice()))) {
    return Status::IOError("Unable to insert into tree");
  }

  anchorer_.AnchorIfMinimum(op_id.index());

  if (update.is_delete()) {
    deleted_row_count_.Increment();
  }

  std::lock_guard<simple_spinlock> l(ts_lock_);
  highest_timestamp_ = std::max(highest_timestamp_, timestamp);
  return Status::OK();
}

Status DeltaMemStore::FlushToFile(DeltaFileWriter *dfw) {
  unique_ptr<DeltaStats> stats(new DeltaStats());

  unique_ptr<DMSTreeIter> iter(tree_.NewIterator());
  iter->SeekToStart();
  while (iter->IsValid()) {
    Slice key_slice;
    Slice val;
    iter->GetCurrentEntry(&key_slice, &val);
    DeltaKey key;
    RETURN_NOT_OK(key.DecodeFrom(&key_slice));
    RowChangeList rcl(val);
    RETURN_NOT_OK_PREPEND(dfw->AppendDelta<REDO>(key, rcl), "Failed to append delta");
    stats->UpdateStats(key.timestamp(), rcl);
    iter->Next();
  }
  dfw->WriteDeltaStats(std::move(stats));
  return Status::OK();
}

Status DeltaMemStore::NewDeltaIterator(const RowIteratorOptions& opts,
                                       unique_ptr<DeltaIterator>* iterator) const {
  iterator->reset(new DMSIterator(shared_from_this(), opts));
  return Status::OK();
}

Status DeltaMemStore::CheckRowDeleted(rowid_t row_idx,
                                      const IOContext* /*io_context*/,
                                      bool *deleted) const {
  *deleted = false;
  DeltaKey key(row_idx, Timestamp(Timestamp::kMax));
  faststring buf;
  key.EncodeTo(&buf);
  Slice key_slice(buf);

  bool exact;

  unique_ptr<DMSTreeIter> iter(tree_.NewIterator());
  if (!iter->SeekAtOrBefore(key_slice, &exact)) {
    return Status::OK();
  }

  DCHECK(!exact);

  Slice current_key_slice, v;
  iter->GetCurrentEntry(&current_key_slice, &v);
  RETURN_NOT_OK(key.DecodeFrom(&current_key_slice));
  if (key.row_idx() != row_idx) {
    return Status::OK();
  }
  RowChangeListDecoder decoder((RowChangeList(v)));
  decoder.InitNoSafetyChecks();
  *deleted = decoder.is_delete();
  return Status::OK();
}

void DeltaMemStore::DebugPrint() const {
  tree_.DebugPrint();
}

int64_t DeltaMemStore::deleted_row_count() const {
  int64_t count = deleted_row_count_.Load();
  DCHECK_GE(count, 0);
  return count;
}

DeltaMemStoreIterator::DeltaMemStoreIterator(const shared_ptr<const DeltaMemStore>& dms)
    : dms_(dms), iter_(dms->tree_.NewIterator()) {}

void DeltaMemStoreIterator::IterateNext() {
  iter_->Next();
}

Status DeltaMemStoreIterator::Init(ScanSpec* /*spec*/) {
  return Status::OK();
}

Status DeltaMemStoreIterator::SeekToOrdinal(rowid_t idx) {
  faststring buf;
  DeltaKey key(idx, Timestamp(0));
  key.EncodeTo(&buf);
  bool exact; // unused
  iter_->SeekAtOrAfter(Slice(buf), &exact);
  return Status::OK();
}

Status DeltaMemStoreIterator::PrepareForBatch(size_t /*nrows*/) {
  return Status::OK();
}

bool DeltaMemStoreIterator::HasMoreDeltas() const {
  return iter_->IsValid();
}

bool DeltaMemStoreIterator::HasPreparedNext() const {
  return iter_->IsValid();
}

Status DeltaMemStoreIterator::GetNextDelta(DeltaKey* key, Slice* slice) {
  DCHECK(iter_->IsValid());
  Slice key_slice;
  Slice val_slice;
  iter_->GetCurrentEntry(&key_slice, &val_slice);
  DeltaKey delta_key;
  RETURN_NOT_OK(delta_key.DecodeFrom(&key_slice));
  *key = delta_key;
  *slice = val_slice;
  return Status::OK();
}

void DeltaMemStoreIterator::Finish(size_t /*nrows*/) {
}

////////////////////////////////////////////////////////////
// DMSIterator
////////////////////////////////////////////////////////////

DMSIterator::DMSIterator(const shared_ptr<const DeltaMemStore>& dms,
                         RowIteratorOptions opts)
    : store_iter_(dms),
      preparer_(std::move(opts)) {}

} // namespace tablet
} // namespace kudu
