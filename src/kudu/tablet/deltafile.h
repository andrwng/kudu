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

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/cfile/binary_plain_block.h"
#include "kudu/cfile/block_handle.h"
#include "kudu/cfile/block_pointer.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/index_btree.h"
#include "kudu/common/rowid.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/rowset.h"
#include "kudu/util/faststring.h"
#include "kudu/util/locks.h"
#include "kudu/util/once.h"
#include "kudu/util/status.h"

namespace kudu {

class BlockId;
class FsManager;
class MemTracker;
class RowChangeList;
class ScanSpec;
class Slice;

namespace cfile {
struct ReaderOptions;
} // namespace cfile

namespace fs {
class BlockCreationTransaction;
class ReadableBlock;
class WritableBlock;
struct IOContext;
} // namespace fs

namespace tablet {

class MvccSnapshot;

class DeltaFileWriter {
 public:
  // Construct a new delta file writer.
  //
  // The writer takes ownership of the block and will Close it in Finish().
  explicit DeltaFileWriter(std::unique_ptr<fs::WritableBlock> block);

  Status Start();

  // Closes the delta file, including the underlying writable block.
  // Returns Status::Aborted() if no deltas were ever appended to this
  // writer.
  Status Finish();

  // Closes the delta file, finalizing the underlying block and releasing
  // it to 'transaction'.
  //
  // Returns Status::Aborted() if no deltas were ever appended to this
  // writer.
  Status FinishAndReleaseBlock(fs::BlockCreationTransaction* transaction);

  // Append a given delta to the file. This must be called in ascending order
  // of (key, timestamp) for REDOS and ascending order of key, descending order
  // of timestamp for UNDOS.
  template<DeltaType Type>
  Status AppendDelta(const DeltaKey &key, const RowChangeList &delta);

  void WriteDeltaStats(std::unique_ptr<DeltaStats> stats);

  std::unique_ptr<DeltaStats> release_delta_stats() {
    return std::move(delta_stats_);
  }

  size_t written_size() const {
    return writer_->written_size();
  }

 private:
  Status DoAppendDelta(const DeltaKey &key, const RowChangeList &delta);

  std::unique_ptr<DeltaStats> delta_stats_;
  std::unique_ptr<cfile::CFileWriter> writer_;

  // Buffer used as a temporary for storing the serialized form
  // of the deltas
  faststring tmp_buf_;

  #ifndef NDEBUG
  // The index of the previously written row.
  // This is used in debug mode to make sure that rows are appended
  // in order.
  DeltaKey last_key_;
  bool has_appended_;
  #endif

  DISALLOW_COPY_AND_ASSIGN(DeltaFileWriter);
};

class DeltaFileReader : public DeltaStore,
                        public std::enable_shared_from_this<DeltaFileReader> {
 public:
  static const char * const kDeltaStatsEntryName;

  // Fully open a delta file using a previously opened block.
  //
  // After this call, the delta reader is safe for use.
  static Status Open(std::unique_ptr<fs::ReadableBlock> block,
                     DeltaType delta_type,
                     cfile::ReaderOptions options,
                     std::shared_ptr<DeltaFileReader>* reader_out);

  // Lazily opens a delta file using a previously opened block. A lazy open
  // does not incur additional I/O, nor does it validate the contents of
  // the delta file.
  //
  // Init() must be called before using the file's stats.
  static Status OpenNoInit(std::unique_ptr<fs::ReadableBlock> block,
                           DeltaType delta_type,
                           cfile::ReaderOptions options,
                           std::unique_ptr<DeltaStats> delta_stats,
                           TxnMetadata* txn_metadata,
                           std::shared_ptr<DeltaFileReader>* reader_out);

  Status Init(const fs::IOContext* io_context) override;

  bool Initted() const override {
    return init_once_.init_succeeded();
  }

  // See DeltaStore::NewDeltaIterator(...)
  Status NewDeltaIterator(const RowIteratorOptions& opts,
                          std::unique_ptr<DeltaIterator>* iterator) const override;

  Status NewDeltaStoreIterator(const RowIteratorOptions& opts,
                               std::unique_ptr<DeltaStoreIterator>* iterator) const override;

  // See DeltaStore::CheckRowDeleted
  Status CheckRowDeleted(rowid_t row_idx,
                         const fs::IOContext* io_context,
                         bool *deleted) const override;

  uint64_t EstimateSize() const override;

  const BlockId& block_id() const { return reader_->block_id(); }

  const DeltaStats& delta_stats() const override {
    std::lock_guard<simple_spinlock> l(stats_lock_);
    DCHECK(delta_stats_);
    return *delta_stats_;
  }

  bool has_delta_stats() const override {
    std::lock_guard<simple_spinlock> l(stats_lock_);
    return delta_stats_ != nullptr;
  }

  std::string ToString() const override {
    if (!init_once_.init_succeeded()) return reader_->ToString();
    return strings::Substitute("$0 ($1)", reader_->ToString(), delta_stats_->ToString());
  }

  // Returns true if this delta file may include any deltas which need to be
  // applied when scanning the given snapshots, or if the file has not yet
  // been fully initialized.
  bool IsRelevantForSnapshots(const boost::optional<MvccSnapshot>& snap_to_exclude,
                              const MvccSnapshot& snap_to_include) const;

  // Clone this DeltaFileReader for testing and validation purposes (such as
  // while in DEBUG mode). The resulting object will not be Initted().
  Status CloneForDebugging(FsManager* fs_manager,
                           const std::shared_ptr<MemTracker>& parent_mem_tracker,
                           std::shared_ptr<DeltaFileReader>* out) const;

  TxnMetadata* txn_metadata() {
    return txn_metadata_.get();
  }

 private:
  template<DeltaType Type>
  friend class DeltaFileIterator;
  friend class DeltaFileStoreIterator;

  DISALLOW_COPY_AND_ASSIGN(DeltaFileReader);

  const std::shared_ptr<cfile::CFileReader> &cfile_reader() const {
    return reader_;
  }

  DeltaFileReader(std::unique_ptr<cfile::CFileReader> cf_reader,
                  std::unique_ptr<DeltaStats> delta_stats,
                  DeltaType delta_type,
                  TxnMetadata* txn_metadata);

  // Callback used in 'init_once_' to initialize this delta file.
  Status InitOnce(const fs::IOContext* io_context);

  Status ReadDeltaStats();

  scoped_refptr<TxnMetadata> txn_metadata_;

  std::shared_ptr<cfile::CFileReader> reader_;

  // TODO(awong): it'd be nice to not heap-allocate this and other usages of
  // delta stats.
  mutable simple_spinlock stats_lock_;
  std::unique_ptr<DeltaStats> delta_stats_;

  // The type of this delta, i.e. UNDO or REDO.
  const DeltaType delta_type_;

  KuduOnceLambda init_once_;
};

// PrepareBatch() will read forward all blocks from the deltafile
// which overlap with the block being prepared, enqueueing them onto
// the 'delta_blocks_' deque. The prepared blocks are then used to
// actually apply deltas in ApplyUpdates().
struct PreparedDeltaBlock {
  // The pointer from which this block was read. This is only used for
  // logging, etc.
  cfile::BlockPointer block_ptr_;

  // Handle to the block, so it doesn't get freed from underneath us.
  scoped_refptr<cfile::BlockHandle> block_;

  // The block decoder, to avoid having to re-parse the block header
  // on every ApplyUpdates() call
  std::unique_ptr<cfile::BinaryPlainBlockDecoder> decoder_;

  // The first row index for which there is an update in this delta block.
  rowid_t first_updated_idx_;

  // The last row index for which there is an update in this delta block.
  rowid_t last_updated_idx_;

  // Within this block, the positional index of the first update that needs to
  // be consulted. This allows deltas to be skipped at the beginning of the
  // block when the row block starts towards the end of the delta block.
  // For example:
  // <-- delta block ---->
  //                   <--- prepared row block --->
  // Here, we can skip a bunch of deltas at the beginning of the delta block
  // which we know don't apply to the prepared row block.
  rowid_t prepared_block_start_idx_;

  // Return a string description of this prepared block, for logging.
  std::string ToString() const {
    return StringPrintf("%d-%d (%s)", first_updated_idx_, last_updated_idx_,
                        block_ptr_.ToString().c_str());
  }
};

class DeltaFileStoreIterator : public DeltaStoreIterator {
 public:
  DeltaFileStoreIterator(RowIteratorOptions opts,
                         std::shared_ptr<DeltaFileReader> dfr)
      : opts_(std::move(opts)),
        cache_blocks_(cfile::CFileReader::CACHE_BLOCK),
        initted_(false),
        dfr_(std::move(dfr)) {}
  Status Init(ScanSpec* spec) override;
  Status SeekToOrdinal(rowid_t idx) override;

  // Loads delta blocks for at least the next 'nrows' and prepares to start
  // iterating through them.
  Status PrepareForBatch(size_t nrows) override;

  // Returns whether the delta store has the capacity to prepare more batches.
  bool HasMoreDeltas() const override;

  // Returns whether there might be another delta in the current batch.
  bool HasPreparedNext() const override;

  Status GetNextDelta(DeltaKey* key, Slice* slice) override;

  void IterateNext() override;

  // Used to indicate that the previously prepared batch of 'nrows' has been
  // iterated through.
  void Finish(size_t nrows) override;

  std::string ToString() const {
    return dfr_->ToString();
  }
 private:
  // Determine the row index of the first update in the block currently
  // pointed to by index_iter_.
  Status GetFirstRowIndexInCurrentBlock(rowid_t *idx);

  // Determine the last updated row index contained in the given decoded block.
  static Status GetLastRowIndexInDecodedBlock(
      const cfile::BinaryPlainBlockDecoder& dec, rowid_t *idx);

  // Read the current block of data from the current position in the file
  // onto the end of the load_blocks_.
  Status LoadCurrentBlock();

  const RowIteratorOptions opts_;
  cfile::CFileReader::CacheControl cache_blocks_;
  bool initted_;

  std::shared_ptr<DeltaFileReader> dfr_;
  std::unique_ptr<cfile::IndexTreeIterator> index_iter_;

  // Indicates whether the index iter has more blocks available to be prepared.
  bool index_has_more_blocks_ = false;

  // Set when initially seeking and finishing a batch.
  rowid_t cur_batch_start_idx_;

  int cur_block_idx_ = 0;

  // The default is an invalid value.
  int idx_in_cur_block_ = -1;

  std::deque<std::unique_ptr<PreparedDeltaBlock>> loaded_blocks_;
};

// Iterator over the deltas contained in a delta file.
//
// See DeltaIterator for details.
template <DeltaType Type>
class DeltaFileIterator :
    public DeltaPreparingIterator<DeltaPreparer<DeltaFilePreparerTraits<Type>>,
                                  DeltaFileStoreIterator> {
 public:
  std::string ToString() const override {
    return strings::Substitute("DeltaFileIterator($0)", store_iter_.ToString());
  }
  const DeltaFileStoreIterator* store_iter() const override {
    return &store_iter_;
  }
  DeltaFileStoreIterator* store_iter() override {
    return &store_iter_;
  }
  const DeltaPreparer<DeltaFilePreparerTraits<Type>>* preparer() const override {
    return &preparer_;
  }
  DeltaPreparer<DeltaFilePreparerTraits<Type>>* preparer() override {
    return &preparer_;
  }
 private:
  friend class DeltaFileReader;

  // The pointers in 'opts' and 'dfr' must remain valid for the lifetime of the iterator.
  DeltaFileIterator(std::shared_ptr<DeltaFileReader> dfr,
                    const RowIteratorOptions& opts);

  DeltaFileStoreIterator store_iter_;
  DeltaPreparer<DeltaFilePreparerTraits<Type>> preparer_;

  bool prepared_;
  DISALLOW_COPY_AND_ASSIGN(DeltaFileIterator);
};


} // namespace tablet
} // namespace kudu
