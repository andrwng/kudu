//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <limits>
#include <set>
#include <utility>
#include <vector>
#include <string>

#include "kudu/rocksdb/db/column_family.h"
#include "kudu/rocksdb/db/dbformat.h"
#include "kudu/rocksdb/db/flush_scheduler.h"
#include "kudu/rocksdb/db/internal_stats.h"
#include "kudu/rocksdb/db/job_context.h"
#include "kudu/rocksdb/db/log_writer.h"
#include "kudu/rocksdb/db/logs_with_prep_tracker.h"
#include "kudu/rocksdb/db/memtable_list.h"
#include "kudu/rocksdb/db/snapshot_impl.h"
#include "kudu/rocksdb/db/version_edit.h"
#include "kudu/rocksdb/db/write_controller.h"
#include "kudu/rocksdb/db/write_thread.h"
#include "kudu/rocksdb/monitoring/instrumented_mutex.h"
#include "kudu/rocksdb/options/db_options.h"
#include "kudu/rocksdb/port/port.h"
#include "kudu/rocksdb/db.h"
#include "kudu/rocksdb/env.h"
#include "kudu/rocksdb/memtablerep.h"
#include "kudu/rocksdb/transaction_log.h"
#include "kudu/rocksdb/table/scoped_arena_iterator.h"
#include "kudu/rocksdb/util/autovector.h"
#include "kudu/rocksdb/util/event_logger.h"
#include "kudu/rocksdb/util/stop_watch.h"
#include "kudu/rocksdb/util/thread_local.h"

namespace rocksdb {

class DBImpl;
class MemTable;
class SnapshotChecker;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class Arena;

class FlushJob {
 public:
  // TODO(icanadi) make effort to reduce number of parameters here
  // IMPORTANT: mutable_cf_options needs to be alive while FlushJob is alive
  FlushJob(const std::string& dbname, ColumnFamilyData* cfd,
           const ImmutableDBOptions& db_options,
           const MutableCFOptions& mutable_cf_options,
           const uint64_t* max_memtable_id, const EnvOptions& env_options,
           VersionSet* versions, InstrumentedMutex* db_mutex,
           std::atomic<bool>* shutting_down,
           std::vector<SequenceNumber> existing_snapshots,
           SequenceNumber earliest_write_conflict_snapshot,
           SnapshotChecker* snapshot_checker, JobContext* job_context,
           LogBuffer* log_buffer, Directory* db_directory,
           Directory* output_file_directory, CompressionType output_compression,
           Statistics* stats, EventLogger* event_logger, bool measure_io_stats,
           const bool sync_output_directory, const bool write_manifest,
           Env::Priority thread_pri);

  ~FlushJob();

  // Require db_mutex held.
  // Once PickMemTable() is called, either Run() or Cancel() has to be called.
  void PickMemTable();
  Status Run(LogsWithPrepTracker* prep_tracker = nullptr,
             FileMetaData* file_meta = nullptr);
  void Cancel();
  TableProperties GetTableProperties() const { return table_properties_; }
  const autovector<MemTable*>& GetMemTables() const { return mems_; }

 private:
  void ReportStartedFlush();
  void ReportFlushInputSize(const autovector<MemTable*>& mems);
  void RecordFlushIOStats();
  Status WriteLevel0Table();

  const std::string& dbname_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const MutableCFOptions& mutable_cf_options_;
  // Pointer to a variable storing the largest memtable id to flush in this
  // flush job. RocksDB uses this variable to select the memtables to flush in
  // this job. All memtables in this column family with an ID smaller than or
  // equal to *max_memtable_id_ will be selected for flush. If null, then all
  // memtables in the column family will be selected.
  const uint64_t* max_memtable_id_;
  const EnvOptions env_options_;
  VersionSet* versions_;
  InstrumentedMutex* db_mutex_;
  std::atomic<bool>* shutting_down_;
  std::vector<SequenceNumber> existing_snapshots_;
  SequenceNumber earliest_write_conflict_snapshot_;
  SnapshotChecker* snapshot_checker_;
  JobContext* job_context_;
  LogBuffer* log_buffer_;
  Directory* db_directory_;
  Directory* output_file_directory_;
  CompressionType output_compression_;
  Statistics* stats_;
  EventLogger* event_logger_;
  TableProperties table_properties_;
  bool measure_io_stats_;
  // True if this flush job should call fsync on the output directory. False
  // otherwise.
  // Usually sync_output_directory_ is true. A flush job needs to call sync on
  // the output directory before committing to the MANIFEST.
  // However, an individual flush job does not have to call sync on the output
  // directory if it is part of an atomic flush. After all flush jobs in the
  // atomic flush succeed, call sync once on each distinct output directory.
  const bool sync_output_directory_;
  // True if this flush job should write to MANIFEST after successfully
  // flushing memtables. False otherwise.
  // Usually write_manifest_ is true. A flush job commits to the MANIFEST after
  // flushing the memtables.
  // However, an individual flush job cannot rashly write to the MANIFEST
  // immediately after it finishes the flush if it is part of an atomic flush.
  // In this case, only after all flush jobs succeed in flush can RocksDB
  // commit to the MANIFEST.
  const bool write_manifest_;

  // Variables below are set by PickMemTable():
  FileMetaData meta_;
  autovector<MemTable*> mems_;
  VersionEdit* edit_;
  Version* base_;
  bool pick_memtable_called;
  Env::Priority thread_pri_;
};

}  // namespace rocksdb
