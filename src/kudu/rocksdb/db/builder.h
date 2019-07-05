//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <string>
#include <utility>
#include <vector>
#include "kudu/rocksdb/db/range_tombstone_fragmenter.h"
#include "kudu/rocksdb/db/table_properties_collector.h"
#include "kudu/rocksdb/options/cf_options.h"
#include "kudu/rocksdb/comparator.h"
#include "kudu/rocksdb/env.h"
#include "kudu/rocksdb/listener.h"
#include "kudu/rocksdb/options.h"
#include "kudu/rocksdb/status.h"
#include "kudu/rocksdb/table_properties.h"
#include "kudu/rocksdb/types.h"
#include "kudu/rocksdb/table/scoped_arena_iterator.h"
#include "kudu/rocksdb/util/event_logger.h"

namespace rocksdb {

struct Options;
struct FileMetaData;

class Env;
struct EnvOptions;
class Iterator;
class SnapshotChecker;
class TableCache;
class VersionEdit;
class TableBuilder;
class WritableFileWriter;
class InternalStats;

// @param column_family_name Name of the column family that is also identified
//    by column_family_id, or empty string if unknown. It must outlive the
//    TableBuilder returned by this function.
TableBuilder* NewTableBuilder(
    const ImmutableCFOptions& options, const MutableCFOptions& moptions,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    WritableFileWriter* file, const CompressionType compression_type,
    const uint64_t sample_for_compression,
    const CompressionOptions& compression_opts, int level,
    const bool skip_filters = false, const uint64_t creation_time = 0,
    const uint64_t oldest_key_time = 0, const uint64_t target_file_size = 0);

// Build a Table file from the contents of *iter.  The generated file
// will be named according to number specified in meta. On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
//
// @param column_family_name Name of the column family that is also identified
//    by column_family_id, or empty string if unknown.
extern Status BuildTable(
    const std::string& dbname, Env* env, const ImmutableCFOptions& options,
    const MutableCFOptions& mutable_cf_options, const EnvOptions& env_options,
    TableCache* table_cache, InternalIterator* iter,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters,
    FileMetaData* meta, const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, const CompressionType compression,
    const uint64_t sample_for_compression,
    const CompressionOptions& compression_opts, bool paranoid_file_checks,
    InternalStats* internal_stats, TableFileCreationReason reason,
    EventLogger* event_logger = nullptr, int job_id = 0,
    const Env::IOPriority io_priority = Env::IO_HIGH,
    TableProperties* table_properties = nullptr, int level = -1,
    const uint64_t creation_time = 0, const uint64_t oldest_key_time = 0,
    Env::WriteLifeTimeHint write_hint = Env::WLTH_NOT_SET);

}  // namespace rocksdb
