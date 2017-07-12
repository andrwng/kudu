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

#include "kudu/fs/fs_manager.h"

#include <deque>
#include <iostream>
#include <map>
#include <stack>
#include <unordered_set>

#include <boost/optional.hpp>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <google/protobuf/message.h>

#include "kudu/fs/block_id.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/strtoint.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env_util.h"
#include "kudu/util/errno.h"
#include "kudu/util/flags.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"

DEFINE_bool(enable_data_block_fsync, true,
            "Whether to enable fsync() of data blocks, metadata, and their parent directories. "
            "Disabling this flag may cause data loss in the event of a system crash.");
TAG_FLAG(enable_data_block_fsync, unsafe);

#if defined(__linux__)
DEFINE_string(block_manager, "log", "Which block manager to use for storage. "
              "Valid options are 'file' and 'log'.");
#else
DEFINE_string(block_manager, "file", "Which block manager to use for storage. "
              "Only the file block manager is supported for non-Linux systems.");
#endif
TAG_FLAG(block_manager, advanced);

DEFINE_string(fs_wal_dir, "",
              "Directory with write-ahead logs. If this is not specified, the "
              "program will not start. May be the same as fs_data_dirs");
TAG_FLAG(fs_wal_dir, stable);
DEFINE_string(fs_data_dirs, "",
              "Comma-separated list of directories with data blocks. If this "
              "is not specified, fs_wal_dir will be used as the sole data "
              "block directory.");
TAG_FLAG(fs_data_dirs, stable);

DEFINE_bool(fs_stripe_metadata, false, "Whether or not to spread tablet "
            "metadata across data dirs. If false, metadata will be stored in "
            "the WAL dir.");
TAG_FLAG(fs_stripe_metadata, experimental);

using kudu::env_util::ScopedFileDeleter;
using kudu::fs::BlockManagerOptions;
using kudu::fs::CreateBlockOptions;
using kudu::fs::DataDirManager;
using kudu::fs::FsErrorManager;
using kudu::fs::FileBlockManager;
using kudu::fs::FsReport;
using kudu::fs::LogBlockManager;
using kudu::fs::ReadableBlock;
using kudu::fs::WritableBlock;
using std::map;
using std::stack;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

// ==========================================================================
//  FS Paths
// ==========================================================================
const char *FsManager::kWalDirName = "wals";
const char *FsManager::kWalFileNamePrefix = "wal";
const char *FsManager::kWalsRecoveryDirSuffix = ".recovery";
const char *FsManager::kTabletMetadataDirName = "tablet-meta";
const char *FsManager::kDataDirName = "data";
const char *FsManager::kCorruptedSuffix = ".corrupted";
const char *FsManager::kInstanceMetadataFileName = "instance";
const char *FsManager::kConsensusMetadataDirName = "consensus-meta";

FsManagerOpts::FsManagerOpts()
  : wal_path(FLAGS_fs_wal_dir),
    read_only(false) {
  data_paths = strings::Split(FLAGS_fs_data_dirs, ",", strings::SkipEmpty());
}

FsManagerOpts::~FsManagerOpts() {
}

FsManager::FsManager(Env* env, const string& root_path)
  : env_(DCHECK_NOTNULL(env)),
    read_only_(false),
    wal_fs_root_(root_path),
    data_fs_roots_({ root_path }),
    metric_entity_(nullptr),
    error_manager_(new FsErrorManager()),
    initted_(false) {
}

FsManager::FsManager(Env* env,
                     const FsManagerOpts& opts)
  : env_(DCHECK_NOTNULL(env)),
    read_only_(opts.read_only),
    wal_fs_root_(opts.wal_path),
    data_fs_roots_(opts.data_paths),
    metric_entity_(opts.metric_entity),
    parent_mem_tracker_(opts.parent_mem_tracker),
    error_manager_(new FsErrorManager()),
    initted_(false) {
}

FsManager::~FsManager() {
}

void FsManager::SetErrorNotificationCb(fs::ErrorNotificationCb cb) {
  error_manager_->SetErrorNotificationCb(std::move(cb));
}

void FsManager::UnsetErrorNotificationCb() {
  error_manager_->UnsetErrorNotificationCb();
}

Status FsManager::SanitizePath(const string& path) {
  if (path.empty()) {
    return Status::IOError("Empty string provided for path");
  }
  if (path[0] != '/') {
    return Status::IOError(
        Substitute("Relative path $0 provided", path));
  }
  string path_copy = path;
  StripWhiteSpace(&path_copy);
  if (path_copy.size() != path.size()) {
    return Status::IOError(
        Substitute("Path $0 contains illegal whitespace", path));
  }
  return Status::OK();
}

Status FsManager::Init() {
  if (initted_) {
    return Status::OK();
  }

  // Initialize the directory manager. This sanitizes and canonicalizes the
  // data roots.
  RETURN_NOT_OK(InitDataDirManager());
  if (!data_fs_roots_.empty()) {
    // Sanitize and canonicalize the wal root.
    string canonicalized;
    RETURN_NOT_OK_PREPEND(FsManager::SanitizePath(wal_fs_root_),
        "Failed to sanitize Write-Ahead-Log directory (fs_wal_dir)");
    RETURN_NOT_OK_PREPEND(env_->Canonicalize(DirName(wal_fs_root_), &canonicalized),
        Substitute("Failed to canonicalize $0", DirName(wal_fs_root_)));
    canonicalized_wal_fs_root_ = JoinPathSegments(canonicalized, BaseName(wal_fs_root_));
  } else {
    DCHECK_EQ(1, dd_manager_->GetDataRootDirs().size());
    canonicalized_wal_fs_root_ = DirName(dd_manager_->GetDataRootDirs()[0]);
  }
  canonicalized_all_fs_roots_.insert(canonicalized_wal_fs_root_);
  canonicalized_metadata_fs_root_ = DirName(dd_manager_->GetDataRootDirs()[0]);
  for (const string& data_root : dd_manager_->GetDataRootDirs()) {
    canonicalized_data_fs_roots_.insert(DirName(data_root));
    canonicalized_all_fs_roots_.insert(DirName(data_root));
  }
  if (FLAGS_fs_stripe_metadata) {
    canonicalized_metadata_fs_root_ = "";
  } else {
    canonicalized_metadata_fs_root_ = canonicalized_wal_fs_root_;
  }

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "WAL root: " << canonicalized_wal_fs_root_;
    if (!canonicalized_metadata_fs_root_.empty()) {
      VLOG(1) << "Metadata root: " << canonicalized_metadata_fs_root_;
    } else {
      VLOG(1) << "Metadata will be striped accross data roots.";
    }
    VLOG(1) << "Data roots: " << canonicalized_data_fs_roots_;
    VLOG(1) << "All roots: " << canonicalized_all_fs_roots_;
  }

  // With the data dir manager initialized, we can initialize the block manager.
  InitBlockManager();

  initted_ = true;
  return Status::OK();
}

// TODO(awong): consider renaming to InitStorageManagers() and reusing the FsManagerOpts.
void FsManager::InitBlockManager() {
  BlockManagerOptions opts;
  opts.metric_entity = metric_entity_;
  opts.parent_mem_tracker = parent_mem_tracker_;
  opts.read_only = read_only_;
  if (FLAGS_block_manager == "file") {
    block_manager_.reset(new FileBlockManager(env_, dd_manager_.get(), error_manager_.get(), opts));
  } else if (FLAGS_block_manager == "log") {
    block_manager_.reset(new LogBlockManager(env_, dd_manager_.get(), error_manager_.get(), opts));
  } else {
    LOG(FATAL) << "Invalid block manager: " << FLAGS_block_manager;
  }
}

Status FsManager::InitDataDirManager() {
  vector<string> data_roots = data_fs_roots_;
  if (data_roots.empty()) {
    LOG(INFO) << "Data directories (fs_data_dirs) not provided";
    LOG(INFO) << "Using write-ahead log directory (fs_wal_dir) as data directory";
    data_roots = { wal_fs_root_ };
  }
  dd_manager_.reset(new DataDirManager(env_, metric_entity_, FLAGS_block_manager, data_roots,
      read_only_ ? DataDirManager::AccessMode::READ_ONLY : DataDirManager::AccessMode::READ_WRITE));
  return dd_manager_->Init();
}

Status FsManager::Open(FsReport* report) {
  RETURN_NOT_OK(Init());

  // Load and verify the instance metadata files.
  //
  // Done first to minimize side effects in the case that the configured roots
  // are not yet initialized on disk.
  for (const string& root : canonicalized_all_fs_roots_) {
    gscoped_ptr<InstanceMetadataPB> pb(new InstanceMetadataPB);
    Status s = pb_util::ReadPBContainerFromPath(env_, GetInstanceMetadataPath(root), pb.get());
    if (PREDICT_FALSE(!s.ok())) {
      if (s.IsDiskFailure()) {
        // Disk failure should be handled when creating the in-memory DataDirs.
        LOG(ERROR) << "Failed to read metadata file from " << root;
        continue;
      }
      return s;
    }
    if (!metadata_) {
      metadata_.reset(pb.release());
    } else if (pb->uuid() != metadata_->uuid()) {
      return Status::Corruption(Substitute(
          "Mismatched UUIDs across filesystem roots: $0 vs. $1",
          metadata_->uuid(), pb->uuid()));
    }
  }

  // Remove leftover temporary files from the WAL root and fix permissions.
  //
  // Temporary files in the data directory roots will be removed by the block
  // manager.
  if (!read_only_) {
    // Disk failures here can be ignored with just a warning. They will be
    // handled in BlockManager::Open().
    CleanTmpFiles();
    CheckAndFixPermissions();
  }

  LOG_TIMING(INFO, "opening directory manager") {
    RETURN_NOT_OK(dd_manager_->Open());
  }
  LOG_TIMING(INFO, "opening block manager") {
    RETURN_NOT_OK(block_manager_->Open(report));
  }
  LOG(INFO) << "Opened local filesystem: " << JoinStrings(canonicalized_all_fs_roots_, ",")
            << std::endl << SecureDebugString(*metadata_);
  return Status::OK();
}

Status FsManager::CreateInitialFileSystemLayout(boost::optional<string> uuid) {
  CHECK(!read_only_);

  RETURN_NOT_OK(Init());

  // It's OK if a root already exists as long as there's nothing in it.
  for (const string& root : canonicalized_all_fs_roots_) {
    if (!env_->FileExists(root)) {
      // We'll create the directory below.
      continue;
    }
    bool is_empty;
    RETURN_NOT_OK_PREPEND(IsDirectoryEmpty(root, &is_empty),
                          "Unable to check if FSManager root is empty");
    if (!is_empty) {
      return Status::AlreadyPresent("FSManager root is not empty", root);
    }
  }

  // All roots are either empty or non-existent. Create missing roots and all
  // subdirectories.
  //
  // In the event of failure, delete everything we created.
  deque<ScopedFileDeleter*> delete_on_failure;
  ElementDeleter d(&delete_on_failure);

  InstanceMetadataPB metadata;
  RETURN_NOT_OK(CreateInstanceMetadata(std::move(uuid), &metadata));
  unordered_set<string> to_sync;
  for (const string& root : canonicalized_all_fs_roots_) {
    bool created;
    Status s = CreateDirIfMissing(root, &created);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Unable to create FSManager root: " << s.ToString();
      if (s.IsDiskFailure()) {
        continue;
      }
      return s;
    }
    if (created) {
      delete_on_failure.push_front(new ScopedFileDeleter(env_, root));
      to_sync.insert(DirName(root));
    }
    s = WriteInstanceMetadata(metadata, root);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Unable to write instance metadata" << s.ToString();
      if (s.IsDiskFailure()) {
        continue;
      }
    }
    delete_on_failure.push_front(new ScopedFileDeleter(
        env_, GetInstanceMetadataPath(root)));
  }

  // Initialize ancillary directories.
  vector<string> ancillary_dirs = { GetWalsRootDir(),
                                    GetConsensusMetadataDir() };
  if (!FLAGS_fs_stripe_metadata) {
    ancillary_dirs.emplace_back(GetTabletMetadataDir());
  }
  for (const string& dir : ancillary_dirs) {
    bool created;
    RETURN_NOT_OK_PREPEND(CreateDirIfMissing(dir, &created),
                          Substitute("Unable to create directory $0", dir));
    if (created) {
      delete_on_failure.push_front(new ScopedFileDeleter(env_, dir));
      to_sync.insert(DirName(dir));
    }
  }

  // Ensure newly created directories are synchronized to disk.
  if (FLAGS_enable_data_block_fsync) {
    for (const string& dir : to_sync) {
      Status s = env_->SyncDir(dir);
      if (PREDICT_FALSE(!s.ok())) {
        LOG(ERROR) << Substitute("Unable to synchronize directory $0: $1", dir, s.ToString());
        // Non-disk-failures and WAL or TabletMetadata dirs failures are fatal.
        if (!s.IsDiskFailure() ||
            DirName(GetWalsRootDir()) == dir ||
            DirName(GetTabletMetadataDir()) == dir) {
          return s;
        }
      }
    }
  }

  // And lastly, the directory manager.
  RETURN_NOT_OK_PREPEND(dd_manager_->Create(),
      "Unable to create directory manager");

  // Success: don't delete any files.
  for (ScopedFileDeleter* deleter : delete_on_failure) {
    deleter->Cancel();
  }
  return Status::OK();
}

Status FsManager::CreateInstanceMetadata(boost::optional<string> uuid,
                                         InstanceMetadataPB* metadata) {
  ObjectIdGenerator oid_generator;
  if (uuid) {
    string canonicalized_uuid;
    RETURN_NOT_OK(oid_generator.Canonicalize(uuid.get(), &canonicalized_uuid));
    metadata->set_uuid(canonicalized_uuid);
  } else {
    metadata->set_uuid(oid_generator.Next());
  }

  string time_str;
  StringAppendStrftime(&time_str, "%Y-%m-%d %H:%M:%S", time(nullptr), false);
  string hostname;
  if (!GetHostname(&hostname).ok()) {
    hostname = "<unknown host>";
  }
  metadata->set_format_stamp(Substitute("Formatted at $0 on $1", time_str, hostname));
  return Status::OK();
}

Status FsManager::WriteInstanceMetadata(const InstanceMetadataPB& metadata,
                                        const string& root) {
  const string path = GetInstanceMetadataPath(root);

  // The instance metadata is written effectively once per TS, so the
  // durability cost is negligible.
  RETURN_NOT_OK(pb_util::WritePBContainerToPath(env_, path,
                                                metadata,
                                                pb_util::NO_OVERWRITE,
                                                pb_util::SYNC));
  LOG(INFO) << "Generated new instance metadata in path " << path << ":\n"
            << SecureDebugString(metadata);
  return Status::OK();
}

Status FsManager::IsDirectoryEmpty(const string& path, bool* is_empty) {
  vector<string> children;
  RETURN_NOT_OK(env_->GetChildren(path, &children));
  for (const string& child : children) {
    if (child == "." || child == "..") {
      continue;
    } else {
      *is_empty = false;
      return Status::OK();
    }
  }
  *is_empty = true;
  return Status::OK();
}

Status FsManager::CreateDirIfMissing(const string& path, bool* created) {
  return env_util::CreateDirIfMissing(env_, path, created);
}

const string& FsManager::uuid() const {
  return CHECK_NOTNULL(metadata_.get())->uuid();
}

vector<string> FsManager::GetDataRootDirs() const {
  // Add the data subdirectory to each data root.
  return dd_manager_->GetDataRootDirs();
}

string FsManager::GetTabletMetadataDir() const {
  DCHECK(initted_);
  if (FLAGS_fs_stripe_metadata) {
    return "";
  }
  CHECK(!canonicalized_metadata_fs_root_.empty());
  return JoinPathSegments(canonicalized_metadata_fs_root_, kTabletMetadataDirName);
}

string FsManager::GetTabletMetadataPath(const string& tablet_id) const {
  string metadata_dir;
  if (FLAGS_fs_stripe_metadata) {
    metadata_dir = JoinPathSegments(dd_manager_->GetTabletMetadataDir(tablet_id),
                                    kTabletMetadataDirName);
  } else {
    metadata_dir = GetTabletMetadataDir();
    CHECK(!metadata_dir.empty());
  }
  return JoinPathSegments(metadata_dir, tablet_id);
}

namespace {
// Return true if 'fname' is a valid tablet ID.
bool IsValidTabletId(const string& fname) {
  if (fname.find(kTmpInfix) != string::npos ||
      fname.find(kOldTmpInfix) != string::npos) {
    LOG(WARNING) << "Ignoring tmp file in tablet metadata dir: " << fname;
    return false;
  }

  if (HasPrefixString(fname, ".")) {
    // Hidden file or ./..
    VLOG(1) << "Ignoring hidden file in tablet metadata dir: " << fname;
    return false;
  }

  return true;
}
} // anonymous namespace

TabletsByDirMap FsManager::ListTabletIdsPerDir() const {
  TabletsByDirMap tablet_ids_per_dir;
  for (const string& dir : canonicalized_all_fs_roots_) {
    vector<string> tablet_ids;
    string metadata_path = JoinPathSegments(dir, kTabletMetadataDirName);
    bool is_dir = false;
    Status s = env_->IsDirectory(metadata_path, &is_dir);
    if (!is_dir || !s.ok()) {
      continue;
    }
    vector<string> children;
    s = ListDir(metadata_path, &children);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("Couldn't list files in $0", metadata_path);
    }
    for (string& child : children) {
      if (!IsValidTabletId(child)) {
        continue;
      }
      tablet_ids.emplace_back(std::move(child));
    }
    if (!tablet_ids.empty()) {
      InsertOrDie(&tablet_ids_per_dir, metadata_path, tablet_ids);
    }
  }
  return tablet_ids_per_dir;
}

Status FsManager::ListTabletIds(vector<string>* tablet_ids) const {
  // TODO(awong): Change to search every directory for a tablet metadata.
  //
  // /data-0/kTabletMetadataDirName
  // /data-1/kTabletMetadataDirName
  // /data-2/kTabletMetadataDirName
  //
  // Flag to specify the metadata dirs.
  string dir = GetTabletMetadataDir();
  vector<string> children;
  RETURN_NOT_OK_PREPEND(ListDir(dir, &children),
                        Substitute("Couldn't list tablets in metadata directory $0", dir));

  vector<string> tablets;
  for (const string& child : children) {
    if (!IsValidTabletId(child)) {
      continue;
    }
    tablet_ids->push_back(child);
  }
  return Status::OK();
}

string FsManager::GetInstanceMetadataPath(const string& root) const {
  return JoinPathSegments(root, kInstanceMetadataFileName);
}

string FsManager::GetTabletWalRecoveryDir(const string& tablet_id) const {
  string path = JoinPathSegments(GetWalsRootDir(), tablet_id);
  StrAppend(&path, kWalsRecoveryDirSuffix);
  return path;
}

string FsManager::GetWalSegmentFileName(const string& tablet_id,
                                        uint64_t sequence_number) const {
  return JoinPathSegments(GetTabletWalDir(tablet_id),
                          strings::Substitute("$0-$1",
                                              kWalFileNamePrefix,
                                              StringPrintf("%09" PRIu64, sequence_number)));
}

void FsManager::CleanTmpFiles() {
  DCHECK(!read_only_);
  for (const auto& s : { GetWalsRootDir(),
                         GetTabletMetadataDir(),
                         GetConsensusMetadataDir() }) {
    WARN_NOT_OK(env_util::DeleteTmpFilesRecursively(env_, s),
                Substitute("Error deleting tmp files in $0", s));
  }
}

void FsManager::CheckAndFixPermissions() {
  for (const string& root : canonicalized_all_fs_roots_) {
    WARN_NOT_OK(env_->EnsureFileModeAdheresToUmask(root),
                Substitute("could not check and fix permissions for path: $0",
                           root));
  }
}

// ==========================================================================
//  Dump/Debug utils
// ==========================================================================

void FsManager::DumpFileSystemTree(ostream& out) {
  DCHECK(initted_);

  for (const string& root : canonicalized_all_fs_roots_) {
    out << "File-System Root: " << root << std::endl;

    vector<string> objects;
    Status s = env_->GetChildren(root, &objects);
    if (!s.ok()) {
      LOG(ERROR) << "Unable to list the fs-tree: " << s.ToString();
      return;
    }

    DumpFileSystemTree(out, "|-", root, objects);
  }
}

void FsManager::DumpFileSystemTree(ostream& out, const string& prefix,
                                   const string& path, const vector<string>& objects) {
  for (const string& name : objects) {
    if (name == "." || name == "..") continue;

    vector<string> sub_objects;
    string sub_path = JoinPathSegments(path, name);
    Status s = env_->GetChildren(sub_path, &sub_objects);
    if (s.ok()) {
      out << prefix << name << "/" << std::endl;
      DumpFileSystemTree(out, prefix + "---", sub_path, sub_objects);
    } else {
      out << prefix << name << std::endl;
    }
  }
}

// ==========================================================================
//  Data read/write interfaces
// ==========================================================================

Status FsManager::CreateNewBlock(const CreateBlockOptions& opts, unique_ptr<WritableBlock>* block) {
  CHECK(!read_only_);

  return block_manager_->CreateBlock(opts, block);
}

Status FsManager::OpenBlock(const BlockId& block_id, unique_ptr<ReadableBlock>* block) {
  return block_manager_->OpenBlock(block_id, block);
}

Status FsManager::DeleteBlock(const BlockId& block_id) {
  CHECK(!read_only_);

  return block_manager_->DeleteBlock(block_id);
}

bool FsManager::BlockExists(const BlockId& block_id) const {
  unique_ptr<ReadableBlock> block;
  return block_manager_->OpenBlock(block_id, &block).ok();
}

std::ostream& operator<<(std::ostream& o, const BlockId& block_id) {
  return o << block_id.ToString();
}

} // namespace kudu
