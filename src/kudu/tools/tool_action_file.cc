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

#include "kudu/tools/tool_action.h"

#include <cstdlib>
#include <exception>
#include <fstream>  // IWYU pragma: keep
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/status.h>
#include <google/protobuf/stubs/stringpiece.h>
#include <google/protobuf/util/json_util.h>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/fs/block_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/array_view.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

namespace kudu {
namespace tools {

using cfile::CFileIterator;
using cfile::CFileReader;
using cfile::ReaderOptions;
using fs::ReadableBlock;
using fs::WritableBlock;
using fs::BlockCreationTransaction;
using std::cout;
using std::endl;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace {

const char *const kPathArg = "path";
const char *const kReadCFileArg = "read_path";
const char *const kWriteCFileArg = "write_path";

class DirectBlockCreationTransaction : public BlockCreationTransaction {
 public:
  virtual void AddCreatedBlock(unique_ptr<WritableBlock> block) override {
    created_blocks_.emplace_back(std::move(block));
  }

  virtual Status CommitCreatedBlocks() override {
    for (const auto& block : created_blocks_) {
      RETURN_NOT_OK(block->Close());
    }
    return Status::OK();
  }
 private:
  vector<unique_ptr<WritableBlock>> created_blocks_;
};


class DirectWritableBlock : public WritableBlock {
 public:
  DirectWritableBlock(unique_ptr<WritableFile> writer)
    : writer_(std::move(writer)),
      state_(CLEAN),
      id_(BlockId()),
      bytes_appended_(0) {}

  virtual Status Close() override { return Status::OK(); }

  virtual Status Abort() override { return Status::OK(); }

  virtual fs::BlockManager* block_manager() const override { return nullptr; }

  virtual const BlockId& id() const override { return id_; }

  virtual Status Append(const Slice& data) override {
    return AppendV(ArrayView<const Slice>(&data, 1));
  }

  virtual Status AppendV(ArrayView<const Slice> data) override {
    RETURN_NOT_OK(writer_->AppendV(data));
    return Status::OK();
  }

  virtual Status Finalize() override { return Status::OK(); }

  virtual size_t BytesAppended() const override { return 0; }

  virtual State state() const override { return state_; }

  virtual void NewCreationTransaction(
      unique_ptr<BlockCreationTransaction>* transaction) const override {
    transaction->reset(new DirectBlockCreationTransaction());
  }

 private:
  unique_ptr<WritableFile> writer_;
  State state_;
  BlockId id_;
  size_t bytes_appended_;

  DISALLOW_COPY_AND_ASSIGN(DirectWritableBlock);
};

////////////////////////////////////////////////////////////
// FileReadableBlock
////////////////////////////////////////////////////////////

// A file-backed block that has been opened for reading directly from a path.
class DirectFileReadableBlock : public ReadableBlock {
 public:
  DirectFileReadableBlock(shared_ptr<RandomAccessFile> reader);

  virtual ~DirectFileReadableBlock();

  virtual Status Close() override;

  virtual const BlockId &id() const override;

  virtual Status Size(uint64_t *sz) const override;

  virtual Status Read(uint64_t offset, Slice result) const override;

  virtual Status
  ReadV(uint64_t offset, ArrayView<Slice> results) const override;

  virtual size_t memory_footprint() const override;

  void HandleError(const Status &s) const;

 private:
  // The underlying opened file backing this block.
  shared_ptr<RandomAccessFile> reader_;

  const BlockId block_id_ = BlockId();

  DISALLOW_COPY_AND_ASSIGN(DirectFileReadableBlock);
};

void DirectFileReadableBlock::HandleError(const Status &s) const {
  // no-op
}

DirectFileReadableBlock::DirectFileReadableBlock(
    shared_ptr<RandomAccessFile> reader)
    : reader_(std::move(reader)) {}

DirectFileReadableBlock::~DirectFileReadableBlock() {
  WARN_NOT_OK(Close(), Substitute("Failed to close block $0",
                                  id().ToString()));
}

Status DirectFileReadableBlock::Close() {
  return Status::OK();
}

const BlockId &DirectFileReadableBlock::id() const {
  return block_id_;
}

Status DirectFileReadableBlock::Size(uint64_t *sz) const {
  RETURN_NOT_OK(reader_->Size(sz));
  return Status::OK();
}

Status DirectFileReadableBlock::Read(uint64_t offset, Slice result) const {
  return ReadV(offset, ArrayView<Slice>(&result, 1));
}

Status DirectFileReadableBlock::ReadV(uint64_t offset,
                                      ArrayView<Slice> results) const {
  RETURN_NOT_OK(reader_->ReadV(offset, results));
  return Status::OK();
}

size_t DirectFileReadableBlock::memory_footprint() const {
  return reader_->memory_footprint();
}

Status ReadCFile(const RunnerContext &context) {
  const string &path = FindOrDie(context.required_args, kPathArg);

  Env *env = Env::Default();
  unique_ptr<RandomAccessFile> raf;
  env->NewRandomAccessFile(path, &raf);

  unique_ptr<fs::ReadableBlock> block;
  block.reset(new DirectFileReadableBlock(std::move(raf)));

  unique_ptr<CFileReader> cfile;
  RETURN_NOT_OK(CFileReader::Open(std::move(block), ReaderOptions(), &cfile));

  cout << "Header:\n" << pb_util::SecureDebugString(cfile->header()) << endl;
  cout << "Footer:\n" << pb_util::SecureDebugString(cfile->footer()) << endl;

  gscoped_ptr<CFileIterator> it;
  RETURN_NOT_OK(cfile->NewIterator(&it, CFileReader::DONT_CACHE_BLOCK));
  RETURN_NOT_OK(it->SeekToFirst());
  RETURN_NOT_OK(DumpIterator(*cfile, it.get(), &cout, 0, 0));

  return Status::OK();
}

// From a CFileReader, creates a CFileWriter.
Status RewriteCFile(const RunnerContext& context) {
  const string& read_path = FindOrDie(context.required_args, kReadCFileArg);
  Env* env = Env::Default();
  unique_ptr<RandomAccessFile> rfile;
  env->NewRandomAccessFile(read_path, &rfile);

  unique_ptr<fs::ReadableBlock> rblock;
  rblock.reset(new DirectFileReadableBlock(std::move(rfile)));

  ReaderOptions reader_opts;
  reader_opts.fill_corrupt_values = true;
  unique_ptr<CFileReader> cfile_reader;
  RETURN_NOT_OK(CFileReader::Open(std::move(rblock), reader_opts, &cfile_reader));

  CFileIterator* it_ptr;
  RETURN_NOT_OK(cfile_reader->NewIterator(&it_ptr, CFileReader::DONT_CACHE_BLOCK));
  unique_ptr<CFileIterator> it(it_ptr);
  // TODO(awong): If SeekToFirst() fails because of some corruption, but we
  // successfully read the CFile footer and know how many rows are in the
  // CFile, we could use a DefaultColumnValueIterator instead.
  RETURN_NOT_OK(it->SeekToFirst());

  // Create a bare WritableBlock file to write to.
  const string& write_path = FindOrDie(context.required_args, kWriteCFileArg);
  unique_ptr<WritableFile> wfile;
  env->NewWritableFile(write_path, &wfile);
  unique_ptr<WritableBlock> wblock;
  wblock.reset(new DirectWritableBlock(std::move(wfile)));

  unique_ptr<cfile::CFileWriter> cfile_writer;
  RETURN_NOT_OK(DumpToCFileWriter(*cfile_reader, it.get(), std::move(wblock), &cfile_writer));
  RETURN_NOT_OK(cfile_writer->Finish());
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildFileMode() {
  unique_ptr<Action> dump =
      ActionBuilder("read_cfile", &ReadCFile)
          .Description("Read a CFile")
          .AddRequiredParameter({kPathArg, "path to a CFile"})
          .AddOptionalParameter("cfile_verify_checksums")
          .Build();

  unique_ptr<Action> rewrite =
      ActionBuilder("rewrite_cfile", &RewriteCFile)
          .Description("Read a CFile")
          .AddRequiredParameter({kReadCFileArg, "path to a src CFile"})
          .AddRequiredParameter({kWriteCFileArg, "path to a dst CFile"})
          .Build();

  return ModeBuilder("file")
      .Description("Operate on Kudu files")
      .AddAction(std::move(dump))
      .AddAction(std::move(rewrite))
      .Build();
}

} // namespace tools
} // namespace kudu
