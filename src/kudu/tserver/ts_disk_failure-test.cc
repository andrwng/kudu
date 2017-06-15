#include <set>
#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

DECLARE_string(block_manager);
DECLARE_double(env_inject_eio);
DECLARE_string(env_inject_eio_globs);
DECLARE_uint32(fs_target_data_dirs_per_tablet);
DECLARE_bool(suicide_on_eio);

namespace kudu {
namespace tserver {

using std::set;
using std::string;
using strings::Substitute;
using fs::CreateBlockOptions;
using fs::DataDir;
using fs::DataDirManager;
using rpc::RpcController;
using tablet::Tablet;
using tablet::TabletReplica;
using tablet::TabletStatePB;

class TSDiskFailureTest : public TabletServerTestBase {
 public:
  virtual void SetUp() override {
    TabletServerTestBase::SetUp();
    FLAGS_fs_target_data_dirs_per_tablet = 1;
    NO_FATALS(StartTabletServer(3));
  }

  // Fails a single directory belonging to the tablet.
  void FailSingleTabletDirectory(const string& tablet_id = kTabletId) {
    DataDirManager* dd_manager = mini_server_->server()->fs_manager()->dd_manager();
    DataDir* dir_with_tablet;
    ASSERT_OK(dd_manager->GetNextDataDir(CreateBlockOptions({ tablet_id }), &dir_with_tablet));
    FLAGS_suicide_on_eio = false;
    FLAGS_env_inject_eio = 1.0;
    FLAGS_env_inject_eio_globs = GlobForDir(dir_with_tablet);
  }

  string GlobForDir(const DataDir* dir) {
    if (FLAGS_block_manager == "file") {
      return JoinPathSegments(dir->dir(), Substitute("$0/**/$1", string(2, '?'), string(16, '?')));
    }
    vector<string> lbm_globs = { JoinPathSegments(dir->dir(), "*.data"),
                                 JoinPathSegments(dir->dir(), "*.metadata") };
    return JoinStrings(lbm_globs, ",");
  }
};

// Test disk failure during a write.
TEST_F(TSDiskFailureTest, TestFailDuringWrite) {
  InsertTestRowsDirect(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  FailSingleTabletDirectory();

  InsertTestRowsDirect(1, 100);
  Status s = tablet_replica_->tablet()->Flush();
  ASSERT_EQ(EIO, s.posix_code());
  ASSERT_STR_CONTAINS(s.ToString(), "INJECTED FAILURE");
}

// Test disk failure during a read.
TEST_F(TSDiskFailureTest, TestFailDuringRead) {
  InsertTestRowsDirect(0, 100);
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  FailSingleTabletDirectory();

  // TODO(awong): it would be nice to avoid getting an UNKNOWN_ERROR here.
  VerifyScanRequestFailure(schema_, TabletServerErrorPB::UNKNOWN_ERROR, "INJECTED FAILURE");
}

// Test disk failure during compaction.
TEST_F(TSDiskFailureTest, TestFailDuringCompaction) {
  // Flush a couple of times.
  InsertTestRowsDirect(0, 100);
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  InsertTestRowsDirect(101, 100);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  // Fail during a compaction.
  FailSingleTabletDirectory();
  Status s = tablet_replica_->tablet()->Compact(Tablet::FORCE_COMPACT_ALL);
  ASSERT_EQ(EIO, s.posix_code());
  ASSERT_STR_CONTAINS(s.ToString(), "INJECTED FAILURE");
}

// Test disk failure while opening tablets.
TEST_F(TSDiskFailureTest, TestFailureDuringTabletStartup) {
  // Put some initial data on the tablet.
  InsertTestRowsDirect(0, 100);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  DataDirManager* dd_manager = mini_server_->server()->fs_manager()->dd_manager();
  DataDir* dir_with_tablet;
  ASSERT_OK(dd_manager->GetNextDataDir(CreateBlockOptions({ kTabletId }), &dir_with_tablet));
  string glob_for_tablet = GlobForDir(dir_with_tablet);

  ShutdownTablet();

  // Fail the tablet's directories.
  FLAGS_suicide_on_eio = false;
  FLAGS_env_inject_eio = 1.0;
  FLAGS_env_inject_eio_globs = glob_for_tablet;
  mini_server_.reset(new MiniTabletServer(GetTestPath("TabletServerTest-fsroot"), 0, 3));
  mini_server_->options()->master_addresses.clear();
  mini_server_->options()->master_addresses.emplace_back("255.255.255.255", 1);
  Status s = mini_server_->Start();
  ASSERT_OK(s);
}

}  // tserver
}  // kudu
