#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

DECLARE_double(env_inject_eio);
DECLARE_string(env_inject_eio_globs);
DECLARE_uint32(fs_target_data_dirs_per_tablet);
DECLARE_bool(suicide_on_eio);

namespace kudu {
namespace tserver {

using fs::CreateBlockOptions;
using fs::DataDir;

class TSDiskFailureTest : public TabletServerTestBase {
 public:
  virtual void SetUp() {
    TabletServerTestBase::SetUp();
    data_dirs_failed_ = METRIC_data_dirs_failed.Instantiate(
        mini_server_->server()->metric_entity(), 0);
  }

  void WaitForDiskFailures(int64_t target_failed_disks = 1) const {
    ASSERT_EVENTUALLY([&] {
      int64_t failed_on_ts = data_dirs_failed_->value();
      ASSERT_EQ(target_failed_disks, failed_on_ts);
    });
  }

 private:
  scoped_refptr<AtomicGauge<uint64_t>> data_dirs_failed_;
};

// Test disk failure during a write.
TEST_F(TSDiskFailureTest, TestFailDuringWrite) {
  NO_FATALS(StartTabletServer(3));
  InsertTestRowsDirect(0, 1);
  DataDir* dir_with_tablet;
  ASSERT_OK(mini_server_->server()->fs_manager()->dd_manager()->GetNextDataDir(
      CreateBlockOptions({ kTabletId }), &dir_with_tablet));
  FLAGS_suicide_on_eio = false;
  FLAGS_fs_target_data_dirs_per_tablet = 1;
  FLAGS_env_inject_eio = 1.0;
  FLAGS_env_inject_eio_globs = dir_with_tablet->dir();
  InsertTestRowsDirect(1, 100);
  WaitForDiskFailures();
}

// Test disk failure during a read.
TEST_F(TSDiskFailureTest, TestFailDuringRead) {
  StartTabletServer();
  InsertTestRowsDirect(0, 100);
  FLAGS_suicide_on_eio = false;
  FLAGS_env_inject_eio_globs = "";
}

// Test disk failure during compaction.

// Test disk failure while opening tablets.

// Test disk failure while lading path instance.

}  // tserver
}  // kudu
