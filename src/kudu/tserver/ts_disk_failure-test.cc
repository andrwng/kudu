#include <string>

#include <gtest/gtest.h>

#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/util/test_util.h"

DECLARE_bool(suicide_on_eio);
DECLARE_double(env_inject_eio);
DECLARE_string(env_inject_eio_globs);

namespace kudu {
namespace tserver {

using std::unique_ptr;

class TSDiskFailureTest : public TabletServerTestBase {
 public:
  TSDiskFailureTest();
};

// Test disk failure during a write.
TEST_F(TSDiskFailureTest, TestFailDuringWrite) {
  StartTabletServer();
  FLAGS_suicide_on_eio = false;
  FLAGS_env_inject_eio_globs = "";
  InsertTestRowsDirect(0, 100);
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
