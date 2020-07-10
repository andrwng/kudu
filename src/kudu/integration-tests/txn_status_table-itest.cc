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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_coordinator.h"
#include "kudu/transactions/txn_system_client.h"
#include "kudu/transactions/txn_status_manager.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::tablet::TabletReplica;
using kudu::tablet::TxnCoordinator;
using kudu::transactions::TxnStatusTablet;
using kudu::transactions::TxnStatusManager;
using kudu::transactions::TxnSystemClient;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

namespace {
string ParticipantId(int i) {
  return Substitute("strawhat-$0", i);
}
} // anonymous namespace

class TxnStatusTableITest : public KuduTest {
 public:
  TxnStatusTableITest() {}

  void SetUp() override {
    KuduTest::SetUp();
    cluster_.reset(new InternalMiniCluster(env_, {}));
    ASSERT_OK(cluster_->Start());

    // Create the txn system client with which to communicate with the cluster.
    vector<string> master_addrs;
    for (const auto& hp : cluster_->master_rpc_addrs()) {
      master_addrs.emplace_back(hp.ToString());
    }
    ASSERT_OK(TxnSystemClient::Create(master_addrs, &txn_sys_client_));
  }

  // Ensures that all replicas have the right table type set.
  void CheckTableTypes(const map<TableTypePB, int>& expected_counts) {
    // Check that the tablets of the table all have transaction coordinators.
    vector<scoped_refptr<TabletReplica>> replicas;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* ts = cluster_->mini_tablet_server(i);
      const auto& tablets = ts->ListTablets();
      for (const auto& t : tablets) {
        scoped_refptr<TabletReplica> r;
        ASSERT_TRUE(ts->server()->tablet_manager()->LookupTablet(t, &r));
        replicas.emplace_back(std::move(r));
      }
    }
    map<TableTypePB, int> type_counts;
    for (const auto& r : replicas) {
      const auto optional_table_type = r->tablet_metadata()->table_type();
      if (boost::none == optional_table_type) {
        LookupOrEmplace(&type_counts, TableTypePB::DEFAULT_TABLE, 0)++;
      } else {
        ASSERT_EQ(TableTypePB::TXN_STATUS_TABLE, *optional_table_type);
        ASSERT_NE(nullptr, r->txn_coordinator());
        LookupOrEmplace(&type_counts, *optional_table_type, 0)++;
      }
    }
    ASSERT_EQ(expected_counts, type_counts);
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<TxnSystemClient> txn_sys_client_;
};

// Test that DDL for the transaction status table results in the creation
// transaction status tablets.
TEST_F(TxnStatusTableITest, TestCreateTxnStatusTablePartitions) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  Status s = txn_sys_client_->CreateTxnStatusTable(100);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  NO_FATALS(CheckTableTypes({ { TableTypePB::TXN_STATUS_TABLE, 1 } }));

  // Now add more partitions and try again.
  ASSERT_OK(txn_sys_client_->AddTxnStatusTableRange(100, 200));
  s = txn_sys_client_->AddTxnStatusTableRange(100, 200);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  NO_FATALS(CheckTableTypes({ { TableTypePB::TXN_STATUS_TABLE, 2 } }));

  // Ensure we still create transaction status tablets even after the master is
  // restarted.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(txn_sys_client_->AddTxnStatusTableRange(200, 300));
  NO_FATALS(CheckTableTypes({ { TableTypePB::TXN_STATUS_TABLE, 3 } }));
}

// Test that tablet servers can host both transaction status tablets and
// regular tablets.
TEST_F(TxnStatusTableITest, TestTxnStatusTableColocatedWithTables) {
  TestWorkload w(cluster_.get());
  w.set_num_replicas(1);
  w.Setup();
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  NO_FATALS(CheckTableTypes({
      { TableTypePB::TXN_STATUS_TABLE, 1 },
      { TableTypePB::DEFAULT_TABLE, 1 }
  }));
}

TEST_F(TxnStatusTableITest, TestSystemClientFindTablets) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());
  ASSERT_OK(txn_sys_client_->BeginTransaction(1, "user"));

  // If we write out of range, we should see an error.
  Status s = txn_sys_client_->BeginTransaction(100, "user");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // Once we add a new range, we should be able to leverage it.
  ASSERT_OK(txn_sys_client_->AddTxnStatusTableRange(100, 200));
  ASSERT_OK(txn_sys_client_->BeginTransaction(100, "user"));
}

TEST_F(TxnStatusTableITest, TestSystemClientBeginTransactionErrors) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());
  ASSERT_OK(txn_sys_client_->BeginTransaction(1, "user"));

  // Trying to start another transaction with a used ID should yield an error.
  Status s = txn_sys_client_->BeginTransaction(1, "user");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "not higher than the highest ID");
}

TEST_F(TxnStatusTableITest, TestSystemClientRegisterParticipantErrors) {
  ASSERT_OK(txn_sys_client_->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client_->OpenTxnStatusTable());
  Status s = txn_sys_client_->RegisterParticipant(1, "participant", "user");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "transaction ID.*not found, current highest txn ID:.*");

  ASSERT_OK(txn_sys_client_->BeginTransaction(1, "user"));
  ASSERT_OK(txn_sys_client_->RegisterParticipant(1, ParticipantId(1), "user"));

  s = txn_sys_client_->RegisterParticipant(1, ParticipantId(2), "stranger");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

class ReplicatedTxnStatusTableITest : public TxnStatusTableITest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = 4;
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());

    vector<string> master_addrs;
    for (const auto& hp : cluster_->master_rpc_addrs()) {
      master_addrs.emplace_back(hp.ToString());
    }
    ASSERT_OK(TxnSystemClient::Create(master_addrs, &txn_sys_client_));
  }
};

// TODO
TEST_F(ReplicatedTxnStatusTableITest, TestSystemClientLeaderTransfer) {
}

TEST_F(ReplicatedTxnStatusTableITest, TestSystemClientFailedReplica) {
}

TEST_F(ReplicatedTxnStatusTableITest, TestSystemClientCrashedNodes) {
}

TEST_F(ReplicatedTxnStatusTableITest, TestSystemClientQueueOverflow) {
}

} // namespace itest
} // namespace kudu
