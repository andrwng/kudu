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

#include "kudu/ranger/mini_ranger.h"

#include <cstdlib>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/path_util.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace ranger {

using std::string;
using strings::Substitute;

Status MiniRanger::Start() {
  if (data_root_.empty()) {
    data_root_ = GetTestDataDirectory();
  }
  RETURN_NOT_OK(StartPostgres());
  return StartRanger();
}

Status MiniRanger::Stop() {
  RETURN_NOT_OK(StopRanger());
  return StopPostgres();
}

Status MiniRanger::StartPostgres() {
  CHECK(!postgres_process_);

  VLOG(1) << "Starting Postgres";

  Env* env = Env::Default();

  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  const string bin_dir = DirName(exe);

  string postgres_root = JoinPathSegments(data_root_, "postgres");

  if (!env->FileExists(postgres_root)) {
    Subprocess* initdb = new Subprocess({
        JoinPathSegments(bin_dir, "postgres/initdb"),
        "-D", postgres_root});
    RETURN_NOT_OK(initdb->Start());
    RETURN_NOT_OK(initdb->Wait());

    RETURN_NOT_OK(CreatePostgresConfigs(postgres_root));
  }

  postgres_process_.reset(new Subprocess({
        JoinPathSegments(bin_dir, "postgres/postgres"),
        "-D", postgres_root}));
  RETURN_NOT_OK(postgres_process_->Start());
  string ip = "127.0.0.1";
  Status wait = WaitForTcpBind(postgres_process_->pid(), &postgres_port_, ip, 
                               MonoDelta::FromMilliseconds(10000));
  if (!wait.ok()) {
    WARN_NOT_OK(postgres_process_->Kill(SIGINT), "failed to send SIGQUIT to Postgres");
  }
  LOG(INFO) << "Postgres bound to " << postgres_port_;

  return wait;
}

Status MiniRanger::StartRanger() {
  Env* env = Env::Default();
  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  const string bin_dir = DirName(exe);

  RETURN_NOT_OK(FindHomeDir("hadoop", bin_dir, &hadoop_home_));
  RETURN_NOT_OK(FindHomeDir("ranger", bin_dir, &ranger_home_));
  RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home_));

  string ranger_root = JoinPathSegments(data_root_, "ranger");

  if (!env->FileExists(ranger_root)) {
    env->CreateDir(ranger_root);
  }

  RETURN_NOT_OK(CreateRangerConfigs(ranger_root));

  setenv("RANGER_ADMIN_CONF", ranger_root.c_str(), 1);
  Subprocess* ranger_init = new Subprocess({JoinPathSegments(ranger_home_, "setup.sh")});
  RETURN_NOT_OK(ranger_init->Start());
  RETURN_NOT_OK(ranger_init->Wait());

  setenv("XAPOLICYMGR_DIR", ranger_home_.c_str(), 1);

  string classpath =
    Substitute("$0/webapp/WEB-INF/classes/conf:$0/lib/*:$1/lib/*:$2/*:$$CLASSPATH",
               JoinPathSegments(ranger_home_, "ews"), java_home_, hadoop_home_);

  LOG(INFO) << classpath;

  string fqdn;
  RETURN_NOT_OK(GetFQDN(&fqdn));

  LOG(INFO) << fqdn;

  Subprocess* ranger_start = new Subprocess({
      "java", "-Dproc_rangeradmin",
      Substitute("-Dlog4j.configuration=file:$0",
                 JoinPathSegments(ranger_home_,
                                  "ews/webapp/WEB-INF/log4j.properties")),
      "-Duser=${USER}", "-Dranger.service.host=" + fqdn, "-Dservername=rangeradmin",
      Substitute("-Dlogdir=$0", JoinPathSegments(ranger_home_, "logs")),
      "-Dranger.audit.solr.bootstrap.enabled=false",
      "-cp", classpath, "org.apache.ranger.server.tomcat.EmbeddedServer"
  });
  RETURN_NOT_OK(ranger_start->Start());
  RETURN_NOT_OK(ranger_start->Wait());

  return Status::OK();
}

Status MiniRanger::CreatePostgresConfigs(string data_root) {
  Env* env = Env::Default();

  static const string kFileTemplate = R"(
max_connections = 100                   # (change requires restart)
shared_buffers = 128MB                  # min 128kB
dynamic_shared_memory_type = posix      # the default is the first option
max_wal_size = 1GB
min_wal_size = 80MB
datestyle = 'iso, mdy'
lc_messages = 'en_US.UTF-8'                     # locale for system error message
lc_monetary = 'en_US.UTF-8'                     # locale for monetary formatting
lc_numeric = 'en_US.UTF-8'                      # locale for number formatting
lc_time = 'en_US.UTF-8'                         # locale for time formatting
default_text_search_config = 'pg_catalog.english'
port=$0
  )";

  std::unique_ptr<WritableFile> file;
  env->NewWritableFile(JoinPathSegments(data_root, "postgresql.conf"), &file);
  file->Append(Substitute(kFileTemplate, GetRandomPort()));
  file->Flush(WritableFile::FlushMode::FLUSH_SYNC);
  file->Close();

  return Status::OK();
}

Status MiniRanger::CreateRangerConfigs(string data_root) {
  Env* env = Env::Default();

  static const string kFileTemplate = R"(
PYTHON_COMMAND_INVOKER=python

DB_FLAVOR=POSTGRES
SQL_CONNECTOR_JAR=$0/postgresql.jar
db_root_user=postgres
db_host=localhost:$1
db_root_password=
db_ssl_enabled=false
db_ssl_required=false
db_ssl_verifyServerCertificate=false
db_name=ranger
db_user=rangeradmin
db_password=
rangerAdmin_password=miniranger
rangerTagsync_password=miniranger
rangerUsersync_password=miniranger
keyadmin_password=miniranger
policymgr_external_url=http://localhost:$2
policymgr_http_enabled=true
policymgr_https_keystore_file=
policymgr_https_keystore_keyalias=rangeradmin
policymgr_https_keystore_password=
policymgr_supportedcomponents=kudu
#
# ------- PolicyManager CONFIG - END ---------------
#


#
# ------- UNIX User CONFIG ----------------
#
unix_user=ranger
unix_user_pwd=ranger
unix_group=ranger

#
# ------- UNIX User CONFIG  - END ----------------
#
#

#
# UNIX authentication service for Policy Manager
#
# PolicyManager can authenticate using UNIX username/password
# The UNIX server specified here as authServiceHostName needs to be installed with ranger-unix-ugsync package.
# Once the service is installed on authServiceHostName, the UNIX username/password from the host <authServiceHostName> can be used to login into policy manager
#
# ** The installation of ranger-unix-ugsync package can be installed after the policymanager installation is finished.
#
#LDAP|ACTIVE_DIRECTORY|UNIX|NONE
authentication_method=NONE
remoteLoginEnabled=true
authServiceHostName=localhost
authServicePort=5151
ranger_unixauth_keystore=keystore.jks
ranger_unixauth_keystore_password=password
ranger_unixauth_truststore=cacerts
ranger_unixauth_truststore_password=changeit

####LDAP settings - Required only if have selected LDAP authentication ####
#
# Sample Settings
#
#xa_ldap_url=ldap://127.0.0.1:389
#xa_ldap_userDNpattern=uid={0},ou=users,dc=xasecure,dc=net
#xa_ldap_groupSearchBase=ou=groups,dc=xasecure,dc=net
#xa_ldap_groupSearchFilter=(member=uid={0},ou=users,dc=xasecure,dc=net)
#xa_ldap_groupRoleAttribute=cn
#xa_ldap_base_dn=dc=xasecure,dc=net
#xa_ldap_bind_dn=cn=admin,ou=users,dc=xasecure,dc=net
#xa_ldap_bind_password=
#xa_ldap_referral=follow|ignore
#xa_ldap_userSearchFilter=(uid={0})

xa_ldap_url=
xa_ldap_userDNpattern=
xa_ldap_groupSearchBase=
xa_ldap_groupSearchFilter=
xa_ldap_groupRoleAttribute=
xa_ldap_base_dn=
xa_ldap_bind_dn=
xa_ldap_bind_password=
xa_ldap_referral=
xa_ldap_userSearchFilter=
####ACTIVE_DIRECTORY settings - Required only if have selected AD authentication ####
#
# Sample Settings
#
#xa_ldap_ad_domain=xasecure.net
#xa_ldap_ad_url=ldap://127.0.0.1:389
#xa_ldap_ad_base_dn=dc=xasecure,dc=net
#xa_ldap_ad_bind_dn=cn=administrator,ou=users,dc=xasecure,dc=net
#xa_ldap_ad_bind_password=
#xa_ldap_ad_referral=follow|ignore
#xa_ldap_ad_userSearchFilter=(sAMAccountName={0})

xa_ldap_ad_domain=
xa_ldap_ad_url=
xa_ldap_ad_base_dn=
xa_ldap_ad_bind_dn=
xa_ldap_ad_bind_password=
xa_ldap_ad_referral=
xa_ldap_ad_userSearchFilter=

#------------ Kerberos Config -----------------
spnego_principal=
spnego_keytab=
token_valid=30
cookie_domain=
cookie_path=/
admin_principal=
admin_keytab=
lookup_principal=
lookup_keytab=
hadoop_conf=$3/etc/hadoop
#
#-------- SSO CONFIG - Start ------------------
#
sso_enabled=false
sso_providerurl=https://127.0.0.1:8443/gateway/knoxsso/api/v1/websso
sso_publickey=

#
#-------- SSO CONFIG - END ------------------

# Custom log directory path
RANGER_ADMIN_LOG_DIR=$$PWD

# PID file path
RANGER_PID_DIR_PATH=/var/run/ranger

# #################  DO NOT MODIFY ANY VARIABLES BELOW #########################
#
# --- These deployment variables are not to be modified unless you understand the full impact of the changes
#
################################################################################
XAPOLICYMGR_DIR=$$PWD
app_home=$$PWD/ews/webapp
TMPFILE=$$PWD/.fi_tmp
LOGFILE=$$PWD/logfile
LOGFILES="$$LOGFILE"

JAVA_BIN='java'
JAVA_VERSION_REQUIRED='1.8'
JAVA_ORACLE='Java(TM) SE Runtime Environment'

ranger_admin_max_heap_size=1g
#retry DB and Java patches after the given time in seconds.
PATCH_RETRY_INTERVAL=120
STALE_PATCH_ENTRY_HOLD_TIME=10

#mysql_create_user_file=$${PWD}/db/mysql/create_dev_user.sql
mysql_core_file=db/mysql/optimized/current/ranger_core_db_mysql.sql
#mysql_audit_file=db/mysql/xa_audit_db.sql
#mysql_asset_file=$${PWD}/db/mysql/reset_asset.sql

#oracle_create_user_file=$${PWD}/db/oracle/create_dev_user_oracle.sql
oracle_core_file=db/oracle/optimized/current/ranger_core_db_oracle.sql
#oracle_audit_file=db/oracle/xa_audit_db_oracle.sql
#oracle_asset_file=$${PWD}/db/oracle/reset_asset_oracle.sql
#
postgres_core_file=db/postgres/optimized/current/ranger_core_db_postgres.sql
#postgres_audit_file=db/postgres/xa_audit_db_postgres.sql
#
sqlserver_core_file=db/sqlserver/optimized/current/ranger_core_db_sqlserver.sql
#sqlserver_audit_file=db/sqlserver/xa_audit_db_sqlserver.sql
#
sqlanywhere_core_file=db/sqlanywhere/optimized/current/ranger_core_db_sqlanywhere.sql
#sqlanywhere_audit_file=db/sqlanywhere/xa_audit_db_sqlanywhere.sql
cred_keystore_filename=$$app_home/WEB-INF/classes/conf/.jceks/rangeradmin.jceks
  )";

  std::unique_ptr<WritableFile> file;
  env->NewWritableFile(JoinPathSegments(data_root, "install.properties"), &file);
  ranger_port_ = GetRandomPort();
  file->Append(Substitute(kFileTemplate, data_root, postgres_port_, ranger_port_, hadoop_home_));
  file->Flush(WritableFile::FlushMode::FLUSH_SYNC);
  file->Close();

  return Status::OK();
}

Status MiniRanger::StopPostgres() {
  return postgres_process_->KillAndWait(SIGQUIT);
}

Status MiniRanger::StopRanger() {
  return Status::OK();
}

int MiniRanger::GetRandomPort() {
  Sockaddr address;
  address.ParseString("127.0.0.1", 0);
  Socket listener;

  CHECK_OK(listener.Init(0));
  CHECK_OK(listener.BindAndListen(address, 0));
  Sockaddr listen_address;
  CHECK_OK(listener.GetSocketAddress(&listen_address));
  listener.Close();

  return listen_address.port();
}

} // namespace ranger
} // namespace kudu
