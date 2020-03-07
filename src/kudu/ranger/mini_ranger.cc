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
#include "kudu/ranger/mini_postgres.h"
#include "kudu/util/env.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/path_util.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace ranger {

namespace {

// Taken and modified from:
// https://github.com/apache/ranger/blob/master/security-admin/scripts/install.properties
static const string kInstallProperties = R"(
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
RANGER_ADMIN_LOG_DIR=$4

# PID file path
RANGER_PID_DIR_PATH=$5

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


// For port info, see:
// https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.5/bk_reference/content/ranger-ports.html
// postgres DB hardcoded as "ranger"
// ranger jdbc user: miniranger
// ranger jdbc pw: miniranger
// external ranger port (this is the one that actually matters)
// hardcoded auth NONE
static const string kRangerAdminSiteTemplate = R"(
<configuration>
  <property>
    <name>ranger.jpa.jdbc.driver</name>
    <value>org.postgresql.Driver</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.jdbc.url</name>
    <value>jdbc:postgresql://localhost/ranger</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.jdbc.user</name>
    <value>miniranger</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.jdbc.password</name>
    <value>miniranger</value>
    <description/>
  </property>
  <property>
    <name>ranger.externalurl</name>
    <value>http://localhost:$0</value>
    <description/>
  </property>
  <property>
    <name>ranger.scheduler.enabled</name>
    <value>true</value>
    <description/>
  </property>
  <property>
    <name>ranger.audit.solr.urls</name>
    <value>http://localhost:6083/solr/ranger_audits</value>
    <description/>
  </property>
  <property>
    <name>ranger.audit.source.type</name>
    <value>db</value>
    <description/>
  </property>

  <property>
    <name>ranger.service.http.enabled</name>
    <value>true</value>
    <description/>
  </property>
  <property>
    <name>ranger.authentication.method</name>
    <value>NONE</value>
    <description/>
  </property>
  <property>
    <name>ranger.ldap.url</name>
    <value>ldap://</value>
    <description/>
  </property>
  <property>
    <name>ranger.ldap.user.dnpattern</name>
    <value>uid={0},ou=users,dc=xasecure,dc=net</value>
    <description/>
  </property>
  <property>
    <name>ranger.ldap.group.searchbase</name>
    <value>ou=groups,dc=xasecure,dc=net</value>
    <description/>
  </property>
  <property>
    <name>ranger.ldap.group.searchfilter</name>
    <value>(member=uid={0},ou=users,dc=xasecure,dc=net)</value>
    <description/>
  </property>
  <property>
    <name>ranger.ldap.group.roleattribute</name>
    <value>cn</value>
    <description/>
  </property>
  <property>
    <name>ranger.ldap.base.dn</name>
    <value/>
    <description>LDAP base dn or search base</description>
  </property>
  <property>
    <name>ranger.ldap.bind.dn</name>
    <value/>
    <description>LDAP bind dn or manager dn</description>
  </property>
  <property>
    <name>ranger.ldap.bind.password</name>
    <value/>
    <description>LDAP bind password</description>
  </property>
  <property>
    <name>ranger.ldap.default.role</name>
    <value>ROLE_USER</value>
  </property>
  <property>
    <name>ranger.ldap.referral</name>
    <value/>
    <description>follow or ignore</description>
  </property>
  <property>
    <name>ranger.ldap.ad.domain</name>
    <value>example.com</value>
    <description/>
  </property>
  <property>
    <name>ranger.ldap.ad.url</name>
    <value/>
    <description>ldap://</description>
  </property>

  <property>
    <name>ranger.ldap.ad.base.dn</name>
    <value>dc=example,dc=com</value>
    <description>AD base dn or search base</description>
  </property>
  <property>
    <name>ranger.ldap.ad.bind.dn</name>
    <value>cn=administrator,ou=users,dc=example,dc=com</value>
    <description>AD bind dn or manager dn</description>
  </property>
  <property>
    <name>ranger.ldap.ad.bind.password</name>
    <value/>
    <description>AD bind password</description>
  </property>

  <property>
    <name>ranger.ldap.ad.referral</name>
    <value/>
    <description>follow or ignore</description>
  </property>
  <property>
    <name>ranger.service.https.attrib.ssl.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>ranger.service.https.attrib.keystore.keyalias</name>
    <value>myKey</value>
  </property>

  <property>
    <name>ranger.service.https.attrib.keystore.pass</name>
    <value>_</value>
  </property>

  <property>
    <name>ranger.service.host</name>
    <value>localhost</value>
  </property>

  <property>
    <name>ranger.service.http.port</name>
    <value>6080</value>
  </property>

  <property>
    <name>ranger.service.https.port</name>
    <value>6182</value>
  </property>

  <property>
    <name>ranger.service.https.attrib.keystore.file</name>
    <value>/etc/ranger/admin/keys/server.jks</value>
  </property>

  <property>
    <name>ranger.solr.audit.user</name>
    <value/>
    <description/>
  </property>
  <property>
    <name>ranger.solr.audit.user.password</name>
    <value/>
    <description/>
  </property>
  <property>
    <name>ranger.audit.solr.zookeepers</name>
    <value/>
    <description/>
  </property>
  <property>
    <name>ranger.ldap.user.searchfilter</name>
    <value>(uid={0})</value>
    <description/>
  </property>
  <property>
    <name>ranger.ldap.ad.user.searchfilter</name>
    <value>(sAMAccountName={0})</value>
    <description/>
  </property>

  <property>
    <name>ranger.sso.providerurl</name>
    <value>https://127.0.0.1:8443/gateway/knoxsso/api/v1/websso</value>
  </property>
  <property>
    <name>ranger.sso.publicKey</name>
    <value/>
  </property>
  <property>
    <name>ranger.sso.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.sso.browser.useragent</name>
    <value>Mozilla,chrome</value>
  </property>

  <property>
    <name>ranger.admin.kerberos.token.valid.seconds</name>
    <value>30</value>
  </property>
  <property>
    <name>ranger.admin.kerberos.cookie.domain</name>
    <value/>
  </property>
  <property>
    <name>ranger.admin.kerberos.cookie.path</name>
    <value>/</value>
  </property>
  <property>
    <name>ranger.admin.kerberos.principal</name>
    <value>rangeradmin/_HOST@REALM</value>
  </property>
  <property>
    <name>ranger.admin.kerberos.keytab</name>
    <value/>
  </property>
  <property>
    <name>ranger.spnego.kerberos.principal</name>
    <value>HTTP/_HOST@REALM</value>
  </property>
  <property>
    <name>ranger.spnego.kerberos.keytab</name>
    <value/>
  </property>
  <property>
    <name>ranger.lookup.kerberos.principal</name>
    <value>rangerlookup/_HOST@REALM</value>
  </property>
  <property>
    <name>ranger.lookup.kerberos.keytab</name>
    <value/>
  </property>

  <property>
    <name>ranger.supportedcomponents</name>
    <value/>
  </property>
  <property>
    <name>ranger.downloadpolicy.session.log.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>ranger.kms.service.user.hdfs</name>
    <value>hdfs</value>
  </property>
  <property>
    <name>ranger.kms.service.user.hive</name>
    <value>hive</value>
  </property>

  <property>
    <name>ranger.audit.hive.query.visibility</name>
    <value>true</value>
    <description/>
  </property>
  <property>
    <name>ranger.service.https.attrib.keystore.credential.alias</name>
    <value>keyStoreCredentialAlias</value>
  </property>
  <property>
    <name>ranger.tomcat.ciphers</name>
    <value/>
  </property>
</configuration>
)";


// - postgres JDBC driver path
// - RANGER_HOME (needed for jceks/KMS), impala says this is ranger-home, but the
//   conf/jcsks directory doesn't exist for us.
static const string kRangerAdminDefaultSiteTemplate = R"(
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>ranger.jdbc.sqlconnectorjar</name>
    <value>$0</value>
    <description/>
  </property>
  <property>
    <name>ranger.service.user</name>
    <value>ranger</value>
    <description/>
  </property>
  <property>
    <name>ranger.service.group</name>
    <value>ranger</value>
    <description/>
  </property>

  <property>
    <name>ajp.enabled</name>
    <value>false</value>
    <description/>
  </property>

  <property>
    <name>ranger.db.maxrows.default</name>
    <value>200</value>
  </property>
  <property>
    <name>ranger.db.min_inlist</name>
    <value>20</value>
  </property>
  <property>
    <name>ranger.ui.defaultDateformat</name>
    <value>MM/dd/yyyy</value>
  </property>
  <property>
    <name>ranger.db.defaultDateformat</name>
    <value>yyyy-MM-dd</value>
  </property>

  <property>
    <name>ranger.ajax.auth.required.code</name>
    <value>401</value>
  </property>
  <property>
    <name>ranger.ajax.auth.success.page</name>
    <value>/ajax_success.html</value>
  </property>
  <property>
    <name>ranger.logout.success.page</name>
    <value>/login.jsp?action=logged_out</value>
  </property>
  <property>
    <name>ranger.ajax.auth.failure.page</name>
    <value>/ajax_failure.jsp</value>
  </property>

  <property>
    <name>ranger.users.roles.list</name>
    <value>
      ROLE_SYS_ADMIN, ROLE_USER, ROLE_OTHER, ROLE_ANON, ROLE_KEY_ADMIN,
      ROLE_ADMIN_AUDITOR, ROLE_KEY_ADMIN_AUDITOR
    </value>
  </property>

  <property>
    <name>ranger.mail.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.mail.smtp.auth</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.mail.retry.sleep.ms</name>
    <value>2000</value>
  </property>
  <property>
    <name>ranger.mail.retry.max.count</name>
    <value>5</value>
  </property>
  <property>
    <name>ranger.mail.retry.sleep.incr_factor</name>
    <value>1</value>
  </property>
  <property>
    <name>ranger.mail.listener.enable</name>
    <value>false</value>
  </property>

  <property>
    <name>ranger.second_level_cache</name>
    <value>true</value>
  </property>
  <property>
    <name>ranger.use_query_cache</name>
    <value>true</value>
  </property>

  <property>
    <name>ranger.user.firstname.maxlength</name>
    <value>16</value>
  </property>
  <property>
    <name>ranger.bookmark.name.maxlen</name>
    <value>150</value>
  </property>

  <property>
    <name>ranger.rbac.enable</name>
    <value>false</value>
  </property>

  <property>
    <name>ranger.rest.paths</name>
    <value>org.apache.ranger.rest,xa.rest</value>
  </property>

  <property>
    <name>ranger.password.hidden</name>
    <value>*****</value>
  </property>
  <property>
    <name>ranger.resource.accessControl.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>ranger.xuser.createdByUserId</name>
    <value>1</value>
  </property>

  <property>
    <name>ranger.allow.hack</name>
    <value>1</value>
  </property>

  <property>
    <name>ranger.log.SC_NOT_MODIFIED</name>
    <value>false</value>
  </property>

  <property>
    <name>ranger.servlet.mapping.url.pattern</name>
    <value>service</value>
  </property>

  <property>
    <name>ranger.file.separator</name>
    <value>/</value>
  </property>

  <property>
    <name>ranger.db.access.filter.enable</name>
    <value>true</value>
  </property>
  <property>
    <name>ranger.moderation.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.userpref.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>ranger.unixauth.remote.login.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>ranger.unixauth.service.hostname</name>
    <value>localhost</value>
  </property>
  <property>
    <name>ranger.unixauth.service.port</name>
    <value>5151</value>
  </property>
  <property>
    <name>ranger.unixauth.ssl.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>ranger.unixauth.debug</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.unixauth.server.cert.validation</name>
    <value>false</value>
  </property>

  <property>
    <name>ranger.unixauth.keystore</name>
    <value>keystore.jks</value>
  </property>
  <property>
    <name>ranger.unixauth.keystore.credential.alias</name>
    <value>unixAuthKeyStoreAlias</value>
  </property>
  <property>
    <name>ranger.unixauth.keystore.password</name>
    <value>_</value>
  </property>
  <property>
    <name>ranger.unixauth.truststore</name>
    <value>cacerts</value>
  </property>
  <property>
    <name>ranger.unixauth.truststore.credential.alias</name>
    <value>unixAuthTrustStoreAlias</value>
  </property>
  <property>
    <name>ranger.unixauth.truststore.password</name>
    <value>_</value>
  </property>

  <property>
    <name>maven.project.version</name>
    <value>0.5.0</value>
    <description/>
  </property>

  <property>
    <name>ranger.service.shutdown.port</name>
    <value>6085</value>
  </property>

  <property>
    <name>ranger.service.shutdown.command</name>
    <value>SHUTDOWN</value>
  </property>

  <property>
    <name>ranger.service.https.attrib.ssl.protocol</name>
    <value>TLS</value>
  </property>

  <property>
    <name>ranger.service.https.attrib.client.auth</name>
    <value>false</value>
  </property>

  <property>
    <name>ranger.accesslog.dateformat</name>
    <value>yyyy-MM-dd</value>
  </property>

  <property>
    <name>ranger.accesslog.pattern</name>
    <value>%h %l %u %t "%r" %s %b "%{Referer}i" "%{User-Agent}i"</value>
  </property>

  <property>
    <name>ranger.contextName</name>
    <value>/</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.showsql</name>
    <value>false</value>
    <description/>
  </property>

  <property>
    <name>ranger.env.local</name>
    <value>true</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.dialect</name>
    <value>org.eclipse.persistence.platform.database.PostgreSQLPlatform</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.maxpoolsize</name>
    <value>40</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.minpoolsize</name>
    <value>5</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.initialpoolsize</name>
    <value>5</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.maxidletime</name>
    <value>300</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.maxstatements</name>
    <value>500</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.preferredtestquery</name>
    <value>select 1;</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.idleconnectiontestperiod</name>
    <value>60</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.jdbc.credential.alias</name>
    <value>ranger.db.password</value>
    <description/>
  </property>

  <property>
    <name>ranger.credential.provider.path</name>
    <value>$1/ews/webapp/WEB-INF/classes/conf/.jceks/rangeradmin.jceks</value>
    <description/>
  </property>

  <property>
    <name>ranger.logs.base.dir</name>
    <value>user.home</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.audit.jdbc.dialect</name>
    <value>org.eclipse.persistence.platform.database.PostgreSQLPlatform</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.audit.jdbc.credential.alias</name>
    <value>ranger.auditdb.password</value>
    <description/>
  </property>

  <property>
    <name>ranger.ldap.binddn.credential.alias</name>
    <value>ranger.ldap.binddn.password</value>
    <description/>
  </property>

  <property>
    <name>ranger.ldap.ad.binddn.credential.alias</name>
    <value>ranger.ad.binddn.password</value>
    <description/>
  </property>

  <property>
    <name>ranger.resource.lookup.timeout.value.in.ms</name>
    <value>1000</value>
    <description/>
  </property>

  <property>
    <name>ranger.validate.config.timeout.value.in.ms</name>
    <value>10000</value>
    <description/>
  </property>

  <property>
    <name>ranger.timed.executor.max.threadpool.size</name>
    <value>10</value>
    <description/>
  </property>

  <property>
    <name>ranger.timed.executor.queue.size</name>
    <value>100</value>
    <description/>
  </property>
  <property>
    <name>ranger.solr.audit.credential.alias</name>
    <value>ranger.solr.password</value>
    <description/>
  </property>
  <property>
    <name>ranger.sha256Password.update.disable</name>
    <value>true</value>
    <description/>
  </property>

  <property>
    <name>ranger.jpa.audit.jdbc.driver</name>
    <value>org.postgresql.Driver</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.audit.jdbc.url</name>
    <value>jdbc:log4jdbc:mysql://localhost/rangeraudit</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.audit.jdbc.user</name>
    <value>rangerlogger</value>
    <description/>
  </property>
  <property>
    <name>ranger.jpa.audit.jdbc.password</name>
    <value>rangerlogger</value>
    <description/>
  </property>
  <property>
    <name>ranger.supportedcomponents</name>
    <value/>
  </property>

  <property>
    <name>ranger.sso.cookiename</name>
    <value>hadoop-jwt</value>
  </property>
  <property>
    <name>ranger.sso.query.param.originalurl</name>
    <value>originalUrl</value>
  </property>
  <property>
    <name>ranger.rest-csrf.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>ranger.rest-csrf.custom-header</name>
    <value>X-XSRF-HEADER</value>
  </property>
  <property>
    <name>ranger.rest-csrf.methods-to-ignore</name>
    <value>GET,OPTIONS,HEAD,TRACE</value>
  </property>
  <property>
    <name>ranger.rest-csrf.browser-useragents-regex</name>
    <value>Mozilla,Opera,Chrome</value>
  </property>
  <property>
    <name>ranger.krb.browser-useragents-regex</name>
    <value>Mozilla,Opera,Chrome</value>
  </property>
  <property>
    <name>ranger.db.ssl.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.db.ssl.required</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.db.ssl.verifyServerCertificate</name>
    <value>false</value>
  </property>
  <property>
    <name>ranger.db.ssl.auth.type</name>
    <value>2-way</value>
  </property>
  <property>
    <name>ranger.keystore.file</name>
    <value/>
  </property>
  <property>
    <name>ranger.keystore.alias</name>
    <value>keyStoreAlias</value>
  </property>
  <property>
    <name>ranger.keystore.password</name>
    <value/>
  </property>
  <property>
    <name>ranger.truststore.file</name>
    <value/>
  </property>
  <property>
    <name>ranger.truststore.alias</name>
    <value>trustStoreAlias</value>
  </property>
  <property>
    <name>ranger.truststore.password</name>
    <value/>
  </property>
  <property>
    <name>ranger.service.https.attrib.ssl.enabled.protocols</name>
    <value>SSLv2Hello, TLSv1, TLSv1.1, TLSv1.2</value>
  </property>

  <property>
    <name>ranger.password.encryption.key</name>
    <value>tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV</value>
  </property>
  <property>
    <name>ranger.password.salt</name>
    <value>f77aLYLo</value>
  </property>
  <property>
    <name>ranger.password.iteration.count</name>
    <value>1000</value>
  </property>
  <property>
    <name>ranger.password.encryption.algorithm</name>
    <value>PBEWithMD5AndDES</value>
  </property>
  <property>
    <name>ranger.default.browser-useragents</name>
    <value>Mozilla,Opera,Chrome</value>
  </property>
</configuration>
)";

} // anonymous namespace

Status MiniRanger::Start() {
  RETURN_NOT_OK(mini_pg_.Start());
  // TODO(awong): codify that we're waiting for Postgres to become usable.
  SleepFor(MonoDelta::FromSeconds(5));
  RETURN_NOT_OK(mini_pg_.AddUser("rangeradmin", /*super*/true));
  RETURN_NOT_OK(mini_pg_.CreateDb("ranger", "rangeradmin"));
  return StartRanger();
}

Status MiniRanger::Stop() {
  RETURN_NOT_OK(StopRanger());
  return mini_pg_.Stop();
}

Status MiniRanger::StartRanger() {
  if (data_root_.empty()) {
    data_root_ = GetTestDataDirectory();
  }
  Env* env = Env::Default();
  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  const string bin_dir = DirName(exe);
  RETURN_NOT_OK(FindHomeDir("hadoop", bin_dir, &hadoop_home_));
  RETURN_NOT_OK(FindHomeDir("ranger", bin_dir, &ranger_home_));
  RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home_));
  const string admin_home = ranger_admin_home();
  if (!env->FileExists(admin_home)) {
    env->CreateDir(admin_home);
  }
  LOG(INFO) << "Starting Ranger out of " << admin_home;

  // Create an install.properties file.
  // Put this in the config directory.
  ranger_port_ = GetRandomPort();
  LOG(INFO) << "Ranger using port " << ranger_port_;
  RETURN_NOT_OK(
      CreateRangerConfigs(Substitute(kInstallProperties, mini_pg_.pg_bin_dir(),
                                     mini_pg_.bound_port(), ranger_port_, hadoop_home_,
                                     admin_home, JoinPathSegments(data_root_, "tmppid")),
                          JoinPathSegments(admin_home, "install.properties")));
  RETURN_NOT_OK(
      CreateRangerConfigs(Substitute(kRangerAdminSiteTemplate, ranger_port_),
                          JoinPathSegments(admin_home, "ranger-admin-site.xml")));
  RETURN_NOT_OK(
      CreateRangerConfigs(Substitute(kRangerAdminDefaultSiteTemplate,
                                     JoinPathSegments(mini_pg_.pg_bin_dir(), "postgresql.jar"),
                                     admin_home),
                          JoinPathSegments(admin_home, "ranger-admin-default-site.xml")));

  // Much of this encapsulates setup.sh from apache/ranger, excluding some of
  // the system-level configs.

  // Run db_setup.py
  // What do I need to set up for this to run successfully?
  // RANGER_ADMIN_HOME:
  // - Should be the downloaded Ranger files, which has some JARs that will be
  //   used to determine the Ranger version
  // RANGER_ADMIN_CONF (defaults to using RANGER_ADMIN_HOME)
  // - This is where install.properties goes
  // - Will be used to determine properties of the database we're setting up
  unique_ptr<Subprocess> db_setup(new Subprocess({
      "python",
      JoinPathSegments(ranger_home_, "db_setup.py")
  }));
  db_setup->SetEnvVars({
      { "JAVA_HOME", java_home_ },
      { "RANGER_ADMIN_HOME", ranger_home_ },
      { "RANGER_ADMIN_CONF", admin_home },
  });
  db_setup->SetCurrentDir(admin_home);
  RETURN_NOT_OK(db_setup->Start());
  RETURN_NOT_OK(db_setup->Wait());

  // From bin/create-test-configuration.sh in Impala
  // Create $RANGER_ADMIN_HOME/ews/webapp/WEB-INF/classes/conf that will house
  // - java_home.sh
  // - ranger-admin-env-logdir.sh
  // - ranger-admin-env-piddir.sh
  // - (though I think we can push all of those through environment variables)
  // - security-applicationContext.xml
  //   - XXX(awong): do we actually need this? Let's try without it.
  // - This is where we'll generate/put
  //   - ranger-admin-default-site.xml
  //   - ranger-admin-site.xml
  //   - XXX(awong): For now, let's use default configs.
  //
  // RANGER_ADMIN_HOME/ews/webapp/WEB-INF/lib
  // - JDBC driver for postgres
  const string kEwsDir = JoinPathSegments(admin_home, "ews");
  const string kWebAppDir = JoinPathSegments(kEwsDir, "webapp");
  const string kWebInfDir = JoinPathSegments(kWebAppDir, "WEB-INF");
  const string kClassesDir = JoinPathSegments(kWebInfDir, "classes");
  const string kConfDir = JoinPathSegments(kClassesDir, "conf");
  const string kConfDistDir = JoinPathSegments(kClassesDir, "conf.dist");
  RETURN_NOT_OK(env->CreateDir(kEwsDir));
  RETURN_NOT_OK(env->CreateDir(kWebAppDir));
  RETURN_NOT_OK(env->CreateDir(kWebInfDir));
  RETURN_NOT_OK(env->CreateDir(kClassesDir));
  RETURN_NOT_OK(env->CreateDir(kConfDir));
  RETURN_NOT_OK(env->CreateDir(kConfDistDir));

  RETURN_NOT_OK(env->SyncDir(kConfDir));
  RETURN_NOT_OK(env->SyncDir(kConfDistDir));

  // Encapsulates ranger-admin-services.sh
  // Set XAPOLICYMGR_DIR = RANGER_ADMIN_HOME
  // - XAPOLICYMGR_EWS_DIR = ""/ews
  // - RANGER_JAAS_LIB_DIR = ""/ews/webapp/ranger_jaas
  // - RANGER_JAAS_CONF_DIR = ""/ews/WEB-INF/classes/conf/ranger_jaas
  // Set JAVA_HOME
  // Set ranger-admin-env* variables
  // - RANGER_PID_DIR_PATH = /tmp (probably don't need)
  // - RANGER_ADMIN_PID_NAME=rangeradmin.pid
  // - RANGER_USER
  // Set JAVA_OPTS and JAVA_HOME
  // RANGER_ADMIN_LOG_DIR=${XAPOLICYMGR_EWS_DIR}/logs
  // Set up pid file (probably don't need)
  // Run tomcat server

  string classpath =
    Substitute("$0/webapp/WEB-INF/classes/conf:$1/lib/*:$2/lib/*:$3/*:$$CLASSPATH",
               kEwsDir, JoinPathSegments(ranger_home_, "ews"), java_home_, hadoop_home_);

  LOG(INFO) << "Using Ranger class path: " << classpath;

  string fqdn;
  RETURN_NOT_OK(GetFQDN(&fqdn));
  LOG(INFO) << "Using FQDN: " << fqdn;
  unique_ptr<Subprocess> ranger_start(new Subprocess({
      "java",
      "-Dproc_rangeradmin",
      Substitute("-Dlog4j.configuration=file:$0",
          JoinPathSegments(ranger_home_, "ews/webapp/WEB-INF/log4j.properties")),
      "-Duser=miniranger",
      Substitute("-Dranger.service.host=", fqdn),
      "-Dservername=miniranger",
      Substitute("-Dcatalina.base=$0", kEwsDir),
      Substitute("-Dlogdir=$0", JoinPathSegments(admin_home, "logs")),
      "-Dranger.audit.solr.bootstrap.enabled=false",
      "-cp", classpath, "org.apache.ranger.server.tomcat.EmbeddedServer"
  }));
  ranger_start->SetEnvVars({
      { "XAPOLICYMGR_DIR", admin_home },
      { "XAPOLICYMGR_EWS_DIR", kEwsDir },
      { "RANGER_JAAS_LIB_DIR", JoinPathSegments(kWebAppDir, "ranger_jaas") },
      { "RANGER_JAAS_CONF_DIR", JoinPathSegments(kConfDir, "ranger_jaas") },
      { "JAVA_HOME", java_home_ },
      // XXX(awong): don't think our pid dir matters much
      { "RANGER_PID_DIR_PATH", JoinPathSegments(data_root_, "tmppid") },
      { "RANGER_ADMIN_PID_NAME", "rangeradmin.pid" },
      { "RANGER_USER", "miniranger" },
  });
  RETURN_NOT_OK(ranger_start->Start());
  RETURN_NOT_OK(ranger_start->Wait());
  return Status::Incomplete("");
}

Status MiniRanger::CreateRangerConfigs(const string& config_string, const string& file_path) {
  Env* env = Env::Default();
  std::unique_ptr<WritableFile> file;
  RETURN_NOT_OK(env->NewWritableFile(file_path, &file));
  RETURN_NOT_OK(file->Append(config_string));
  RETURN_NOT_OK(file->Flush(WritableFile::FlushMode::FLUSH_SYNC));
  return file->Close();
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
