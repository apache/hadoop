/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HSQLDB implementation of {@link FederationStateStore}.
 */
public class HSQLDBFederationStateStore extends SQLFederationStateStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(HSQLDBFederationStateStore.class);

  private Connection conn;

  private static final String TABLE_APPLICATIONSHOMESUBCLUSTER =
      " CREATE TABLE applicationsHomeSubCluster ("
          + " applicationId varchar(64) NOT NULL,"
          + " homeSubCluster varchar(256) NOT NULL,"
          + " createTime datetime NOT NULL,"
          + " applicationContext BLOB NULL,"
          + " CONSTRAINT pk_applicationId PRIMARY KEY (applicationId))";

  private static final String TABLE_MEMBERSHIP =
      "CREATE TABLE membership ( subClusterId varchar(256) NOT NULL,"
          + " amRMServiceAddress varchar(256) NOT NULL,"
          + " clientRMServiceAddress varchar(256) NOT NULL,"
          + " rmAdminServiceAddress varchar(256) NOT NULL,"
          + " rmWebServiceAddress varchar(256) NOT NULL,"
          + " lastHeartBeat datetime NOT NULL, state varchar(32) NOT NULL,"
          + " lastStartTime bigint NULL, capability varchar(6000) NOT NULL,"
          + " CONSTRAINT pk_subClusterId PRIMARY KEY (subClusterId))";

  private static final String TABLE_POLICIES =
      "CREATE TABLE policies ( queue varchar(256) NOT NULL,"
          + " policyType varchar(256) NOT NULL, params varbinary(512),"
          + " CONSTRAINT pk_queue PRIMARY KEY (queue))";

  private static final String TABLE_RESERVATIONSHOMESUBCLUSTER =
      " CREATE TABLE reservationsHomeSubCluster ("
           + " reservationId varchar(128) NOT NULL,"
           + " homeSubCluster varchar(256) NOT NULL,"
           + " CONSTRAINT pk_reservationId PRIMARY KEY (reservationId))";

  private static final String TABLE_MASTERKEYS =
      " CREATE TABLE masterKeys ("
          + " keyId bigint NOT NULL,"
          + " masterKey varchar(1024) NOT NULL,"
          + " CONSTRAINT pk_keyId PRIMARY KEY (keyId))";

  private static final String TABLE_DELEGATIONTOKENS =
      " CREATE TABLE delegationTokens ("
          + " sequenceNum bigint NOT NULL,"
          + " tokenIdent varchar(1024) NOT NULL,"
          + " token varchar(1024) NOT NULL,"
          + " renewDate bigint NOT NULL,"
          + " CONSTRAINT pk_sequenceNum PRIMARY KEY (sequenceNum))";

  private static final String TABLE_SEQUENCETABLE =
      " CREATE TABLE sequenceTable ("
          + " sequenceName varchar(255) NOT NULL,"
          + " nextVal bigint NOT NULL,"
          + " CONSTRAINT pk_sequenceName PRIMARY KEY (sequenceName))";

  private static final String TABLE_VERSIONS =
      "CREATE TABLE versions ("
          + " fedVersion varbinary(1024) NOT NULL,"
          + " versionComment VARCHAR(255),"
          + " CONSTRAINT pk_fedVersion PRIMARY KEY (fedVersion))";
  private static final String SP_REGISTERSUBCLUSTER =
      "CREATE PROCEDURE sp_registerSubCluster("
          + " IN subClusterId_IN varchar(256),"
          + " IN amRMServiceAddress_IN varchar(256),"
          + " IN clientRMServiceAddress_IN varchar(256),"
          + " IN rmAdminServiceAddress_IN varchar(256),"
          + " IN rmWebServiceAddress_IN varchar(256),"
          + " IN state_IN varchar(256),"
          + " IN lastStartTime_IN bigint, IN capability_IN varchar(6000),"
          + " OUT rowCount_OUT int)MODIFIES SQL DATA BEGIN ATOMIC"
          + " DELETE FROM membership WHERE (subClusterId = subClusterId_IN);"
          + " INSERT INTO membership ( subClusterId,"
          + " amRMServiceAddress, clientRMServiceAddress,"
          + " rmAdminServiceAddress, rmWebServiceAddress,"
          + " lastHeartBeat, state, lastStartTime,"
          + " capability) VALUES ( subClusterId_IN,"
          + " amRMServiceAddress_IN, clientRMServiceAddress_IN,"
          + " rmAdminServiceAddress_IN, rmWebServiceAddress_IN,"
          + " NOW() AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE,"
          + " state_IN, lastStartTime_IN, capability_IN);"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; END";

  private static final String SP_DEREGISTERSUBCLUSTER =
      "CREATE PROCEDURE sp_deregisterSubCluster("
          + " IN subClusterId_IN varchar(256),"
          + " IN state_IN varchar(64), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " UPDATE membership SET state = state_IN WHERE ("
          + " subClusterId = subClusterId_IN AND state != state_IN);"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; END";

  private static final String SP_SUBCLUSTERHEARTBEAT =
      "CREATE PROCEDURE sp_subClusterHeartbeat("
          + " IN subClusterId_IN varchar(256), IN state_IN varchar(64),"
          + " IN capability_IN varchar(6000), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC UPDATE membership"
          + " SET capability = capability_IN, state = state_IN,"
          + " lastHeartBeat = NOW() AT TIME ZONE INTERVAL '0:00'"
          + " HOUR TO MINUTE WHERE subClusterId = subClusterId_IN;"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; END";

  private static final String SP_GETSUBCLUSTER =
      "CREATE PROCEDURE sp_getSubCluster( IN subClusterId_IN varchar(256),"
          + " OUT amRMServiceAddress_OUT varchar(256),"
          + " OUT clientRMServiceAddress_OUT varchar(256),"
          + " OUT rmAdminServiceAddress_OUT varchar(256),"
          + " OUT rmWebServiceAddress_OUT varchar(256),"
          + " OUT lastHeartBeat_OUT datetime, OUT state_OUT varchar(64),"
          + " OUT lastStartTime_OUT bigint,"
          + " OUT capability_OUT varchar(6000))"
          + " MODIFIES SQL DATA BEGIN ATOMIC SELECT amRMServiceAddress,"
          + " clientRMServiceAddress,"
          + " rmAdminServiceAddress, rmWebServiceAddress,"
          + " lastHeartBeat, state, lastStartTime, capability"
          + " INTO amRMServiceAddress_OUT, clientRMServiceAddress_OUT,"
          + " rmAdminServiceAddress_OUT,"
          + " rmWebServiceAddress_OUT, lastHeartBeat_OUT,"
          + " state_OUT, lastStartTime_OUT, capability_OUT"
          + " FROM membership WHERE subClusterId = subClusterId_IN; END";

  private static final String SP_GETSUBCLUSTERS =
      "CREATE PROCEDURE sp_getSubClusters()"
          + " MODIFIES SQL DATA DYNAMIC RESULT SETS 1 BEGIN ATOMIC"
          + " DECLARE result CURSOR FOR"
          + " SELECT subClusterId, amRMServiceAddress, clientRMServiceAddress,"
          + " rmAdminServiceAddress, rmWebServiceAddress, lastHeartBeat,"
          + " state, lastStartTime, capability"
          + " FROM membership; OPEN result; END";

  private static final String SP_ADDAPPLICATIONHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_addApplicationHomeSubCluster("
          + " IN applicationId_IN varchar(64),"
          + " IN homeSubCluster_IN varchar(256),"
          + " IN applicationContext_IN BLOB,"
          + " OUT storedHomeSubCluster_OUT varchar(256), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " INSERT INTO applicationsHomeSubCluster "
          + " (applicationId,homeSubCluster,createTime,applicationContext) "
          + " (SELECT applicationId_IN, homeSubCluster_IN, "
          + " NOW() AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE, "
          + " applicationContext_IN "
          + " FROM applicationsHomeSubCluster"
          + " WHERE applicationId = applicationId_IN"
          + " HAVING COUNT(*) = 0 );"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT;"
          + " SELECT homeSubCluster INTO storedHomeSubCluster_OUT"
          + " FROM applicationsHomeSubCluster"
          + " WHERE applicationId = applicationID_IN; END";

  private static final String SP_UPDATEAPPLICATIONHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_updateApplicationHomeSubCluster("
          + " IN applicationId_IN varchar(64),"
          + " IN homeSubCluster_IN varchar(256), "
          + " IN applicationContext_IN BLOB, OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " UPDATE applicationsHomeSubCluster"
          + " SET homeSubCluster = homeSubCluster_IN, "
          + " applicationContext = applicationContext_IN "
          + " WHERE applicationId = applicationId_IN;"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; END";

  private static final String SP_GETAPPLICATIONHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_getApplicationHomeSubCluster("
          + " IN applicationId_IN varchar(64),"
          + " OUT homeSubCluster_OUT varchar(256),"
          + " OUT createTime_OUT datetime,"
          + " OUT applicationContext_OUT BLOB)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " SELECT homeSubCluster, applicationContext, createTime "
          + " INTO homeSubCluster_OUT, applicationContext_OUT, createTime_OUT "
          + " FROM applicationsHomeSubCluster"
          + " WHERE applicationId = applicationID_IN; END";

  private static final String SP_GETAPPLICATIONSHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_getApplicationsHomeSubCluster("
          + "IN limit_IN int, IN homeSubCluster_IN varchar(256))"
          + " MODIFIES SQL DATA DYNAMIC RESULT SETS 1 BEGIN ATOMIC"
          + " DECLARE result CURSOR FOR"
          + " SELECT applicationId, homeSubCluster, createTime"
          + " FROM applicationsHomeSubCluster "
          + " WHERE ROWNUM() <= limit_IN AND "
          + " (homeSubCluster_IN = '' OR homeSubCluster = homeSubCluster_IN) "
          + " ORDER BY createTime desc; "
          + " OPEN result; END";

  private static final String SP_DELETEAPPLICATIONHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_deleteApplicationHomeSubCluster("
          + " IN applicationId_IN varchar(64), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " DELETE FROM applicationsHomeSubCluster"
          + " WHERE applicationId = applicationId_IN;"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; END";

  private static final String SP_SETPOLICYCONFIGURATION =
      "CREATE PROCEDURE sp_setPolicyConfiguration("
          + " IN queue_IN varchar(256), IN policyType_IN varchar(256),"
          + " IN params_IN varbinary(512), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " DELETE FROM policies WHERE queue = queue_IN;"
          + " INSERT INTO policies (queue, policyType, params)"
          + " VALUES (queue_IN, policyType_IN, params_IN);"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; END";

  private static final String SP_GETPOLICYCONFIGURATION =
      "CREATE PROCEDURE sp_getPolicyConfiguration("
          + " IN queue_IN varchar(256), OUT policyType_OUT varchar(256),"
          + " OUT params_OUT varbinary(512)) MODIFIES SQL DATA BEGIN ATOMIC"
          + " SELECT policyType, params INTO policyType_OUT, params_OUT"
          + " FROM policies WHERE queue = queue_IN; END";

  private static final String SP_GETPOLICIESCONFIGURATIONS =
      "CREATE PROCEDURE sp_getPoliciesConfigurations()"
          + " MODIFIES SQL DATA DYNAMIC RESULT SETS 1 BEGIN ATOMIC"
          + " DECLARE result CURSOR FOR"
          + " SELECT * FROM policies; OPEN result; END";

  private static final String SP_ADDRESERVATIONHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_addReservationHomeSubCluster("
          + " IN reservationId_IN varchar(128),"
          + " IN homeSubCluster_IN varchar(256),"
          + " OUT storedHomeSubCluster_OUT varchar(256), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " INSERT INTO reservationsHomeSubCluster "
          + " (reservationId,homeSubCluster) "
          + " (SELECT reservationId_IN, homeSubCluster_IN"
          + " FROM reservationsHomeSubCluster"
          + " WHERE reservationId = reservationId_IN"
          + " HAVING COUNT(*) = 0 );"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT;"
          + " SELECT homeSubCluster INTO storedHomeSubCluster_OUT"
          + " FROM reservationsHomeSubCluster"
          + " WHERE reservationId = reservationId_IN; END";

  private static final String SP_GETRESERVATIONHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_getReservationHomeSubCluster("
          + " IN reservationId_IN varchar(128),"
          + " OUT homeSubCluster_OUT varchar(256))"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " SELECT homeSubCluster INTO homeSubCluster_OUT"
          + " FROM reservationsHomeSubCluster"
          + " WHERE reservationId = reservationId_IN; END";

  private static final String SP_GETRESERVATIONSHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_getReservationsHomeSubCluster()"
          + " MODIFIES SQL DATA DYNAMIC RESULT SETS 1 BEGIN ATOMIC"
          + " DECLARE result CURSOR FOR"
          + " SELECT reservationId, homeSubCluster"
          + " FROM reservationsHomeSubCluster; OPEN result; END";

  private static final String SP_DELETERESERVATIONHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_deleteReservationHomeSubCluster("
          + " IN reservationId_IN varchar(128), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " DELETE FROM reservationsHomeSubCluster"
          + " WHERE reservationId = reservationId_IN;"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; END";

  private static final String SP_UPDATERESERVATIONHOMESUBCLUSTER =
      "CREATE PROCEDURE sp_updateReservationHomeSubCluster("
          + " IN reservationId_IN varchar(128),"
          + " IN homeSubCluster_IN varchar(256), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " UPDATE reservationsHomeSubCluster"
          + " SET homeSubCluster = homeSubCluster_IN"
          + " WHERE reservationId = reservationId_IN;"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; END";

  protected static final String SP_DROP_ADDRESERVATIONHOMESUBCLUSTER =
      "DROP PROCEDURE sp_addReservationHomeSubCluster";

  protected static final String SP_ADDRESERVATIONHOMESUBCLUSTER2 =
      "CREATE PROCEDURE sp_addReservationHomeSubCluster("
          + " IN reservationId_IN varchar(128),"
          + " IN homeSubCluster_IN varchar(256),"
          + " OUT storedHomeSubCluster_OUT varchar(256), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " INSERT INTO reservationsHomeSubCluster "
          + " (reservationId,homeSubCluster) "
          + " (SELECT reservationId_IN, homeSubCluster_IN"
          + " FROM reservationsHomeSubCluster"
          + " WHERE reservationId = reservationId_IN"
          + " HAVING COUNT(*) = 0 );"
          + " SELECT homeSubCluster, 2 INTO storedHomeSubCluster_OUT, rowCount_OUT"
          + " FROM reservationsHomeSubCluster"
          + " WHERE reservationId = reservationId_IN; END";

  protected static final String SP_DROP_UPDATERESERVATIONHOMESUBCLUSTER =
      "DROP PROCEDURE sp_updateReservationHomeSubCluster";

  protected static final String SP_UPDATERESERVATIONHOMESUBCLUSTER2 =
      "CREATE PROCEDURE sp_updateReservationHomeSubCluster("
          + " IN reservationId_IN varchar(128),"
          + " IN homeSubCluster_IN varchar(256), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " UPDATE reservationsHomeSubCluster"
          + " SET homeSubCluster = homeSubCluster_IN"
          + " WHERE reservationId = reservationId_IN;"
          + " SET rowCount_OUT = 2; END";

  protected static final String SP_DROP_DELETERESERVATIONHOMESUBCLUSTER =
      "DROP PROCEDURE sp_deleteReservationHomeSubCluster";

  protected static final String SP_DELETERESERVATIONHOMESUBCLUSTER2 =
      "CREATE PROCEDURE sp_deleteReservationHomeSubCluster("
          + " IN reservationId_IN varchar(128), OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " DELETE FROM reservationsHomeSubCluster"
          + " WHERE reservationId = reservationId_IN;"
          + " SET rowCount_OUT = 2; END";

  protected static final String SP_DROP_ADDMASTERKEY = "DROP PROCEDURE sp_addMasterKey";

  protected static final String SP_ADDMASTERKEY =
      "CREATE PROCEDURE sp_addMasterKey("
          + " IN keyId_IN int, IN masterKey_IN varchar(1024),"
          + " OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC "
          + " INSERT INTO masterKeys(keyId, masterKey)"
          + " (SELECT keyId_IN, masterKey_IN"
          + " FROM masterKeys "
          + " WHERE keyId = keyId_IN "
          + " HAVING COUNT(*) = 0);"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT;"
          + " END";

  protected static final String SP_DROP_GETMASTERKEY = "DROP PROCEDURE sp_getMasterKey";

  protected static final String SP_GETMASTERKEY =
      "CREATE PROCEDURE sp_getMasterKey("
          + " IN keyId_IN int,"
          + " OUT masterKey_OUT varchar(1024))"
          + " MODIFIES SQL DATA BEGIN ATOMIC "
          + " SELECT masterKey INTO masterKey_OUT "
          + " FROM masterKeys "
          + " WHERE keyId = keyId_IN; "
          + " END ";

  protected static final String SP_DROP_DELETEMASTERKEY = "DROP PROCEDURE sp_deleteMasterKey";

  protected static final String SP_DELETEMASTERKEY =
      "CREATE PROCEDURE sp_deleteMasterKey("
          + " IN keyId_IN int, OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " DELETE FROM masterKeys WHERE keyId = keyId_IN;"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; END";

  protected static final String SP_DROP_ADD_DELEGATIONTOKEN =
      "DROP PROCEDURE sp_addDelegationToken";

  protected static final String SP_ADD_DELEGATIONTOKEN =
      "CREATE PROCEDURE sp_addDelegationToken("
          + " IN sequenceNum_IN bigint, IN tokenIdent_IN varchar(1024),"
          + " IN token_IN varchar(1024), IN renewDate_IN bigint, OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC "
          + " INSERT INTO delegationTokens(sequenceNum, tokenIdent, token, renewDate)"
          + " (SELECT sequenceNum_IN, tokenIdent_IN, token_IN, renewDate_IN"
          + " FROM delegationTokens"
          + " WHERE sequenceNum = sequenceNum_IN"
          + " HAVING COUNT(*) = 0);"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT;"
          + " END";

  protected static final String SP_DROP_GET_DELEGATIONTOKEN =
      "DROP PROCEDURE sp_getDelegationToken";

  protected static final String SP_GET_DELEGATIONTOKEN =
      "CREATE PROCEDURE sp_getDelegationToken("
          + " IN sequenceNum_IN bigint, OUT tokenIdent_OUT varchar(1024), "
          + " OUT token_OUT varchar(1024), OUT renewDate_OUT bigint)"
          + " MODIFIES SQL DATA BEGIN ATOMIC "
          + " SELECT tokenIdent, token, renewDate INTO "
          + " tokenIdent_OUT, token_OUT, renewDate_OUT"
          + " FROM delegationTokens"
          + " WHERE sequenceNum = sequenceNum_IN; "
          + " END ";

  protected static final String SP_DROP_UPDATE_DELEGATIONTOKEN =
      "DROP PROCEDURE sp_updateDelegationToken";

  protected static final String SP_UPDATE_DELEGATIONTOKEN =
      "CREATE PROCEDURE sp_updateDelegationToken("
          + " IN sequenceNum_IN bigint, IN tokenIdent_IN varchar(1024),"
          + " IN token_IN varchar(1024), IN renewDate_IN bigint, OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " UPDATE delegationTokens"
          + " SET tokenIdent = tokenIdent_IN,"
          + " token = token_IN, renewDate = renewDate_IN"
          + " WHERE sequenceNum = sequenceNum_IN;"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; "
          + " END ";

  protected static final String SP_DROP_DELETE_DELEGATIONTOKEN =
      "DROP PROCEDURE sp_deleteDelegationToken";

  protected static final String SP_DELETE_DELEGATIONTOKEN =
      "CREATE PROCEDURE sp_deleteDelegationToken("
          + " IN sequenceNum_IN bigint, OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " DELETE FROM delegationTokens"
          + " WHERE sequenceNum = sequenceNum_IN;"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; "
          + " END ";

  protected static final String SP_STORE_VERSION =
      "CREATE PROCEDURE sp_storeVersion("
          + " IN fedVersion_IN varbinary(1024), IN versionComment_IN varchar(256), "
          + " OUT rowCount_OUT int)"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " DELETE FROM versions;"
          + " INSERT INTO versions (fedVersion, versionComment)"
          + " VALUES (fedVersion_IN, versionComment_IN);"
          + " GET DIAGNOSTICS rowCount_OUT = ROW_COUNT; "
          + " END ";

  protected static final String SP_GET_VERSION =
      "CREATE PROCEDURE sp_getVersion("
          + " OUT fedVersion_OUT varbinary(1024), OUT versionComment_OUT varchar(256))"
          + " MODIFIES SQL DATA BEGIN ATOMIC"
          + " SELECT fedVersion, versionComment INTO fedVersion_OUT, versionComment_OUT"
          + " FROM versions; "
          + " END ";

  private List<String> tables = new ArrayList<>();

  @Override
  public void init(Configuration conf) {
    try {
      conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS, 10);
      super.init(conf);
      conn = super.getConn();

      LOG.info("Database Init: Start");

      conn.prepareStatement(TABLE_APPLICATIONSHOMESUBCLUSTER).execute();
      conn.prepareStatement(TABLE_MEMBERSHIP).execute();
      conn.prepareStatement(TABLE_POLICIES).execute();
      conn.prepareStatement(TABLE_RESERVATIONSHOMESUBCLUSTER).execute();
      conn.prepareStatement(TABLE_MASTERKEYS).execute();
      conn.prepareStatement(TABLE_DELEGATIONTOKENS).execute();
      conn.prepareStatement(TABLE_SEQUENCETABLE).execute();
      conn.prepareStatement(TABLE_VERSIONS).execute();

      conn.prepareStatement(SP_REGISTERSUBCLUSTER).execute();
      conn.prepareStatement(SP_DEREGISTERSUBCLUSTER).execute();
      conn.prepareStatement(SP_SUBCLUSTERHEARTBEAT).execute();
      conn.prepareStatement(SP_GETSUBCLUSTER).execute();
      conn.prepareStatement(SP_GETSUBCLUSTERS).execute();

      conn.prepareStatement(SP_ADDAPPLICATIONHOMESUBCLUSTER).execute();
      conn.prepareStatement(SP_UPDATEAPPLICATIONHOMESUBCLUSTER).execute();
      conn.prepareStatement(SP_GETAPPLICATIONHOMESUBCLUSTER).execute();
      conn.prepareStatement(SP_GETAPPLICATIONSHOMESUBCLUSTER).execute();
      conn.prepareStatement(SP_DELETEAPPLICATIONHOMESUBCLUSTER).execute();

      conn.prepareStatement(SP_SETPOLICYCONFIGURATION).execute();
      conn.prepareStatement(SP_GETPOLICYCONFIGURATION).execute();
      conn.prepareStatement(SP_GETPOLICIESCONFIGURATIONS).execute();

      conn.prepareStatement(SP_ADDRESERVATIONHOMESUBCLUSTER).execute();
      conn.prepareStatement(SP_GETRESERVATIONHOMESUBCLUSTER).execute();
      conn.prepareStatement(SP_GETRESERVATIONSHOMESUBCLUSTER).execute();
      conn.prepareStatement(SP_DELETERESERVATIONHOMESUBCLUSTER).execute();
      conn.prepareStatement(SP_UPDATERESERVATIONHOMESUBCLUSTER).execute();

      conn.prepareStatement(SP_ADDMASTERKEY).execute();
      conn.prepareStatement(SP_GETMASTERKEY).execute();
      conn.prepareStatement(SP_DELETEMASTERKEY).execute();

      conn.prepareStatement(SP_ADD_DELEGATIONTOKEN).execute();
      conn.prepareStatement(SP_GET_DELEGATIONTOKEN).execute();
      conn.prepareStatement(SP_UPDATE_DELEGATIONTOKEN).execute();
      conn.prepareStatement(SP_DELETE_DELEGATIONTOKEN).execute();

      conn.prepareStatement(SP_STORE_VERSION).execute();
      conn.prepareStatement(SP_GET_VERSION).execute();

      LOG.info("Database Init: Complete");
    } catch (Exception e) {
      LOG.error("ERROR: failed to initialize HSQLDB {}.", e.getMessage());
    }
  }

  public void initConnection(Configuration conf) {
    try {
      super.init(conf);
      conn = super.getConn();
    } catch (YarnException e1) {
      LOG.error("ERROR: failed open connection to HSQLDB DB {}.", e1.getMessage());
    }
  }

  public void closeConnection() {
    try {
      conn.close();
    } catch (SQLException e) {
      LOG.error("ERROR: failed to close connection to HSQLDB DB {}.", e.getMessage());
    }
  }

  /**
   * Extract The Create Table Sql From The Script.
   *
   * @param dbIdentifier database identifier, Like Mysql / SqlServer
   * @param regex the regex
   * @throws IOException IO exception.
   */
  protected void extractCreateTableSQL(String dbIdentifier, String regex) throws IOException {

    String[] createTableScriptPathItems = new String[] {
        ".", "target", "test-classes", dbIdentifier, "FederationStateStoreTables.sql" };
    String createTableScriptPath = StringUtils.join(createTableScriptPathItems, File.separator);

    String createTableSQL =
        FileUtils.readFileToString(new File(createTableScriptPath), StandardCharsets.UTF_8);
    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(createTableSQL);
    while (m != null && m.find()) {
      String group = m.group();
      tables.add(group);
    }
  }

  public List<String> getTables() {
    return tables;
  }

  public void setTables(List<String> tables) {
    this.tables = tables;
  }
}
