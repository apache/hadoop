/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- Script to generate all the stored procedures for the Federation StateStore in MySQL

USE FederationStateStore;

DELIMITER //

CREATE PROCEDURE sp_registerSubCluster(
   IN subClusterId_IN varchar(256),
   IN amRMServiceAddress_IN varchar(256),
   IN clientRMServiceAddress_IN varchar(256),
   IN rmAdminServiceAddress_IN varchar(256),
   IN rmWebServiceAddress_IN varchar(256),
   IN state_IN varchar(256),
   IN lastStartTime_IN bigint, IN capability_IN varchar(6000),
   OUT rowCount_OUT int)
BEGIN
   DELETE FROM membership WHERE (subClusterId = subClusterId_IN);
   INSERT INTO membership (subClusterId, amRMServiceAddress, clientRMServiceAddress,
          rmAdminServiceAddress, rmWebServiceAddress, lastHeartBeat, state, lastStartTime, capability)
      VALUES (subClusterId_IN, amRMServiceAddress_IN, clientRMServiceAddress_IN,
      rmAdminServiceAddress_IN, rmWebServiceAddress_IN, NOW(), state_IN, lastStartTime_IN, capability_IN);
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_deregisterSubCluster(
    IN subClusterId_IN varchar(256),
    IN state_IN varchar(64),
    OUT rowCount_OUT int)
BEGIN
    UPDATE membership SET state = state_IN
    WHERE (subClusterId = subClusterId_IN AND state != state_IN);
    SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_subClusterHeartbeat(
    IN subClusterId_IN varchar(256), IN state_IN varchar(64),
    IN capability_IN varchar(6000), OUT rowCount_OUT int)
BEGIN
   UPDATE membership
   SET capability = capability_IN,
       state = state_IN,
       lastHeartBeat = NOW()
   WHERE subClusterId = subClusterId_IN;
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_getSubCluster(
   IN subClusterId_IN varchar(256),
   OUT amRMServiceAddress_OUT varchar(256),
   OUT clientRMServiceAddress_OUT varchar(256),
   OUT rmAdminServiceAddress_OUT varchar(256),
   OUT rmWebServiceAddress_OUT varchar(256),
   OUT lastHeartBeat_OUT datetime, OUT state_OUT varchar(64),
   OUT lastStartTime_OUT bigint,
   OUT capability_OUT varchar(6000))
BEGIN
   SELECT amRMServiceAddress, clientRMServiceAddress, rmAdminServiceAddress, rmWebServiceAddress,
          lastHeartBeat, state, lastStartTime, capability
   INTO amRMServiceAddress_OUT, clientRMServiceAddress_OUT, rmAdminServiceAddress_OUT,
        rmWebServiceAddress_OUT, lastHeartBeat_OUT, state_OUT, lastStartTime_OUT, capability_OUT
   FROM membership WHERE subClusterId = subClusterId_IN;
END //

CREATE PROCEDURE sp_getSubClusters()
BEGIN
   SELECT subClusterId, amRMServiceAddress, clientRMServiceAddress,
          rmAdminServiceAddress, rmWebServiceAddress, lastHeartBeat,
          state, lastStartTime, capability
   FROM membership;
END //

CREATE PROCEDURE sp_addApplicationHomeSubCluster(
   IN applicationId_IN varchar(64), IN homeSubCluster_IN varchar(256),
   IN applicationContext_IN BLOB,
   OUT storedHomeSubCluster_OUT varchar(256), OUT rowCount_OUT int)
BEGIN
   INSERT INTO applicationsHomeSubCluster
      (applicationId, homeSubCluster, createTime, applicationContext)
      (SELECT applicationId_IN, homeSubCluster_IN, NOW(), applicationContext_IN
       FROM applicationsHomeSubCluster
       WHERE applicationId = applicationId_IN
       HAVING COUNT(*) = 0 );
   SELECT ROW_COUNT() INTO rowCount_OUT;
   SELECT homeSubCluster INTO storedHomeSubCluster_OUT
   FROM applicationsHomeSubCluster
   WHERE applicationId = applicationID_IN;
END //

CREATE PROCEDURE sp_updateApplicationHomeSubCluster(
   IN applicationId_IN varchar(64),
   IN homeSubCluster_IN varchar(256), IN applicationContext_IN BLOB, OUT rowCount_OUT int)
BEGIN
   UPDATE applicationsHomeSubCluster
     SET homeSubCluster = homeSubCluster_IN,
         applicationContext = applicationContext_IN
   WHERE applicationId = applicationId_IN;
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_getApplicationHomeSubCluster(
   IN applicationId_IN varchar(64),
   OUT homeSubCluster_OUT varchar(256),
   OUT createTime_OUT datetime,
   OUT applicationContext_OUT BLOB)
BEGIN
   SELECT homeSubCluster, applicationContext, createTime
       INTO homeSubCluster_OUT, applicationContext_OUT, createTime_OUT
   FROM applicationsHomeSubCluster
   WHERE applicationId = applicationID_IN;
END //

CREATE PROCEDURE sp_getApplicationsHomeSubCluster(IN limit_IN int, IN homeSubCluster_IN varchar(256))
BEGIN
   SELECT
       applicationId,
       homeSubCluster,
       createTime
   FROM (SELECT
             applicationId,
             homeSubCluster,
             createTime,
             @rownum := 0
         FROM applicationshomesubcluster
         ORDER BY createTime DESC) AS applicationshomesubcluster
   WHERE (homeSubCluster_IN = '' OR homeSubCluster = homeSubCluster_IN)
     AND (@rownum := @rownum + 1) <= limit_IN;
END //

CREATE PROCEDURE sp_deleteApplicationHomeSubCluster(
   IN applicationId_IN varchar(64), OUT rowCount_OUT int)
BEGIN
   DELETE FROM applicationsHomeSubCluster
   WHERE applicationId = applicationId_IN;
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_setPolicyConfiguration(
   IN queue_IN varchar(256), IN policyType_IN varchar(256),
   IN params_IN varbinary(32768), OUT rowCount_OUT int)
BEGIN
   DELETE FROM policies WHERE queue = queue_IN;
   INSERT INTO policies (queue, policyType, params)
   VALUES (queue_IN, policyType_IN, params_IN);
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_getPoliciesConfigurations()
BEGIN
   SELECT queue, policyType, params FROM policies;
END //

CREATE PROCEDURE sp_getPolicyConfiguration(
   IN queue_IN varchar(256), OUT policyType_OUT varchar(256),
   OUT params_OUT varbinary(32768))
BEGIN
   SELECT policyType, params INTO policyType_OUT, params_OUT
   FROM policies WHERE queue = queue_IN;
END //

CREATE PROCEDURE sp_addReservationHomeSubCluster(
   IN reservationId_IN varchar(128), IN homeSubCluster_IN varchar(256),
   OUT storedHomeSubCluster_OUT varchar(256), OUT rowCount_OUT int)
BEGIN
   INSERT INTO reservationsHomeSubCluster
      (reservationId,homeSubCluster)
      (SELECT reservationId_IN, homeSubCluster_IN
       FROM applicationsHomeSubCluster
       WHERE reservationId = reservationId_IN
       HAVING COUNT(*) = 0 );
   SELECT ROW_COUNT() INTO rowCount_OUT;
   SELECT homeSubCluster INTO storedHomeSubCluster_OUT
   FROM reservationsHomeSubCluster
   WHERE reservationId = reservationId_IN;
END //

CREATE PROCEDURE sp_getReservationHomeSubCluster(
   IN reservationId_IN varchar(128),
   OUT homeSubCluster_OUT varchar(256))
BEGIN
   SELECT homeSubCluster INTO homeSubCluster_OUT
   FROM reservationsHomeSubCluster
   WHERE reservationId = reservationId_IN;
END //

CREATE PROCEDURE sp_getReservationsHomeSubCluster()
BEGIN
   SELECT reservationId, homeSubCluster
   FROM reservationsHomeSubCluster;
END //

CREATE PROCEDURE sp_updateReservationHomeSubCluster(
   IN reservationId_IN varchar(128),
   IN homeSubCluster_IN varchar(256), OUT rowCount_OUT int)
BEGIN
   UPDATE reservationsHomeSubCluster
     SET homeSubCluster = homeSubCluster_IN
   WHERE reservationId = reservationId_IN;
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_deleteReservationHomeSubCluster(
   IN reservationId_IN varchar(128), OUT rowCount_OUT int)
BEGIN
   DELETE FROM reservationsHomeSubCluster
   WHERE reservationId = reservationId_IN;
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_addMasterKey(
   IN keyId_IN bigint, IN masterKey_IN varchar(1024),
   OUT rowCount_OUT int)
BEGIN
   INSERT INTO masterKeys(keyId, masterKey)
     (SELECT keyId_IN, masterKey_IN
        FROM masterKeys
       WHERE keyId = keyId_IN
      HAVING COUNT(*) = 0);
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_getMasterKey(
   IN keyId_IN bigint,
   OUT masterKey_OUT varchar(1024))
BEGIN
   SELECT masterKey INTO masterKey_OUT
   FROM masterKeys
   WHERE keyId = keyId_IN;
END //

CREATE PROCEDURE sp_deleteMasterKey(
   IN keyId_IN bigint, OUT rowCount_OUT int)
BEGIN
   DELETE FROM masterKeys
   WHERE keyId = keyId_IN;
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_addDelegationToken(
   IN sequenceNum_IN bigint, IN tokenIdent_IN varchar(1024),
   IN token_IN varchar(1024), IN renewDate_IN bigint,
   OUT rowCount_OUT int)
BEGIN
   INSERT INTO delegationTokens(sequenceNum, tokenIdent, token, renewDate)
     (SELECT sequenceNum_IN, tokenIdent_IN, token_IN, renewDate_IN
        FROM delegationTokens
       WHERE sequenceNum = sequenceNum_IN
      HAVING COUNT(*) = 0);
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_getDelegationToken(
   IN sequenceNum_IN bigint, OUT tokenIdent_OUT varchar(1024),
   OUT token_OUT varchar(1024), OUT renewDate_OUT bigint)
BEGIN
   SELECT tokenIdent, token,  renewDate INTO tokenIdent_OUT, token_OUT, renewDate_OUT
     FROM delegationTokens
    WHERE sequenceNum = sequenceNum_IN;
END //

CREATE PROCEDURE sp_updateDelegationToken(
   IN sequenceNum_IN bigint, IN tokenIdent_IN varchar(1024),
   IN token_IN varchar(1024), IN renewDate_IN bigint, OUT rowCount_OUT int)
BEGIN
   UPDATE delegationTokens
      SET tokenIdent = tokenIdent_IN,
          token = token_IN,
          renewDate = renewDate_IN
    WHERE sequenceNum = sequenceNum_IN;
    SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_deleteDelegationToken(
   IN sequenceNum_IN bigint, OUT rowCount_OUT int)
BEGIN
   DELETE FROM delegationTokens
   WHERE sequenceNum = sequenceNum_IN;
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_storeVersion(
   IN fedVersion_IN varbinary(1024), IN versionComment_IN varchar(255), OUT rowCount_OUT int)
BEGIN
   DELETE FROM versions;
   INSERT INTO versions (fedVersion, versionComment)
   VALUES (fedVersion_IN, versionComment_IN);
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_getVersion(
   OUT fedVersion_OUT varbinary(1024), OUT versionComment_OUT varchar(255))
BEGIN
   SELECT fedVersion, versionComment INTO fedVersion_OUT, versionComment_OUT
   FROM versions
   LIMIT 1;
END //

DELIMITER ;
