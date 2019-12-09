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

USE FederationStateStore

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
   OUT storedHomeSubCluster_OUT varchar(256), OUT rowCount_OUT int)
BEGIN
   INSERT INTO applicationsHomeSubCluster
      (applicationId,homeSubCluster)
      (SELECT applicationId_IN, homeSubCluster_IN
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
   IN homeSubCluster_IN varchar(256), OUT rowCount_OUT int)
BEGIN
   UPDATE applicationsHomeSubCluster
     SET homeSubCluster = homeSubCluster_IN
   WHERE applicationId = applicationId_IN;
   SELECT ROW_COUNT() INTO rowCount_OUT;
END //

CREATE PROCEDURE sp_getApplicationHomeSubCluster(
   IN applicationId_IN varchar(64),
   OUT homeSubCluster_OUT varchar(256))
BEGIN
   SELECT homeSubCluster INTO homeSubCluster_OUT
   FROM applicationsHomeSubCluster
   WHERE applicationId = applicationID_IN;
END //

CREATE PROCEDURE sp_getApplicationsHomeSubCluster()
BEGIN
   SELECT applicationId, homeSubCluster
   FROM applicationsHomeSubCluster;
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

DELIMITER ;
