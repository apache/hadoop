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

-- Script to drop all the stored procedures for the Federation StateStore in MySQL

USE FederationStateStore

DROP PROCEDURE sp_registerSubCluster;

DROP PROCEDURE sp_deregisterSubCluster;

DROP PROCEDURE sp_subClusterHeartbeat;

DROP PROCEDURE sp_getSubCluster;

DROP PROCEDURE sp_getSubClusters;

DROP PROCEDURE sp_addApplicationHomeSubCluster;

DROP PROCEDURE sp_updateApplicationHomeSubCluster;

DROP PROCEDURE sp_getApplicationHomeSubCluster;

DROP PROCEDURE sp_getApplicationsHomeSubCluster;

DROP PROCEDURE sp_deleteApplicationHomeSubCluster;

DROP PROCEDURE sp_setPolicyConfiguration;

DROP PROCEDURE sp_getPolicyConfiguration;

DROP PROCEDURE sp_getPoliciesConfigurations;

DROP PROCEDURE sp_addReservationHomeSubCluster;

DROP PROCEDURE sp_getReservationHomeSubCluster;

DROP PROCEDURE sp_getReservationsHomeSubCluster;

DROP PROCEDURE sp_deleteReservationHomeSubCluster;

DROP PROCEDURE sp_updateReservationHomeSubCluster;

DROP PROCEDURE sp_addMasterKey;

DROP PROCEDURE sp_getMasterKey;

DROP PROCEDURE sp_deleteMasterKey;

DROP PROCEDURE sp_addDelegationToken;

DROP PROCEDURE sp_getDelegationToken;

DROP PROCEDURE sp_updateDelegationToken;

DROP PROCEDURE sp_deleteDelegationToken;

DROP PROCEDURE sp_storeVersion;

DROP PROCEDURE sp_getVersion;