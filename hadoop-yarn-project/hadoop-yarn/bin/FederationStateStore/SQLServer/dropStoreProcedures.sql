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

-- Script to drop all the stored procedures for the Federation StateStore in SQLServer

USE [FederationStateStore]
GO

IF OBJECT_ID ( '[sp_addApplicationHomeSubCluster]', 'P' ) IS NOT NULL
  DROP PROCEDURE [sp_addApplicationHomeSubCluster];
GO

IF OBJECT_ID ( '[sp_updateApplicationHomeSubCluster]', 'P' ) IS NOT NULL
  DROP PROCEDURE [sp_updateApplicationHomeSubCluster];
GO

IF OBJECT_ID ( '[sp_getApplicationsHomeSubCluster]', 'P' ) IS NOT NULL
  DROP PROCEDURE [sp_getApplicationsHomeSubCluster];
GO

IF OBJECT_ID ( '[sp_getApplicationHomeSubCluster]', 'P' ) IS NOT NULL
  DROP PROCEDURE [sp_getApplicationHomeSubCluster];
GO

IF OBJECT_ID ( '[sp_deleteApplicationHomeSubCluster]', 'P' ) IS NOT NULL
  DROP PROCEDURE [sp_deleteApplicationHomeSubCluster];
GO

IF OBJECT_ID ( '[sp_registerSubCluster]', 'P' ) IS NOT NULL
  DROP PROCEDURE [sp_registerSubCluster];
GO

IF OBJECT_ID ( '[sp_getSubClusters]', 'P' ) IS NOT NULL
  DROP PROCEDURE [sp_getSubClusters];
GO

IF OBJECT_ID ( '[sp_getSubCluster]', 'P' ) IS NOT NULL
  DROP PROCEDURE [sp_getSubCluster];
GO

IF OBJECT_ID ( '[sp_subClusterHeartbeat]', 'P' ) IS NOT NULL
  DROP PROCEDURE IF EXISTS [sp_subClusterHeartbeat];
GO

IF OBJECT_ID ( '[sp_deregisterSubCluster]', 'P' ) IS NOT NULL
  DROP PROCEDURE IF EXISTS [sp_deregisterSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_setPolicyConfiguration];
GO

DROP PROCEDURE IF EXISTS [sp_getPolicyConfiguration];
GO

DROP PROCEDURE IF EXISTS [sp_getPoliciesConfigurations];
GO

DROP PROCEDURE IF EXISTS [sp_addApplicationHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_updateReservationHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_getReservationsHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_getReservationHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_deleteReservationHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_addMasterKey];
GO

DROP PROCEDURE IF EXISTS [sp_getMasterKey];
GO

DROP PROCEDURE IF EXISTS [sp_deleteMasterKey];
GO

DROP PROCEDURE IF EXISTS [sp_addDelegationToken];
GO

DROP PROCEDURE IF EXISTS [sp_getDelegationToken];
GO

DROP PROCEDURE IF EXISTS [sp_updateDelegationToken];
GO

DROP PROCEDURE IF EXISTS [sp_deleteDelegationToken];
GO