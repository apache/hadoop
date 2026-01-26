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

IF OBJECT_ID ('[sp_addApplicationHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_addApplicationHomeSubCluster];
GO

IF OBJECT_ID ('[sp_updateApplicationHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_updateApplicationHomeSubCluster];
GO

IF OBJECT_ID ('[sp_getApplicationsHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getApplicationsHomeSubCluster];
GO

IF OBJECT_ID ('[sp_getApplicationHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getApplicationHomeSubCluster];
GO

IF OBJECT_ID ('[sp_deleteApplicationHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_deleteApplicationHomeSubCluster];
GO

IF OBJECT_ID ('[sp_registerSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_registerSubCluster];
GO

IF OBJECT_ID ('[sp_getSubClusters]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getSubClusters];
GO

IF OBJECT_ID ('[sp_getSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getSubCluster];
GO

IF OBJECT_ID ('[sp_subClusterHeartbeat]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_subClusterHeartbeat];
GO

IF OBJECT_ID ('[sp_deregisterSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_deregisterSubCluster];
GO

IF OBJECT_ID ('[sp_setPolicyConfiguration]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_setPolicyConfiguration];
GO

IF OBJECT_ID ('[sp_getPolicyConfiguration]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getPolicyConfiguration];
GO

IF OBJECT_ID ('[sp_getPoliciesConfigurations]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getPoliciesConfigurations];
GO

IF OBJECT_ID ('[sp_addReservationHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_addReservationHomeSubCluster];
GO

IF OBJECT_ID ('[sp_updateReservationHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_updateReservationHomeSubCluster];
GO

IF OBJECT_ID ('[sp_getReservationsHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getReservationsHomeSubCluster];
GO

IF OBJECT_ID ('[sp_getReservationHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getReservationHomeSubCluster];
GO

IF OBJECT_ID ('[sp_deleteReservationHomeSubCluster]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_deleteReservationHomeSubCluster];
GO

IF OBJECT_ID ('[sp_addMasterKey]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_addMasterKey];
GO

IF OBJECT_ID ('[sp_getMasterKey]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getMasterKey];
GO

IF OBJECT_ID ('[sp_deleteMasterKey]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_deleteMasterKey];
GO

IF OBJECT_ID ('[sp_addDelegationToken]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_addDelegationToken];
GO

IF OBJECT_ID ('[sp_getDelegationToken]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getDelegationToken];
GO

IF OBJECT_ID ('[sp_updateDelegationToken]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_updateDelegationToken];
GO

IF OBJECT_ID ('[sp_deleteDelegationToken]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_deleteDelegationToken];
GO

IF OBJECT_ID ('[sp_storeVersion]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_storeVersion];
GO

IF OBJECT_ID ('[sp_getVersion]', 'P') IS NOT NULL
  DROP PROCEDURE [sp_getVersion];
GO