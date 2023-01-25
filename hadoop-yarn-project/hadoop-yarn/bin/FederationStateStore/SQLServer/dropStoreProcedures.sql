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

DROP PROCEDURE IF EXISTS [sp_addApplicationHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_updateApplicationHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_getApplicationsHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_getApplicationHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_deleteApplicationHomeSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_registerSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_getSubClusters];
GO

DROP PROCEDURE IF EXISTS [sp_getSubCluster];
GO

DROP PROCEDURE IF EXISTS [sp_subClusterHeartbeat];
GO

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
