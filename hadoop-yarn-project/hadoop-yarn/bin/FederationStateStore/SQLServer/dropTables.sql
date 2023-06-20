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

-- Script to drop all the tables from the Federation StateStore in SQLServer

USE [FederationStateStore]
GO

IF OBJECT_ID ( '[sp_deregisterSubCluster]', 'U' ) IS NOT NULL
  DROP TABLE [sp_deregisterSubCluster];
GO

IF OBJECT_ID ( '[membership]', 'U' ) IS NOT NULL
  DROP TABLE [membership];
GO

IF OBJECT_ID ( '[policies]', 'U' ) IS NOT NULL
  DROP TABLE [policies];
GO

IF OBJECT_ID ( '[applicationsHomeSubCluster]', 'U' ) IS NOT NULL
  DROP TABLE [applicationsHomeSubCluster];
GO

IF OBJECT_ID ( '[reservationsHomeSubCluster]', 'U' ) IS NOT NULL
  DROP TABLE [reservationsHomeSubCluster];
GO

IF OBJECT_ID ( '[masterKeys]', 'U' ) IS NOT NULL
  DROP TABLE [masterKeys];
GO

IF OBJECT_ID ( '[delegationTokens]', 'U' ) IS NOT NULL
  DROP TABLE [delegationTokens];
GO

IF OBJECT_ID ( '[sequenceTable]', 'U' ) IS NOT NULL
  DROP TABLE [sequenceTable];
GO

IF OBJECT_ID ( '[versions]', 'U' ) IS NOT NULL
  DROP TABLE [versions];
GO
