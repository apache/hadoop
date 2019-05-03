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

USE [FederationStateStore]
GO

IF NOT EXISTS ( SELECT * FROM [FederationStateStore].sys.tables
    WHERE name = 'applicationsHomeSubCluster'
    AND schema_id = SCHEMA_ID('dbo'))
    BEGIN
        PRINT 'Table applicationsHomeSubCluster does not exist, create it...'

        SET ANSI_NULLS ON

        SET QUOTED_IDENTIFIER ON

        SET ANSI_PADDING ON

        CREATE TABLE [dbo].[applicationsHomeSubCluster](
            applicationId   VARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
            homeSubCluster  VARCHAR(256) NOT NULL,
            createTime      DATETIME2 NOT NULL CONSTRAINT ts_createAppTime DEFAULT GETUTCDATE(),

            CONSTRAINT [pk_applicationId] PRIMARY KEY
            (
                [applicationId]
            )
        )

        SET ANSI_PADDING OFF

        PRINT 'Table applicationsHomeSubCluster created.'
    END
ELSE
    PRINT 'Table applicationsHomeSubCluster exists, no operation required...'
    GO
GO

IF NOT EXISTS ( SELECT * FROM [FederationStateStore].sys.tables
    WHERE name = 'membership'
    AND schema_id = SCHEMA_ID('dbo'))
    BEGIN
        PRINT 'Table membership does not exist, create it...'

        SET ANSI_NULLS ON

        SET QUOTED_IDENTIFIER ON

        SET ANSI_PADDING ON

        CREATE TABLE [dbo].[membership](
            [subClusterId]            VARCHAR(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
            [amRMServiceAddress]      VARCHAR(256) NOT NULL,
            [clientRMServiceAddress]  VARCHAR(256) NOT NULL,
            [rmAdminServiceAddress]   VARCHAR(256) NOT NULL,
            [rmWebServiceAddress]     VARCHAR(256) NOT NULL,
            [lastHeartBeat]           DATETIME2 NOT NULL,
            [state]                   VARCHAR(32) NOT NULL,
            [lastStartTime]           BIGINT NOT NULL,
            [capability]              VARCHAR(6000) NOT NULL,

            CONSTRAINT [pk_subClusterId] PRIMARY KEY
            (
                [subClusterId]
            )
            CONSTRAINT [uc_lastStartTime] UNIQUE
            (
                [lastStartTime]
            )
        )

        SET ANSI_PADDING OFF

        PRINT 'Table membership created.'
    END
ELSE
    PRINT 'Table membership exists, no operation required...'
    GO
GO

IF NOT EXISTS ( SELECT * FROM [FederationStateStore].sys.tables
    WHERE name = 'policies'
    AND schema_id = SCHEMA_ID('dbo'))
    BEGIN
        PRINT 'Table policies does not exist, create it...'

        SET ANSI_NULLS ON

        SET QUOTED_IDENTIFIER ON

        SET ANSI_PADDING ON

        CREATE TABLE [dbo].[policies](
            queue       VARCHAR(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
            policyType  VARCHAR(256) NOT NULL,
            params      VARBINARY(6000) NOT NULL,

            CONSTRAINT [pk_queue] PRIMARY KEY
            (
                [queue]
            )
        )

        SET ANSI_PADDING OFF

        PRINT 'Table policies created.'
    END
ELSE
    PRINT 'Table policies exists, no operation required...'
    GO
GO
