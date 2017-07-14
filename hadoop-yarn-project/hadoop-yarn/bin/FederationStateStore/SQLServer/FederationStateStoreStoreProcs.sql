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

IF OBJECT_ID ( '[sp_addApplicationHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_addApplicationHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_addApplicationHomeSubCluster]
    @applicationId VARCHAR(64),
    @homeSubCluster VARCHAR(256),
    @storedHomeSubCluster VARCHAR(256) OUTPUT,
    @rowCount int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN
            -- If application to sub-cluster map doesn't exist, insert it.
            -- Otherwise don't change the current mapping.
            IF NOT EXISTS (SELECT TOP 1 *
                       FROM [dbo].[applicationsHomeSubCluster]
                       WHERE [applicationId] = @applicationId)

                INSERT INTO [dbo].[applicationsHomeSubCluster] (
                    [applicationId],
                    [homeSubCluster])
                VALUES (
                    @applicationId,
                    @homeSubCluster);
            -- End of the IF block

            SELECT @rowCount = @@ROWCOUNT;

            SELECT @storedHomeSubCluster = [homeSubCluster]
            FROM [dbo].[applicationsHomeSubCluster]
            WHERE [applicationId] = @applicationId;

        COMMIT TRAN
    END TRY

    BEGIN CATCH
        ROLLBACK TRAN

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_updateApplicationHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_updateApplicationHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_updateApplicationHomeSubCluster]
    @applicationId VARCHAR(64),
    @homeSubCluster VARCHAR(256),
    @rowCount int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            UPDATE [dbo].[applicationsHomeSubCluster]
            SET [homeSubCluster] = @homeSubCluster
            WHERE [applicationId] = @applicationid;
            SELECT @rowCount = @@ROWCOUNT;

        COMMIT TRAN
    END TRY

    BEGIN CATCH
        ROLLBACK TRAN

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_getApplicationsHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getApplicationsHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_getApplicationsHomeSubCluster]
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        SELECT [applicationId], [homeSubCluster], [createTime]
        FROM [dbo].[applicationsHomeSubCluster]
    END TRY

    BEGIN CATCH
        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_getApplicationHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getApplicationHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_getApplicationHomeSubCluster]
    @applicationId VARCHAR(64),
    @homeSubCluster VARCHAR(256) OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        SELECT @homeSubCluster = [homeSubCluster]
        FROM [dbo].[applicationsHomeSubCluster]
        WHERE [applicationId] = @applicationid;

    END TRY

    BEGIN CATCH

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_deleteApplicationHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_deleteApplicationHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_deleteApplicationHomeSubCluster]
    @applicationId VARCHAR(64),
    @rowCount int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[applicationsHomeSubCluster]
            WHERE [applicationId] = @applicationId;
            SELECT @rowCount = @@ROWCOUNT;

        COMMIT TRAN
    END TRY

    BEGIN CATCH
        ROLLBACK TRAN

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_registerSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_registerSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_registerSubCluster]
    @subClusterId VARCHAR(256),
    @amRMServiceAddress VARCHAR(256),
    @clientRMServiceAddress VARCHAR(256),
    @rmAdminServiceAddress VARCHAR(256),
    @rmWebServiceAddress VARCHAR(256),
    @state VARCHAR(32),
    @lastStartTime BIGINT,
    @capability VARCHAR(6000),
    @rowCount int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[membership]
            WHERE [subClusterId] = @subClusterId;
            INSERT INTO [dbo].[membership] (
                [subClusterId],
                [amRMServiceAddress],
                [clientRMServiceAddress],
                [rmAdminServiceAddress],
                [rmWebServiceAddress],
                [lastHeartBeat],
                [state],
                [lastStartTime],
                [capability] )
            VALUES (
                @subClusterId,
                @amRMServiceAddress,
                @clientRMServiceAddress,
                @rmAdminServiceAddress,
                @rmWebServiceAddress,
                GETUTCDATE(),
                @state,
                @lastStartTime,
                @capability);
            SELECT @rowCount = @@ROWCOUNT;

        COMMIT TRAN
    END TRY

    BEGIN CATCH
        ROLLBACK TRAN

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_getSubClusters]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getSubClusters];
GO

CREATE PROCEDURE [dbo].[sp_getSubClusters]
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        SELECT [subClusterId], [amRMServiceAddress], [clientRMServiceAddress],
               [rmAdminServiceAddress], [rmWebServiceAddress], [lastHeartBeat],
               [state], [lastStartTime], [capability]
        FROM [dbo].[membership]
    END TRY

    BEGIN CATCH
        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_getSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_getSubCluster]
    @subClusterId VARCHAR(256),
    @amRMServiceAddress VARCHAR(256) OUTPUT,
    @clientRMServiceAddress VARCHAR(256) OUTPUT,
    @rmAdminServiceAddress VARCHAR(256) OUTPUT,
    @rmWebServiceAddress VARCHAR(256) OUTPUT,
    @lastHeartbeat DATETIME2 OUTPUT,
    @state VARCHAR(256) OUTPUT,
    @lastStartTime BIGINT OUTPUT,
    @capability VARCHAR(6000) OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            SELECT @subClusterId = [subClusterId],
                   @amRMServiceAddress = [amRMServiceAddress],
                   @clientRMServiceAddress = [clientRMServiceAddress],
                   @rmAdminServiceAddress = [rmAdminServiceAddress],
                   @rmWebServiceAddress = [rmWebServiceAddress],
                   @lastHeartBeat = [lastHeartBeat],
                   @state = [state],
                   @lastStartTime = [lastStartTime],
                   @capability = [capability]
            FROM [dbo].[membership]
            WHERE [subClusterId] = @subClusterId

        COMMIT TRAN
    END TRY

    BEGIN CATCH
        ROLLBACK TRAN

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO


IF OBJECT_ID ( '[sp_subClusterHeartbeat]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_subClusterHeartbeat];
GO

CREATE PROCEDURE [dbo].[sp_subClusterHeartbeat]
    @subClusterId VARCHAR(256),
    @state VARCHAR(256),
    @capability VARCHAR(6000),
    @rowCount int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            UPDATE [dbo].[membership]
            SET [state] = @state,
                [lastHeartbeat] = GETUTCDATE(),
                [capability] = @capability
            WHERE [subClusterId] = @subClusterId;
            SELECT @rowCount = @@ROWCOUNT;

        COMMIT TRAN
    END TRY

    BEGIN CATCH
        ROLLBACK TRAN

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_deregisterSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_deregisterSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_deregisterSubCluster]
    @subClusterId VARCHAR(256),
    @state VARCHAR(256),
    @rowCount int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            UPDATE [dbo].[membership]
            SET [state] = @state
            WHERE [subClusterId] = @subClusterId;
            SELECT @rowCount = @@ROWCOUNT;

        COMMIT TRAN
    END TRY

    BEGIN CATCH
        ROLLBACK TRAN

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_setPolicyConfiguration]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_setPolicyConfiguration];
GO

CREATE PROCEDURE [dbo].[sp_setPolicyConfiguration]
    @queue VARCHAR(256),
    @policyType VARCHAR(256),
    @params VARBINARY(512),
    @rowCount int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[policies]
            WHERE [queue] = @queue;
            INSERT INTO [dbo].[policies] (
                [queue],
                [policyType],
                [params])
            VALUES (
                @queue,
                @policyType,
                @params);
            SELECT @rowCount = @@ROWCOUNT;

        COMMIT TRAN
    END TRY

    BEGIN CATCH
        ROLLBACK TRAN

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_getPolicyConfiguration]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getPolicyConfiguration];
GO

CREATE PROCEDURE [dbo].[sp_getPolicyConfiguration]
    @queue VARCHAR(256),
    @policyType VARCHAR(256) OUTPUT,
    @params VARBINARY(6000) OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        SELECT @policyType = [policyType],
               @params = [params]
        FROM [dbo].[policies]
        WHERE [queue] = @queue

    END TRY

    BEGIN CATCH

        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO

IF OBJECT_ID ( '[sp_getPoliciesConfigurations]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getPoliciesConfigurations];
GO

CREATE PROCEDURE [dbo].[sp_getPoliciesConfigurations]
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        SELECT  [queue], [policyType], [params] FROM [dbo].[policies]
    END TRY

    BEGIN CATCH
        SET @errorMessage = dbo.func_FormatErrorMessage(ERROR_MESSAGE(), ERROR_LINE())

        /*  raise error and terminate the execution */
        RAISERROR(@errorMessage, --- Error Message
            1, -- Severity
            -1 -- State
        ) WITH log
    END CATCH
END;
GO