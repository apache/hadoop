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
    @applicationId_IN VARCHAR(64),
    @homeSubCluster_IN VARCHAR(256),
    @applicationContext_IN VARBINARY(MAX),
    @storedHomeSubCluster_OUT VARCHAR(256) OUTPUT,
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN
            -- If application to sub-cluster map doesn't exist, insert it.
            -- Otherwise don't change the current mapping.
            IF NOT EXISTS (SELECT TOP 1 *
                       FROM [dbo].[applicationsHomeSubCluster]
                       WHERE [applicationId] = @applicationId_IN)

                INSERT INTO [dbo].[applicationsHomeSubCluster] (
                    [applicationId],
                    [homeSubCluster],
                    [createTime],
                    [applicationContext])
                VALUES (
                    @applicationId_IN,
                    @homeSubCluster_IN,
                    GETUTCDATE(),
                    @applicationContext_IN);
            -- End of the IF block

            SELECT @rowCount_OUT = @@ROWCOUNT;

            SELECT @storedHomeSubCluster_OUT = [homeSubCluster]
            FROM [dbo].[applicationsHomeSubCluster]
            WHERE [applicationId] = @applicationId_IN;

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
    @applicationId_IN VARCHAR(64),
    @homeSubCluster_IN VARCHAR(256),
    @applicationContext_IN VARBINARY(MAX),
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            UPDATE [dbo].[applicationsHomeSubCluster]
            SET [homeSubCluster] = @homeSubCluster_IN,
                [applicationContext] = @applicationContext_IN
            WHERE [applicationId] = @applicationId_IN;
            SELECT @rowCount_OUT = @@ROWCOUNT;

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
    @limit_IN int,
    @homeSubCluster_IN VARCHAR(256)
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        SELECT
            [applicationId],
            [homeSubCluster],
            [createTime]
        FROM(SELECT
                 [applicationId],
                 [homeSubCluster],
                 [createTime],
                 row_number() over(order by [createTime] desc) AS app_rank
             FROM [dbo].[applicationsHomeSubCluster]
             WHERE [homeSubCluster] = @homeSubCluster_IN OR @homeSubCluster_IN = '') AS applicationsHomeSubCluster
        WHERE app_rank <= @limit_IN;

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
    @applicationId_IN VARCHAR(64),
    @homeSubCluster_OUT VARCHAR(256) OUTPUT,
    @createTime_OUT datetime OUT,
    @applicationContext_OUT VARBINARY(MAX) OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        SELECT @homeSubCluster_OUT = [homeSubCluster],
            @createTime_OUT = [createTime],
            @applicationContext_OUT = [applicationContext]
        FROM [dbo].[applicationsHomeSubCluster]
        WHERE [applicationId] = @applicationId_IN;

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
    @applicationId_IN VARCHAR(64),
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[applicationsHomeSubCluster]
            WHERE [applicationId] = @applicationId_IN;
            SELECT @rowCount_OUT = @@ROWCOUNT;

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
    @subClusterId_IN VARCHAR(256),
    @amRMServiceAddress_IN VARCHAR(256),
    @clientRMServiceAddress_IN VARCHAR(256),
    @rmAdminServiceAddress_IN VARCHAR(256),
    @rmWebServiceAddress_IN VARCHAR(256),
    @state_IN VARCHAR(32),
    @lastStartTime_IN BIGINT,
    @capability_IN VARCHAR(6000),
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[membership]
            WHERE [subClusterId] = @subClusterId_IN;
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
                @subClusterId_IN,
                @amRMServiceAddress_IN,
                @clientRMServiceAddress_IN,
                @rmAdminServiceAddress_IN,
                @rmWebServiceAddress_IN,
                GETUTCDATE(),
                @state_IN,
                @lastStartTime_IN,
                @capability_IN);
            SELECT @rowCount_OUT = @@ROWCOUNT;

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
    @subClusterId_IN VARCHAR(256),
    @amRMServiceAddress_OUT VARCHAR(256) OUTPUT,
    @clientRMServiceAddress_OUT VARCHAR(256) OUTPUT,
    @rmAdminServiceAddress_OUT VARCHAR(256) OUTPUT,
    @rmWebServiceAddress_OUT VARCHAR(256) OUTPUT,
    @lastHeartBeat_OUT DATETIME2 OUTPUT,
    @state_OUT VARCHAR(256) OUTPUT,
    @lastStartTime_OUT BIGINT OUTPUT,
    @capability_OUT VARCHAR(6000) OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            SELECT @subClusterId_IN = [subClusterId],
                   @amRMServiceAddress_OUT = [amRMServiceAddress],
                   @clientRMServiceAddress_OUT = [clientRMServiceAddress],
                   @rmAdminServiceAddress_OUT = [rmAdminServiceAddress],
                   @rmWebServiceAddress_OUT = [rmWebServiceAddress],
                   @lastHeartBeat_OUT = [lastHeartBeat],
                   @state_OUT = [state],
                   @lastStartTime_OUT = [lastStartTime],
                   @capability_OUT = [capability]
            FROM [dbo].[membership]
            WHERE [subClusterId] = @subClusterId_IN

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
    @subClusterId_IN VARCHAR(256),
    @state_IN VARCHAR(256),
    @capability_IN VARCHAR(6000),
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            UPDATE [dbo].[membership]
            SET [state] = @state_IN,
                [lastHeartbeat] = GETUTCDATE(),
                [capability] = @capability_IN
            WHERE [subClusterId] = @subClusterId_IN;
            SELECT @rowCount_OUT = @@ROWCOUNT;

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
    @subClusterId_IN VARCHAR(256),
    @state_IN VARCHAR(256),
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            UPDATE [dbo].[membership]
            SET [state] = @state_IN
            WHERE [subClusterId] = @subClusterId_IN;
            SELECT @rowCount_OUT = @@ROWCOUNT;

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
    @queue_IN VARCHAR(256),
    @policyType_IN VARCHAR(256),
    @params_IN VARBINARY(512),
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[policies]
            WHERE [queue] = @queue_IN;
            INSERT INTO [dbo].[policies] (
                [queue],
                [policyType],
                [params])
            VALUES (
                @queue_IN,
                @policyType_IN,
                @params_IN);
            SELECT @rowCount_OUT = @@ROWCOUNT;

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
    @queue_IN VARCHAR(256),
    @policyType_OUT VARCHAR(256) OUTPUT,
    @params_OUT VARBINARY(6000) OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        SELECT @policyType_OUT = [policyType],
               @params_OUT = [params]
        FROM [dbo].[policies]
        WHERE [queue] = @queue_IN

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

IF OBJECT_ID ( '[sp_addReservationHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_addReservationHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_addReservationHomeSubCluster]
    @reservationId_IN VARCHAR(128),
    @homeSubCluster_IN VARCHAR(256),
    @storedHomeSubCluster_OUT VARCHAR(256) OUTPUT,
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN
            -- If application to sub-cluster map doesn't exist, insert it.
            -- Otherwise don't change the current mapping.
            IF NOT EXISTS (SELECT TOP 1 *
                       FROM [dbo].[reservationsHomeSubCluster]
                       WHERE [reservationId] = @reservationId_IN)

                INSERT INTO [dbo].[reservationsHomeSubCluster] (
                    [reservationId],
                    [homeSubCluster])
                VALUES (
                    @reservationId_IN,
                    @homeSubCluster_IN);
            -- End of the IF block

            SELECT @rowCount_OUT = @@ROWCOUNT;

            SELECT @storedHomeSubCluster_OUT = [homeSubCluster]
            FROM [dbo].[reservationsHomeSubCluster]
            WHERE [reservationId] = @reservationId_IN;

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

IF OBJECT_ID ( '[sp_updateReservationHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_updateReservationHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_updateReservationHomeSubCluster]
    @reservationId_IN VARCHAR(128),
    @homeSubCluster_IN VARCHAR(256),
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            UPDATE [dbo].[reservationsHomeSubCluster]
            SET [homeSubCluster] = @homeSubCluster_IN
            WHERE [reservationId] = @reservationId_IN;
            SELECT @rowCount_OUT = @@ROWCOUNT;

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

IF OBJECT_ID ( '[sp_getReservationsHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getReservationsHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_getReservationsHomeSubCluster]
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        SELECT [reservationId], [homeSubCluster], [createTime]
        FROM [dbo].[reservationsHomeSubCluster]
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

IF OBJECT_ID ( '[sp_getReservationHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getReservationHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_getReservationHomeSubCluster]
    @reservationId_IN VARCHAR(128),
    @homeSubCluster_OUT VARCHAR(256) OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        SELECT @homeSubCluster_OUT = [homeSubCluster]
        FROM [dbo].[reservationsHomeSubCluster]
        WHERE [reservationId] = @reservationId_IN;

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

IF OBJECT_ID ( '[sp_deleteReservationHomeSubCluster]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_deleteReservationHomeSubCluster];
GO

CREATE PROCEDURE [dbo].[sp_deleteReservationHomeSubCluster]
    @reservationId_IN VARCHAR(128),
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[reservationsHomeSubCluster]
            WHERE [reservationId] = @reservationId_IN;
            SELECT @rowCount_OUT = @@ROWCOUNT;

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

IF OBJECT_ID ( '[sp_addMasterKey]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_addMasterKey];
GO

CREATE PROCEDURE [dbo].[sp_addMasterKey]
    @keyId_IN BIGINT,
    @masterKey_IN VARCHAR(1024),
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN
            -- If application to sub-cluster map doesn't exist, insert it.
            -- Otherwise don't change the current mapping.
            IF NOT EXISTS (SELECT TOP 1 *
                       FROM [dbo].[masterKeys]
                       WHERE [keyId] = @keyId_IN)

                INSERT INTO [dbo].[masterKeys] (
                    [keyId],
                    [masterKey])
                VALUES (
                    @keyId_IN,
                    @masterKey_IN);
            -- End of the IF block

            SELECT @rowCount_OUT = @@ROWCOUNT;

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

IF OBJECT_ID ( '[sp_getMasterKey]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getMasterKey];
GO

CREATE PROCEDURE [dbo].[sp_getMasterKey]
    @keyId_IN bigint,
    @masterKey_OUT VARCHAR(1024) OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        SELECT @masterKey_OUT = [masterKey]
        FROM [dbo].[masterKeys]
        WHERE [keyId] = @keyId_IN;

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

IF OBJECT_ID ( '[sp_deleteMasterKey]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_deleteMasterKey];
GO

CREATE PROCEDURE [dbo].[sp_deleteMasterKey]
    @keyId_IN bigint,
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[masterKeys]
            WHERE [keyId] = @keyId_IN;
            SELECT @rowCount_OUT = @@ROWCOUNT;

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

IF OBJECT_ID ( '[sp_addDelegationToken]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_addDelegationToken];
GO

CREATE PROCEDURE [dbo].[sp_addDelegationToken]
    @sequenceNum_IN BIGINT,
    @tokenIdent_IN VARCHAR(1024),
    @token_IN VARCHAR(1024),
    @renewDate_IN BIGINT,
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN
            -- If application to sub-cluster map doesn't exist, insert it.
            -- Otherwise don't change the current mapping.
            IF NOT EXISTS (SELECT TOP 1 *
                       FROM [dbo].[delegationTokens]
                       WHERE [sequenceNum] = @sequenceNum_IN)

                INSERT INTO [dbo].[delegationTokens] (
                    [sequenceNum],
                    [tokenIdent],
                    [token],
                    [renewDate])
                VALUES (
                    @sequenceNum_IN,
                    @tokenIdent_IN,
                    @token_IN,
                    @renewDate_IN);
            -- End of the IF block

            SELECT @rowCount_OUT = @@ROWCOUNT;

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

IF OBJECT_ID ( '[sp_getDelegationToken]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getDelegationToken];
GO

CREATE PROCEDURE [dbo].[sp_getDelegationToken]
    @sequenceNum_IN BIGINT,
    @tokenIdent_OUT VARCHAR(1024) OUTPUT,
    @token_OUT VARCHAR(1024) OUTPUT,
    @renewDate_OUT BIGINT OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        SELECT @tokenIdent_OUT = [tokenIdent],
               @token_OUT = [token],
               @renewDate_OUT = [renewDate]
        FROM [dbo].[delegationTokens]
        WHERE [sequenceNum] = @sequenceNum_IN;

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

IF OBJECT_ID ( '[sp_updateDelegationToken]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_updateDelegationToken];
GO

CREATE PROCEDURE [dbo].[sp_updateDelegationToken]
    @sequenceNum_IN BIGINT,
    @tokenIdent_IN VARCHAR(1024),
    @token_IN VARCHAR(1024),
    @renewDate_IN BIGINT,
    @rowCount_OUT BIGINT OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        UPDATE [dbo].[delegationTokens]
           SET [tokenIdent] = @tokenIdent_IN,
               [token] = @token_IN,
               [renewDate] = @renewDate_IN
        WHERE [sequenceNum] = @sequenceNum_IN;
        SELECT @rowCount_OUT = @@ROWCOUNT;

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

IF OBJECT_ID ( '[sp_deleteDelegationToken]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_deleteDelegationToken];
GO

CREATE PROCEDURE [dbo].[sp_deleteDelegationToken]
    @sequenceNum_IN bigint,
    @rowCount_OUT int OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[delegationTokens]
            WHERE [sequenceNum] = @sequenceNum_IN;
            SELECT @rowCount_OUT = @@ROWCOUNT;

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

IF OBJECT_ID ( '[sp_storeVersion]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_storeVersion];
GO

CREATE PROCEDURE [dbo].[sp_storeVersion]
    @fedVersion_IN VARBINARY(1024),
    @versionComment_IN VARCHAR(255),
    @rowCount_OUT BIGINT OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY
        BEGIN TRAN

            DELETE FROM [dbo].[versions];
            INSERT INTO [dbo].[versions] (
                [fedVersion],
                [versionComment])
            VALUES (
                @fedVersion_IN,
                @versionComment_IN);
            SELECT @rowCount_OUT = @@ROWCOUNT;
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

IF OBJECT_ID ( '[sp_getVersion]', 'P' ) IS NOT NULL
    DROP PROCEDURE [sp_getVersion];
GO

CREATE PROCEDURE [dbo].[sp_getVersion]
    @fedVersion_OUT VARCHAR(1024) OUTPUT,
    @versionComment_OUT VARCHAR(255) OUTPUT
AS BEGIN
    DECLARE @errorMessage nvarchar(4000)

    BEGIN TRY

        SELECT @fedVersion_OUT = [fedVersion],
               @versionComment_OUT = [versionComment]
        FROM [dbo].[versions]
        LIMIT 1;

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