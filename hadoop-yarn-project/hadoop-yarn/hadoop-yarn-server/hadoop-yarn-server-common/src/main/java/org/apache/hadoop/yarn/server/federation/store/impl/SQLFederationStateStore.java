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

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Blob;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.metrics.FederationStateStoreClientMetrics;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationApplicationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationMembershipStateStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationPolicyStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationReservationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationRouterRMTokenInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.server.federation.store.sql.FederationSQLOutParameter;
import org.apache.hadoop.yarn.server.federation.store.sql.FederationQueryRunner;
import org.apache.hadoop.yarn.server.federation.store.sql.RouterMasterKeyHandler;
import org.apache.hadoop.yarn.server.federation.store.sql.RouterStoreTokenHandler;
import org.apache.hadoop.yarn.server.federation.store.sql.RowCountHandler;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import com.zaxxer.hikari.HikariDataSource;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.BIGINT;
import static org.apache.hadoop.yarn.server.federation.store.sql.FederationQueryRunner.YARN_ROUTER_CURRENT_KEY_ID;
import static org.apache.hadoop.yarn.server.federation.store.sql.FederationQueryRunner.YARN_ROUTER_SEQUENCE_NUM;
import static org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils.convertMasterKeyToDelegationKey;

/**
 * SQL implementation of {@link FederationStateStore}.
 */
public class SQLFederationStateStore implements FederationStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(SQLFederationStateStore.class);

  // Stored procedures patterns

  private static final String CALL_SP_REGISTER_SUBCLUSTER =
      "{call sp_registerSubCluster(?, ?, ?, ?, ?, ?, ?, ?, ?)}";

  private static final String CALL_SP_DEREGISTER_SUBCLUSTER =
      "{call sp_deregisterSubCluster(?, ?, ?)}";

  private static final String CALL_SP_GET_SUBCLUSTER =
      "{call sp_getSubCluster(?, ?, ?, ?, ?, ?, ?, ?, ?)}";

  private static final String CALL_SP_GET_SUBCLUSTERS =
      "{call sp_getSubClusters()}";

  private static final String CALL_SP_SUBCLUSTER_HEARTBEAT =
      "{call sp_subClusterHeartbeat(?, ?, ?, ?)}";

  private static final String CALL_SP_ADD_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_addApplicationHomeSubCluster(?, ?, ?, ?, ?)}";

  private static final String CALL_SP_UPDATE_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_updateApplicationHomeSubCluster(?, ?, ?, ?)}";

  private static final String CALL_SP_DELETE_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_deleteApplicationHomeSubCluster(?, ?)}";

  private static final String CALL_SP_GET_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_getApplicationHomeSubCluster(?, ?, ?, ?)}";

  private static final String CALL_SP_GET_APPLICATIONS_HOME_SUBCLUSTER =
      "{call sp_getApplicationsHomeSubCluster(?, ?)}";

  private static final String CALL_SP_SET_POLICY_CONFIGURATION =
      "{call sp_setPolicyConfiguration(?, ?, ?, ?)}";

  private static final String CALL_SP_GET_POLICY_CONFIGURATION =
      "{call sp_getPolicyConfiguration(?, ?, ?)}";

  private static final String CALL_SP_GET_POLICIES_CONFIGURATIONS =
      "{call sp_getPoliciesConfigurations()}";

  protected static final String CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER =
      "{call sp_addReservationHomeSubCluster(?, ?, ?, ?)}";

  protected static final String CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER =
      "{call sp_getReservationHomeSubCluster(?, ?)}";

  protected static final String CALL_SP_GET_RESERVATIONS_HOME_SUBCLUSTER =
      "{call sp_getReservationsHomeSubCluster()}";

  protected static final String CALL_SP_DELETE_RESERVATION_HOME_SUBCLUSTER =
      "{call sp_deleteReservationHomeSubCluster(?, ?)}";

  protected static final String CALL_SP_UPDATE_RESERVATION_HOME_SUBCLUSTER =
      "{call sp_updateReservationHomeSubCluster(?, ?, ?)}";

  protected static final String CALL_SP_ADD_MASTERKEY =
      "{call sp_addMasterKey(?, ?, ?)}";

  protected static final String CALL_SP_GET_MASTERKEY =
      "{call sp_getMasterKey(?, ?)}";

  protected static final String CALL_SP_DELETE_MASTERKEY =
      "{call sp_deleteMasterKey(?, ?)}";

  protected static final String CALL_SP_ADD_DELEGATIONTOKEN =
      "{call sp_addDelegationToken(?, ?, ?, ?, ?)}";

  protected static final String CALL_SP_GET_DELEGATIONTOKEN =
      "{call sp_getDelegationToken(?, ?, ?, ?)}";

  protected static final String CALL_SP_UPDATE_DELEGATIONTOKEN =
      "{call sp_updateDelegationToken(?, ?, ?, ?, ?)}";

  protected static final String CALL_SP_DELETE_DELEGATIONTOKEN =
      "{call sp_deleteDelegationToken(?, ?)}";

  private static final String CALL_SP_STORE_VERSION =
      "{call sp_storeVersion(?, ?, ?)}";

  private static final String CALL_SP_LOAD_VERSION =
      "{call sp_getVersion(?, ?)}";

  private Calendar utcCalendar =
      Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  // SQL database configurations

  private String userName;
  private String password;
  private String driverClass;
  private String url;
  private int maximumPoolSize;
  private HikariDataSource dataSource = null;
  private final Clock clock = new MonotonicClock();
  @VisibleForTesting
  private Connection conn = null;
  private int maxAppsInStateStore;
  private int minimumIdle;
  private String dataSourcePoolName;
  private long maxLifeTime;
  private long idleTimeout;
  private long connectionTimeout;

  protected static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 1);

  @Override
  public void init(Configuration conf) throws YarnException {
    // Database connection configuration
    driverClass = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_JDBC_CLASS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_SQL_JDBC_CLASS);
    maximumPoolSize = conf.getInt(YarnConfiguration.FEDERATION_STATESTORE_SQL_MAXCONNECTIONS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_SQL_MAXCONNECTIONS);
    minimumIdle = conf.getInt(YarnConfiguration.FEDERATION_STATESTORE_SQL_MINIMUMIDLE,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_SQL_MINIMUMIDLE);
    dataSourcePoolName = conf.get(YarnConfiguration.FEDERATION_STATESTORE_POOL_NAME,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_POOL_NAME);
    maxLifeTime = conf.getTimeDuration(YarnConfiguration.FEDERATION_STATESTORE_CONN_MAX_LIFE_TIME,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CONN_MAX_LIFE_TIME, TimeUnit.MILLISECONDS);
    idleTimeout = conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_CONN_IDLE_TIMEOUT_TIME,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CONN_IDLE_TIMEOUT_TIME,
        TimeUnit.MILLISECONDS);
    connectionTimeout = conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_CONNECTION_TIMEOUT,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CONNECTION_TIMEOUT_TIME,
        TimeUnit.MILLISECONDS);

    // An helper method avoids to assign a null value to these property
    userName = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_USERNAME);
    password = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_PASSWORD);
    url = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_URL);

    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException e) {
      FederationStateStoreUtils.logAndThrowException(LOG, "Driver class not found.", e);
    }

    // Create the data source to pool connections in a thread-safe manner
    dataSource = new HikariDataSource();
    dataSource.setDataSourceClassName(driverClass);
    FederationStateStoreUtils.setUsername(dataSource, userName);
    FederationStateStoreUtils.setPassword(dataSource, password);
    FederationStateStoreUtils.setProperty(dataSource,
        FederationStateStoreUtils.FEDERATION_STORE_URL, url);

    dataSource.setMaximumPoolSize(maximumPoolSize);
    dataSource.setPoolName(dataSourcePoolName);
    dataSource.setMinimumIdle(minimumIdle);
    dataSource.setMaxLifetime(maxLifeTime);
    dataSource.setIdleTimeout(idleTimeout);
    dataSource.setConnectionTimeout(connectionTimeout);

    LOG.info("Initialized connection pool to the Federation StateStore database at address: {}.",
        url);

    try {
      conn = getConnection();
      LOG.debug("Connection created");
    } catch (SQLException e) {
      FederationStateStoreUtils.logAndThrowRetriableException(LOG, "Not able to get Connection", e);
    }

    maxAppsInStateStore = conf.getInt(
        YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_MAX_APPLICATIONS);
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest registerSubClusterRequest) throws YarnException {

    // Input validator
    FederationMembershipStateStoreInputValidator.validate(registerSubClusterRequest);

    CallableStatement cstmt = null;

    SubClusterInfo subClusterInfo = registerSubClusterRequest.getSubClusterInfo();
    SubClusterId subClusterId = subClusterInfo.getSubClusterId();

    try {
      cstmt = getCallableStatement(CALL_SP_REGISTER_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString("subClusterId_IN", subClusterId.getId());
      cstmt.setString("amRMServiceAddress_IN", subClusterInfo.getAMRMServiceAddress());
      cstmt.setString("clientRMServiceAddress_IN", subClusterInfo.getClientRMServiceAddress());
      cstmt.setString("rmAdminServiceAddress_IN", subClusterInfo.getRMAdminServiceAddress());
      cstmt.setString("rmWebServiceAddress_IN", subClusterInfo.getRMWebServiceAddress());
      cstmt.setString("state_IN", subClusterInfo.getState().toString());
      cstmt.setLong("lastStartTime_IN", subClusterInfo.getLastStartTime());
      cstmt.setString("capability_IN", subClusterInfo.getCapability());
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not add a new subcluster into FederationStateStore
      int rowCount = cstmt.getInt("rowCount_OUT");
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "SubCluster %s was not registered into the StateStore.", subClusterId);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (rowCount != 1) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during registration of SubCluster %s into the StateStore",
            subClusterId);
      }

      LOG.info("Registered the SubCluster {} into the StateStore.", subClusterId);
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e,
          LOG, "Unable to register the SubCluster %s into the StateStore.", subClusterId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }

    return SubClusterRegisterResponse.newInstance();
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest subClusterDeregisterRequest) throws YarnException {

    // Input validator
    FederationMembershipStateStoreInputValidator.validate(subClusterDeregisterRequest);

    CallableStatement cstmt = null;

    SubClusterId subClusterId = subClusterDeregisterRequest.getSubClusterId();
    SubClusterState state = subClusterDeregisterRequest.getState();

    try {
      cstmt = getCallableStatement(CALL_SP_DEREGISTER_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString("subClusterId_IN", subClusterId.getId());
      cstmt.setString("state_IN", state.toString());
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not deregister the subcluster into FederationStateStore
      int rowCount = cstmt.getInt("rowCount_OUT");
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "SubCluster %s not found.", subClusterId);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (rowCount != 1) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during deregistration of SubCluster %s from the StateStore.",
            subClusterId);
      }
      LOG.info("Deregistered the SubCluster {} state to {}.", subClusterId, state.toString());
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to deregister the sub-cluster %s state to %s.", subClusterId, state.toString());
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    return SubClusterDeregisterResponse.newInstance();
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest subClusterHeartbeatRequest) throws YarnException {

    // Input validator
    FederationMembershipStateStoreInputValidator.validate(subClusterHeartbeatRequest);

    CallableStatement cstmt = null;

    SubClusterId subClusterId = subClusterHeartbeatRequest.getSubClusterId();
    SubClusterState state = subClusterHeartbeatRequest.getState();

    try {
      cstmt = getCallableStatement(CALL_SP_SUBCLUSTER_HEARTBEAT);

      // Set the parameters for the stored procedure
      cstmt.setString("subClusterId_IN", subClusterId.getId());
      cstmt.setString("state_IN", state.toString());
      cstmt.setString("capability_IN", subClusterHeartbeatRequest.getCapability());
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not update the subcluster into FederationStateStore
      int rowCount = cstmt.getInt("rowCount_OUT");
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "SubCluster %s does not exist; cannot heartbeat.", subClusterId);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (rowCount != 1) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during the heartbeat of SubCluster %s.", subClusterId);
      }

      LOG.info("Heartbeated the StateStore for the specified SubCluster {}.", subClusterId);
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to heartbeat the StateStore for the specified SubCluster %s.", subClusterId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    return SubClusterHeartbeatResponse.newInstance();
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest subClusterRequest) throws YarnException {

    // Input validator
    FederationMembershipStateStoreInputValidator.validate(subClusterRequest);

    CallableStatement cstmt = null;

    SubClusterInfo subClusterInfo = null;
    SubClusterId subClusterId = subClusterRequest.getSubClusterId();

    try {
      cstmt = getCallableStatement(CALL_SP_GET_SUBCLUSTER);
      cstmt.setString("subClusterId_IN", subClusterId.getId());

      // Set the parameters for the stored procedure
      cstmt.registerOutParameter("amRMServiceAddress_OUT", VARCHAR);
      cstmt.registerOutParameter("clientRMServiceAddress_OUT", VARCHAR);
      cstmt.registerOutParameter("rmAdminServiceAddress_OUT", VARCHAR);
      cstmt.registerOutParameter("rmWebServiceAddress_OUT", VARCHAR);
      cstmt.registerOutParameter("lastHeartBeat_OUT", java.sql.Types.TIMESTAMP);
      cstmt.registerOutParameter("state_OUT", VARCHAR);
      cstmt.registerOutParameter("lastStartTime_OUT", BIGINT);
      cstmt.registerOutParameter("capability_OUT", VARCHAR);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.execute();
      long stopTime = clock.getTime();

      String amRMAddress = cstmt.getString("amRMServiceAddress_OUT");
      String clientRMAddress = cstmt.getString("clientRMServiceAddress_OUT");
      String rmAdminAddress = cstmt.getString("rmAdminServiceAddress_OUT");
      String webAppAddress = cstmt.getString("rmWebServiceAddress_OUT");

      // first check if the subCluster exists
      if((amRMAddress == null) || (clientRMAddress == null)) {
        LOG.warn("The queried SubCluster: {} does not exist.", subClusterId);
        return null;
      }

      Timestamp heartBeatTimeStamp = cstmt.getTimestamp("lastHeartBeat_OUT", utcCalendar);
      long lastHeartBeat = heartBeatTimeStamp != null ? heartBeatTimeStamp.getTime() : 0;

      SubClusterState state = SubClusterState.fromString(cstmt.getString("state_OUT"));
      long lastStartTime = cstmt.getLong("lastStartTime_OUT");
      String capability = cstmt.getString("capability_OUT");

      subClusterInfo = SubClusterInfo.newInstance(subClusterId, amRMAddress,
          clientRMAddress, rmAdminAddress, webAppAddress, lastHeartBeat, state,
          lastStartTime, capability);

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

      // Check if the output it is a valid subcluster
      try {
        FederationMembershipStateStoreInputValidator.checkSubClusterInfo(subClusterInfo);
      } catch (FederationStateStoreInvalidInputException e) {
        FederationStateStoreUtils.logAndThrowStoreException(e, LOG,
            "SubCluster %s does not exist.", subClusterId);
      }
      LOG.debug("Got the information about the specified SubCluster {}", subClusterInfo);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to obtain the SubCluster information for %s.", subClusterId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    return GetSubClusterInfoResponse.newInstance(subClusterInfo);
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest subClustersRequest) throws YarnException {
    CallableStatement cstmt = null;
    ResultSet rs = null;
    List<SubClusterInfo> subClusters = new ArrayList<>();

    try {
      cstmt = getCallableStatement(CALL_SP_GET_SUBCLUSTERS);

      // Execute the query
      long startTime = clock.getTime();
      rs = cstmt.executeQuery();
      long stopTime = clock.getTime();

      while (rs.next()) {

        // Extract the output for each tuple
        String subClusterName = rs.getString("subClusterId");
        String amRMAddress = rs.getString("amRMServiceAddress");
        String clientRMAddress = rs.getString("clientRMServiceAddress");
        String rmAdminAddress = rs.getString("rmAdminServiceAddress");
        String webAppAddress = rs.getString("rmWebServiceAddress");
        long lastHeartBeat = rs.getTimestamp("lastHeartBeat", utcCalendar).getTime();
        SubClusterState state = SubClusterState.fromString(rs.getString("state"));
        long lastStartTime = rs.getLong("lastStartTime");
        String capability = rs.getString("capability");

        SubClusterId subClusterId = SubClusterId.newInstance(subClusterName);
        SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
            amRMAddress, clientRMAddress, rmAdminAddress, webAppAddress,
            lastHeartBeat, state, lastStartTime, capability);

        FederationStateStoreClientMetrics
            .succeededStateStoreCall(stopTime - startTime);

        // Check if the output it is a valid subcluster
        try {
          FederationMembershipStateStoreInputValidator.checkSubClusterInfo(subClusterInfo);
        } catch (FederationStateStoreInvalidInputException e) {
          FederationStateStoreUtils.logAndThrowStoreException(e, LOG,
              "SubCluster %s is not valid.", subClusterId);
        }

        // Filter the inactive
        if (!subClustersRequest.getFilterInactiveSubClusters()
            || subClusterInfo.getState().isActive()) {
          subClusters.add(subClusterInfo);
        }
      }

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the information for all the SubClusters ", e);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt, null, rs);
    }
    return GetSubClustersInfoResponse.newInstance(subClusters);
  }

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {

    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;

    String subClusterHome = null;
    ApplicationHomeSubCluster applicationHomeSubCluster =
        request.getApplicationHomeSubCluster();
    ApplicationId appId = applicationHomeSubCluster.getApplicationId();
    SubClusterId subClusterId = applicationHomeSubCluster.getHomeSubCluster();
    ApplicationSubmissionContext appSubmissionContext =
        applicationHomeSubCluster.getApplicationSubmissionContext();

    try {
      cstmt = getCallableStatement(CALL_SP_ADD_APPLICATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString("applicationId_IN", appId.toString());
      cstmt.setString("homeSubCluster_IN", subClusterId.getId());
      if (appSubmissionContext != null) {
        cstmt.setBlob("applicationContext_IN", new ByteArrayInputStream(
            ((ApplicationSubmissionContextPBImpl) appSubmissionContext).getProto().toByteArray()));
      } else {
        cstmt.setNull("applicationContext_IN", Types.BLOB);
      }
      cstmt.registerOutParameter("storedHomeSubCluster_OUT", VARCHAR);
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      subClusterHome = cstmt.getString("storedHomeSubCluster_OUT");
      SubClusterId subClusterIdHome = SubClusterId.newInstance(subClusterHome);

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

      // For failover reason, we check the returned SubClusterId.
      // If it is equal to the subclusterId we sent, the call added the new
      // application into FederationStateStore. If the call returns a different
      // SubClusterId it means we already tried to insert this application but a
      // component (Router/StateStore/RM) failed during the submission.
      int rowCount = cstmt.getInt("rowCount_OUT");
      if (subClusterId.equals(subClusterIdHome)) {
        // Check the ROWCOUNT value, if it is equal to 0 it means the call
        // did not add a new application into FederationStateStore
        if (rowCount == 0) {
          LOG.info("The application {} was not inserted in the StateStore because it"
              + " was already present in SubCluster {}", appId, subClusterHome);
        } else if (cstmt.getInt("rowCount_OUT") != 1) {
          // Check the ROWCOUNT value, if it is different from 1 it means the
          // call had a wrong behavior. Maybe the database is not set correctly.
          FederationStateStoreUtils.logAndThrowStoreException(LOG,
              "Wrong behavior during the insertion of SubCluster %s.", subClusterId);
        }

        LOG.info("Insert into the StateStore the application: {} in SubCluster: {}.",
            appId, subClusterHome);
      } else {
        // Check the ROWCOUNT value, if it is different from 0 it means the call
        // did edited the table
        if (rowCount != 0) {
          FederationStateStoreUtils.logAndThrowStoreException(LOG,
              "The application %s does exist but was overwritten.", appId);
        }
        LOG.info("Application: {} already present with SubCluster: {}.", appId, subClusterHome);
      }

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to insert the newly generated application %s.", appId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }

    return AddApplicationHomeSubClusterResponse
        .newInstance(SubClusterId.newInstance(subClusterHome));
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {

    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;

    ApplicationHomeSubCluster applicationHomeSubCluster =
        request.getApplicationHomeSubCluster();
    ApplicationId appId = applicationHomeSubCluster.getApplicationId();
    SubClusterId subClusterId = applicationHomeSubCluster.getHomeSubCluster();
    ApplicationSubmissionContext appSubmissionContext =
        applicationHomeSubCluster.getApplicationSubmissionContext();

    try {
      cstmt = getCallableStatement(CALL_SP_UPDATE_APPLICATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString("applicationId_IN", appId.toString());
      cstmt.setString("homeSubCluster_IN", subClusterId.getId());
      if (appSubmissionContext != null) {
        cstmt.setBlob("applicationContext_IN", new ByteArrayInputStream(
            ((ApplicationSubmissionContextPBImpl) appSubmissionContext).getProto().toByteArray()));
      } else {
        cstmt.setNull("applicationContext_IN", Types.BLOB);
      }
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not update the application into FederationStateStore
      int rowCount = cstmt.getInt("rowCount_OUT");
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Application %s does not exist.", appId);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (cstmt.getInt("rowCount_OUT") != 1) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during the update of SubCluster %s.", subClusterId);
      }

      LOG.info("Update the SubCluster to {} for application {} in the StateStore",
          subClusterId, appId);
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to update the application %s.", appId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    return UpdateApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {
    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;

    SubClusterId homeRM = null;
    Long createTime = 0L;
    ApplicationId applicationId = request.getApplicationId();
    ApplicationSubmissionContext appSubmissionContext = null;

    try {
      cstmt = getCallableStatement(CALL_SP_GET_APPLICATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString("applicationId_IN", applicationId.toString());
      cstmt.registerOutParameter("homeSubCluster_OUT", VARCHAR);
      cstmt.registerOutParameter("createTime_OUT", java.sql.Types.TIMESTAMP);
      cstmt.registerOutParameter("applicationContext_OUT", Types.BLOB);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.execute();
      long stopTime = clock.getTime();

      String homeSubCluster = cstmt.getString("homeSubCluster_OUT");
      if (homeSubCluster != null) {
        homeRM = SubClusterId.newInstance(homeSubCluster);
      } else {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Application %s does not exist.", applicationId);
      }

      Timestamp createTimeStamp = cstmt.getTimestamp("createTime_OUT", utcCalendar);
      createTime = createTimeStamp != null ? createTimeStamp.getTime() : 0;

      Blob blobAppContextData = cstmt.getBlob("applicationContext_OUT");
      if (blobAppContextData != null && request.getContainsAppSubmissionContext()) {
        appSubmissionContext = new ApplicationSubmissionContextPBImpl(
            ApplicationSubmissionContextProto.parseFrom(blobAppContextData.getBinaryStream()));
      }

      LOG.debug("Got the information about the specified application {}."
          + " The AM is running in {}", applicationId, homeRM);

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to obtain the application information for the specified application %s.",
          applicationId);
    } catch (IOException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to obtain the application information for the specified application %s.",
          applicationId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    return GetApplicationHomeSubClusterResponse.newInstance(applicationId, homeRM,
        createTime, appSubmissionContext);
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {

    if (request == null) {
      throw new YarnException("Missing getApplicationsHomeSubCluster request");
    }

    CallableStatement cstmt = null;
    ResultSet rs = null;
    List<ApplicationHomeSubCluster> appsHomeSubClusters = new ArrayList<>();

    try {
      cstmt = getCallableStatement(CALL_SP_GET_APPLICATIONS_HOME_SUBCLUSTER);
      cstmt.setInt("limit_IN", maxAppsInStateStore);
      String homeSubClusterIN = StringUtils.EMPTY;
      SubClusterId subClusterId = request.getSubClusterId();
      if (subClusterId != null) {
        homeSubClusterIN = subClusterId.toString();
      }
      cstmt.setString("homeSubCluster_IN", homeSubClusterIN);

      // Execute the query
      long startTime = clock.getTime();
      rs = cstmt.executeQuery();
      long stopTime = clock.getTime();

      while (rs.next() && appsHomeSubClusters.size() <= maxAppsInStateStore) {

        // Extract the output for each tuple
        String applicationId = rs.getString("applicationId");
        String homeSubCluster = rs.getString("homeSubCluster");

        appsHomeSubClusters.add(ApplicationHomeSubCluster.newInstance(
            ApplicationId.fromString(applicationId),
            SubClusterId.newInstance(homeSubCluster)));
      }

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the information for all the applications ", e);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt, null, rs);
    }

    return GetApplicationsHomeSubClusterResponse.newInstance(appsHomeSubClusters);
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException {

    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    ApplicationId applicationId = request.getApplicationId();
    try {
      cstmt = getCallableStatement(CALL_SP_DELETE_APPLICATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString("applicationId_IN", applicationId.toString());
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not delete the application from FederationStateStore
      int rowCount = cstmt.getInt("rowCount_OUT");
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Application %s does not exist.", applicationId);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (cstmt.getInt("rowCount_OUT") != 1) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during deleting the application %s.", applicationId);
      }

      LOG.info("Delete from the StateStore the application: {}", request.getApplicationId());
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to delete the application %s.", applicationId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    return DeleteApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {

    // Input validator
    FederationPolicyStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    SubClusterPolicyConfiguration subClusterPolicyConfiguration = null;

    try {
      cstmt = getCallableStatement(CALL_SP_GET_POLICY_CONFIGURATION);

      // Set the parameters for the stored procedure
      cstmt.setString("queue_IN", request.getQueue());
      cstmt.registerOutParameter("policyType_OUT", VARCHAR);
      cstmt.registerOutParameter("params_OUT", java.sql.Types.VARBINARY);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check if the output it is a valid policy
      String policyType = cstmt.getString("policyType_OUT");
      byte[] param = cstmt.getBytes("params_OUT");
      if (policyType != null && param != null) {
        subClusterPolicyConfiguration = SubClusterPolicyConfiguration.newInstance(
            request.getQueue(), policyType, ByteBuffer.wrap(param));
        LOG.debug("Selected from StateStore the policy for the queue: {}",
            subClusterPolicyConfiguration);
      } else {
        LOG.warn("Policy for queue: {} does not exist.", request.getQueue());
        return null;
      }

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to select the policy for the queue : %s." + request.getQueue());
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    return GetSubClusterPolicyConfigurationResponse.newInstance(subClusterPolicyConfiguration);
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {

    // Input validator
    FederationPolicyStoreInputValidator.validate(request);

    CallableStatement cstmt = null;

    SubClusterPolicyConfiguration policyConf = request.getPolicyConfiguration();

    try {
      cstmt = getCallableStatement(CALL_SP_SET_POLICY_CONFIGURATION);

      // Set the parameters for the stored procedure
      cstmt.setString("queue_IN", policyConf.getQueue());
      cstmt.setString("policyType_IN", policyConf.getType());
      cstmt.setBytes("params_IN", getByteArray(policyConf.getParams()));
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not add a new policy into FederationStateStore
      int rowCount = cstmt.getInt("rowCount_OUT");
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "The policy %s was not insert into the StateStore.", policyConf.getQueue());
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (rowCount != 1) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during insert the policy %s.", policyConf.getQueue());
      }

      LOG.info("Insert into the state store the policy for the queue: {}.", policyConf.getQueue());
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to insert the newly generated policy for the queue : %s.", policyConf.getQueue());
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    return SetSubClusterPolicyConfigurationResponse.newInstance();
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {

    CallableStatement cstmt = null;
    ResultSet rs = null;
    List<SubClusterPolicyConfiguration> policyConfigurations = new ArrayList<>();

    try {
      cstmt = getCallableStatement(CALL_SP_GET_POLICIES_CONFIGURATIONS);

      // Execute the query
      long startTime = clock.getTime();
      rs = cstmt.executeQuery();
      long stopTime = clock.getTime();

      while (rs.next()) {
        // Extract the output for each tuple
        String queue = rs.getString("queue");
        String type = rs.getString("policyType");
        byte[] policyInfo = rs.getBytes("params");

        SubClusterPolicyConfiguration subClusterPolicyConfiguration =
            SubClusterPolicyConfiguration.newInstance(queue, type, ByteBuffer.wrap(policyInfo));
        policyConfigurations.add(subClusterPolicyConfiguration);
      }

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the policy information for all the queues.", e);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt, null, rs);
    }

    return GetSubClusterPoliciesConfigurationsResponse.newInstance(policyConfigurations);
  }

  @Override
  public Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  public Version loadVersion() throws Exception {
    return getVersion();
  }

  /**
   * Query the Version information of Federation from the database.
   *
   * @return Version Info.
   * @throws Exception Exception Information.
   */
  public Version getVersion() throws Exception {
    CallableStatement callableStatement = null;
    Version version = null;
    try {
      callableStatement = getCallableStatement(CALL_SP_LOAD_VERSION);

      // Set the parameters for the stored procedure
      callableStatement.registerOutParameter("fedVersion_OUT", java.sql.Types.VARBINARY);
      callableStatement.registerOutParameter("versionComment_OUT", VARCHAR);

      // Execute the query
      long startTime = clock.getTime();
      callableStatement.executeUpdate();
      long stopTime = clock.getTime();

      // Parsing version information.
      String versionComment = callableStatement.getString("versionComment_OUT");
      byte[] fedVersion = callableStatement.getBytes("fedVersion_OUT");
      if (versionComment != null && fedVersion != null) {
        version = new VersionPBImpl(YarnServerCommonProtos.VersionProto.parseFrom(fedVersion));
        FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
      }
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to select the version.");
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, callableStatement);
    }
    return version;
  }

  @Override
  public void storeVersion() throws Exception {
    byte[] fedVersion = ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
    String versionComment = CURRENT_VERSION_INFO.toString();
    storeVersion(fedVersion, versionComment);
  }

  /**
   * Store the Federation Version in the database.
   *
   * @param fedVersion Federation Version.
   * @param versionComment Federation Version Comment,
   *                       We use the result of Version toString as version Comment.
   * @throws YarnException indicates exceptions from yarn servers.
   */
  public void storeVersion(byte[] fedVersion, String versionComment) throws YarnException {
    CallableStatement callableStatement = null;

    try {
      callableStatement = getCallableStatement(CALL_SP_STORE_VERSION);

      // Set the parameters for the stored procedure
      callableStatement.setBytes("fedVersion_IN", fedVersion);
      callableStatement.setString("versionComment_IN", versionComment);
      callableStatement.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      callableStatement.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not add a new version into FederationStateStore
      int rowCount = callableStatement.getInt("rowCount_OUT");
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "The version %s was not insert into the StateStore.", versionComment);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (rowCount != 1) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during insert the version %s.", versionComment);
      }
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
      LOG.info("Insert into the state store the version : {}.", versionComment);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to insert the newly version : %s.", versionComment);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, callableStatement);
    }
  }

  @Override
  public void close() throws Exception {
    if (dataSource != null) {
      dataSource.close();
      LOG.debug("Connection closed");
      FederationStateStoreClientMetrics.decrConnections();
    }
  }

  /**
   * Get a connection from the DataSource pool.
   *
   * @return a connection from the DataSource pool.
   * @throws SQLException on failure
   */
  @VisibleForTesting
  protected Connection getConnection() throws SQLException {
    FederationStateStoreClientMetrics.incrConnections();
    return dataSource.getConnection();
  }

  /**
   * Get a connection from the DataSource pool.
   *
   * @param isCommitted Whether to enable automatic transaction commit.
   * If set to true, turn on transaction autocommit,
   * if set to false, turn off transaction autocommit.
   *
   * @return a connection from the DataSource pool.
   * @throws SQLException on failure.
   */
  protected Connection getConnection(boolean isCommitted) throws SQLException {
    Connection dbConn = getConnection();
    dbConn.setAutoCommit(isCommitted);
    return dbConn;
  }

  @VisibleForTesting
  protected CallableStatement getCallableStatement(String procedure)
      throws SQLException {
    return conn.prepareCall(procedure);
  }

  private static byte[] getByteArray(ByteBuffer bb) {
    byte[] ba = new byte[bb.limit()];
    bb.get(ba);
    return ba;
  }

  @Override
  public AddReservationHomeSubClusterResponse addReservationHomeSubCluster(
      AddReservationHomeSubClusterRequest request) throws YarnException {

    // validate
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);
    CallableStatement cstmt = null;

    ReservationHomeSubCluster reservationHomeSubCluster = request.getReservationHomeSubCluster();
    ReservationId reservationId = reservationHomeSubCluster.getReservationId();
    SubClusterId subClusterId = reservationHomeSubCluster.getHomeSubCluster();
    SubClusterId subClusterHomeId = null;

    try {

      // Defined the sp_addReservationHomeSubCluster procedure
      // this procedure requires 4 parameters
      // Input parameters
      // 1）IN reservationId_IN varchar(128)
      // 2）IN homeSubCluster_IN varchar(256)
      // Output parameters
      // 3）OUT storedHomeSubCluster_OUT varchar(256)
      // 4）OUT rowCount_OUT int

      // Call procedure
      cstmt = getCallableStatement(CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      // 1）IN reservationId_IN varchar(128)
      cstmt.setString("reservationId_IN", reservationId.toString());
      // 2）IN homeSubCluster_IN varchar(256)
      cstmt.setString("homeSubCluster_IN", subClusterId.getId());
      // 3) OUT storedHomeSubCluster_OUT varchar(256)
      cstmt.registerOutParameter("storedHomeSubCluster_OUT", VARCHAR);
      // 4) OUT rowCount_OUT int
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Get SubClusterHome
      String subClusterHomeIdString = cstmt.getString("storedHomeSubCluster_OUT");
      subClusterHomeId = SubClusterId.newInstance(subClusterHomeIdString);

      // Get rowCount
      int rowCount = cstmt.getInt("rowCount_OUT");

      // For failover reason, we check the returned subClusterId.
      // 1.If it is equal to the subClusterId we sent, the call added the new
      // reservation into FederationStateStore.
      // 2.If the call returns a different subClusterId
      // it means we already tried to insert this reservation
      // but a component (Router/StateStore/RM) failed during the submission.
      if (subClusterId.equals(subClusterHomeId)) {
        // if it is equal to 0
        // it means the call did not add a new reservation into FederationStateStore.
        if (rowCount == 0) {
          LOG.info("The reservation {} was not inserted in the StateStore because it" +
              " was already present in subCluster {}", reservationId, subClusterHomeId);
        } else if (rowCount != 1) {
          // if it is different from 1
          // it means the call had a wrong behavior. Maybe the database is not set correctly.
          FederationStateStoreUtils.logAndThrowStoreException(LOG,
              "Wrong behavior during the insertion of subCluster %s according to reservation %s. " +
              "The database expects to insert 1 record, but the number of " +
              "inserted changes is greater than 1, " +
              "please check the records of the database.",
              subClusterId, reservationId);
        }
      } else {
        // If it is different from 0,
        // it means that there is a data situation that does not meet the expectations,
        // and an exception should be thrown at this time
        if (rowCount != 0) {
          FederationStateStoreUtils.logAndThrowStoreException(LOG,
              "The reservation %s does exist but was overwritten.", reservationId);
        }
        LOG.info("Reservation: {} already present with subCluster: {}.",
            reservationId, subClusterHomeId);
      }

      // Record successful call time
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to insert the newly generated reservation %s to subCluster %s.",
          reservationId, subClusterId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }

    return AddReservationHomeSubClusterResponse.newInstance(subClusterHomeId);
  }

  @Override
  public GetReservationHomeSubClusterResponse getReservationHomeSubCluster(
      GetReservationHomeSubClusterRequest request) throws YarnException {
    // validate
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    ReservationId reservationId = request.getReservationId();
    SubClusterId subClusterId = null;

    try {

      // Defined the sp_getReservationHomeSubCluster procedure
      // this procedure requires 2 parameters
      // Input parameters
      // 1）IN reservationId_IN varchar(128)
      // Output parameters
      // 2）OUT homeSubCluster_OUT varchar(256)

      cstmt = getCallableStatement(CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      // 1）IN reservationId_IN varchar(128)
      cstmt.setString("reservationId_IN", reservationId.toString());
      // 2）OUT homeSubCluster_OUT varchar(256)
      cstmt.registerOutParameter("homeSubCluster_OUT", VARCHAR);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.execute();
      long stopTime = clock.getTime();

      // Get Result
      String subClusterHomeIdString = cstmt.getString("homeSubCluster_OUT");

      if (StringUtils.isNotBlank(subClusterHomeIdString)) {
        subClusterId = SubClusterId.newInstance(subClusterHomeIdString);
      } else {
        // If subClusterHomeIdString blank, we need to throw an exception
        FederationStateStoreUtils.logAndThrowRetriableException(LOG,
            "Reservation %s does not exist", reservationId);
      }

      LOG.info("Got the information about the specified reservation {} in subCluster = {}.",
          reservationId, subClusterId);

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

      ReservationHomeSubCluster homeSubCluster =
          ReservationHomeSubCluster.newInstance(reservationId, subClusterId);
      return GetReservationHomeSubClusterResponse.newInstance(homeSubCluster);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to obtain the reservation information according to %s.", reservationId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }

    throw new YarnException(
        "Unable to obtain the reservation information according to " + reservationId);
  }

  @Override
  public GetReservationsHomeSubClusterResponse getReservationsHomeSubCluster(
      GetReservationsHomeSubClusterRequest request) throws YarnException {
    CallableStatement cstmt = null;
    ResultSet rs = null;
    List<ReservationHomeSubCluster> reservationsHomeSubClusters = new ArrayList<>();

    try {

      // Defined the sp_getReservationsHomeSubCluster procedure
      // This procedure requires no input parameters, but will have 2 output parameters
      // Output parameters
      // 1）OUT reservationId
      // 2）OUT homeSubCluster

      cstmt = getCallableStatement(CALL_SP_GET_RESERVATIONS_HOME_SUBCLUSTER);

      // Execute the query
      long startTime = clock.getTime();
      rs = cstmt.executeQuery();
      long stopTime = clock.getTime();

      while (rs.next()) {
        // Extract the output for each tuple
        // 1）OUT reservationId
        String dbReservationId = rs.getString("reservationId");
        // 2）OUT homeSubCluster
        String dbHomeSubCluster = rs.getString("homeSubCluster");

        // Generate parameters
        ReservationId reservationId = ReservationId.parseReservationId(dbReservationId);
        SubClusterId homeSubCluster = SubClusterId.newInstance(dbHomeSubCluster);
        ReservationHomeSubCluster reservationHomeSubCluster =
            ReservationHomeSubCluster.newInstance(reservationId, homeSubCluster);
        reservationsHomeSubClusters.add(reservationHomeSubCluster);
      }

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

      return GetReservationsHomeSubClusterResponse.newInstance(
          reservationsHomeSubClusters);
    } catch (Exception e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the information for all the reservations.", e);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt, null, rs);
    }

    throw new YarnException("Unable to obtain the information for all the reservations.");
  }

  @Override
  public DeleteReservationHomeSubClusterResponse deleteReservationHomeSubCluster(
      DeleteReservationHomeSubClusterRequest request) throws YarnException {

    // validate
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    ReservationId reservationId = request.getReservationId();

    try {

      // Defined the sp_deleteReservationHomeSubCluster procedure
      // This procedure requires 1 input parameters, 1 output parameters
      // Input parameters
      // 1）IN reservationId_IN varchar(128)
      // Output parameters
      // 2）OUT rowCount_OUT int

      cstmt = getCallableStatement(CALL_SP_DELETE_RESERVATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      // 1）IN reservationId_IN varchar(128)
      cstmt.setString("reservationId_IN", reservationId.toString());
      // 2）OUT rowCount_OUT int
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      int rowCount = cstmt.getInt("rowCount_OUT");

      // if it is equal to 0 it means the call
      // did not delete the reservation from FederationStateStore
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Reservation %s does not exist", reservationId);
      } else if (rowCount != 1) {
        // if it is different from 1 it means the call
        // had a wrong behavior. Maybe the database is not set correctly.
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during deleting the reservation %s. " +
            "The database is expected to delete 1 record, " +
            "but the number of deleted records returned by the database is greater than 1, " +
            "indicating that a duplicate reservationId occurred during the deletion process.",
            reservationId);
      }

      LOG.info("Delete from the StateStore the reservation: {}.", reservationId);
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
      return DeleteReservationHomeSubClusterResponse.newInstance();
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to delete the reservation %s.", reservationId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    throw new YarnException("Unable to delete the reservation " + reservationId);
  }

  @Override
  public UpdateReservationHomeSubClusterResponse updateReservationHomeSubCluster(
      UpdateReservationHomeSubClusterRequest request) throws YarnException {

    // validate
    FederationReservationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    ReservationHomeSubCluster reservationHomeSubCluster = request.getReservationHomeSubCluster();
    ReservationId reservationId = reservationHomeSubCluster.getReservationId();
    SubClusterId subClusterId = reservationHomeSubCluster.getHomeSubCluster();

    try {

      // Defined the sp_updateReservationHomeSubCluster procedure
      // This procedure requires 2 input parameters, 1 output parameters
      // Input parameters
      // 1）IN reservationId_IN varchar(128)
      // 2）IN homeSubCluster_IN varchar(256)
      // Output parameters
      // 3）OUT rowCount_OUT int

      cstmt = getCallableStatement(CALL_SP_UPDATE_RESERVATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      // 1）IN reservationId_IN varchar(128)
      cstmt.setString("reservationId_IN", reservationId.toString());
      // 2）IN homeSubCluster_IN varchar(256)
      cstmt.setString("homeSubCluster_IN", subClusterId.getId());
      // 3）OUT rowCount_OUT int
      cstmt.registerOutParameter("rowCount_OUT", INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      int rowCount = cstmt.getInt("rowCount_OUT");

      // if it is equal to 0 it means the call
      // did not update the reservation into FederationStateStore
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Reservation %s does not exist", reservationId);
      } else if (rowCount != 1) {
        // if it is different from 1 it means the call
        // had a wrong behavior. Maybe the database is not set correctly.
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during update the subCluster %s according to reservation %s. " +
            "The database is expected to update 1 record, " +
            "but the number of database update records is greater than 1, " +
            "the records of the database should be checked.",
            subClusterId, reservationId);
      }
      LOG.info("Update the subCluster to {} for reservation {} in the StateStore.",
          subClusterId, reservationId);
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
      return UpdateReservationHomeSubClusterResponse.newInstance();
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to update the subCluster %s according to reservation %s.",
          subClusterId, reservationId);
    } finally {
      // Return to the pool the CallableStatement
      FederationStateStoreUtils.returnToPool(LOG, cstmt);
    }
    throw new YarnException(
        "Unable to update the subCluster " + subClusterId +
        " according to reservation" + reservationId);
  }

  @VisibleForTesting
  public Connection getConn() {
    return conn;
  }

  /**
   * SQLFederationStateStore Supports Store New MasterKey.
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey.
   * @return routerMasterKeyResponse, the response contains the RouterMasterKey.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterMasterKeyResponse storeNewMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {

    // Step1: Verify parameters to ensure that key fields are not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    // Step2: Parse the parameters and serialize the DelegationKey as a string.
    DelegationKey delegationKey = convertMasterKeyToDelegationKey(request);
    int keyId = delegationKey.getKeyId();
    String delegationKeyStr = FederationStateStoreUtils.encodeWritable(delegationKey);

    // Step3. store data in database.
    try {

      FederationSQLOutParameter<Integer> rowCountOUT =
          new FederationSQLOutParameter<>("rowCount_OUT", INTEGER, Integer.class);

      // Execute the query
      long startTime = clock.getTime();
      Integer rowCount = getRowCountByProcedureSQL(CALL_SP_ADD_MASTERKEY, keyId,
          delegationKeyStr, rowCountOUT);
      long stopTime = clock.getTime();

      // We hope that 1 record can be written to the database.
      // If the number of records is not 1, it means that the data was written incorrectly.
      if (rowCount != 1) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during the insertion of masterKey, keyId = %s. " +
            "please check the records of the database.", String.valueOf(keyId));
      }
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to insert the newly masterKey, keyId = %s.", String.valueOf(keyId));
    }

    // Step4. Query Data from the database and return the result.
    return getMasterKeyByDelegationKey(request);
  }

  /**
   * SQLFederationStateStore Supports Remove MasterKey.
   *
   * Defined the sp_deleteMasterKey procedure.
   * This procedure requires 1 input parameters, 1 output parameters.
   * Input parameters
   * 1. IN keyId_IN int
   * Output parameters
   * 2. OUT rowCount_OUT int
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey
   * @return routerMasterKeyResponse, the response contains the RouterMasterKey.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterMasterKeyResponse removeStoredMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {

    // Step1: Verify parameters to ensure that key fields are not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    // Step2: Parse parameters and get KeyId.
    RouterMasterKey paramMasterKey = request.getRouterMasterKey();
    int paramKeyId = paramMasterKey.getKeyId();

    // Step3. Clear data from database.
    try {

      // Execute the query
      long startTime = clock.getTime();
      FederationSQLOutParameter<Integer> rowCountOUT =
          new FederationSQLOutParameter<>("rowCount_OUT", INTEGER, Integer.class);
      Integer rowCount = getRowCountByProcedureSQL(CALL_SP_DELETE_MASTERKEY,
          paramKeyId, rowCountOUT);
      long stopTime = clock.getTime();

      // if it is equal to 0 it means the call
      // did not delete the reservation from FederationStateStore
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "masterKeyId = %s does not exist.", String.valueOf(paramKeyId));
      } else if (rowCount != 1) {
        // if it is different from 1 it means the call
        // had a wrong behavior. Maybe the database is not set correctly.
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during deleting the keyId %s. " +
            "The database is expected to delete 1 record, " +
            "but the number of deleted records returned by the database is greater than 1, " +
            "indicating that a duplicate masterKey occurred during the deletion process.",
            paramKeyId);
      }

      LOG.info("Delete from the StateStore the keyId: {}.", paramKeyId);
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
      return RouterMasterKeyResponse.newInstance(paramMasterKey);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to delete the keyId %s.", paramKeyId);
    }

    throw new YarnException("Unable to delete the masterKey, keyId = " + paramKeyId);
  }

  /**
   * SQLFederationStateStore Supports Remove MasterKey.
   *
   * Defined the sp_getMasterKey procedure.
   * this procedure requires 2 parameters.
   * Input parameters:
   * 1. IN keyId_IN int
   * Output parameters:
   * 2. OUT masterKey_OUT varchar(1024)
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey
   * @return routerMasterKeyResponse, the response contains the RouterMasterKey.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterMasterKeyResponse getMasterKeyByDelegationKey(RouterMasterKeyRequest request)
      throws YarnException, IOException {
    // Step1: Verify parameters to ensure that key fields are not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    // Step2: Parse parameters and get KeyId.
    RouterMasterKey paramMasterKey = request.getRouterMasterKey();
    int paramKeyId = paramMasterKey.getKeyId();

    // Step3: Call the stored procedure to get the result.
    try {

      FederationQueryRunner runner = new FederationQueryRunner();
      FederationSQLOutParameter<String> masterKeyOUT =
          new FederationSQLOutParameter<>("masterKey_OUT", VARCHAR, String.class);

      // Execute the query
      long startTime = clock.getTime();
      RouterMasterKey routerMasterKey = runner.execute(
          conn, CALL_SP_GET_MASTERKEY, new RouterMasterKeyHandler(), paramKeyId, masterKeyOUT);
      long stopTime = clock.getTime();

      LOG.info("Got the information about the specified masterKey = {} according to keyId = {}.",
          routerMasterKey, paramKeyId);

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);

      // Return query result.
      return RouterMasterKeyResponse.newInstance(routerMasterKey);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to obtain the masterKey information according to %s.",
          String.valueOf(paramKeyId));
    }

    // Throw exception information
    throw new YarnException(
        "Unable to obtain the masterKey information according to " + paramKeyId);
  }

  /**
   * SQLFederationStateStore Supports Store RMDelegationTokenIdentifier.
   *
   * Defined the sp_addDelegationToken procedure.
   * This procedure requires 4 input parameters, 1 output parameters.
   * Input parameters:
   * 1. IN sequenceNum_IN int
   * 2. IN tokenIdent_IN varchar(1024)
   * 3. IN token_IN varchar(1024)
   * 4. IN renewDate_IN bigint
   * Output parameters:
   * 5. OUT rowCount_OUT int
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return routerRMTokenResponse, the response contains the RouterStoreToken.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterRMTokenResponse storeNewToken(RouterRMTokenRequest request)
      throws YarnException, IOException {

    // Step1: Verify parameters to ensure that key fields are not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    // Step2. store data in database.
    try {
      long duration = addOrUpdateToken(request, true);
      FederationStateStoreClientMetrics.succeededStateStoreCall(duration);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      throw new YarnException(e);
    }

    // Step3. Query Data from the database and return the result.
    return getTokenByRouterStoreToken(request);
  }

  /**
   * SQLFederationStateStore Supports Update RMDelegationTokenIdentifier.
   *
   * Defined the sp_updateDelegationToken procedure.
   * This procedure requires 4 input parameters, 1 output parameters.
   * Input parameters:
   * 1. IN sequenceNum_IN int
   * 2. IN tokenIdent_IN varchar(1024)
   * 3. IN token_IN varchar(1024)
   * 4. IN renewDate_IN bigint
   * Output parameters:
   * 5. OUT rowCount_OUT int
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return routerRMTokenResponse, the response contains the RouterStoreToken.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterRMTokenResponse updateStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException {

    // Step1: Verify parameters to ensure that key fields are not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    // Step2. update data in database.
    try {
      long duration = addOrUpdateToken(request, false);
      FederationStateStoreClientMetrics.succeededStateStoreCall(duration);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      throw new YarnException(e);
    }

    // Step3. Query Data from the database and return the result.
    return getTokenByRouterStoreToken(request);
  }

  /**
   * Add Or Update RMDelegationTokenIdentifier.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @param isAdd   true, addData; false, updateData.
   * @return method operation time.
   * @throws IOException   An IO Error occurred.
   * @throws SQLException  An SQL Error occurred.
   * @throws YarnException if the call to the state store is unsuccessful.
   */
  private long addOrUpdateToken(RouterRMTokenRequest request, boolean isAdd)
      throws IOException, SQLException, YarnException {

    // Parse parameters and get KeyId.
    RouterStoreToken routerStoreToken = request.getRouterStoreToken();
    YARNDelegationTokenIdentifier identifier = routerStoreToken.getTokenIdentifier();
    String tokenIdentifier = FederationStateStoreUtils.encodeWritable(identifier);
    String tokenInfo = routerStoreToken.getTokenInfo();
    long renewDate = routerStoreToken.getRenewDate();
    int sequenceNum = identifier.getSequenceNumber();

    FederationQueryRunner runner = new FederationQueryRunner();
    FederationSQLOutParameter<Integer> rowCountOUT =
        new FederationSQLOutParameter<>("rowCount_OUT", INTEGER, Integer.class);

    // Execute the query
    long startTime = clock.getTime();
    String procedure = isAdd ? CALL_SP_ADD_DELEGATIONTOKEN : CALL_SP_UPDATE_DELEGATIONTOKEN;
    Integer rowCount = runner.execute(conn, procedure, new RowCountHandler("rowCount_OUT"),
        sequenceNum, tokenIdentifier, tokenInfo, renewDate, rowCountOUT);
    long stopTime = clock.getTime();

    // Get rowCount
    // In the process of updating the code, rowCount may be 0 or 1;
    // if rowCount=1, it is as expected, indicating that we have updated the Token correctly;
    // if rowCount=0, it is not as expected,
    // indicating that we have not updated the Token correctly.
    if (rowCount != 1) {
      FederationStateStoreUtils.logAndThrowStoreException(LOG,
          "Wrong behavior during the insertion of delegationToken, tokenId = %s. " +
          "Please check the records of the database.", String.valueOf(sequenceNum));
    }

    // return execution time
    return (stopTime - startTime);
  }

  /**
   * SQLFederationStateStore Supports Remove RMDelegationTokenIdentifier.
   *
   * Defined the sp_deleteDelegationToken procedure.
   * This procedure requires 1 input parameters, 1 output parameters.
   * Input parameters:
   * 1. IN sequenceNum_IN bigint
   * Output parameters:
   * 2. OUT rowCount_OUT int
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return routerRMTokenResponse, the response contains the RouterStoreToken.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterRMTokenResponse removeStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException {

    // Step1: Verify parameters to ensure that key fields are not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    // Step2: Parse parameters and get KeyId.
    RouterStoreToken routerStoreToken = request.getRouterStoreToken();
    YARNDelegationTokenIdentifier identifier = routerStoreToken.getTokenIdentifier();
    int sequenceNum = identifier.getSequenceNumber();

    try {

      FederationSQLOutParameter<Integer> rowCountOUT =
          new FederationSQLOutParameter<>("rowCount_OUT", INTEGER, Integer.class);

      // Execute the query
      long startTime = clock.getTime();
      Integer rowCount = getRowCountByProcedureSQL(CALL_SP_DELETE_DELEGATIONTOKEN,
          sequenceNum, rowCountOUT);
      long stopTime = clock.getTime();

      // if it is equal to 0 it means the call
      // did not delete the reservation from FederationStateStore
      if (rowCount == 0) {
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "TokenId %s does not exist", String.valueOf(sequenceNum));
      } else if (rowCount != 1) {
        // if it is different from 1 it means the call
        // had a wrong behavior. Maybe the database is not set correctly.
        FederationStateStoreUtils.logAndThrowStoreException(LOG,
            "Wrong behavior during deleting the delegationToken %s. " +
            "The database is expected to delete 1 record, " +
            "but the number of deleted records returned by the database is greater than 1, " +
            "indicating that a duplicate tokenId occurred during the deletion process.",
            String.valueOf(sequenceNum));
      }

      LOG.info("Delete from the StateStore the delegationToken, tokenId = {}.", sequenceNum);
      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
      return RouterRMTokenResponse.newInstance(routerStoreToken);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to delete the delegationToken, tokenId = %s.", sequenceNum);
    }
    throw new YarnException("Unable to delete the delegationToken, tokenId = " + sequenceNum);
  }

  /**
   * The Router Supports GetTokenByRouterStoreToken.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return RouterRMTokenResponse.
   * @throws YarnException if the call to the state store is unsuccessful.
   * @throws IOException An IO Error occurred.
   */
  @Override
  public RouterRMTokenResponse getTokenByRouterStoreToken(RouterRMTokenRequest request)
      throws YarnException, IOException {
    // Step1: Verify parameters to ensure that key fields are not empty.
    FederationRouterRMTokenInputValidator.validate(request);

    // Step2: Parse parameters and get KeyId.
    RouterStoreToken routerStoreToken = request.getRouterStoreToken();
    YARNDelegationTokenIdentifier identifier = routerStoreToken.getTokenIdentifier();
    int sequenceNum = identifier.getSequenceNumber();

    try {
      FederationQueryRunner runner = new FederationQueryRunner();
      FederationSQLOutParameter<String> tokenIdentOUT =
          new FederationSQLOutParameter<>("tokenIdent_OUT", VARCHAR, String.class);
      FederationSQLOutParameter<String> tokenOUT =
          new FederationSQLOutParameter<>("token_OUT", VARCHAR, String.class);
      FederationSQLOutParameter<Long> renewDateOUT =
          new FederationSQLOutParameter<>("renewDate_OUT", BIGINT, Long.class);

      // Execute the query
      long startTime = clock.getTime();
      RouterStoreToken resultToken = runner.execute(conn, CALL_SP_GET_DELEGATIONTOKEN,
          new RouterStoreTokenHandler(), sequenceNum, tokenIdentOUT, tokenOUT, renewDateOUT);
      long stopTime = clock.getTime();

      FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
      return RouterRMTokenResponse.newInstance(resultToken);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(e, LOG,
          "Unable to get the delegationToken, tokenId = %s.", String.valueOf(sequenceNum));
    }

    // Throw exception information
    throw new YarnException("Unable to get the delegationToken, tokenId = " + sequenceNum);
  }

  /**
   * Call Procedure to get RowCount.
   *
   * @param procedure procedureSQL.
   * @param params procedure params.
   * @return RowCount.
   * @throws SQLException An exception occurred when calling a stored procedure.
   */
  private int getRowCountByProcedureSQL(String procedure, Object... params) throws SQLException {
    FederationQueryRunner runner = new FederationQueryRunner();
    // Execute the query
    Integer rowCount = runner.execute(conn, procedure,
        new RowCountHandler("rowCount_OUT"), params);
    return rowCount;
  }

  /**
   * Increment DelegationToken SeqNum.
   *
   * @return delegationTokenSeqNum.
   */
  @Override
  public int incrementDelegationTokenSeqNum() {
    return querySequenceTable(YARN_ROUTER_SEQUENCE_NUM, true);
  }

  /**
   * Get DelegationToken SeqNum.
   *
   * @return delegationTokenSeqNum.
   */
  @Override
  public int getDelegationTokenSeqNum() {
    return querySequenceTable(YARN_ROUTER_SEQUENCE_NUM, false);
  }

  @Override
  public void setDelegationTokenSeqNum(int seqNum) {
    Connection connection = null;
    try {
      connection = getConnection(false);
      FederationQueryRunner runner = new FederationQueryRunner();
      runner.updateSequenceTable(connection, YARN_ROUTER_SEQUENCE_NUM, seqNum);
    } catch (Exception e) {
      throw new RuntimeException("Could not update sequence table!!", e);
    } finally {
      // Return to the pool the CallableStatement
      try {
        FederationStateStoreUtils.returnToPool(LOG, null, connection);
      } catch (YarnException e) {
        LOG.error("close connection error.", e);
      }
    }
  }

  /**
   * Get Current KeyId.
   *
   * @return currentKeyId.
   */
  @Override
  public int getCurrentKeyId() {
    return querySequenceTable(YARN_ROUTER_CURRENT_KEY_ID, false);
  }

  /**
   * The Router Supports incrementCurrentKeyId.
   *
   * @return CurrentKeyId.
   */
  @Override
  public int incrementCurrentKeyId() {
    return querySequenceTable(YARN_ROUTER_CURRENT_KEY_ID, true);
  }

  private int querySequenceTable(String sequenceName, boolean isUpdate){
    Connection connection = null;
    try {
      connection = getConnection(false);
      FederationQueryRunner runner = new FederationQueryRunner();
      return runner.selectOrUpdateSequenceTable(connection, sequenceName, isUpdate);
    } catch (Exception e) {
      throw new RuntimeException("Could not query sequence table!!", e);
    } finally {
      // Return to the pool the CallableStatement
      try {
        FederationStateStoreUtils.returnToPool(LOG, null, connection);
      } catch (YarnException e) {
        LOG.error("close connection error.", e);
      }
    }
  }

  @VisibleForTesting
  public HikariDataSource getDataSource() {
    return dataSource;
  }
}