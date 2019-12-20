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

import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
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
import org.apache.hadoop.yarn.server.federation.store.utils.FederationApplicationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationMembershipStateStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationPolicyStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

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
      "{call sp_addApplicationHomeSubCluster(?, ?, ?, ?)}";

  private static final String CALL_SP_UPDATE_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_updateApplicationHomeSubCluster(?, ?, ?)}";

  private static final String CALL_SP_DELETE_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_deleteApplicationHomeSubCluster(?, ?)}";

  private static final String CALL_SP_GET_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_getApplicationHomeSubCluster(?, ?)}";

  private static final String CALL_SP_GET_APPLICATIONS_HOME_SUBCLUSTER =
      "{call sp_getApplicationsHomeSubCluster()}";

  private static final String CALL_SP_SET_POLICY_CONFIGURATION =
      "{call sp_setPolicyConfiguration(?, ?, ?, ?)}";

  private static final String CALL_SP_GET_POLICY_CONFIGURATION =
      "{call sp_getPolicyConfiguration(?, ?, ?)}";

  private static final String CALL_SP_GET_POLICIES_CONFIGURATIONS =
      "{call sp_getPoliciesConfigurations()}";

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

  @Override
  public void init(Configuration conf) throws YarnException {
    driverClass =
        conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_JDBC_CLASS,
            YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_SQL_JDBC_CLASS);
    maximumPoolSize =
        conf.getInt(YarnConfiguration.FEDERATION_STATESTORE_SQL_MAXCONNECTIONS,
            YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_SQL_MAXCONNECTIONS);

    // An helper method avoids to assign a null value to these property
    userName = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_USERNAME);
    password = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_PASSWORD);
    url = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_URL);

    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException e) {
      FederationStateStoreUtils.logAndThrowException(LOG,
          "Driver class not found.", e);
    }

    // Create the data source to pool connections in a thread-safe manner
    dataSource = new HikariDataSource();
    dataSource.setDataSourceClassName(driverClass);
    FederationStateStoreUtils.setUsername(dataSource, userName);
    FederationStateStoreUtils.setPassword(dataSource, password);
    FederationStateStoreUtils.setProperty(dataSource,
        FederationStateStoreUtils.FEDERATION_STORE_URL, url);
    dataSource.setMaximumPoolSize(maximumPoolSize);
    LOG.info("Initialized connection pool to the Federation StateStore "
        + "database at address: " + url);
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest registerSubClusterRequest)
      throws YarnException {

    // Input validator
    FederationMembershipStateStoreInputValidator
        .validate(registerSubClusterRequest);

    CallableStatement cstmt = null;
    Connection conn = null;

    SubClusterInfo subClusterInfo =
        registerSubClusterRequest.getSubClusterInfo();
    SubClusterId subClusterId = subClusterInfo.getSubClusterId();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_REGISTER_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString(1, subClusterId.getId());
      cstmt.setString(2, subClusterInfo.getAMRMServiceAddress());
      cstmt.setString(3, subClusterInfo.getClientRMServiceAddress());
      cstmt.setString(4, subClusterInfo.getRMAdminServiceAddress());
      cstmt.setString(5, subClusterInfo.getRMWebServiceAddress());
      cstmt.setString(6, subClusterInfo.getState().toString());
      cstmt.setLong(7, subClusterInfo.getLastStartTime());
      cstmt.setString(8, subClusterInfo.getCapability());
      cstmt.registerOutParameter(9, java.sql.Types.INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not add a new subcluster into FederationStateStore
      if (cstmt.getInt(9) == 0) {
        String errMsg = "SubCluster " + subClusterId
            + " was not registered into the StateStore";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (cstmt.getInt(9) != 1) {
        String errMsg = "Wrong behavior during registration of SubCluster "
            + subClusterId + " into the StateStore";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }

      LOG.info(
          "Registered the SubCluster " + subClusterId + " into the StateStore");
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to register the SubCluster " + subClusterId
              + " into the StateStore",
          e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
    }
    return SubClusterRegisterResponse.newInstance();
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest subClusterDeregisterRequest)
      throws YarnException {

    // Input validator
    FederationMembershipStateStoreInputValidator
        .validate(subClusterDeregisterRequest);

    CallableStatement cstmt = null;
    Connection conn = null;

    SubClusterId subClusterId = subClusterDeregisterRequest.getSubClusterId();
    SubClusterState state = subClusterDeregisterRequest.getState();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_DEREGISTER_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString(1, subClusterId.getId());
      cstmt.setString(2, state.toString());
      cstmt.registerOutParameter(3, java.sql.Types.INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not deregister the subcluster into FederationStateStore
      if (cstmt.getInt(3) == 0) {
        String errMsg = "SubCluster " + subClusterId + " not found";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (cstmt.getInt(3) != 1) {
        String errMsg = "Wrong behavior during deregistration of SubCluster "
            + subClusterId + " from the StateStore";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }

      LOG.info("Deregistered the SubCluster " + subClusterId + " state to "
          + state.toString());
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to deregister the sub-cluster " + subClusterId + " state to "
              + state.toString(),
          e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
    }
    return SubClusterDeregisterResponse.newInstance();
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest subClusterHeartbeatRequest)
      throws YarnException {

    // Input validator
    FederationMembershipStateStoreInputValidator
        .validate(subClusterHeartbeatRequest);

    CallableStatement cstmt = null;
    Connection conn = null;

    SubClusterId subClusterId = subClusterHeartbeatRequest.getSubClusterId();
    SubClusterState state = subClusterHeartbeatRequest.getState();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_SUBCLUSTER_HEARTBEAT);

      // Set the parameters for the stored procedure
      cstmt.setString(1, subClusterId.getId());
      cstmt.setString(2, state.toString());
      cstmt.setString(3, subClusterHeartbeatRequest.getCapability());
      cstmt.registerOutParameter(4, java.sql.Types.INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not update the subcluster into FederationStateStore
      if (cstmt.getInt(4) == 0) {
        String errMsg = "SubCluster " + subClusterId.toString()
            + " does not exist; cannot heartbeat";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (cstmt.getInt(4) != 1) {
        String errMsg =
            "Wrong behavior during the heartbeat of SubCluster " + subClusterId;
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }

      LOG.info("Heartbeated the StateStore for the specified SubCluster "
          + subClusterId);
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to heartbeat the StateStore for the specified SubCluster "
              + subClusterId,
          e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
    }
    return SubClusterHeartbeatResponse.newInstance();
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest subClusterRequest) throws YarnException {

    // Input validator
    FederationMembershipStateStoreInputValidator.validate(subClusterRequest);

    CallableStatement cstmt = null;
    Connection conn = null;

    SubClusterInfo subClusterInfo = null;
    SubClusterId subClusterId = subClusterRequest.getSubClusterId();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_GET_SUBCLUSTER);
      cstmt.setString(1, subClusterId.getId());

      // Set the parameters for the stored procedure
      cstmt.registerOutParameter(2, java.sql.Types.VARCHAR);
      cstmt.registerOutParameter(3, java.sql.Types.VARCHAR);
      cstmt.registerOutParameter(4, java.sql.Types.VARCHAR);
      cstmt.registerOutParameter(5, java.sql.Types.VARCHAR);
      cstmt.registerOutParameter(6, java.sql.Types.TIMESTAMP);
      cstmt.registerOutParameter(7, java.sql.Types.VARCHAR);
      cstmt.registerOutParameter(8, java.sql.Types.BIGINT);
      cstmt.registerOutParameter(9, java.sql.Types.VARCHAR);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.execute();
      long stopTime = clock.getTime();

      String amRMAddress = cstmt.getString(2);
      String clientRMAddress = cstmt.getString(3);
      String rmAdminAddress = cstmt.getString(4);
      String webAppAddress = cstmt.getString(5);

      // first check if the subCluster exists
      if((amRMAddress == null) || (clientRMAddress == null)) {
        LOG.warn("The queried SubCluster: {} does not exist.", subClusterId);
        return null;
      }

      Timestamp heartBeatTimeStamp = cstmt.getTimestamp(6, utcCalendar);
      long lastHeartBeat =
          heartBeatTimeStamp != null ? heartBeatTimeStamp.getTime() : 0;

      SubClusterState state = SubClusterState.fromString(cstmt.getString(7));
      long lastStartTime = cstmt.getLong(8);
      String capability = cstmt.getString(9);

      subClusterInfo = SubClusterInfo.newInstance(subClusterId, amRMAddress,
          clientRMAddress, rmAdminAddress, webAppAddress, lastHeartBeat, state,
          lastStartTime, capability);

      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

      // Check if the output it is a valid subcluster
      try {
        FederationMembershipStateStoreInputValidator
            .checkSubClusterInfo(subClusterInfo);
      } catch (FederationStateStoreInvalidInputException e) {
        String errMsg =
            "SubCluster " + subClusterId.toString() + " does not exist";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
      LOG.debug("Got the information about the specified SubCluster {}",
          subClusterInfo);
    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the SubCluster information for " + subClusterId, e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
    }
    return GetSubClusterInfoResponse.newInstance(subClusterInfo);
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest subClustersRequest) throws YarnException {
    CallableStatement cstmt = null;
    Connection conn = null;
    ResultSet rs = null;
    List<SubClusterInfo> subClusters = new ArrayList<SubClusterInfo>();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_GET_SUBCLUSTERS);

      // Execute the query
      long startTime = clock.getTime();
      rs = cstmt.executeQuery();
      long stopTime = clock.getTime();

      while (rs.next()) {

        // Extract the output for each tuple
        String subClusterName = rs.getString(1);
        String amRMAddress = rs.getString(2);
        String clientRMAddress = rs.getString(3);
        String rmAdminAddress = rs.getString(4);
        String webAppAddress = rs.getString(5);
        long lastHeartBeat = rs.getTimestamp(6, utcCalendar).getTime();
        SubClusterState state = SubClusterState.fromString(rs.getString(7));
        long lastStartTime = rs.getLong(8);
        String capability = rs.getString(9);

        SubClusterId subClusterId = SubClusterId.newInstance(subClusterName);
        SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
            amRMAddress, clientRMAddress, rmAdminAddress, webAppAddress,
            lastHeartBeat, state, lastStartTime, capability);

        FederationStateStoreClientMetrics
            .succeededStateStoreCall(stopTime - startTime);


        // Check if the output it is a valid subcluster
        try {
          FederationMembershipStateStoreInputValidator
              .checkSubClusterInfo(subClusterInfo);
        } catch (FederationStateStoreInvalidInputException e) {
          String errMsg =
              "SubCluster " + subClusterId.toString() + " is not valid";
          FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
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
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn, rs);
    }
    return GetSubClustersInfoResponse.newInstance(subClusters);
  }

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {

    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    Connection conn = null;

    String subClusterHome = null;
    ApplicationId appId =
        request.getApplicationHomeSubCluster().getApplicationId();
    SubClusterId subClusterId =
        request.getApplicationHomeSubCluster().getHomeSubCluster();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_ADD_APPLICATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString(1, appId.toString());
      cstmt.setString(2, subClusterId.getId());
      cstmt.registerOutParameter(3, java.sql.Types.VARCHAR);
      cstmt.registerOutParameter(4, java.sql.Types.INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      subClusterHome = cstmt.getString(3);
      SubClusterId subClusterIdHome = SubClusterId.newInstance(subClusterHome);

      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

      // For failover reason, we check the returned SubClusterId.
      // If it is equal to the subclusterId we sent, the call added the new
      // application into FederationStateStore. If the call returns a different
      // SubClusterId it means we already tried to insert this application but a
      // component (Router/StateStore/RM) failed during the submission.
      if (subClusterId.equals(subClusterIdHome)) {
        // Check the ROWCOUNT value, if it is equal to 0 it means the call
        // did not add a new application into FederationStateStore
        if (cstmt.getInt(4) == 0) {
          LOG.info(
              "The application {} was not inserted in the StateStore because it"
                  + " was already present in SubCluster {}",
              appId, subClusterHome);
        } else if (cstmt.getInt(4) != 1) {
          // Check the ROWCOUNT value, if it is different from 1 it means the
          // call had a wrong behavior. Maybe the database is not set correctly.
          String errMsg = "Wrong behavior during the insertion of SubCluster "
              + subClusterId;
          FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
        }

        LOG.info("Insert into the StateStore the application: " + appId
            + " in SubCluster:  " + subClusterHome);
      } else {
        // Check the ROWCOUNT value, if it is different from 0 it means the call
        // did edited the table
        if (cstmt.getInt(4) != 0) {
          String errMsg =
              "The application " + appId + " does exist but was overwritten";
          FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
        }
        LOG.info("Application: " + appId + " already present with SubCluster:  "
            + subClusterHome);
      }

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils
          .logAndThrowRetriableException(LOG,
              "Unable to insert the newly generated application "
                  + request.getApplicationHomeSubCluster().getApplicationId(),
              e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
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
    Connection conn = null;

    ApplicationId appId =
        request.getApplicationHomeSubCluster().getApplicationId();
    SubClusterId subClusterId =
        request.getApplicationHomeSubCluster().getHomeSubCluster();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_UPDATE_APPLICATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString(1, appId.toString());
      cstmt.setString(2, subClusterId.getId());
      cstmt.registerOutParameter(3, java.sql.Types.INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not update the application into FederationStateStore
      if (cstmt.getInt(3) == 0) {
        String errMsg = "Application " + appId + " does not exist";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (cstmt.getInt(3) != 1) {
        String errMsg =
            "Wrong behavior during the update of SubCluster " + subClusterId;
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }

      LOG.info(
          "Update the SubCluster to {} for application {} in the StateStore",
          subClusterId, appId);
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils
          .logAndThrowRetriableException(LOG,
              "Unable to update the application "
                  + request.getApplicationHomeSubCluster().getApplicationId(),
              e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
    }
    return UpdateApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {
    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    Connection conn = null;

    SubClusterId homeRM = null;

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_GET_APPLICATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString(1, request.getApplicationId().toString());
      cstmt.registerOutParameter(2, java.sql.Types.VARCHAR);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.execute();
      long stopTime = clock.getTime();

      if (cstmt.getString(2) != null) {
        homeRM = SubClusterId.newInstance(cstmt.getString(2));
      } else {
        String errMsg =
            "Application " + request.getApplicationId() + " does not exist";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }

      LOG.debug("Got the information about the specified application {}."
          + " The AM is running in {}", request.getApplicationId(), homeRM);

      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the application information "
              + "for the specified application " + request.getApplicationId(),
          e);
    } finally {

      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
    }
    return GetApplicationHomeSubClusterResponse
        .newInstance(ApplicationHomeSubCluster
            .newInstance(request.getApplicationId(), homeRM));
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {
    CallableStatement cstmt = null;
    Connection conn = null;
    ResultSet rs = null;
    List<ApplicationHomeSubCluster> appsHomeSubClusters =
        new ArrayList<ApplicationHomeSubCluster>();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_GET_APPLICATIONS_HOME_SUBCLUSTER);

      // Execute the query
      long startTime = clock.getTime();
      rs = cstmt.executeQuery();
      long stopTime = clock.getTime();

      while (rs.next()) {

        // Extract the output for each tuple
        String applicationId = rs.getString(1);
        String homeSubCluster = rs.getString(2);

        appsHomeSubClusters.add(ApplicationHomeSubCluster.newInstance(
            ApplicationId.fromString(applicationId),
            SubClusterId.newInstance(homeSubCluster)));
      }

      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the information for all the applications ", e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn, rs);
    }
    return GetApplicationsHomeSubClusterResponse
        .newInstance(appsHomeSubClusters);
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException {

    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    Connection conn = null;

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_DELETE_APPLICATION_HOME_SUBCLUSTER);

      // Set the parameters for the stored procedure
      cstmt.setString(1, request.getApplicationId().toString());
      cstmt.registerOutParameter(2, java.sql.Types.INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not delete the application from FederationStateStore
      if (cstmt.getInt(2) == 0) {
        String errMsg =
            "Application " + request.getApplicationId() + " does not exist";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (cstmt.getInt(2) != 1) {
        String errMsg = "Wrong behavior during deleting the application "
            + request.getApplicationId();
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }

      LOG.info("Delete from the StateStore the application: {}",
          request.getApplicationId());
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to delete the application " + request.getApplicationId(), e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
    }
    return DeleteApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {

    // Input validator
    FederationPolicyStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    Connection conn = null;
    SubClusterPolicyConfiguration subClusterPolicyConfiguration = null;

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_GET_POLICY_CONFIGURATION);

      // Set the parameters for the stored procedure
      cstmt.setString(1, request.getQueue());
      cstmt.registerOutParameter(2, java.sql.Types.VARCHAR);
      cstmt.registerOutParameter(3, java.sql.Types.VARBINARY);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check if the output it is a valid policy
      if (cstmt.getString(2) != null && cstmt.getBytes(3) != null) {
        subClusterPolicyConfiguration =
            SubClusterPolicyConfiguration.newInstance(request.getQueue(),
                cstmt.getString(2), ByteBuffer.wrap(cstmt.getBytes(3)));
        LOG.debug("Selected from StateStore the policy for the queue: {}",
            subClusterPolicyConfiguration);
      } else {
        LOG.warn("Policy for queue: {} does not exist.", request.getQueue());
        return null;
      }

      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to select the policy for the queue :" + request.getQueue(),
          e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
    }
    return GetSubClusterPolicyConfigurationResponse
        .newInstance(subClusterPolicyConfiguration);
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {

    // Input validator
    FederationPolicyStoreInputValidator.validate(request);

    CallableStatement cstmt = null;
    Connection conn = null;

    SubClusterPolicyConfiguration policyConf = request.getPolicyConfiguration();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_SET_POLICY_CONFIGURATION);

      // Set the parameters for the stored procedure
      cstmt.setString(1, policyConf.getQueue());
      cstmt.setString(2, policyConf.getType());
      cstmt.setBytes(3, getByteArray(policyConf.getParams()));
      cstmt.registerOutParameter(4, java.sql.Types.INTEGER);

      // Execute the query
      long startTime = clock.getTime();
      cstmt.executeUpdate();
      long stopTime = clock.getTime();

      // Check the ROWCOUNT value, if it is equal to 0 it means the call
      // did not add a new policy into FederationStateStore
      if (cstmt.getInt(4) == 0) {
        String errMsg = "The policy " + policyConf.getQueue()
            + " was not insert into the StateStore";
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
      // Check the ROWCOUNT value, if it is different from 1 it means the call
      // had a wrong behavior. Maybe the database is not set correctly.
      if (cstmt.getInt(4) != 1) {
        String errMsg =
            "Wrong behavior during insert the policy " + policyConf.getQueue();
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }

      LOG.info("Insert into the state store the policy for the queue: "
          + policyConf.getQueue());
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to insert the newly generated policy for the queue :"
              + policyConf.getQueue(),
          e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn);
    }
    return SetSubClusterPolicyConfigurationResponse.newInstance();
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {

    CallableStatement cstmt = null;
    Connection conn = null;
    ResultSet rs = null;
    List<SubClusterPolicyConfiguration> policyConfigurations =
        new ArrayList<SubClusterPolicyConfiguration>();

    try {
      conn = getConnection();
      cstmt = conn.prepareCall(CALL_SP_GET_POLICIES_CONFIGURATIONS);

      // Execute the query
      long startTime = clock.getTime();
      rs = cstmt.executeQuery();
      long stopTime = clock.getTime();

      while (rs.next()) {

        // Extract the output for each tuple
        String queue = rs.getString(1);
        String type = rs.getString(2);
        byte[] policyInfo = rs.getBytes(3);

        SubClusterPolicyConfiguration subClusterPolicyConfiguration =
            SubClusterPolicyConfiguration.newInstance(queue, type,
                ByteBuffer.wrap(policyInfo));
        policyConfigurations.add(subClusterPolicyConfiguration);
      }

      FederationStateStoreClientMetrics
          .succeededStateStoreCall(stopTime - startTime);

    } catch (SQLException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the policy information for all the queues.", e);
    } finally {
      // Return to the pool the CallableStatement and the Connection
      FederationStateStoreUtils.returnToPool(LOG, cstmt, conn, rs);
    }

    return GetSubClusterPoliciesConfigurationsResponse
        .newInstance(policyConfigurations);
  }

  @Override
  public Version getCurrentVersion() {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public Version loadVersion() {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public void close() throws Exception {
    if (dataSource != null) {
      dataSource.close();
    }
  }

  /**
   * Get a connection from the DataSource pool.
   *
   * @return a connection from the DataSource pool.
   * @throws SQLException on failure
   */
  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  private static byte[] getByteArray(ByteBuffer bb) {
    byte[] ba = new byte[bb.limit()];
    bb.get(ba);
    return ba;
  }

}
