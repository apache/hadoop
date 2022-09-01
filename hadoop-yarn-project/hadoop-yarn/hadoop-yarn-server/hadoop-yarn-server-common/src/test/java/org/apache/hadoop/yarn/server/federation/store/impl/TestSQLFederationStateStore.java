/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.metrics.FederationStateStoreClientMetrics;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.SQLException;

import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER;

/**
 * Unit tests for SQLFederationStateStore.
 */
public class TestSQLFederationStateStore extends FederationStateStoreBaseTest {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestSQLFederationStateStore.class);
  private static final String HSQLDB_DRIVER = "org.hsqldb.jdbc.JDBCDataSource";
  private static final String DATABASE_URL = "jdbc:hsqldb:mem:state";
  private static final String DATABASE_USERNAME = "SA";
  private static final String DATABASE_PASSWORD = "";

  @Override
  protected FederationStateStore createStateStore() {

    YarnConfiguration conf = new YarnConfiguration();

    conf.set(YarnConfiguration.FEDERATION_STATESTORE_SQL_JDBC_CLASS,
        HSQLDB_DRIVER);
    conf.set(YarnConfiguration.FEDERATION_STATESTORE_SQL_USERNAME,
        DATABASE_USERNAME);
    conf.set(YarnConfiguration.FEDERATION_STATESTORE_SQL_PASSWORD,
        DATABASE_PASSWORD);
    conf.set(YarnConfiguration.FEDERATION_STATESTORE_SQL_URL,
        DATABASE_URL + System.currentTimeMillis());
    super.setConf(conf);
    return new HSQLDBFederationStateStore();
  }

  @Test
  public void testSqlConnectionsCreatedCount() throws YarnException {
    FederationStateStore stateStore = getStateStore();
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    ApplicationId appId = ApplicationId.newInstance(1, 1);

    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));
    Assert.assertEquals(subClusterInfo, querySubClusterInfo(subClusterId));

    addApplicationHomeSC(appId, subClusterId);
    Assert.assertEquals(subClusterId, queryApplicationHomeSC(appId));

    // Verify if connection is created only once at statestore init
    Assert.assertEquals(1,
        FederationStateStoreClientMetrics.getNumConnections());
  }

  private CallableStatement AddReservationHomeSubCluster(SQLFederationStateStore sqlFederationStateStore,
      String procedure, ReservationId reservationId, String subHomeClusterId) throws SQLException {
    // procedure call parameter preparation
    CallableStatement cstmt = sqlFederationStateStore.getCallableStatement(procedure);
    cstmt.setString("reservationId_IN", reservationId.toString());
    cstmt.setString("homeSubCluster_IN", subHomeClusterId);
    cstmt.registerOutParameter("storedHomeSubCluster_OUT", java.sql.Types.VARCHAR);
    cstmt.registerOutParameter("rowCount_OUT", java.sql.Types.INTEGER);
    // execute procedure
    cstmt.executeUpdate();
    return cstmt;
  }

  @Test
  public void testCheckAddReservationHomeSubCluster() throws Exception {
    FederationStateStore stateStore = getStateStore();
    Assert.assertTrue(stateStore instanceof SQLFederationStateStore);

    SQLFederationStateStore sqlFederationStateStore = (SQLFederationStateStore) stateStore;

    // procedure call parameter preparation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    String subHomeClusterId = "SC-1";
    CallableStatement cstmt = AddReservationHomeSubCluster( sqlFederationStateStore,
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER,  reservationId,  subHomeClusterId);

    // get call result
    String dbStoredHomeSubCluster = cstmt.getString("storedHomeSubCluster_OUT");
    int dbRowCount = cstmt.getInt("rowCount_OUT");

    // validation results
    Assert.assertEquals(subHomeClusterId, dbStoredHomeSubCluster);
    Assert.assertEquals(1, dbRowCount);
  }

  @Test
  public void testCheckGetReservationHomeSubCluster() throws Exception {
    FederationStateStore stateStore = getStateStore();
    Assert.assertTrue(stateStore instanceof SQLFederationStateStore);

    SQLFederationStateStore sqlFederationStateStore = (SQLFederationStateStore) stateStore;

    // procedure call parameter preparation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    String subHomeClusterId = "SC-1";
    AddReservationHomeSubCluster( sqlFederationStateStore,
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER,  reservationId,  subHomeClusterId);

    CallableStatement cstmt =
        sqlFederationStateStore.getCallableStatement(CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER);
    cstmt.setString("reservationId_IN", reservationId.toString());
    cstmt.registerOutParameter("homeSubCluster_OUT", java.sql.Types.VARCHAR);
    cstmt.execute();

    // get call result
    String dBSubClusterHomeId = cstmt.getString("homeSubCluster_OUT");
    Assert.assertEquals(subHomeClusterId, dBSubClusterHomeId);
  }
}