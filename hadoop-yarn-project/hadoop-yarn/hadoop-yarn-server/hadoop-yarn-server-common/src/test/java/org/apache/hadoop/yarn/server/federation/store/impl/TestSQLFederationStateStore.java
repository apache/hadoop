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

import org.apache.commons.lang3.NotImplementedException;
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

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_GET_RESERVATIONS_HOME_SUBCLUSTER;

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

  @Test(expected = NotImplementedException.class)
  public void testAddReservationHomeSubCluster() throws Exception {
    super.testAddReservationHomeSubCluster();
  }

  @Test(expected = NotImplementedException.class)
  public void testAddReservationHomeSubClusterReservationAlreadyExists() throws Exception {
    super.testAddReservationHomeSubClusterReservationAlreadyExists();
  }

  @Test(expected = NotImplementedException.class)
  public void testAddReservationHomeSubClusterAppAlreadyExistsInTheSameSC() throws Exception {
    super.testAddReservationHomeSubClusterAppAlreadyExistsInTheSameSC();
  }

  @Test(expected = NotImplementedException.class)
  public void testDeleteReservationHomeSubCluster() throws Exception {
    super.testDeleteReservationHomeSubCluster();
  }

  @Test(expected = NotImplementedException.class)
  public void testDeleteReservationHomeSubClusterUnknownApp() throws Exception {
    super.testDeleteReservationHomeSubClusterUnknownApp();
  }

  @Test(expected = NotImplementedException.class)
  public void testUpdateReservationHomeSubCluster() throws Exception {
    super.testUpdateReservationHomeSubCluster();
  }

  @Test(expected = NotImplementedException.class)
  public void testUpdateReservationHomeSubClusterUnknownApp() throws Exception {
    super.testUpdateReservationHomeSubClusterUnknownApp();
  }

  class ReservationHomeSubCluster {
    ReservationId reservationId;
    String subHomeClusterId;
    int dbUpdateCount;

    public ReservationHomeSubCluster(ReservationId rId, String subHomeSCId, int dbUpdateCount) {
      this.reservationId = rId;
      this.subHomeClusterId = subHomeSCId;
      this.dbUpdateCount = dbUpdateCount;
    }
  }

  private ReservationHomeSubCluster AddReservationHomeSubCluster(
      SQLFederationStateStore sqlFederationStateStore, String procedure,
      ReservationId reservationId, String subHomeClusterId) throws SQLException {
    // procedure call parameter preparation
    CallableStatement cstmt = sqlFederationStateStore.getCallableStatement(procedure);
    cstmt.setString("reservationId_IN", reservationId.toString());
    cstmt.setString("homeSubCluster_IN", subHomeClusterId);
    cstmt.registerOutParameter("storedHomeSubCluster_OUT", java.sql.Types.VARCHAR);
    cstmt.registerOutParameter("rowCount_OUT", java.sql.Types.INTEGER);

    // execute procedure
    cstmt.executeUpdate();

    // get call result
    String dbStoredHomeSubCluster = cstmt.getString("storedHomeSubCluster_OUT");
    int dbRowCount = cstmt.getInt("rowCount_OUT");

    return new ReservationHomeSubCluster(reservationId, dbStoredHomeSubCluster, dbRowCount);
  }

  private ReservationHomeSubCluster getReservationHomeSubCluster(
      SQLFederationStateStore sqlFederationStateStore, String procedure,
      ReservationId reservationId) throws SQLException, YarnException {

    // procedure call parameter preparation
    CallableStatement cstmt = sqlFederationStateStore.getCallableStatement(procedure);
    cstmt.setString("reservationId_IN", reservationId.toString());
    cstmt.registerOutParameter("homeSubCluster_OUT", java.sql.Types.VARCHAR);

    // execute procedure
    cstmt.execute();

    // get call result
    String dBSubClusterHomeId = cstmt.getString("homeSubCluster_OUT");

    // return cstmt to pool
    FederationStateStoreUtils.returnToPool(LOG, cstmt);

    // returns the ReservationHomeSubCluster object
    return new ReservationHomeSubCluster(reservationId, dBSubClusterHomeId, 0);
  }

  private List<ReservationHomeSubCluster> getReservationsHomeSubCluster(
      SQLFederationStateStore sqlFederationStateStore, String procedure)
      throws SQLException, IOException {

    List<ReservationHomeSubCluster> results = new ArrayList<>();

    // procedure call parameter preparation
    CallableStatement cstmt = sqlFederationStateStore.getCallableStatement(procedure);

    // execute procedure
    ResultSet rs = cstmt.executeQuery();
    while (rs.next()) {
      // 1）OUT reservationId
      String dbReservationId = rs.getString("reservationId");
      ReservationId reservationId = ReservationId.parseReservationId(dbReservationId);

      // 2）OUT homeSubCluster
      String dbHomeSubCluster = rs.getString("homeSubCluster");
      results.add(new ReservationHomeSubCluster(reservationId, dbHomeSubCluster, 0));
    }

    return results;
  }

  /**
   * This test case is used to check whether the procedure
   * sp_addReservationHomeSubCluster can be executed normally.
   *
   * This test case will write 1 record to the database, and check returns the result.
   *
   * @throws Exception when the error occurs
   */
  @Test
  public void testCheckAddReservationHomeSubCluster() throws Exception {
    FederationStateStore stateStore = getStateStore();
    Assert.assertTrue(stateStore instanceof SQLFederationStateStore);

    SQLFederationStateStore sqlFederationStateStore = (SQLFederationStateStore) stateStore;

    // procedure call parameter preparation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    String subHomeClusterId = "SC-1";
    ReservationHomeSubCluster resultHC = AddReservationHomeSubCluster( sqlFederationStateStore,
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER,  reservationId,  subHomeClusterId);

    // validation results
    Assert.assertNotNull(resultHC);
    Assert.assertEquals(subHomeClusterId, resultHC.subHomeClusterId);
    Assert.assertEquals(1, resultHC.dbUpdateCount);
  }

  /**
   * This test case is used to check whether the procedure
   * sp_getReservationHomeSubCluster can be executed normally.
   *
   * Query according to reservationId, expect accurate query results,
   * and check the homeSubCluster field.
   *
   * @throws Exception when the error occurs
   */
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

    // Call getReservationHomeSubCluster to get the result
    ReservationHomeSubCluster resultHC = getReservationHomeSubCluster(sqlFederationStateStore,
        CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER, reservationId);

    Assert.assertNotNull(resultHC);
    Assert.assertEquals(subHomeClusterId, resultHC.subHomeClusterId);
    Assert.assertEquals(reservationId, resultHC.reservationId);
  }

  /**
   * This test case is used to check whether the procedure
   * sp_getReservationsHomeSubCluster can be executed normally.
   *
   * This test case will write 2 record to the database, and check returns the result.
   *
   * The test case will compare the number of returned records from the database
   * and whether the content of each returned record is accurate.
   *
   * @throws Exception when the error occurs
   */
  @Test
  public void testGetReservationsHomeSubCluster() throws Exception {
    FederationStateStore stateStore = getStateStore();
    Assert.assertTrue(stateStore instanceof SQLFederationStateStore);

    SQLFederationStateStore sqlFederationStateStore = (SQLFederationStateStore) stateStore;

    // add 1st record
    ReservationId reservationId1 = ReservationId.newInstance(Time.now(), 1);
    String subHomeClusterId1 = "SC-1";
    AddReservationHomeSubCluster(sqlFederationStateStore,
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER,  reservationId1,  subHomeClusterId1);

    // add 2nd record
    ReservationId reservationId2 = ReservationId.newInstance(Time.now(), 2);
    String subHomeClusterId2 = "SC-2";
    AddReservationHomeSubCluster(sqlFederationStateStore,
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER,  reservationId2,  subHomeClusterId2);

    List<ReservationHomeSubCluster> reservationHomeSubClusters =
    getReservationsHomeSubCluster(
        sqlFederationStateStore, CALL_SP_GET_RESERVATIONS_HOME_SUBCLUSTER);

    Assert.assertNotNull(reservationHomeSubClusters);
    Assert.assertEquals(2, reservationHomeSubClusters.size());

    ReservationHomeSubCluster resultHC1 = reservationHomeSubClusters.get(0);
    Assert.assertNotNull(resultHC1);
    Assert.assertEquals(reservationId1, resultHC1.reservationId);
    Assert.assertEquals(subHomeClusterId1, resultHC1.subHomeClusterId);

    ReservationHomeSubCluster resultHC2 = reservationHomeSubClusters.get(1);
    Assert.assertNotNull(resultHC2);
    Assert.assertEquals(reservationId2, resultHC2.reservationId);
    Assert.assertEquals(subHomeClusterId2, resultHC2.subHomeClusterId);
  }
}