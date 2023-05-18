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

import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.metrics.FederationStateStoreClientMetrics;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.sql.DatabaseProduct;
import org.apache.hadoop.yarn.server.federation.store.sql.FederationSQLOutParameter;
import org.apache.hadoop.yarn.server.federation.store.sql.FederationQueryRunner;
import org.apache.hadoop.yarn.server.federation.store.sql.RouterMasterKeyHandler;
import org.apache.hadoop.yarn.server.federation.store.sql.RouterStoreTokenHandler;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_GET_MASTERKEY;
import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_GET_RESERVATIONS_HOME_SUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_DELETE_RESERVATION_HOME_SUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_GET_DELEGATIONTOKEN;
import static org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore.CALL_SP_UPDATE_RESERVATION_HOME_SUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.sql.DatabaseProduct.DbType;
import static org.apache.hadoop.yarn.server.federation.store.impl.HSQLDBFederationStateStore.SP_DROP_ADDRESERVATIONHOMESUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.HSQLDBFederationStateStore.SP_ADDRESERVATIONHOMESUBCLUSTER2;
import static org.apache.hadoop.yarn.server.federation.store.impl.HSQLDBFederationStateStore.SP_DROP_UPDATERESERVATIONHOMESUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.HSQLDBFederationStateStore.SP_UPDATERESERVATIONHOMESUBCLUSTER2;
import static org.apache.hadoop.yarn.server.federation.store.impl.HSQLDBFederationStateStore.SP_DROP_DELETERESERVATIONHOMESUBCLUSTER;
import static org.apache.hadoop.yarn.server.federation.store.impl.HSQLDBFederationStateStore.SP_DELETERESERVATIONHOMESUBCLUSTER2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.BIGINT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
  private SQLFederationStateStore sqlFederationStateStore = null;

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
    conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS, 10);
    super.setConf(conf);
    sqlFederationStateStore = new HSQLDBFederationStateStore();
    return sqlFederationStateStore;
  }

  @Test
  public void testSqlConnectionsCreatedCount() throws YarnException {
    FederationStateStore stateStore = getStateStore();
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    ApplicationId appId = ApplicationId.newInstance(1, 1);

    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));
    assertEquals(subClusterInfo, querySubClusterInfo(subClusterId));

    addApplicationHomeSC(appId, subClusterId);
    assertEquals(subClusterId, queryApplicationHomeSC(appId));

    // Verify if connection is created only once at statestore init
    assertEquals(1,
        FederationStateStoreClientMetrics.getNumConnections());
  }

  class ReservationHomeSC {
    private String reservationId;
    private String subHomeClusterId;
    private int dbUpdateCount;

    ReservationHomeSC(String rId, String subHomeSCId, int dbUpdateCount) {
      this.reservationId = rId;
      this.subHomeClusterId = subHomeSCId;
      this.dbUpdateCount = dbUpdateCount;
    }
  }

  private ReservationHomeSC addReservationHomeSubCluster(String procedure,
      String reservationId, String subHomeClusterId) throws SQLException, YarnException {
    // procedure call parameter preparation
    CallableStatement cstmt = sqlFederationStateStore.getCallableStatement(procedure);
    cstmt.setString("reservationId_IN", reservationId);
    cstmt.setString("homeSubCluster_IN", subHomeClusterId);
    cstmt.registerOutParameter("storedHomeSubCluster_OUT", java.sql.Types.VARCHAR);
    cstmt.registerOutParameter("rowCount_OUT", java.sql.Types.INTEGER);

    // execute procedure
    cstmt.executeUpdate();

    // get call result
    String dbStoredHomeSubCluster = cstmt.getString("storedHomeSubCluster_OUT");
    int dbRowCount = cstmt.getInt("rowCount_OUT");

    // return cstmt to pool
    FederationStateStoreUtils.returnToPool(LOG, cstmt);

    return new ReservationHomeSC(reservationId, dbStoredHomeSubCluster, dbRowCount);
  }

  private ReservationHomeSC getReservationHomeSubCluster(String procedure,
      String reservationId) throws SQLException, YarnException {

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
    return new ReservationHomeSC(reservationId, dBSubClusterHomeId, 0);
  }

  private List<ReservationHomeSC> getReservationsHomeSubCluster(String procedure)
      throws SQLException, IOException, YarnException {

    List<ReservationHomeSC> results = new ArrayList<>();

    // procedure call parameter preparation
    CallableStatement cstmt = sqlFederationStateStore.getCallableStatement(procedure);

    // execute procedure
    ResultSet rs = cstmt.executeQuery();
    while (rs.next()) {
      // 1）OUT reservationId
      String dbReservationId = rs.getString("reservationId");

      // 2）OUT homeSubCluster
      String dbHomeSubCluster = rs.getString("homeSubCluster");
      results.add(new ReservationHomeSC(dbReservationId, dbHomeSubCluster, 0));
    }

    // return cstmt to pool
    FederationStateStoreUtils.returnToPool(LOG, cstmt);

    // return ReservationHomeSubCluster List
    return results;
  }

  private ReservationHomeSC updateReservationHomeSubCluster(String procedure,
      String reservationId, String subHomeClusterId)
      throws SQLException, IOException {

    // procedure call parameter preparation
    CallableStatement cstmt = sqlFederationStateStore.getCallableStatement(procedure);

    // 1）IN reservationId_IN varchar(128)
    cstmt.setString("reservationId_IN", reservationId);
    // 2）IN homeSubCluster_IN varchar(256)
    cstmt.setString("homeSubCluster_IN", subHomeClusterId);
    // 3）OUT rowCount_OUT int
    cstmt.registerOutParameter("rowCount_OUT", java.sql.Types.INTEGER);

    // execute procedure
    cstmt.executeUpdate();

    // get rowcount
    int rowCount = cstmt.getInt("rowCount_OUT");

    // returns the ReservationHomeSubCluster object
    return new ReservationHomeSC(reservationId, subHomeClusterId, rowCount);
  }

  private ReservationHomeSC deleteReservationHomeSubCluster(String procedure,
      String reservationId) throws SQLException {
    // procedure call parameter preparation
    CallableStatement cstmt = sqlFederationStateStore.getCallableStatement(procedure);

    // Set the parameters for the stored procedure
    // 1）IN reservationId_IN varchar(128)
    cstmt.setString("reservationId_IN", reservationId);
    // 2）OUT rowCount_OUT int
    cstmt.registerOutParameter("rowCount_OUT", java.sql.Types.INTEGER);

    // execute procedure
    cstmt.executeUpdate();

    // get rowcount
    int rowCount = cstmt.getInt("rowCount_OUT");

    // returns the ReservationHomeSubCluster object
    return new ReservationHomeSC(reservationId, "-", rowCount);
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

    // procedure call parameter preparation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    String subHomeClusterId = "SC-1";
    ReservationHomeSC resultHC = addReservationHomeSubCluster(
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER, reservationId.toString(), subHomeClusterId);

    // validation results
    assertNotNull(resultHC);
    assertEquals(subHomeClusterId, resultHC.subHomeClusterId);
    assertEquals(1, resultHC.dbUpdateCount);
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

    // procedure call parameter preparation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    String subHomeClusterId = "SC-1";
    addReservationHomeSubCluster(
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER, reservationId.toString(), subHomeClusterId);

    // Call getReservationHomeSubCluster to get the result
    ReservationHomeSC resultHC = getReservationHomeSubCluster(
        CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER, reservationId.toString());

    assertNotNull(resultHC);
    assertEquals(subHomeClusterId, resultHC.subHomeClusterId);
    assertEquals(reservationId.toString(), resultHC.reservationId);
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
  public void testCheckGetReservationsHomeSubCluster() throws Exception {

    // add 1st record
    ReservationId reservationId1 = ReservationId.newInstance(Time.now(), 1);
    String subHomeClusterId1 = "SC-1";
    addReservationHomeSubCluster(
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER,  reservationId1.toString(),  subHomeClusterId1);

    // add 2nd record
    ReservationId reservationId2 = ReservationId.newInstance(Time.now(), 2);
    String subHomeClusterId2 = "SC-2";
    addReservationHomeSubCluster(
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER,  reservationId2.toString(),  subHomeClusterId2);

    List<ReservationHomeSC> reservationHomeSubClusters = getReservationsHomeSubCluster(
        CALL_SP_GET_RESERVATIONS_HOME_SUBCLUSTER);

    assertNotNull(reservationHomeSubClusters);
    assertEquals(2, reservationHomeSubClusters.size());

    ReservationHomeSC resultHC1 = reservationHomeSubClusters.get(0);
    assertNotNull(resultHC1);
    assertEquals(reservationId1.toString(), resultHC1.reservationId);
    assertEquals(subHomeClusterId1, resultHC1.subHomeClusterId);

    ReservationHomeSC resultHC2 = reservationHomeSubClusters.get(1);
    assertNotNull(resultHC2);
    assertEquals(reservationId2.toString(), resultHC2.reservationId);
    assertEquals(subHomeClusterId2, resultHC2.subHomeClusterId);
  }

  /**
   * This test case is used to check whether the procedure
   * sp_updateReservationHomeSubCluster can be executed normally.
   *
   * This test case will first insert 1 record into the database,
   * and then update the new SubHomeClusterId according to the reservationId.
   *
   * It will check whether the SubHomeClusterId is as expected after the addition and update.
   * For the first time, the HomeClusterId of the database should be SC-1,
   * and for the second time, the HomeClusterId of the database should be SC-2.
   *
   * @throws Exception when the error occurs
   */
  @Test
  public void testCheckUpdateReservationHomeSubCluster() throws Exception {

    // procedure call parameter preparation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    String subHomeClusterId = "SC-1";
    addReservationHomeSubCluster(
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER, reservationId.toString(), subHomeClusterId);

    // verify that the subHomeClusterId corresponding to reservationId is SC-1
    ReservationHomeSC resultHC = getReservationHomeSubCluster(
        CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER, reservationId.toString());
    assertNotNull(resultHC);
    assertEquals(subHomeClusterId, resultHC.subHomeClusterId);

    // prepare to update parameters
    String newSubHomeClusterId = "SC-2";
    ReservationHomeSC reservationHomeSubCluster =
        updateReservationHomeSubCluster(
        CALL_SP_UPDATE_RESERVATION_HOME_SUBCLUSTER, reservationId.toString(), newSubHomeClusterId);

    assertNotNull(reservationHomeSubCluster);
    assertEquals(1, reservationHomeSubCluster.dbUpdateCount);

    // verify that the subHomeClusterId corresponding to reservationId is SC-2
    ReservationHomeSC resultHC2 = getReservationHomeSubCluster(
        CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER, reservationId.toString());
    assertNotNull(resultHC2);
    assertEquals(newSubHomeClusterId, resultHC2.subHomeClusterId);
  }

  /**
   * This test case is used to check whether the procedure
   * sp_deleteReservationHomeSubCluster can be executed normally.
   *
   * This test case will first write 1 record to the database,
   * and then delete the corresponding record according to reservationId.
   *
   * Query the corresponding homeSubCluster according to reservationId,
   * we should get a NULL at this time.
   *
   * @throws Exception when the error occurs
   */
  @Test
  public void testCheckDeleteReservationHomeSubCluster() throws Exception {

    // procedure call parameter preparation
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    String subHomeClusterId = "SC-1";
    addReservationHomeSubCluster(
        CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER, reservationId.toString(), subHomeClusterId);

    // call the delete method of the reservation
    ReservationHomeSC resultHC = deleteReservationHomeSubCluster(
        CALL_SP_DELETE_RESERVATION_HOME_SUBCLUSTER, reservationId.toString());

    assertNotNull(resultHC);
    assertEquals(1, resultHC.dbUpdateCount);

    // call getReservationHomeSubCluster to get the result
    ReservationHomeSC resultHC1 = getReservationHomeSubCluster(
        CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER, reservationId.toString());
    assertNotNull(resultHC1);
    assertEquals(null, resultHC1.subHomeClusterId);
  }

  /**
   * This test case is used to verify the processing logic of the incorrect number of
   * updated records returned by the database when AddReservationHomeSubCluster is used.
   *
   * The probability of the database returning an update record greater than 1 is very low.
   *
   * @throws Exception when the error occurs
   */
  @Test
  public void testAddReservationHomeSubClusterAbnormalSituation() throws Exception {

    Connection conn =  sqlFederationStateStore.getConn();
    conn.prepareStatement(SP_DROP_ADDRESERVATIONHOMESUBCLUSTER).execute();
    conn.prepareStatement(SP_ADDRESERVATIONHOMESUBCLUSTER2).execute();

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId subClusterId = SubClusterId.newInstance("SC");

    ReservationHomeSubCluster reservationHomeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId);
    AddReservationHomeSubClusterRequest request =
        AddReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster);

    String errorMsg = String.format(
        "Wrong behavior during the insertion of subCluster %s according to reservation %s. " +
        "The database expects to insert 1 record, but the number of " +
        "inserted changes is greater than 1, " +
        "please check the records of the database.", subClusterId, reservationId);

    LambdaTestUtils.intercept(YarnException.class, errorMsg,
        () -> sqlFederationStateStore.addReservationHomeSubCluster(request));
  }

  /**
   * This test case is used to verify the logic when calling the updateReservationHomeSubCluster
   * method if the database returns an inaccurate result.
   *
   * The probability of the database returning an update record greater than 1 is very low.
   *
   * @throws Exception when the error occurs
   */
  @Test
  public void testUpdateReservationHomeSubClusterAbnormalSituation() throws Exception {

    Connection conn =  sqlFederationStateStore.getConn();
    conn.prepareStatement(SP_DROP_UPDATERESERVATIONHOMESUBCLUSTER).execute();
    conn.prepareStatement(SP_UPDATERESERVATIONHOMESUBCLUSTER2).execute();

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC");

    // add Reservation data.
    ReservationHomeSubCluster reservationHomeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId1);
    AddReservationHomeSubClusterRequest addRequest =
        AddReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster);
    sqlFederationStateStore.addReservationHomeSubCluster(addRequest);

    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    ReservationHomeSubCluster reservationHomeSubCluster2 =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId2);
    UpdateReservationHomeSubClusterRequest updateRequest =
        UpdateReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster2);

    String errorMsg = String.format(
        "Wrong behavior during update the subCluster %s according to reservation %s. " +
        "The database is expected to update 1 record, " +
        "but the number of database update records is greater than 1, " +
        "the records of the database should be checked.",
        subClusterId2, reservationId);

    LambdaTestUtils.intercept(YarnException.class, errorMsg,
        () -> sqlFederationStateStore.updateReservationHomeSubCluster(updateRequest));
  }

  /**
   * This test case is used to verify the logic when calling the deleteReservationHomeSubCluster
   * method if the database returns an inaccurate result.
   *
   * The probability of the database returning an update record greater than 1 is very low.
   *
   * @throws Exception when the error occurs
   */
  @Test
  public void testDeleteReservationHomeSubClusterAbnormalSituation() throws Exception {

    Connection conn =  sqlFederationStateStore.getConn();
    conn.prepareStatement(SP_DROP_DELETERESERVATIONHOMESUBCLUSTER).execute();
    conn.prepareStatement(SP_DELETERESERVATIONHOMESUBCLUSTER2).execute();

    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC");

    // add Reservation data.
    ReservationHomeSubCluster reservationHomeSubCluster =
        ReservationHomeSubCluster.newInstance(reservationId, subClusterId1);
    AddReservationHomeSubClusterRequest addRequest =
        AddReservationHomeSubClusterRequest.newInstance(reservationHomeSubCluster);
    sqlFederationStateStore.addReservationHomeSubCluster(addRequest);

    DeleteReservationHomeSubClusterRequest delRequest =
        DeleteReservationHomeSubClusterRequest.newInstance(reservationId);

    String errorMsg = String.format(
        "Wrong behavior during deleting the reservation %s. " +
        "The database is expected to delete 1 record, " +
        "but the number of deleted records returned by the database is greater than 1, " +
        "indicating that a duplicate reservationId occurred during the deletion process.",
        reservationId);

    LambdaTestUtils.intercept(YarnException.class, errorMsg,
        () -> sqlFederationStateStore.deleteReservationHomeSubCluster(delRequest));
  }

  @Override
  protected void checkRouterMasterKey(DelegationKey delegationKey,
      RouterMasterKey routerMasterKey) throws YarnException, IOException, SQLException {
    // Check for MasterKey stored in DB.
    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(routerMasterKey);

    // Query Data from DB.
    Connection conn =  sqlFederationStateStore.getConn();
    int paramKeyId = delegationKey.getKeyId();
    FederationQueryRunner runner = new FederationQueryRunner();
    FederationSQLOutParameter<String> masterKeyOUT =
        new FederationSQLOutParameter<>("masterKey_OUT", VARCHAR, String.class);
    RouterMasterKey sqlRouterMasterKey = runner.execute(
        conn, CALL_SP_GET_MASTERKEY, new RouterMasterKeyHandler(), paramKeyId, masterKeyOUT);

    // Check Data.
    RouterMasterKeyResponse response = getStateStore().
        getMasterKeyByDelegationKey(routerMasterKeyRequest);
    assertNotNull(response);
    RouterMasterKey respRouterMasterKey = response.getRouterMasterKey();
    assertEquals(routerMasterKey, respRouterMasterKey);
    assertEquals(routerMasterKey, sqlRouterMasterKey);
    assertEquals(sqlRouterMasterKey, respRouterMasterKey);
  }

  @Override
  protected void checkRouterStoreToken(RMDelegationTokenIdentifier identifier,
      RouterStoreToken token) throws YarnException, IOException, SQLException {
    // Get SequenceNum.
    int sequenceNum = identifier.getSequenceNumber();

    // Query Data from DB.
    Connection conn = sqlFederationStateStore.getConn();
    FederationQueryRunner runner = new FederationQueryRunner();
    FederationSQLOutParameter<String> tokenIdentOUT =
         new FederationSQLOutParameter<>("tokenIdent_OUT", VARCHAR, String.class);
    FederationSQLOutParameter<String> tokenOUT =
         new FederationSQLOutParameter<>("token_OUT", VARCHAR, String.class);
    FederationSQLOutParameter<Long> renewDateOUT =
         new FederationSQLOutParameter<>("renewDate_OUT", BIGINT, Long.class);
    RouterStoreToken sqlRouterStoreToken = runner.execute(conn, CALL_SP_GET_DELEGATIONTOKEN,
        new RouterStoreTokenHandler(), sequenceNum, tokenIdentOUT, tokenOUT, renewDateOUT);

    assertEquals(token, sqlRouterStoreToken);
  }

  @Test
  public void testCheckHSQLDB() throws SQLException {
    Connection conn =  sqlFederationStateStore.getConn();
    DbType dbType = DatabaseProduct.getDbType(conn);
    assertEquals(DbType.HSQLDB, dbType);
  }

  @Test
  public void testGetDbTypeNullConn() throws SQLException {
    DbType dbType = DatabaseProduct.getDbType(null);
    assertEquals(DbType.UNDEFINED, dbType);
  }

  @Test
  public void testGetDBTypeEmptyConn() throws SQLException {
    Connection connection = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(metaData.getDatabaseProductName()).thenReturn("");
    when(connection.getMetaData()).thenReturn(metaData);
    DbType dbType = DatabaseProduct.getDbType(connection);
    assertEquals(DbType.UNDEFINED, dbType);
  }

  @Test
  public void testCheckForHSQLDBUpdateSQL() throws SQLException {
    String sql = "select sequenceName, nextVal from sequenceTable";
    String hsqlDBSQL = DatabaseProduct.addForUpdateClause(DbType.HSQLDB, sql);
    String expectUpdateSQL = "select sequenceName, nextVal from sequenceTable for update";
    assertEquals(expectUpdateSQL, hsqlDBSQL);
  }

  @Test
  public void testCheckForSqlServerDBUpdateSQL() throws SQLException {
    String sql = "select sequenceName, nextVal from sequenceTable";
    String sqlServerDBSQL = DatabaseProduct.addForUpdateClause(DbType.SQLSERVER, sql);
    String expectUpdateSQL = "select sequenceName, nextVal from sequenceTable with (updlock)";
    assertEquals(expectUpdateSQL, sqlServerDBSQL);
  }

  @Test
  public void testCheckHikariDataSourceParam() throws SQLException {
    HikariDataSource dataSource = sqlFederationStateStore.getDataSource();
    long maxLifeTime = dataSource.getMaxLifetime();
    long idleTimeOut = dataSource.getIdleTimeout();
    long connTimeOut = dataSource.getConnectionTimeout();
    String poolName = dataSource.getPoolName();
    int minimumIdle = dataSource.getMinimumIdle();
    int maximumPoolSize = dataSource.getMaximumPoolSize();

    // maxLifeTime 30 minute, 1800000 ms
    assertEquals(1800000, maxLifeTime);
    // idleTimeOut 10 minute, 600000 ms
    assertEquals(600000, idleTimeOut);
    // connTimeOut 10 second, 10000 ms
    assertEquals(10000, connTimeOut);
    assertEquals("YARN-Federation-DataBasePool", poolName);
    assertEquals(1, minimumIdle);
    assertEquals(1, maximumPoolSize);
  }
}