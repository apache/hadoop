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

package org.apache.hadoop.yarn.server.federation.store.utils;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreRetriableException;
import org.apache.hadoop.yarn.server.federation.store.metrics.FederationStateStoreClientMetrics;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

/**
 * Common utility methods used by the store implementations.
 */
public final class FederationStateStoreUtils {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreUtils.class);

  public final static String FEDERATION_STORE_URL = "url";

  private FederationStateStoreUtils() {
  }

  /**
   * Returns the SQL <code>FederationStateStore</code> connections to the pool.
   *
   * @param log the logger interface
   * @param cstmt the interface used to execute SQL stored procedures
   * @param conn the SQL connection
   * @param rs the ResultSet interface used to execute SQL stored procedures
   * @throws YarnException on failure
   */
  public static void returnToPool(Logger log, CallableStatement cstmt,
      Connection conn, ResultSet rs) throws YarnException {
    if (cstmt != null) {
      try {
        cstmt.close();
      } catch (SQLException e) {
        logAndThrowException(log, "Exception while trying to close Statement",
            e);
      }
    }

    if (conn != null) {
      try {
        conn.close();
        FederationStateStoreClientMetrics.decrConnections();
      } catch (SQLException e) {
        logAndThrowException(log, "Exception while trying to close Connection",
            e);
      }
    }

    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        logAndThrowException(log, "Exception while trying to close ResultSet",
            e);
      }
    }
  }

  /**
   * Returns the SQL <code>FederationStateStore</code> connections to the pool.
   *
   * @param log the logger interface
   * @param cstmt the interface used to execute SQL stored procedures
   * @param conn the SQL connection
   * @throws YarnException on failure
   */
  public static void returnToPool(Logger log, CallableStatement cstmt,
      Connection conn) throws YarnException {
    returnToPool(log, cstmt, conn, null);
  }

  /**
   * Returns the SQL <code>FederationStateStore</code> connections to the pool.
   *
   * @param log the logger interface
   * @param cstmt the interface used to execute SQL stored procedures
   * @throws YarnException on failure
   */
  public static void returnToPool(Logger log, CallableStatement cstmt)
      throws YarnException {
    returnToPool(log, cstmt, null);
  }

  /**
   * Throws an exception due to an error in <code>FederationStateStore</code>.
   *
   * @param log the logger interface
   * @param errMsg the error message
   * @param t the throwable raised in the called class.
   * @throws YarnException on failure
   */
  public static void logAndThrowException(Logger log, String errMsg,
      Throwable t) throws YarnException {
    if (t != null) {
      log.error(errMsg, t);
      throw new YarnException(errMsg, t);
    } else {
      log.error(errMsg);
      throw new YarnException(errMsg);
    }
  }

  /**
   * Throws an <code>FederationStateStoreException</code> due to an error in
   * <code>FederationStateStore</code>.
   *
   * @param log the logger interface
   * @param errMsg the error message
   * @throws YarnException on failure
   */
  public static void logAndThrowStoreException(Logger log, String errMsg)
      throws YarnException {
    log.error(errMsg);
    throw new FederationStateStoreException(errMsg);
  }

  /**
   * Throws an <code>FederationStateStoreException</code> due to an error in
   * <code>FederationStateStore</code>.
   *
   * @param log the logger interface
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @throws YarnException on failure
   */
  public static void logAndThrowStoreException(Logger log, String errMsgFormat, Object... args)
      throws YarnException {
    String errMsg = String.format(errMsgFormat, args);
    log.error(errMsg);
    throw new FederationStateStoreException(errMsg);
  }


  /**
   * Throws an <code>FederationStateStoreException</code> due to an error in
   * <code>FederationStateStore</code>.
   *
   * @param t the throwable raised in the called class.
   * @param log the logger interface.
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @throws YarnException on failure
   */
  public static void logAndThrowStoreException(
      Throwable t, Logger log, String errMsgFormat, Object... args) throws YarnException {
    String errMsg = String.format(errMsgFormat, args);
    if (t != null) {
      log.error(errMsg, t);
      throw new FederationStateStoreException(errMsg, t);
    } else {
      log.error(errMsg);
      throw new FederationStateStoreException(errMsg);
    }
  }

  /**
   * Throws an <code>FederationStateStoreInvalidInputException</code> due to an
   * error in <code>FederationStateStore</code>.
   *
   * @param log the logger interface
   * @param errMsg the error message
   * @throws YarnException on failure
   */
  public static void logAndThrowInvalidInputException(Logger log, String errMsg)
      throws YarnException {
    log.error(errMsg);
    throw new FederationStateStoreInvalidInputException(errMsg);
  }

  /**
   * Throws an <code>FederationStateStoreRetriableException</code> due to an
   * error in <code>FederationStateStore</code>.
   *
   * @param log the logger interface
   * @param errMsg the error message
   * @param t the throwable raised in the called class.
   * @throws YarnException on failure
   */
  public static void logAndThrowRetriableException(Logger log, String errMsg,
      Throwable t) throws YarnException {
    if (t != null) {
      log.error(errMsg, t);
      throw new FederationStateStoreRetriableException(errMsg, t);
    } else {
      log.error(errMsg);
      throw new FederationStateStoreRetriableException(errMsg);
    }
  }

  /**
   * Throws an <code>FederationStateStoreRetriableException</code> due to an
   * error in <code>FederationStateStore</code>.
   *
   * @param t the throwable raised in the called class.
   * @param log the logger interface.
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @throws YarnException on failure
   */
  public static void logAndThrowRetriableException(
      Throwable t, Logger log, String errMsgFormat, Object... args) throws YarnException {
    String errMsg = String.format(errMsgFormat, args);
    if (t != null) {
      log.error(errMsg, t);
      throw new FederationStateStoreRetriableException(errMsg, t);
    } else {
      log.error(errMsg);
      throw new FederationStateStoreRetriableException(errMsg);
    }
  }

  /**
   * Throws an <code>FederationStateStoreRetriableException</code> due to an
   * error in <code>FederationStateStore</code>.
   *
   * @param log the logger interface.
   * @param errMsgFormat the error message format string.
   * @param args referenced by the format specifiers in the format string.
   * @throws YarnException on failure
   */
  public static void logAndThrowRetriableException(
      Logger log, String errMsgFormat, Object... args) throws YarnException {
    String errMsg = String.format(errMsgFormat, args);
    log.error(errMsg);
    throw new FederationStateStoreRetriableException(errMsg);
  }

  /**
   * Sets a specific value for a specific property of
   * <code>HikariDataSource</code> SQL connections.
   *
   * @param dataSource the <code>HikariDataSource</code> connections
   * @param property the property to set
   * @param value the value to set
   */
  public static void setProperty(HikariDataSource dataSource, String property,
      String value) {
    LOG.debug("Setting property {} with value {}", property, value);
    if (property != null && !property.isEmpty() && value != null) {
      dataSource.addDataSourceProperty(property, value);
    }
  }

  /**
   * Sets a specific username for <code>HikariDataSource</code> SQL connections.
   *
   * @param dataSource the <code>HikariDataSource</code> connections
   * @param userNameDB the value to set
   */
  public static void setUsername(HikariDataSource dataSource,
      String userNameDB) {
    if (userNameDB != null) {
      dataSource.setUsername(userNameDB);
      LOG.debug("Setting non NULL Username for Store connection");
    } else {
      LOG.debug("NULL Username specified for Store connection, so ignoring");
    }
  }

  /**
   * Sets a specific password for <code>HikariDataSource</code> SQL connections.
   *
   * @param dataSource the <code>HikariDataSource</code> connections
   * @param password the value to set
   */
  public static void setPassword(HikariDataSource dataSource, String password) {
    if (password != null) {
      dataSource.setPassword(password);
      LOG.debug("Setting non NULL Credentials for Store connection");
    } else {
      LOG.debug("NULL Credentials specified for Store connection, so ignoring");
    }
  }

  /**
   * Filter HomeSubCluster based on Filter SubCluster.
   *
   * @param filterSubCluster filter query conditions
   * @param homeSubCluster homeSubCluster
   * @return return true, if match filter conditions,
   *         return false, if not match filter conditions.
   */
  public static boolean filterHomeSubCluster(SubClusterId filterSubCluster,
      SubClusterId homeSubCluster) {

    // If the filter condition is empty,
    // it means that homeSubCluster needs to be added
    if (filterSubCluster == null) {
      return true;
    }

    // If the filter condition filterSubCluster is not empty,
    // and filterSubCluster is equal to homeSubCluster, it needs to be added
    if (filterSubCluster.equals(homeSubCluster)) {
      return true;
    }

    return false;
  }

  /**
   * Encode for Writable objects.
   * This method will convert the writable object to a base64 string.
   *
   * @param key Writable Key.
   * @return base64 string.
   * @throws IOException raised on errors performing I/O.
   */
  public static String encodeWritable(Writable key) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    key.write(dos);
    dos.flush();
    return Base64.getUrlEncoder().encodeToString(bos.toByteArray());
  }

  /**
   * Decode Base64 string to Writable object.
   *
   * @param w Writable Key.
   * @param idStr base64 string.
   * @throws IOException raised on errors performing I/O.
   */
  public static void decodeWritable(Writable w, String idStr) throws IOException {
    DataInputStream in = new DataInputStream(
        new ByteArrayInputStream(Base64.getUrlDecoder().decode(idStr)));
    w.readFields(in);
  }

  /**
   * Convert MasterKey to DelegationKey.
   *
   * Before using this function,
   * please use FederationRouterRMTokenInputValidator to verify the request.
   * By default, the request is not empty, and the internal object is not empty.
   *
   * @param request RouterMasterKeyRequest
   * @return DelegationKey.
   */
  public static DelegationKey convertMasterKeyToDelegationKey(RouterMasterKeyRequest request) {
    RouterMasterKey masterKey = request.getRouterMasterKey();
    return convertMasterKeyToDelegationKey(masterKey);
  }

  /**
   * Convert MasterKey to DelegationKey.
   *
   * @param masterKey masterKey.
   * @return DelegationKey.
   */
  private static DelegationKey convertMasterKeyToDelegationKey(RouterMasterKey masterKey) {
    ByteBuffer keyByteBuf = masterKey.getKeyBytes();
    byte[] keyBytes = new byte[keyByteBuf.remaining()];
    keyByteBuf.get(keyBytes);
    return new DelegationKey(masterKey.getKeyId(), masterKey.getExpiryDate(), keyBytes);
  }
}
