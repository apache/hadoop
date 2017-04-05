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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreErrorCode;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreRetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common utility methods used by the store implementations.
 *
 */
public final class FederationStateStoreUtils {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreUtils.class);

  private FederationStateStoreUtils() {
  }

  /**
   * Returns the SQL <code>FederationStateStore</code> connection to the pool.
   *
   * @param log the logger interface
   * @param cstmt the interface used to execute SQL stored procedures
   * @param conn the SQL connection
   * @throws YarnException on failure
   */
  public static void returnToPool(Logger log, CallableStatement cstmt,
      Connection conn) throws YarnException {
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
      } catch (SQLException e) {
        logAndThrowException(log, "Exception while trying to close Connection",
            e);
      }
    }
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
   * @param code FederationStateStoreErrorCode of the error
   * @param errMsg the error message
   * @throws YarnException on failure
   */
  public static void logAndThrowStoreException(Logger log,
      FederationStateStoreErrorCode code, String errMsg) throws YarnException {
    log.error(errMsg + " " + code.toString());
    throw new FederationStateStoreException(code);
  }

  /**
   * Throws an <code>FederationStateStoreException</code> due to an error in
   * <code>FederationStateStore</code>.
   *
   * @param log the logger interface
   * @param code FederationStateStoreErrorCode of the error
   * @throws YarnException on failure
   */
  public static void logAndThrowStoreException(Logger log,
      FederationStateStoreErrorCode code) throws YarnException {
    log.error(code.toString());
    throw new FederationStateStoreException(code);
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
    LOG.error(errMsg);
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
      LOG.error(errMsg, t);
      throw new FederationStateStoreRetriableException(errMsg, t);
    } else {
      LOG.error(errMsg);
      throw new FederationStateStoreRetriableException(errMsg);
    }
  }
}
