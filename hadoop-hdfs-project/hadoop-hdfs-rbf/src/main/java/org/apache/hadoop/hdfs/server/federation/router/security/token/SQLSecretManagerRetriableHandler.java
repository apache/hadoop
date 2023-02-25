/*
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

package org.apache.hadoop.hdfs.server.federation.router.security.token;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.token.delegation.SQLDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Interface to handle retries when {@link SQLDelegationTokenSecretManagerImpl}
 * throws expected errors.
 */
public interface SQLSecretManagerRetriableHandler {
  void execute(SQLCommandVoid command) throws SQLException;
  <T> T execute(SQLCommand<T> command) throws SQLException;

  @FunctionalInterface
  interface SQLCommandVoid {
    void doCall() throws SQLException;
  }

  @FunctionalInterface
  interface SQLCommand<T> {
    T doCall() throws SQLException;
  }
}

/**
 * Implementation of {@link SQLSecretManagerRetriableHandler} that uses a
 * {@link RetryProxy} to simplify the retryable operations.
 */
class SQLSecretManagerRetriableHandlerImpl implements SQLSecretManagerRetriableHandler {
  public final static String MAX_RETRIES =
      SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX + "max-retries";
  public final static int MAX_RETRIES_DEFAULT = 0;
  public final static String RETRY_SLEEP_TIME_MS =
      SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX + "retry-sleep-time-ms";
  public final static long RETRY_SLEEP_TIME_MS_DEFAULT = 100;

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLSecretManagerRetriableHandlerImpl.class);

  static SQLSecretManagerRetriableHandler getInstance(Configuration conf) {
    return getInstance(conf, new SQLSecretManagerRetriableHandlerImpl());
  }

  static SQLSecretManagerRetriableHandler getInstance(Configuration conf,
      SQLSecretManagerRetriableHandlerImpl retryHandler) {
    RetryPolicy basePolicy = RetryPolicies.exponentialBackoffRetry(
        conf.getInt(MAX_RETRIES, MAX_RETRIES_DEFAULT),
        conf.getLong(RETRY_SLEEP_TIME_MS, RETRY_SLEEP_TIME_MS_DEFAULT),
        TimeUnit.MILLISECONDS);

    // Configure SQLSecretManagerRetriableException to retry with exponential backoff
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<>();
    exceptionToPolicyMap.put(SQLSecretManagerRetriableException.class, basePolicy);

    // Configure all other exceptions to fail after one attempt
    RetryPolicy retryPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);

    return (SQLSecretManagerRetriableHandler) RetryProxy.create(
        SQLSecretManagerRetriableHandler.class, retryHandler, retryPolicy);
  }

  /**
   * Executes a SQL command and raises retryable errors as
   * {@link SQLSecretManagerRetriableException}s so they are recognized by the
   * {@link RetryProxy}.
   * @param command SQL command to execute
   * @throws SQLException When SQL connection errors occur
   */
  @Override
  public void execute(SQLCommandVoid command) throws SQLException {
    try {
      command.doCall();
    } catch (SQLException e) {
      LOG.warn("Failed to execute SQL command", e);
      throw new SQLSecretManagerRetriableException(e);
    }
  }

  /**
   * Executes a SQL command and raises retryable errors as
   * {@link SQLSecretManagerRetriableException}s so they are recognized by the
   * {@link RetryProxy}.
   * @param command SQL command to execute
   * @throws SQLException When SQL connection errors occur
   */
  @Override
  public <T> T execute(SQLCommand<T> command) throws SQLException {
    try {
      return command.doCall();
    } catch (SQLException e) {
      LOG.warn("Failed to execute SQL command", e);
      throw new SQLSecretManagerRetriableException(e);
    }
  }

  /**
   * Class used to identify errors that can be retried.
   */
  static class SQLSecretManagerRetriableException extends SQLException {
    SQLSecretManagerRetriableException(Throwable cause) {
      super(cause);
    }
  }
}
