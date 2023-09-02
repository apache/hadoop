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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.SQLDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link SQLDelegationTokenSecretManager} that
 * persists TokenIdentifiers and DelegationKeys in a SQL database.
 * This implementation relies on the Datanucleus JDO PersistenceManager, which
 * can be configured with datanucleus.* configuration properties.
 */
public class SQLDelegationTokenSecretManagerImpl
    extends SQLDelegationTokenSecretManager<AbstractDelegationTokenIdentifier> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLDelegationTokenSecretManagerImpl.class);
  private static final String SEQ_NUM_COUNTER_FIELD = "sequenceNum";
  private static final String SEQ_NUM_COUNTER_TABLE = "LastSequenceNum";
  private static final String KEY_ID_COUNTER_FIELD = "keyId";
  private static final String KEY_ID_COUNTER_TABLE = "LastDelegationKeyId";

  private final SQLConnectionFactory connectionFactory;
  private final DistributedSQLCounter sequenceNumCounter;
  private final DistributedSQLCounter delegationKeyIdCounter;
  private final SQLSecretManagerRetriableHandler retryHandler;

  public SQLDelegationTokenSecretManagerImpl(Configuration conf) {
    this(conf, new HikariDataSourceConnectionFactory(conf),
        SQLSecretManagerRetriableHandlerImpl.getInstance(conf));
  }

  public SQLDelegationTokenSecretManagerImpl(Configuration conf,
      SQLConnectionFactory connectionFactory, SQLSecretManagerRetriableHandler retryHandler) {
    super(conf);

    this.connectionFactory = connectionFactory;
    this.sequenceNumCounter = new DistributedSQLCounter(SEQ_NUM_COUNTER_FIELD,
        SEQ_NUM_COUNTER_TABLE, connectionFactory);
    this.delegationKeyIdCounter = new DistributedSQLCounter(KEY_ID_COUNTER_FIELD,
        KEY_ID_COUNTER_TABLE, connectionFactory);
    this.retryHandler = retryHandler;

    try {
      super.startThreads();
    } catch (IOException e) {
      throw new RuntimeException("Error starting threads for MySQL secret manager", e);
    }

    LOG.info("MySQL delegation token secret manager instantiated");
  }

  @Override
  public DelegationTokenIdentifier createIdentifier() {
    return new DelegationTokenIdentifier();
  }

  @Override
  public void stopThreads() {
    super.stopThreads();
    connectionFactory.shutdown();
  }

  @Override
  protected void insertToken(int sequenceNum, byte[] tokenIdentifier, byte[] tokenInfo)
      throws SQLException {
    retryHandler.execute(() -> {
      try (Connection connection = connectionFactory.getConnection(true);
          PreparedStatement statement = connection.prepareStatement(
              "INSERT INTO Tokens (sequenceNum, tokenIdentifier, tokenInfo) VALUES (?, ?, ?)")) {
        statement.setInt(1, sequenceNum);
        statement.setBytes(2, tokenIdentifier);
        statement.setBytes(3, tokenInfo);
        statement.execute();
      }
    });
  }

  @Override
  protected void updateToken(int sequenceNum, byte[] tokenIdentifier, byte[] tokenInfo)
      throws SQLException {
    retryHandler.execute(() -> {
      try (Connection connection = connectionFactory.getConnection(true);
          PreparedStatement statement = connection.prepareStatement(
              "UPDATE Tokens SET tokenInfo = ? WHERE sequenceNum = ? AND tokenIdentifier = ?")) {
        statement.setBytes(1, tokenInfo);
        statement.setInt(2, sequenceNum);
        statement.setBytes(3, tokenIdentifier);
        statement.execute();
      }
    });
  }

  @Override
  protected void deleteToken(int sequenceNum, byte[] tokenIdentifier) throws SQLException {
    retryHandler.execute(() -> {
      try (Connection connection = connectionFactory.getConnection(true);
          PreparedStatement statement = connection.prepareStatement(
              "DELETE FROM Tokens WHERE sequenceNum = ? AND tokenIdentifier = ?")) {
        statement.setInt(1, sequenceNum);
        statement.setBytes(2, tokenIdentifier);
        statement.execute();
      }
    });
  }

  @Override
  protected byte[] selectTokenInfo(int sequenceNum, byte[] tokenIdentifier) throws SQLException {
    return retryHandler.execute(() -> {
      try (Connection connection = connectionFactory.getConnection();
          PreparedStatement statement = connection.prepareStatement(
              "SELECT tokenInfo FROM Tokens WHERE sequenceNum = ? AND tokenIdentifier = ?")) {
        statement.setInt(1, sequenceNum);
        statement.setBytes(2, tokenIdentifier);
        try (ResultSet result = statement.executeQuery()) {
          if (result.next()) {
            return result.getBytes("tokenInfo");
          }
        }
      }
      return null;
    });
  }

  @Override
  protected Map<byte[], byte[]> selectStaleTokenInfos(long maxModifiedTime, int maxResults)
      throws SQLException {
    return retryHandler.execute(() -> {
      try (Connection connection = connectionFactory.getConnection();
          PreparedStatement statement = connection.prepareStatement(
              "SELECT tokenIdentifier, tokenInfo FROM Tokens WHERE modifiedTime < ?")) {
        statement.setTimestamp(1, new Timestamp(maxModifiedTime));
        statement.setMaxRows(maxResults);
        try (ResultSet result = statement.executeQuery()) {
          Map<byte[], byte[]> results = new HashMap<>();
          while (result.next()) {
            results.put(result.getBytes("tokenIdentifier"),
                result.getBytes("tokenInfo"));
          }
          return results;
        }
      }
    });
  }

  @Override
  protected void insertDelegationKey(int keyId, byte[] delegationKey) throws SQLException {
    retryHandler.execute(() -> {
      try (Connection connection = connectionFactory.getConnection(true);
          PreparedStatement statement = connection.prepareStatement(
              "INSERT INTO DelegationKeys (keyId, delegationKey) VALUES (?, ?)")) {
        statement.setInt(1, keyId);
        statement.setBytes(2, delegationKey);
        statement.execute();
      }
    });
  }

  @Override
  protected void updateDelegationKey(int keyId, byte[] delegationKey) throws SQLException {
    retryHandler.execute(() -> {
      try (Connection connection = connectionFactory.getConnection(true);
          PreparedStatement statement = connection.prepareStatement(
              "UPDATE DelegationKeys SET delegationKey = ? WHERE keyId = ?")) {
        statement.setBytes(1, delegationKey);
        statement.setInt(2, keyId);
        statement.execute();
      }
    });
  }

  @Override
  protected void deleteDelegationKey(int keyId) throws SQLException {
    retryHandler.execute(() -> {
      try (Connection connection = connectionFactory.getConnection(true);
          PreparedStatement statement = connection.prepareStatement(
              "DELETE FROM DelegationKeys WHERE keyId = ?")) {
        statement.setInt(1, keyId);
        statement.execute();
      }
    });
  }

  @Override
  protected byte[] selectDelegationKey(int keyId) throws SQLException {
    return retryHandler.execute(() -> {
      try (Connection connection = connectionFactory.getConnection();
          PreparedStatement statement = connection.prepareStatement(
              "SELECT delegationKey FROM DelegationKeys WHERE keyId = ?")) {
        statement.setInt(1, keyId);
        try (ResultSet result = statement.executeQuery()) {
          if (result.next()) {
            return result.getBytes("delegationKey");
          }
        }
      }
      return null;
    });
  }

  @Override
  protected int selectSequenceNum() throws SQLException {
    return retryHandler.execute(() -> sequenceNumCounter.selectCounterValue());
  }

  @Override
  protected void updateSequenceNum(int value) throws SQLException {
    retryHandler.execute(() -> sequenceNumCounter.updateCounterValue(value));
  }

  @Override
  protected int incrementSequenceNum(int amount) throws SQLException {
    return retryHandler.execute(() -> sequenceNumCounter.incrementCounterValue(amount));
  }

  @Override
  protected int selectKeyId() throws SQLException {
    return retryHandler.execute(delegationKeyIdCounter::selectCounterValue);
  }

  @Override
  protected void updateKeyId(int value) throws SQLException {
    retryHandler.execute(() -> delegationKeyIdCounter.updateCounterValue(value));
  }

  @Override
  protected int incrementKeyId(int amount) throws SQLException {
    return retryHandler.execute(() -> delegationKeyIdCounter.incrementCounterValue(amount));
  }

  @VisibleForTesting
  protected SQLConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }
}
