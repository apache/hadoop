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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Distributed counter that relies on a SQL database to synchronize
 * between multiple clients. This expects a table with a single int field
 * to exist in the database. One record must exist on the table at all times,
 * representing the last used value reserved by a client.
 */
public class DistributedSQLCounter {
  private static final Logger LOG =
      LoggerFactory.getLogger(DistributedSQLCounter.class);

  private final String field;
  private final String table;
  private final SQLConnectionFactory connectionFactory;

  public DistributedSQLCounter(String field, String table,
      SQLConnectionFactory connectionFactory) {
    this.field = field;
    this.table = table;
    this.connectionFactory = connectionFactory;
  }

  /**
   * Obtains the value of the counter.
   *
   * @return counter value.
   * @throws SQLException if querying the database fails.
   */
  public int selectCounterValue() throws SQLException {
    try (Connection connection = connectionFactory.getConnection()) {
      return selectCounterValue(false, connection);
    }
  }

  private int selectCounterValue(boolean forUpdate, Connection connection) throws SQLException {
    String query = String.format("SELECT %s FROM %s %s", field, table,
        forUpdate ? "FOR UPDATE" : "");
    LOG.debug("Select counter statement: " + query);
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query)) {
      if (result.next()) {
        return result.getInt(field);
      } else {
        throw new IllegalStateException("Counter table not initialized: " + table);
      }
    }
  }

  /**
   * Sets the counter to the given value.
   *
   * @param value Value to assign to counter.
   * @throws SQLException if querying the database fails.
   */
  public void updateCounterValue(int value) throws SQLException {
    try (Connection connection = connectionFactory.getConnection(true)) {
      updateCounterValue(value, connection);
    }
  }

  /**
   * Sets the counter to the given value.
   *
   * @param value Value to assign to counter.
   * @param connection Connection to database hosting the counter table.
   * @throws SQLException if querying the database fails.
   */
  public void updateCounterValue(int value, Connection connection) throws SQLException {
    String queryText = String.format("UPDATE %s SET %s = ?", table, field);
    LOG.debug("Update counter statement: " + queryText + ". Value: " + value);
    try (PreparedStatement statement = connection.prepareStatement(queryText)) {
      statement.setInt(1, value);
      statement.execute();
    }
  }

  /**
   * Increments the counter by the given amount and
   * returns the previous counter value.
   *
   * @param amount Amount to increase the counter.
   * @return Previous counter value.
   * @throws SQLException if querying the database fails.
   */
  public int incrementCounterValue(int amount) throws SQLException {
    // Disabling auto-commit to ensure that all statements on this transaction
    // are committed at once.
    try (Connection connection = connectionFactory.getConnection(false)) {
      // Preventing dirty reads and non-repeatable reads to ensure that the
      // value read will not be updated by a different connection.
      if (connection.getTransactionIsolation() < Connection.TRANSACTION_REPEATABLE_READ) {
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      }

      try {
        // Reading the counter value "FOR UPDATE" to lock the value record,
        // forcing other connections to wait until this transaction is committed.
        int lastValue = selectCounterValue(true, connection);

        // Calculate the new counter value and handling overflow by
        // resetting the counter to 0.
        int newValue = lastValue + amount;
        if (newValue < 0) {
          lastValue = 0;
          newValue = amount;
        }

        updateCounterValue(newValue, connection);
        connection.commit();
        return lastValue;
      } catch (Exception e) {
        // Rollback transaction to release table locks
        connection.rollback();
        throw e;
      }
    }
  }
}
