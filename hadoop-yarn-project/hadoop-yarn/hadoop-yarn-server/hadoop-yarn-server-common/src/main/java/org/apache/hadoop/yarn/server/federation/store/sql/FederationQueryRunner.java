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

package org.apache.hadoop.yarn.server.federation.store.sql;

import org.apache.hadoop.classification.VisibleForTesting;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.util.Arrays;

import org.apache.hadoop.yarn.server.federation.store.sql.DatabaseProduct.DbType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.federation.store.sql.DatabaseProduct.isDuplicateKeyError;

/**
 * QueryRunner is used to execute stored procedure SQL and parse the returned results.
 */
public class FederationQueryRunner {

  public final static String YARN_ROUTER_SEQUENCE_NUM = "YARN_ROUTER_SEQUENCE_NUM";

  public final static String YARN_ROUTER_CURRENT_KEY_ID = "YARN_ROUTER_CURRENT_KEY_ID";

  public final static String QUERY_SEQUENCE_TABLE_SQL =
      "SELECT nextVal FROM sequenceTable WHERE sequenceName = ?";

  public final static String INSERT_SEQUENCE_TABLE_SQL =
      "INSERT INTO sequenceTable(sequenceName, nextVal) VALUES(?, ?)";

  public final static String UPDATE_SEQUENCE_TABLE_SQL =
      "UPDATE sequenceTable SET nextVal = ? WHERE sequenceName = ?";

  public final static String DELETE_QUEUE_SQL = "DELETE FROM policies WHERE queue = ?";

  public static final Logger LOG = LoggerFactory.getLogger(FederationQueryRunner.class);

  /**
   * Execute Stored Procedure SQL.
   *
   * @param conn      Database Connection.
   * @param procedure Stored Procedure SQL.
   * @param rsh       Result Set handler.
   * @param params    List of stored procedure parameters.
   * @param <T>       Generic T.
   * @return Stored Procedure Result Set.
   * @throws SQLException An exception occurred when calling a stored procedure.
   */
  public <T> T execute(Connection conn, String procedure, ResultSetHandler<T> rsh, Object... params)
      throws SQLException {
    if (conn == null) {
      throw new SQLException("Null connection");
    }

    if (procedure == null) {
      throw new SQLException("Null Procedure SQL statement");
    }

    if (rsh == null) {
      throw new SQLException("Null ResultSetHandler");
    }

    CallableStatement stmt = null;
    T results = null;

    try {
      stmt = this.getCallableStatement(conn, procedure);
      this.fillStatement(stmt, params);
      stmt.executeUpdate();
      this.retrieveOutParameters(stmt, params);
      results = rsh.handle(params);
    } catch (SQLException e) {
      this.rethrow(e, procedure, params);
    } finally {
      close(stmt);
    }
    return results;
  }

  /**
   * Get CallableStatement from Conn.
   *
   * @param conn Database Connection.
   * @param procedure Stored Procedure SQL.
   * @return CallableStatement.
   * @throws SQLException An exception occurred when calling a stored procedure.
   */
  @VisibleForTesting
  protected CallableStatement getCallableStatement(Connection conn, String procedure)
      throws SQLException {
    return conn.prepareCall(procedure);
  }

  /**
   * Set Statement parameters.
   *
   * @param stmt CallableStatement.
   * @param params Stored procedure parameters.
   * @throws SQLException An exception occurred when calling a stored procedure.
   */
  public void fillStatement(CallableStatement stmt, Object... params)
      throws SQLException {
    for (int i = 0; i < params.length; i++) {
      if (params[i] != null) {
        if (stmt != null) {
          if (params[i] instanceof FederationSQLOutParameter) {
            FederationSQLOutParameter sqlOutParameter = (FederationSQLOutParameter) params[i];
            sqlOutParameter.register(stmt, i + 1);
          } else {
            stmt.setObject(i + 1, params[i]);
          }
        }
      }
    }
  }

  /**
   * Close Statement.
   *
   * @param stmt CallableStatement.
   * @throws SQLException An exception occurred when calling a stored procedure.
   */
  public void close(Statement stmt) throws SQLException {
    if (stmt != null) {
      stmt.close();
      stmt = null;
    }
  }

  /**
   * Retrieve execution result from CallableStatement.
   *
   * @param stmt CallableStatement.
   * @param params Stored procedure parameters.
   * @throws SQLException An exception occurred when calling a stored procedure.
   */
  private void retrieveOutParameters(CallableStatement stmt, Object[] params) throws SQLException {
    if (params != null && stmt != null) {
      for (int i = 0; i < params.length; i++) {
        if (params[i] instanceof FederationSQLOutParameter) {
          FederationSQLOutParameter sqlOutParameter = (FederationSQLOutParameter) params[i];
          sqlOutParameter.setValue(stmt, i + 1);
        }
      }
    }
  }

  /**
   * Re-throw SQL exception.
   *
   * @param cause SQLException.
   * @param sql Stored Procedure SQL.
   * @param params Stored procedure parameters.
   * @throws SQLException An exception occurred when calling a stored procedure.
   */
  protected void rethrow(SQLException cause, String sql, Object... params)
      throws SQLException {

    String causeMessage = cause.getMessage();
    if (causeMessage == null) {
      causeMessage = "";
    }

    StringBuilder msg = new StringBuilder(causeMessage);
    msg.append(" Query: ");
    msg.append(sql);
    msg.append(" Parameters: ");

    if (params == null) {
      msg.append("[]");
    } else {
      msg.append(Arrays.deepToString(params));
    }

    SQLException e = new SQLException(msg.toString(), cause.getSQLState(), cause.getErrorCode());
    e.setNextException(cause);
    throw e;
  }

  /**
   * We query or update the SequenceTable.
   *
   * @param connection database conn.
   * @param sequenceName sequenceName, We currently have 2 sequences,
   * YARN_ROUTER_SEQUENCE_NUM and YARN_ROUTER_CURRENT_KEY_ID.
   * @param isUpdate true, means we will update the SequenceTable,
   * false, we query the SequenceTable.
   *
   * @return SequenceValue.
   * @throws SQLException An exception occurred when calling a stored procedure.
   */
  public int selectOrUpdateSequenceTable(Connection connection, String sequenceName,
      boolean isUpdate) throws SQLException {

    int maxSequenceValue = 0;
    boolean insertDone = false;
    boolean committed = false;

    try {
      DbType dbType = DatabaseProduct.getDbType(connection);
      // Build the FOR UPDATE variant of the SELECT template once (string op on the
      // parameterized constant, before binding so the ? placeholder is preserved).
      String forUpdateSQL = DatabaseProduct.addForUpdateClause(dbType, QUERY_SEQUENCE_TABLE_SQL);

      // Step1. Query SequenceValue.
      while (maxSequenceValue == 0) {
        try (PreparedStatement select = connection.prepareStatement(forUpdateSQL)) {
          select.setString(1, sequenceName);
          try (ResultSet rs = select.executeQuery()) {
            if (rs.next()) {
              maxSequenceValue = rs.getInt("nextVal");
            } else if (insertDone) {
              throw new SQLException("Invalid state of SEQUENCE_TABLE for " + sequenceName);
            } else {
              insertDone = true;
              try (PreparedStatement insert =
                       connection.prepareStatement(INSERT_SEQUENCE_TABLE_SQL)) {
                insert.setString(1, sequenceName);
                insert.setInt(2, 1);
                try {
                  insert.executeUpdate();
                } catch (SQLException e) {
                  // If the record is already inserted by some other thread continue to select.
                  if (isDuplicateKeyError(dbType, e)) {
                    continue;
                  }
                  LOG.error("Unable to insert into SEQUENCE_TABLE for {}.", sequenceName, e);
                  throw e;
                }
              }
            }
          }
        }
      }

      // Step2. Increase SequenceValue.
      if (isUpdate) {
        int nextSequenceValue = maxSequenceValue + 1;
        try (PreparedStatement update =
                 connection.prepareStatement(UPDATE_SEQUENCE_TABLE_SQL)) {
          update.setInt(1, nextSequenceValue);
          update.setString(2, sequenceName);
          update.executeUpdate();
          maxSequenceValue = nextSequenceValue;
        }
      }

      connection.commit();
      committed = true;
      return maxSequenceValue;
    } catch (SQLException e) {
      throw new SQLException("Unable to selectOrUpdateSequenceTable due to: " + e.getMessage(), e);
    } finally {
      if (!committed) {
        rollbackDBConn(connection);
      }
    }
  }

  public void updateSequenceTable(Connection connection, String sequenceName, int sequenceValue)
      throws SQLException {
    boolean committed = false;
    try (PreparedStatement statement = connection.prepareStatement(UPDATE_SEQUENCE_TABLE_SQL)) {
      statement.setInt(1, sequenceValue);
      statement.setString(2, sequenceName);
      statement.executeUpdate();
      connection.commit();
      committed = true;
    } catch (SQLException e) {
      throw new SQLException("Unable to updateSequenceTable due to: " + e.getMessage());
    } finally {
      if (!committed) {
        rollbackDBConn(connection);
      }
    }
  }

  /**
   * Drop a queue from the policy.
   * @param connection DB connection
   * @param queue queue name
   * @throws SQLException failure
   */
  public void deletePolicyByQueue(Connection connection, String queue)
      throws SQLException {
    boolean committed = false;
    try (PreparedStatement statement = connection.prepareStatement(DELETE_QUEUE_SQL)) {
      statement.setString(1, queue);
      statement.executeUpdate();
      connection.commit();
      committed = true;
    } catch (SQLException e) {
      throw new SQLException("Unable to deletePolicyByQueue due to: " + e.getMessage());
    } finally {
      if (!committed) {
        rollbackDBConn(connection);
      }
    }
  }

  public void truncateTable(Connection connection, String tableName)
      throws SQLException {
    DbType dbType = DatabaseProduct.getDbType(connection);
    String deleteSQL = getTruncateStatement(dbType, tableName);
    boolean committed = false;
    Statement statement = null;
    try {
      statement = connection.createStatement();
      statement.execute(deleteSQL);
      connection.commit();
      committed = true;
    } catch (SQLException e) {
      throw new SQLException("Unable to truncateTable due to: " + e.getMessage());
    } finally {
      if (!committed) {
        rollbackDBConn(connection);
      }
      close(statement);
    }
  }

  private String getTruncateStatement(DbType dbType, String tableName) {
    if (isMYSQL(dbType)) {
      return "DELETE FROM `" + tableName + "`";
    } else {
      return "DELETE FROM " + tableName;
    }
  }

  private boolean isMYSQL(DbType dbType) {
    return dbType == DbType.MYSQL;
  }

  static void rollbackDBConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) {
        dbConn.rollback();
      }
    } catch (SQLException e) {
      LOG.warn("Failed to rollback db connection ", e);
    }
  }
}
