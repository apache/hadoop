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
      "SELECT nextVal FROM sequenceTable WHERE sequenceName = %s";

  public final static String INSERT_SEQUENCE_TABLE_SQL = "" +
      "INSERT INTO sequenceTable(sequenceName, nextVal) VALUES(%s, %d)";

  public final static String UPDATE_SEQUENCE_TABLE_SQL = "" +
      "UPDATE sequenceTable SET nextVal = %d WHERE sequenceName = %s";

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

    StringBuffer msg = new StringBuffer(causeMessage);
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
    Statement statement = null;

    try {

      // Step1. Query SequenceValue.
      while (maxSequenceValue == 0) {
        // Query SQL.
        String sql = String.format(QUERY_SEQUENCE_TABLE_SQL, quoteString(sequenceName));
        DbType dbType = DatabaseProduct.getDbType(connection);
        String forUpdateSQL = DatabaseProduct.addForUpdateClause(dbType, sql);
        statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(forUpdateSQL);
        if (rs.next()) {
          maxSequenceValue = rs.getInt("nextVal");
        } else if (insertDone) {
          throw new SQLException("Invalid state of SEQUENCE_TABLE for " + sequenceName);
        } else {
          insertDone = true;
          close(statement);
          statement = connection.createStatement();
          String insertSQL = String.format(INSERT_SEQUENCE_TABLE_SQL, quoteString(sequenceName), 1);
          try {
            statement.executeUpdate(insertSQL);
          } catch (SQLException e) {
            // If the record is already inserted by some other thread continue to select.
            if (isDuplicateKeyError(dbType, e)) {
              continue;
            }
            LOG.error("Unable to insert into SEQUENCE_TABLE for {}.", sequenceName, e);
            throw e;
          } finally {
            close(statement);
          }
        }
      }

      // Step2. Increase SequenceValue.
      if (isUpdate) {
        int nextSequenceValue = maxSequenceValue + 1;
        close(statement);
        statement = connection.createStatement();
        String updateSQL =
            String.format(UPDATE_SEQUENCE_TABLE_SQL, nextSequenceValue, quoteString(sequenceName));
        statement.executeUpdate(updateSQL);
        maxSequenceValue = nextSequenceValue;
      }

      connection.commit();
      committed = true;
      return maxSequenceValue;
    } catch(SQLException e){
      throw new SQLException("Unable to selectOrUpdateSequenceTable due to: " + e.getMessage(), e);
    } finally {
      if (!committed) {
        rollbackDBConn(connection);
      }
      close(statement);
    }
  }

  public void updateSequenceTable(Connection connection, String sequenceName, int sequenceValue)
      throws SQLException {
    String updateSQL =
        String.format(UPDATE_SEQUENCE_TABLE_SQL, sequenceValue, quoteString(sequenceName));
    boolean committed = false;
    Statement statement = null;
    try {
      statement = connection.createStatement();
      statement.executeUpdate(updateSQL);
      connection.commit();
      committed = true;
    } catch (SQLException e) {
      throw new SQLException("Unable to updateSequenceTable due to: " + e.getMessage());
    } finally {
      if (!committed) {
        rollbackDBConn(connection);
      }
      close(statement);
    }
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

  static String quoteString(String input) {
    return "'" + input + "'";
  }
}
