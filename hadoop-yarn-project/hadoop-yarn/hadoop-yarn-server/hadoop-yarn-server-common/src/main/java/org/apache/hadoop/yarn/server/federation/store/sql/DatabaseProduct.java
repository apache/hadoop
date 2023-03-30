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

import org.apache.hadoop.classification.InterfaceAudience.Private;

import java.sql.Connection;
import java.sql.SQLException;

@Private
public final class DatabaseProduct {

  public enum DbType {MYSQL, SQLSERVER, POSTGRES, UNDEFINED, HSQLDB}

  private static final String SQL_SERVER_NAME = "sqlserver";
  private static final String MYSQL_NAME = "mysql";
  private static final String MARIADB_NAME = "mariadb";
  private static final String HSQLDB_NAME = "hsqldatabase";

  private DatabaseProduct() {
  }

  public static DbType getDbType(Connection conn) throws SQLException {
    if (conn == null) {
      return DbType.UNDEFINED;
    }
    String productName = getProductName(conn);
    return getDbType(productName);
  }

  /**
   * We get DBType based on ProductName.
   *
   * @param productName productName.
   * @return DbType.
   */
  private static DbType getDbType(String productName) {
    DbType dbt;
    productName = productName.replaceAll("\\s+", "").toLowerCase();
    if (productName.contains(SQL_SERVER_NAME)) {
      dbt = DbType.SQLSERVER;
    } else if (productName.contains(MYSQL_NAME) || productName.contains(MARIADB_NAME)) {
      dbt = DbType.MYSQL;
    } else if (productName.contains(HSQLDB_NAME)) {
      dbt = DbType.HSQLDB;
    } else {
      dbt = DbType.UNDEFINED;
    }
    return dbt;
  }

  /**
   * We get ProductName based on metadata in SQL Connection.
   *
   * @param conn SQL Connection
   * @return DB ProductName (Like MySQL SQLSERVER etc.)
   */
  private static String getProductName(Connection conn) throws SQLException {
    return conn.getMetaData().getDatabaseProductName();
  }

  /**
   * We add for update to SQL according to different database types.
   * This statement can ensure that a row of records in the database is only updated by one thread.
   *
   * @param dbType type of database.
   * @param selectStatement querySQL.
   * @return SQL after adding for update.
   * @throws SQLException SQL exception.
   */
  public static String addForUpdateClause(DbType dbType, String selectStatement)
      throws SQLException {
    switch (dbType) {
    case MYSQL:
    case HSQLDB:
      return selectStatement + " for update";
    case SQLSERVER:
      String modifier = " with (updlock)";
      int wherePos = selectStatement.toUpperCase().indexOf(" WHERE ");
      if (wherePos < 0) {
        return selectStatement + modifier;
      }
      return selectStatement.substring(0, wherePos) + modifier +
          selectStatement.substring(wherePos, selectStatement.length());
    default:
      String msg = "Unrecognized database product name <" + dbType + ">";
      throw new SQLException(msg);
    }
  }

  public static boolean isDuplicateKeyError(DbType dbType, SQLException ex) {
    switch (dbType) {
    case MYSQL:
      if((ex.getErrorCode() == 1022 || ex.getErrorCode() == 1062 || ex.getErrorCode() == 1586) &&
          "23000".equals(ex.getSQLState())) {
        return true;
      }
      break;
    case SQLSERVER:
      if ((ex.getErrorCode() == 2627 || ex.getErrorCode() == 2601)
          && "23000".equals(ex.getSQLState())) {
        return true;
      }
      break;
    default:
      return false;
    }
    return false;
  }
}
