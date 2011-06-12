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

package org.apache.hadoop.sqoop.manager;

import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.mapreduce.DataDrivenImportJob;
import org.apache.hadoop.sqoop.util.ImportError;
import org.apache.hadoop.sqoop.util.ResultSetPrinter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * ConnManager implementation for generic SQL-compliant database.
 * This is an abstract class; it requires a database-specific
 * ConnManager implementation to actually create the connection.
 */
public abstract class SqlManager implements ConnManager {

  public static final Log LOG = LogFactory.getLog(SqlManager.class.getName());

  protected ImportOptions options;

  /**
   * Constructs the SqlManager
   * @param opts
   * @param specificMgr
   */
  public SqlManager(final ImportOptions opts) {
    this.options = opts;
  }

  /**
   * @return the SQL query to use in getColumnNames() in case this logic must
   * be tuned per-database, but the main extraction loop is still inheritable.
   */
  protected String getColNamesQuery(String tableName) {
    return "SELECT t.* FROM " + tableName + " AS t";
  }

  @Override
  public String[] getColumnNames(String tableName) {
    String stmt = getColNamesQuery(tableName);

    ResultSet results;
    try {
      results = execute(stmt);
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: " + sqlE.toString());
      return null;
    }

    try {
      int cols = results.getMetaData().getColumnCount();
      ArrayList<String> columns = new ArrayList<String>();
      ResultSetMetaData metadata = results.getMetaData();
      for (int i = 1; i < cols + 1; i++) {
        String colName = metadata.getColumnName(i);
        if (colName == null || colName.equals("")) {
          colName = metadata.getColumnLabel(i);
        }
        columns.add(colName);
      }
      return columns.toArray(new String[0]);
    } catch (SQLException sqlException) {
      LOG.error("Error reading from database: " + sqlException.toString());
      return null;
    } finally {
      try {
        results.close();
        getConnection().commit();
      } catch (SQLException sqlE) {
        LOG.warn("SQLException closing ResultSet: " + sqlE.toString());
      }
    }
  }

  /**
   * @return the SQL query to use in getColumnTypes() in case this logic must
   * be tuned per-database, but the main extraction loop is still inheritable.
   */
  protected String getColTypesQuery(String tableName) {
    return getColNamesQuery(tableName);
  }
  
  @Override
  public Map<String, Integer> getColumnTypes(String tableName) {
    String stmt = getColTypesQuery(tableName);

    ResultSet results;
    try {
      results = execute(stmt);
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: " + sqlE.toString());
      return null;
    }

    try {
      Map<String, Integer> colTypes = new HashMap<String, Integer>();

      int cols = results.getMetaData().getColumnCount();
      ResultSetMetaData metadata = results.getMetaData();
      for (int i = 1; i < cols + 1; i++) {
        int typeId = metadata.getColumnType(i);
        String colName = metadata.getColumnName(i);
        if (colName == null || colName.equals("")) {
          colName = metadata.getColumnLabel(i);
        }

        colTypes.put(colName, Integer.valueOf(typeId));
      }

      return colTypes;
    } catch (SQLException sqlException) {
      LOG.error("Error reading from database: " + sqlException.toString());
      return null;
    } finally {
      try {
        results.close();
        getConnection().commit();
      } catch (SQLException sqlE) { 
        LOG.warn("SQLException closing ResultSet: " + sqlE.toString());
      }
    }
  }

  @Override
  public ResultSet readTable(String tableName, String[] columns) throws SQLException {
    if (columns == null) {
      columns = getColumnNames(tableName);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    boolean first = true;
    for (String col : columns) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(col);
      first = false;
    }
    sb.append(" FROM ");
    sb.append(tableName);
    sb.append(" AS ");   // needed for hsqldb; doesn't hurt anyone else.
    sb.append(tableName);

    return execute(sb.toString());
  }

  @Override
  public String[] listDatabases() {
    // TODO(aaron): Implement this!
    LOG.error("Generic SqlManager.listDatabases() not implemented.");
    return null;
  }

  @Override
  public String[] listTables() {
    ResultSet results = null;
    String [] tableTypes = {"TABLE"};
    try {
      try {
        DatabaseMetaData metaData = this.getConnection().getMetaData();
        results = metaData.getTables(null, null, null, tableTypes);
      } catch (SQLException sqlException) {
        LOG.error("Error reading database metadata: " + sqlException.toString());
        return null;
      }

      if (null == results) {
        return null;
      }

      try {
        ArrayList<String> tables = new ArrayList<String>();
        while (results.next()) {
          String tableName = results.getString("TABLE_NAME");
          tables.add(tableName);
        }

        return tables.toArray(new String[0]);
      } catch (SQLException sqlException) {
        LOG.error("Error reading from database: " + sqlException.toString());
        return null;
      }
    } finally {
      if (null != results) {
        try {
          results.close();
          getConnection().commit();
        } catch (SQLException sqlE) {
          LOG.warn("Exception closing ResultSet: " + sqlE.toString());
        }
      }
    }
  }

  @Override
  public String getPrimaryKey(String tableName) {
    try {
      DatabaseMetaData metaData = this.getConnection().getMetaData();
      ResultSet results = metaData.getPrimaryKeys(null, null, tableName);
      if (null == results) {
        return null;
      }
      
      try {
        if (results.next()) {
          return results.getString("COLUMN_NAME");
        } else {
          return null;
        }
      } finally {
        results.close();
        getConnection().commit();
      }
    } catch (SQLException sqlException) {
      LOG.error("Error reading primary key metadata: " + sqlException.toString());
      return null;
    }
  }

  /**
   * Retrieve the actual connection from the outer ConnManager
   */
  public abstract Connection getConnection() throws SQLException;

  /**
   * Default implementation of importTable() is to launch a MapReduce job
   * via DataDrivenImportJob to read the table with DataDrivenDBInputFormat.
   */
  public void importTable(String tableName, String jarFile, Configuration conf)
      throws IOException, ImportError {
    DataDrivenImportJob importer = new DataDrivenImportJob(options);
    String splitCol = options.getSplitByCol();
    if (null == splitCol) {
      // If the user didn't specify a splitting column, try to infer one.
      splitCol = getPrimaryKey(tableName);
    }

    if (null == splitCol) {
      // Can't infer a primary key.
      throw new ImportError("No primary key could be found for table " + tableName
          + ". Please specify one with --split-by.");
    }

    importer.runImport(tableName, jarFile, splitCol, conf);
  }

  /**
   * executes an arbitrary SQL statement
   * @param stmt The SQL statement to execute
   * @return A ResultSet encapsulating the results or null on error
   */
  protected ResultSet execute(String stmt, Object... args) throws SQLException {
    PreparedStatement statement = null;
    statement = this.getConnection().prepareStatement(stmt,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    if (null != args) {
      for (int i = 0; i < args.length; i++) {
        statement.setObject(i + 1, args[i]);
      }
    }

    LOG.info("Executing SQL statement: " + stmt);
    return statement.executeQuery();
  }

  /**
   * Resolve a database-specific type to the Java type that should contain it.
   * @param sqlType
   * @return the name of a Java type to hold the sql datatype, or null if none.
   */
  public static String toJavaType(int sqlType) {
    // mappings from http://java.sun.com/j2se/1.3/docs/guide/jdbc/getstart/mapping.html
    if (sqlType == Types.INTEGER) {
      return "Integer";
    } else if (sqlType == Types.VARCHAR) {
      return "String";
    } else if (sqlType == Types.CHAR) {
      return "String";
    } else if (sqlType == Types.LONGVARCHAR) {
      return "String";
    } else if (sqlType == Types.NUMERIC) {
      return "java.math.BigDecimal";
    } else if (sqlType == Types.DECIMAL) {
      return "java.math.BigDecimal";
    } else if (sqlType == Types.BIT) {
      return "Boolean";
    } else if (sqlType == Types.BOOLEAN) {
      return "Boolean";
    } else if (sqlType == Types.TINYINT) {
      return "Integer";
    } else if (sqlType == Types.SMALLINT) {
      return "Integer";
    } else if (sqlType == Types.BIGINT) {
      return "Long";
    } else if (sqlType == Types.REAL) {
      return "Float";
    } else if (sqlType == Types.FLOAT) {
      return "Double";
    } else if (sqlType == Types.DOUBLE) {
      return "Double";
    } else if (sqlType == Types.DATE) {
      return "java.sql.Date";
    } else if (sqlType == Types.TIME) {
      return "java.sql.Time";
    } else if (sqlType == Types.TIMESTAMP) {
      return "java.sql.Timestamp";
    } else {
      // TODO(aaron): Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB, BLOB, ARRAY,
      // STRUCT, REF, JAVA_OBJECT.
      return null;
    }
  }


  public void close() throws SQLException {
  }

  /**
   * Poor man's SQL query interface; used for debugging.
   * @param s
   */
  public void execAndPrint(String s) {
    System.out.println("Executing statement: " + s);
    ResultSet results;
    try {
      results = execute(s);
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: " + sqlE.toString());
      return;
    }

    try {
      try {
        int cols = results.getMetaData().getColumnCount();
        System.out.println("Got " + cols + " columns back");
        if (cols > 0) {
          System.out.println("Schema: " + results.getMetaData().getSchemaName(1));
          System.out.println("Table: " + results.getMetaData().getTableName(1));
        }
      } catch (SQLException sqlE) {
        LOG.error("SQLException reading result metadata: " + sqlE.toString());
      }

      try {
        new ResultSetPrinter().printResultSet(System.out, results);
      } catch (IOException ioe) {
        LOG.error("IOException writing results to stdout: " + ioe.toString());
        return;
      }
    } finally {
      try {
        results.close();
        getConnection().commit();
      } catch (SQLException sqlE) {
        LOG.warn("SQLException closing ResultSet: " + sqlE.toString());
      }
    }
  }

  /**
   * Create a connection to the database; usually used only from within
   * getConnection(), which enforces a singleton guarantee around the
   * Connection object.
   */
  protected Connection makeConnection() throws SQLException {

    Connection connection;
    String driverClass = getDriverClass();

    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Could not load db driver class: " + driverClass);
    }

    String username = options.getUsername();
    String password = options.getPassword();
    if (null == username) {
      connection = DriverManager.getConnection(options.getConnectString());
    } else {
      connection = DriverManager.getConnection(options.getConnectString(), username, password);
    }

    // We only use this for metadata queries. Loosest semantics are okay.
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    connection.setAutoCommit(false);

    return connection;
  }
}
