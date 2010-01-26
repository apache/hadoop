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

package org.apache.hadoop.sqoop.testutil;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;

import org.apache.hadoop.sqoop.manager.ConnManager;

import junit.framework.TestCase;

/**
 * Class that implements common methods required for tests
 */
public class BaseSqoopTestCase extends TestCase {

  public static final Log LOG = LogFactory.getLog(BaseSqoopTestCase.class.getName());

  /** Base directory for all temporary data */
  public static final String TEMP_BASE_DIR;

  /** Where to import table data to in the local filesystem for testing */
  public static final String LOCAL_WAREHOUSE_DIR;

  // Initializer for the above
  static {
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    if (!tmpDir.endsWith(File.separator)) {
      tmpDir = tmpDir + File.separator;
    }

    TEMP_BASE_DIR = tmpDir;
    LOCAL_WAREHOUSE_DIR = TEMP_BASE_DIR + "sqoop/warehouse";
  }

  // Used if a test manually sets the table name to be used.
  private String curTableName;

  protected void setCurTableName(String curName) {
    this.curTableName = curName;
  }

  /**
   * Because of how classloading works, we don't actually want to name
   * all the tables the same thing -- they'll actually just use the same
   * implementation of the Java class that was classloaded before. So we
   * use this counter to uniquify table names.
   */
  private static int tableNum = 0;

  /** When creating sequentially-identified tables, what prefix should
   *  be applied to these tables?
   */
  protected String getTablePrefix() {
    return "SQOOP_TABLE_";
  }

  protected String getTableName() {
    if (null != curTableName) {
      return curTableName;
    } else {
      return getTablePrefix() + Integer.toString(tableNum);
    }
  }

  protected String getWarehouseDir() {
    return LOCAL_WAREHOUSE_DIR;
  }

  private String [] colNames;
  protected String [] getColNames() {
    return colNames;
  }

  protected HsqldbTestServer getTestServer() {
    return testServer;
  }

  protected ConnManager getManager() {
    return manager;
  }

  // instance variables populated during setUp, used during tests
  private HsqldbTestServer testServer;
  private ConnManager manager;

  private static boolean isLog4jConfigured = false;

  protected void incrementTableNum() {
    tableNum++;
  }

  @Before
  public void setUp() {

    incrementTableNum();

    if (!isLog4jConfigured) {
      BasicConfigurator.configure();
      isLog4jConfigured = true;
      LOG.info("Configured log4j with console appender.");
    }

    testServer = new HsqldbTestServer();
    try {
      testServer.resetServer();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    } catch (ClassNotFoundException cnfe) {
      LOG.error("Could not find class for db driver: " + cnfe.toString());
      fail("Could not find class for db driver: " + cnfe.toString());
    }

    manager = testServer.getManager();
  }

  @After
  public void tearDown() {
    setCurTableName(null); // clear user-override table name.

    try {
      if (null != manager) {
        manager.close();
      }
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }

  }

  static final String BASE_COL_NAME = "DATA_COL";

  /**
   * Create a table with a set of columns and add a row of values.
   * @param colTypes the types of the columns to make
   * @param vals the SQL text for each value to insert
   */
  protected void createTableWithColTypes(String [] colTypes, String [] vals) {
    Connection conn = null;
    try {
      conn = getTestServer().getConnection();
      PreparedStatement statement = conn.prepareStatement(
          "DROP TABLE " + getTableName() + " IF EXISTS",
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.executeUpdate();
      statement.close();

      String columnDefStr = "";
      String columnListStr = "";
      String valueListStr = "";

      String [] myColNames = new String[colTypes.length];

      for (int i = 0; i < colTypes.length; i++) {
        String colName = BASE_COL_NAME + Integer.toString(i);
        columnDefStr += colName + " " + colTypes[i];
        columnListStr += colName;
        valueListStr += vals[i];
        myColNames[i] = colName;
        if (i < colTypes.length - 1) {
          columnDefStr += ", ";
          columnListStr += ", ";
          valueListStr += ", ";
        }
      }

      statement = conn.prepareStatement(
          "CREATE TABLE " + getTableName() + "(" + columnDefStr + ")",
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.executeUpdate();
      statement.close();

      statement = conn.prepareStatement(
          "INSERT INTO " + getTableName() + "(" + columnListStr + ")"
          + " VALUES(" + valueListStr + ")",
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.executeUpdate();
      statement.close();
      conn.commit();
      this.colNames = myColNames;
    } catch (SQLException sqlException) {
      fail("Could not create table: " + sqlException.toString());
    } finally {
      if (null != conn) {
        try {
          conn.close();
        } catch (SQLException sqlE) {
          LOG.warn("Got SQLException during close: " + sqlE.toString());
        }
      }
    }
  }

  /**
   * Create a table with a single column and put a data element in it.
   * @param colType the type of the column to create
   * @param val the value to insert (reformatted as a string)
   */
  protected void createTableForColType(String colType, String val) {
    String [] types = { colType };
    String [] vals = { val };

    createTableWithColTypes(types, vals);
  }

  protected Path getTablePath() {
    Path warehousePath = new Path(getWarehouseDir());
    Path tablePath = new Path(warehousePath, getTableName());
    return tablePath;
  }

  protected Path getDataFilePath() {
    return new Path(getTablePath(), "part-m-00000");
  }

  protected void removeTableDir() {
    File tableDirFile = new File(getTablePath().toString());
    if (tableDirFile.exists()) {
      // Remove the director where the table will be imported to,
      // prior to running the MapReduce job.
      if (!DirUtil.deleteDir(tableDirFile)) {
        LOG.warn("Could not delete table directory: " + tableDirFile.getAbsolutePath());
      }
    }
  }

  /**
   * verify that the single-column single-row result can be read back from the db.
   */
  protected void verifyReadback(int colNum, String expectedVal) {
    ResultSet results = null;
    try {
      results = getManager().readTable(getTableName(), getColNames());
      assertNotNull("Null results from readTable()!", results);
      assertTrue("Expected at least one row returned", results.next());
      String resultVal = results.getString(colNum);
      if (null != expectedVal) {
        assertNotNull("Expected non-null result value", resultVal);
      }

      assertEquals("Error reading inserted value back from db", expectedVal, resultVal);
      assertFalse("Expected at most one row returned", results.next());
    } catch (SQLException sqlE) {
      fail("Got SQLException: " + sqlE.toString());
    } finally {
      if (null != results) {
        try {
          results.close();
        } catch (SQLException sqlE) {
          fail("Got SQLException in resultset.close(): " + sqlE.toString());
        }
      }
    }
  }
}
