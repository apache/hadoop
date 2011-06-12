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
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;

import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.Sqoop;
import org.apache.hadoop.sqoop.ImportOptions.InvalidOptionsException;
import org.apache.hadoop.sqoop.manager.ConnManager;
import org.apache.hadoop.sqoop.orm.CompilationManager;
import org.apache.hadoop.sqoop.util.ClassLoaderStack;

import junit.framework.TestCase;

/**
 * Class that implements common methods required for tests which import data
 * from SQL into HDFS and verify correct import.
 */
public class ImportJobTestCase extends TestCase {

  public static final Log LOG = LogFactory.getLog(ImportJobTestCase.class.getName());

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

  /** the name of a table that we'll populate with items for each test. */
  static final String TABLE_NAME = "IMPORT_TABLE_";

  protected String getTableName() {
    if (null != curTableName) {
      return curTableName;
    } else {
      return TABLE_NAME + Integer.toString(tableNum);
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

  /**
   * verify that the single-column single-row result can be read back from the db.
   *
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

  /**
   * Create the argv to pass to Sqoop
   * @param includeHadoopFlags if true, then include -D various.settings=values
   * @param colNames the columns to import. If null, all columns are used.
   * @return the argv as an array of strings.
   */
  private String [] getArgv(boolean includeHadoopFlags, String [] colNames) {
    if (null == colNames) {
      colNames = getColNames();
    }

    String splitByCol = colNames[0];
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      args.add("-D");
      args.add("mapreduce.jobtracker.address=local");
      args.add("-D");
      args.add("mapreduce.job.maps=1");
      args.add("-D");
      args.add("fs.default.name=file:///");
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--columns");
    args.add(columnsString);
    args.add("--split-by");
    args.add(splitByCol);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--as-sequencefile");
    args.add("--num-mappers");
    args.add("1");

    return args.toArray(new String[0]);
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
   * Do a MapReduce-based import of the table and verify that the results
   * were imported as expected. (tests readFields(ResultSet) and toString())
   * @param expectedVal the value we injected into the table.
   * @param importCols the columns to import. If null, all columns are used.
   */
  protected void verifyImport(String expectedVal, String [] importCols) {

    // paths to where our output file will wind up.
    Path dataFilePath = getDataFilePath();

    removeTableDir();

    // run the tool through the normal entry-point.
    int ret;
    try {
      Sqoop importer = new Sqoop();
      ret = ToolRunner.run(importer, getArgv(true, importCols));
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop: " + e.toString());
      throw new RuntimeException(e);
    }

    // expect a successful return.
    assertEquals("Failure during job", 0, ret);

    ImportOptions opts = new ImportOptions();
    try {
      opts.parse(getArgv(false, importCols));
    } catch (InvalidOptionsException ioe) {
      fail(ioe.toString());
    }
    CompilationManager compileMgr = new CompilationManager(opts);
    String jarFileName = compileMgr.getJarFilename();
    ClassLoader prevClassLoader = null;
    try {
      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName, getTableName());

      // now actually open the file and check it
      File f = new File(dataFilePath.toString());
      assertTrue("Error: " + dataFilePath.toString() + " does not exist", f.exists());

      Object readValue = SeqFileReader.getFirstValue(dataFilePath.toString());
      // add trailing '\n' to expected value since SqoopRecord.toString() encodes the record delim
      if (null == expectedVal) {
        assertEquals("Error validating result from SeqFile", "null\n", readValue.toString());
      } else {
        assertEquals("Error validating result from SeqFile", expectedVal + "\n",
            readValue.toString());
      }
    } catch (IOException ioe) {
      fail("IOException: " + ioe.toString());
    } finally {
      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  /**
   * Run a MapReduce-based import (using the argv provided to control execution).
   */
  protected void runImport(String [] argv) throws IOException {
    removeTableDir();

    // run the tool through the normal entry-point.
    int ret;
    try {
      Sqoop importer = new Sqoop();
      ret = ToolRunner.run(importer, argv);
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop: " + e.toString());
      e.printStackTrace();
      ret = 1;
    }

    // expect a successful return.
    if (0 != ret) {
      throw new IOException("Failure during job; return status " + ret);
    }
  }

}
