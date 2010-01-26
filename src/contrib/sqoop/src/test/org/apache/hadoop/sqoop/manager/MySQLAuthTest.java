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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.sqoop.SqoopOptions;
import org.apache.hadoop.sqoop.testutil.CommonArgs;
import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;

/**
 * Test authentication and remote access to direct mysqldump-based imports.
 *
 * Since this requires a MySQL installation on your local machine to use, this
 * class is named in such a way that Hadoop's default QA process does not run
 * it. You need to run this manually with -Dtestcase=MySQLAuthTest
 *
 * You need to put MySQL's Connector/J JDBC driver library into a location
 * where Hadoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons).
 *
 * You need to create a database used by Sqoop for password tests:
 *
 * CREATE DATABASE sqooppasstest;
 * use mysql;
 * GRANT ALL PRIVILEGES on sqooppasstest.* TO 'sqooptest'@'localhost' IDENTIFIED BY '12345';
 * flush privileges;
 *
 */
public class MySQLAuthTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(MySQLAuthTest.class.getName());

  static final String HOST_URL = "jdbc:mysql://localhost/";

  static final String AUTH_TEST_DATABASE = "sqooppasstest";
  static final String AUTH_TEST_USER = "sqooptest";
  static final String AUTH_TEST_PASS = "12345";
  static final String AUTH_TABLE_NAME = "authtest";
  static final String AUTH_CONNECT_STRING = HOST_URL + AUTH_TEST_DATABASE;

  // instance variables populated during setUp, used during tests
  private LocalMySQLManager manager;

  @Before
  public void setUp() {
    SqoopOptions options = new SqoopOptions(AUTH_CONNECT_STRING, AUTH_TABLE_NAME);
    options.setUsername(AUTH_TEST_USER);
    options.setPassword(AUTH_TEST_PASS);

    manager = new LocalMySQLManager(options);

    Connection connection = null;
    Statement st = null;

    try {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create the database table and populate it with data. 
      st.executeUpdate("DROP TABLE IF EXISTS " + AUTH_TABLE_NAME);
      st.executeUpdate("CREATE TABLE " + AUTH_TABLE_NAME + " ("
          + "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, "
          + "name VARCHAR(24) NOT NULL)");

      st.executeUpdate("INSERT INTO " + AUTH_TABLE_NAME + " VALUES("
          + "NULL,'Aaron')");
      connection.commit();
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    } finally {
      try {
        if (null != st) {
          st.close();
        }

        if (null != connection) {
          connection.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: " + sqlE);
      }
    }
  }

  @After
  public void tearDown() {
    try {
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  private String [] getArgv(boolean includeHadoopFlags,
      boolean useDirect, String connectString, String tableName) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(tableName);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(connectString);
    if (useDirect) {
      args.add("--direct");
    }
    args.add("--username");
    args.add(AUTH_TEST_USER);
    args.add("--password");
    args.add(AUTH_TEST_PASS);
    args.add("--mysql-delimiters");
    args.add("--num-mappers");
    args.add("1");

    return args.toArray(new String[0]);
  }

  /**
   * Connect to a db and ensure that password-based authentication
   * succeeds.
   */
  @Test
  public void testAuthAccess() {
    String [] argv = getArgv(true, true, AUTH_CONNECT_STRING, AUTH_TABLE_NAME);
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, AUTH_TABLE_NAME);
    Path filePath = new Path(tablePath, "data-00000");

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file", f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      assertEquals("1,'Aaron'", r.readLine());
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

  @Test
  public void testZeroTimestamp() throws IOException, SQLException {
    // MySQL timestamps can hold values whose range causes problems
    // for java.sql.Timestamp. The MySQLManager adds settings to the
    // connect string which configure the driver's handling of
    // zero-valued timestamps. Check that all of these modifications
    // to the connect string are successful.

    try {
      // A connect string with a null 'query' component.
      doZeroTimestampTest(0, true, AUTH_CONNECT_STRING);

      // A connect string with a zero-length query component.
      doZeroTimestampTest(1, true, AUTH_CONNECT_STRING + "?");

      // A connect string with another argument
      doZeroTimestampTest(2, true, AUTH_CONNECT_STRING + "?connectTimeout=0");
      doZeroTimestampTest(3, true, AUTH_CONNECT_STRING + "?connectTimeout=0&");

      // A connect string with the zero-timestamp behavior already
      // configured.
      doZeroTimestampTest(4, true, AUTH_CONNECT_STRING
          + "?zeroDateTimeBehavior=convertToNull");

      // And finally, behavior already configured in such a way as to
      // cause the timestamp import to fail.
      doZeroTimestampTest(5, false, AUTH_CONNECT_STRING
          + "?zeroDateTimeBehavior=exception");
    } finally {
      // Clean up our mess on the way out.
      dropTimestampTables();
    }
  }

  private void dropTimestampTables() throws SQLException {
    SqoopOptions options = new SqoopOptions(AUTH_CONNECT_STRING, null);
    options.setUsername(AUTH_TEST_USER);
    options.setPassword(AUTH_TEST_PASS);

    manager = new LocalMySQLManager(options);

    Connection connection = null;
    Statement st = null;

    connection = manager.getConnection();
    connection.setAutoCommit(false);
    st = connection.createStatement();

    st.executeUpdate("DROP TABLE IF EXISTS mysqlTimestampTable0");
    st.executeUpdate("DROP TABLE IF EXISTS mysqlTimestampTable1");
    st.executeUpdate("DROP TABLE IF EXISTS mysqlTimestampTable2");
    st.executeUpdate("DROP TABLE IF EXISTS mysqlTimestampTable3");
    st.executeUpdate("DROP TABLE IF EXISTS mysqlTimestampTable4");
    st.executeUpdate("DROP TABLE IF EXISTS mysqlTimestampTable5");
    connection.commit();
    st.close();
    connection.close();
  }

  public void doZeroTimestampTest(int testNum, boolean expectSuccess,
      String connectString) throws IOException, SQLException {

    final String tableName = "mysqlTimestampTable" + Integer.toString(testNum);

    // Create a table containing a full-zeros timestamp.
    SqoopOptions options = new SqoopOptions(connectString, tableName);
    options.setUsername(AUTH_TEST_USER);
    options.setPassword(AUTH_TEST_PASS);

    manager = new LocalMySQLManager(options);

    Connection connection = null;
    Statement st = null;

    connection = manager.getConnection();
    connection.setAutoCommit(false);
    st = connection.createStatement();

    // create the database table and populate it with data. 
    st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
    st.executeUpdate("CREATE TABLE " + tableName + " ("
        + "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, "
        + "ts TIMESTAMP NOT NULL)");

    st.executeUpdate("INSERT INTO " + tableName + " VALUES("
        + "NULL,'0000-00-00 00:00:00.0')");
    connection.commit();
    st.close();
    connection.close();

    // Run the import.
    String [] argv = getArgv(true, false, connectString, tableName);
    runImport(argv);

    // Make sure the result file is there.
    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, tableName);
    Path filePath = new Path(tablePath, "part-m-00000");

    File f = new File(filePath.toString());
    if (expectSuccess) {
      assertTrue("Could not find imported data file", f.exists());
      BufferedReader r = new BufferedReader(new InputStreamReader(
          new FileInputStream(f)));
      assertEquals("1,null", r.readLine());
      IOUtils.closeStream(r);
    } else {
      assertFalse("Imported data when expected failure", f.exists());
    }
  }
}
