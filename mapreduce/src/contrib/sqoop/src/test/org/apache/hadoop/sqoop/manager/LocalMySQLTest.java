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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;
import org.apache.hadoop.sqoop.util.FileListing;

/**
 * Test the LocalMySQLManager implementation.
 * This differs from MySQLManager only in its importTable() method, which
 * uses mysqldump instead of mapreduce+DBInputFormat.
 *
 * Since this requires a MySQL installation on your local machine to use, this
 * class is named in such a way that Hadoop's default QA process does not run
 * it. You need to run this manually with -Dtestcase=LocalMySQLTest.
 *
 * You need to put MySQL's Connector/J JDBC driver library into a location
 * where Hadoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons).
 *
 * You should also create a database named 'sqooptestdb' and authorize yourself:
 *
 * CREATE DATABASE sqooptestdb;
 * use mysql;
 * GRANT ALL PRIVILEGES ON sqooptestdb.* TO 'yourusername'@'localhost';
 * flush privileges;
 *
 */
public class LocalMySQLTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(LocalMySQLTest.class.getName());

  static final String HOST_URL = "jdbc:mysql://localhost/";

  static final String MYSQL_DATABASE_NAME = "sqooptestdb";
  static final String TABLE_NAME = "EMPLOYEES_MYSQL";
  static final String CONNECT_STRING = HOST_URL + MYSQL_DATABASE_NAME;

  // instance variables populated during setUp, used during tests
  private LocalMySQLManager manager;

  @Before
  public void setUp() {
    ImportOptions options = new ImportOptions(CONNECT_STRING, TABLE_NAME);
    options.setUsername(getCurrentUser());
    manager = new LocalMySQLManager(options);

    Connection connection = null;
    Statement st = null;

    try {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create the database table and populate it with data. 
      st.executeUpdate("DROP TABLE IF EXISTS " + TABLE_NAME);
      st.executeUpdate("CREATE TABLE " + TABLE_NAME + " ("
          + "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, "
          + "name VARCHAR(24) NOT NULL, "
          + "start_date DATE, "
          + "salary FLOAT, "
          + "dept VARCHAR(32))");

      st.executeUpdate("INSERT INTO " + TABLE_NAME + " VALUES("
          + "NULL,'Aaron','2009-05-14',1000000.00,'engineering')");
      st.executeUpdate("INSERT INTO " + TABLE_NAME + " VALUES("
          + "NULL,'Bob','2009-04-20',400.00,'sales')");
      st.executeUpdate("INSERT INTO " + TABLE_NAME + " VALUES("
          + "NULL,'Fred','2009-01-23',15.00,'marketing')");
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

  /** @return the current username. */
  private String getCurrentUser() {
    // First, check the $USER environment variable.
    String envUser = System.getenv("USER");
    if (null != envUser) {
      return envUser;
    }

    // Try `whoami`
    String [] whoamiArgs = new String[1];
    whoamiArgs[0] = "whoami";
    Process p = null;
    BufferedReader r = null;
    try {
      p = Runtime.getRuntime().exec(whoamiArgs);
      InputStream is = p.getInputStream();
      r = new BufferedReader(new InputStreamReader(is));
      return r.readLine();
    } catch (IOException ioe) {
      LOG.error("IOException reading from `whoami`: " + ioe.toString());
      return null;
    } finally {
      // close our stream.
      if (null != r) {
        try {
          r.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing input stream from `whoami`: " + ioe.toString());
        }
      }

      // wait for whoami to exit.
      while (p != null) {
        try {
          int ret = p.waitFor();
          if (0 != ret) {
            LOG.error("whoami exited with error status " + ret);
            // suppress original return value from this method.
            return null; 
          }
        } catch (InterruptedException ie) {
          continue; // loop around.
        }
      }
    }
  }

  private String [] getArgv(boolean mysqlOutputDelims) {
    ArrayList<String> args = new ArrayList<String>();

    args.add("-D");
    args.add("fs.default.name=file:///");

    args.add("--table");
    args.add(TABLE_NAME);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(CONNECT_STRING);
    args.add("--direct");
    args.add("--username");
    args.add(getCurrentUser());
    args.add("--where");
    args.add("id > 1");
    args.add("--num-mappers");
    args.add("1");

    if (mysqlOutputDelims) {
      args.add("--mysql-delimiters");
    }

    return args.toArray(new String[0]);
  }

  private void doLocalBulkImport(boolean mysqlOutputDelims, String [] expectedResults)
      throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, TABLE_NAME);
    Path filePath = new Path(tablePath, "data-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String [] argv = getArgv(mysqlOutputDelims);
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file", f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      for (String expectedLine : expectedResults) {
        assertEquals(expectedLine, r.readLine());
      }
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

  @Test
  public void testLocalBulkImportWithDefaultDelims() throws IOException {
    // no quoting of strings allowed.
    String [] expectedResults = {
        "2,Bob,2009-04-20,400,sales",
        "3,Fred,2009-01-23,15,marketing"
    };

    doLocalBulkImport(false, expectedResults);
  }

  @Test
  public void testLocalBulkImportWithMysqlQuotes() throws IOException {
    // mysql quotes all string-based output.
    String [] expectedResults = {
        "2,'Bob','2009-04-20',400,'sales'",
        "3,'Fred','2009-01-23',15,'marketing'"
    };

    doLocalBulkImport(true, expectedResults);
  }
}
