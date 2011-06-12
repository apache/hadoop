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
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;
import org.apache.hadoop.sqoop.util.FileListing;

/**
 * Test the PostgresqlManager and DirectPostgresqlManager implementations.
 * The former uses the postgres JDBC driver to perform an import;
 * the latter uses pg_dump to facilitate it.
 *
 * Since this requires a Postgresql installation on your local machine to use, this
 * class is named in such a way that Hadoop's default QA process does not run
 * it. You need to run this manually with -Dtestcase=PostgresqlTest.
 *
 * You need to put Postgresql's JDBC driver library into a location where Hadoop
 * can access it (e.g., $HADOOP_HOME/lib).
 *
 * To configure a postgresql database to allow local connections, put the following
 * in /etc/postgresql/8.3/main/pg_hba.conf:
 *     local  all all trust
 *     host all all 127.0.0.1/32 trust
 * 
 * ... and comment out any other lines referencing 127.0.0.1.
 *
 * For postgresql 8.1, this may be in /var/lib/pgsql/data, instead.
 * You may need to restart the postgresql service after modifying this file.
 *
 * You should also create a sqooptest user and database:
 *
 * $ sudo -u postgres psql -U postgres template1
 * template1=> CREATE USER sqooptest;
 * template1=> CREATE DATABASE sqooptest;
 * template1=> \q
 *
 */
public class PostgresqlTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(PostgresqlTest.class.getName());

  static final String HOST_URL = "jdbc:postgresql://localhost/";

  static final String DATABASE_USER = "sqooptest";
  static final String DATABASE_NAME = "sqooptest";
  static final String TABLE_NAME = "EMPLOYEES_PG";
  static final String CONNECT_STRING = HOST_URL + DATABASE_NAME;

  @Before
  public void setUp() {
    LOG.debug("Setting up another postgresql test...");

    ImportOptions options = new ImportOptions(CONNECT_STRING, TABLE_NAME);
    options.setUsername(DATABASE_USER);

    ConnManager manager = null;
    Connection connection = null;
    Statement st = null;

    try {
      manager = new PostgresqlManager(options);
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create the database table and populate it with data. 

      try {
        // Try to remove the table first. DROP TABLE IF EXISTS didn't
        // get added until pg 8.3, so we just use "DROP TABLE" and ignore
        // any exception here if one occurs.
        st.executeUpdate("DROP TABLE " + TABLE_NAME);
      } catch (SQLException e) {
        LOG.info("Couldn't drop table " + TABLE_NAME + " (ok)");
        LOG.info(e.toString());
        // Now we need to reset the transaction.
        connection.rollback();
      }

      st.executeUpdate("CREATE TABLE " + TABLE_NAME + " ("
          + "id INT NOT NULL PRIMARY KEY, "
          + "name VARCHAR(24) NOT NULL, "
          + "start_date DATE, "
          + "salary FLOAT, "
          + "dept VARCHAR(32))");

      st.executeUpdate("INSERT INTO " + TABLE_NAME + " VALUES("
          + "1,'Aaron','2009-05-14',1000000.00,'engineering')");
      st.executeUpdate("INSERT INTO " + TABLE_NAME + " VALUES("
          + "2,'Bob','2009-04-20',400.00,'sales')");
      st.executeUpdate("INSERT INTO " + TABLE_NAME + " VALUES("
          + "3,'Fred','2009-01-23',15.00,'marketing')");
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

        if (null != manager) {
          manager.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: " + sqlE);
      }
    }

    LOG.debug("setUp complete.");
  }


  private String [] getArgv(boolean isDirect) {
    ArrayList<String> args = new ArrayList<String>();

    args.add("-D");
    args.add("fs.default.name=file:///");

    args.add("--table");
    args.add(TABLE_NAME);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(CONNECT_STRING);
    args.add("--username");
    args.add(DATABASE_USER);
    args.add("--where");
    args.add("id > 1");

    if (isDirect) {
      args.add("--direct");
    }

    return args.toArray(new String[0]);
  }

  private void doImportAndVerify(boolean isDirect, String [] expectedResults)
      throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, TABLE_NAME);

    Path filePath;
    if (isDirect) {
      filePath = new Path(tablePath, "data-00000");
    } else {
      filePath = new Path(tablePath, "part-m-00000");
    }

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String [] argv = getArgv(isDirect);
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file, " + f, f.exists());
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
  public void testJdbcBasedImport() throws IOException {
    String [] expectedResults = {
        "2,Bob,2009-04-20,400.0,sales",
        "3,Fred,2009-01-23,15.0,marketing"
    };

    doImportAndVerify(false, expectedResults);
  }

  @Test
  public void testDirectImport() throws IOException {
    String [] expectedResults = {
        "2,Bob,2009-04-20,400,sales",
        "3,Fred,2009-01-23,15,marketing"
    };

    doImportAndVerify(true, expectedResults);
  }
}
