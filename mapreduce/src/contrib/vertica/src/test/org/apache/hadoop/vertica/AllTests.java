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

package org.apache.hadoop.vertica;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.vertica.VerticaConfiguration;

/**
 * All tests for Vertica Formatters (org.apache.hadoop.vertica)
 * 
 * 
 */
public final class AllTests {
  private static final Log LOG = LogFactory.getLog(AllTests.class);

  static final String VERTICA_HOSTNAME = "localhost";
  static final String VERTICA_USERNAME = "dbadmin";
  static final String VERTICA_PASSWORD = "";
  static final String VERTICA_DATABASE = "db";

  static String hostname;
  static String username;
  static String password;
  static String database;

  static boolean run_tests = false;
  
  public static String getHostname() {
    return hostname;
  }

  public static String getUsername() {
    return username;
  }

  public static String getPassword() {
    return password;
  }

  public static String getDatabase() {
    return database;
  }

  public static boolean isSetup() {
    return run_tests;
  } 

  private AllTests() {
  }

  public static void configure() {
    if (run_tests) {
      return;
    }

    Properties properties = System.getProperties();

    String test_setup = properties.getProperty("vertica.test_setup", "vertica_test.sql");
    hostname = properties.getProperty("vertica.hostname", VERTICA_HOSTNAME);
    username = properties.getProperty("vertica.username", VERTICA_USERNAME);
    password = properties.getProperty("vertica.password", VERTICA_PASSWORD);
    database = properties.getProperty("vertica.database", VERTICA_DATABASE);

    LOG.info("Inititializing database with " + test_setup);
    try {
      Class.forName(VerticaConfiguration.VERTICA_DRIVER_CLASS);
      String url = "jdbc:vertica://" + hostname + ":5433/" + database
          + "?user=" + username + "&password=" + password;
      LOG.info("Conencting to " + url);
      Connection conn = DriverManager.getConnection(url);
      Statement stmt = conn.createStatement();

      InputStream strm_cmds = new FileInputStream(test_setup);

      if (strm_cmds != null) {
        byte[] b = new byte[strm_cmds.available()];
        strm_cmds.read(b);
        String[] cmds = new String(b).split("\n");

        StringBuffer no_comment = new StringBuffer();
        for (String cmd : cmds) {
          if (!cmd.startsWith("--"))
            no_comment.append(cmd).append("\n");
        }

        for (String cmd : no_comment.toString().split(";")) {
          LOG.debug(cmd);
          try {
            stmt.execute(cmd);
          } catch (SQLException e) {
            LOG.debug(e.getSQLState() + " : " + e.getMessage());
            if (e.getSQLState().equals("42V01"))
              continue;
            else
              throw new RuntimeException(e);
          }

        }

        run_tests = true;
      }
    } catch (ClassNotFoundException e) {
      LOG.warn("No vertica driver found: " + e.getMessage() + " - skipping vertica tests");
    } catch (SQLException e) {
      LOG.warn("Could not connect to vertica database: " + e.getMessage() + " - skipping vertica tests");
    } catch (IOException e) {
      LOG.warn("Missing vertica test setup file " + test_setup + ": " + e.getMessage() + " - skipping vertica tests");
    }
  }

  public static Test suite() {
    configure();
    TestSuite suite = new TestSuite("Tests for org.apache.hadoop.vertica");

    if (run_tests) {
      suite.addTestSuite(TestVertica.class);
      suite.addTestSuite(TestExample.class);
    }
    return suite;
  }

}
