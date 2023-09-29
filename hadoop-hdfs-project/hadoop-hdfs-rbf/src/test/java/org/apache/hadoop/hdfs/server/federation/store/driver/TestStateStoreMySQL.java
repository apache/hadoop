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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreMySQLImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.*;

/**
 * Test the MySQL implementation of the State Store driver.
 */
public class TestStateStoreMySQL extends TestStateStoreDriverBase {
  private static final String CONNECTION_URL = "jdbc:derby:memory:StateStore";

  @BeforeClass
  public static void initDatabase() throws Exception {
    Connection connection =  DriverManager.getConnection(CONNECTION_URL + ";create=true");
    Statement s =  connection.createStatement();
    s.execute("CREATE SCHEMA TESTUSER");

    Configuration conf =
        getStateStoreConfiguration(StateStoreMySQLImpl.class);
    conf.set(StateStoreMySQLImpl.CONNECTION_URL, CONNECTION_URL);
    conf.set(StateStoreMySQLImpl.CONNECTION_USERNAME, "testuser");
    conf.set(StateStoreMySQLImpl.CONNECTION_PASSWORD, "testpassword");
    conf.set(StateStoreMySQLImpl.CONNECTION_DRIVER, "org.apache.derby.jdbc.EmbeddedDriver");
    getStateStore(conf);
  }

  @Before
  public void startup() throws IOException {
    removeAll(getStateStoreDriver());
  }

  @AfterClass
  public static void cleanupDatabase() {
    try {
      DriverManager.getConnection(CONNECTION_URL + ";drop=true");
    } catch (SQLException e) {
      // SQLException expected when database is dropped
      if (!e.getMessage().contains("dropped")) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testInsert()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(getStateStoreDriver());
  }

  @Test
  public void testUpdate()
      throws IllegalArgumentException, ReflectiveOperationException,
      IOException, SecurityException {
    testPut(getStateStoreDriver());
  }

  @Test
  public void testDelete()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testRemove(getStateStoreDriver());
  }

  @Test
  public void testFetchErrors()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testFetchErrors(getStateStoreDriver());
  }

  @Test
  public void testMetrics()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testMetrics(getStateStoreDriver());
  }
}