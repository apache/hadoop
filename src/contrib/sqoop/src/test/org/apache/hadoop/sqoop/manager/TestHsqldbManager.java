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

import java.sql.SQLException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.sqoop.testutil.HsqldbTestServer;

/**
 * Test HsqldbManager-specific functionality that overrides SqlManager behavior
 *
 * 
 */
public class TestHsqldbManager extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestHsqldbManager.class.getName());

  // instance variables populated during setUp, used during tests
  private HsqldbTestServer testServer;
  private ConnManager manager;

  @Before
  public void setUp() {
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
    try {
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  // note: hsql returns only the "PUBLIC" schema name; not individual user db names.
  @Test
  public void testListDatabases() {
    String [] databases = manager.listDatabases();

    assertNotNull("manager returned no database list", databases);
    assertEquals("Database list should be length 1", 1, databases.length);
    assertEquals(HsqldbTestServer.getSchemaName(), databases[0]);
  }

}

