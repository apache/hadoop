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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.sqoop.testutil.HsqldbTestServer;

/**
 * Test methods of the generic SqlManager implementation.
 *
 * 
 *
 */
public class TestSqlManager extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestSqlManager.class.getName());

  /** the name of a table that doesn't exist. */
  static final String MISSING_TABLE = "MISSING_TABLE";

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

  @Test
  public void testListColNames() {
    String [] colNames = manager.getColumnNames(HsqldbTestServer.getTableName());
    assertNotNull("manager returned no colname list", colNames);
    assertEquals("Table list should be length 2", 2, colNames.length);
    String [] knownFields = HsqldbTestServer.getFieldNames();
    for (int i = 0; i < colNames.length; i++) {
      assertEquals(knownFields[i], colNames[i]);
    }
  }

  @Test
  public void testListColTypes() {
    Map<String, Integer> types = manager.getColumnTypes(HsqldbTestServer.getTableName());

    assertNotNull("manager returned no types map", types);
    assertEquals("Map should be size=2", 2, types.size());
    assertEquals(types.get("INTFIELD1").intValue(), Types.INTEGER);
    assertEquals(types.get("INTFIELD2").intValue(), Types.INTEGER);
  }

  @Test
  public void testMissingTableColNames() {
    String [] colNames = manager.getColumnNames(MISSING_TABLE);
    assertNull("No column names should be returned for missing table", colNames);
  }

  @Test
  public void testMissingTableColTypes() {
    Map<String, Integer> colTypes = manager.getColumnTypes(MISSING_TABLE);
    assertNull("No column types should be returned for missing table", colTypes);
  }

  @Test
  public void testListTables() {
    String [] tables = manager.listTables();
    for (String table : tables) {
      System.err.println("Got table: " + table);
    }
    assertNotNull("manager returned no table list", tables);
    assertEquals("Table list should be length 1", 1, tables.length);
    assertEquals(HsqldbTestServer.getTableName(), tables[0]);
  }

  // constants related to testReadTable()
  final static int EXPECTED_NUM_ROWS = 4;
  final static int EXPECTED_COL1_SUM = 16;
  final static int EXPECTED_COL2_SUM = 20;

  @Test
  public void testReadTable() {
    ResultSet results = null;
    try {
      results = manager.readTable(HsqldbTestServer.getTableName(),
          HsqldbTestServer.getFieldNames());

      assertNotNull("ResultSet from readTable() is null!", results);

      ResultSetMetaData metaData = results.getMetaData();
      assertNotNull("ResultSetMetadata is null in readTable()", metaData);

      // ensure that we get the correct number of columns back
      assertEquals("Number of returned columns was unexpected!", metaData.getColumnCount(),
          HsqldbTestServer.getFieldNames().length);

      // should get back 4 rows. They are:
      // 1 2
      // 3 4
      // 5 6
      // 7 8
      // .. so while order isn't guaranteed, we should get back 16 on the left and 20 on the right.
      int sumCol1 = 0, sumCol2 = 0, rowCount = 0;
      while (results.next()) {
        rowCount++;
        sumCol1 += results.getInt(1);
        sumCol2 += results.getInt(2);
      }

      assertEquals("Expected 4 rows back", EXPECTED_NUM_ROWS, rowCount);
      assertEquals("Expected left sum of 16", EXPECTED_COL1_SUM, sumCol1);
      assertEquals("Expected right sum of 20", EXPECTED_COL2_SUM, sumCol2);
    } catch (SQLException sqlException) {
      fail("SQL Exception: " + sqlException.toString());
    } finally {
      if (null != results) {
        try {
          results.close();
        } catch (SQLException sqlE) {
          fail("SQL Exception in ResultSet.close(): " + sqlE.toString());
        }
      }
    }
  }

  @Test
  public void testReadMissingTable() {
    ResultSet results = null;
    try {
      String [] colNames = { "*" };
      results = manager.readTable(MISSING_TABLE, colNames);
      assertNull("Expected null resultset from readTable(MISSING_TABLE)", results);
    } catch (SQLException sqlException) {
      // we actually expect this. pass.
    } finally {
      if (null != results) {
        try {
          results.close();
        } catch (SQLException sqlE) {
          fail("SQL Exception in ResultSet.close(): " + sqlE.toString());
        }
      }
    }
  }

  @Test
  public void getPrimaryKeyFromMissingTable() {
    String primaryKey = manager.getPrimaryKey(MISSING_TABLE);
    assertNull("Expected null pkey for missing table", primaryKey);
  }

  @Test
  public void getPrimaryKeyFromTableWithoutKey() {
    String primaryKey = manager.getPrimaryKey(HsqldbTestServer.getTableName());
    assertNull("Expected null pkey for table without key", primaryKey);
  }

  // constants for getPrimaryKeyFromTable()
  static final String TABLE_WITH_KEY = "TABLE_WITH_KEY";
  static final String KEY_FIELD_NAME = "KEYFIELD";

  @Test
  public void getPrimaryKeyFromTable() {
    // first, create a table with a primary key
    Connection conn = null;
    try {
      conn = testServer.getConnection();
      PreparedStatement statement = conn.prepareStatement(
          "CREATE TABLE " + TABLE_WITH_KEY + "(" + KEY_FIELD_NAME
          + " INT NOT NULL PRIMARY KEY, foo INT)",
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.executeUpdate();
    } catch (SQLException sqlException) {
      fail("Could not create table with primary key: " + sqlException.toString());
    } finally {
      if (null != conn) {
        try {
          conn.close();
        } catch (SQLException sqlE) {
          LOG.warn("Got SQLException during close: " + sqlE.toString());
        }
      }
    }

    String primaryKey = manager.getPrimaryKey(TABLE_WITH_KEY);
    assertEquals("Expected null pkey for table without key", primaryKey, KEY_FIELD_NAME);
  }
}
