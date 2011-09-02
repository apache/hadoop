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
package org.apache.hadoop.hbase.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.avro.generated.AColumn;
import org.apache.hadoop.hbase.avro.generated.AColumnValue;
import org.apache.hadoop.hbase.avro.generated.AFamilyDescriptor;
import org.apache.hadoop.hbase.avro.generated.AGet;
import org.apache.hadoop.hbase.avro.generated.APut;
import org.apache.hadoop.hbase.avro.generated.ATableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit testing for AvroServer.HBaseImpl, a part of the
 * org.apache.hadoop.hbase.avro package.
 */
public class TestAvroServer {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // Static names for tables, columns, rows, and values
  // TODO(hammer): Better style to define these in test method?
  private static ByteBuffer tableAname = ByteBuffer.wrap(Bytes.toBytes("tableA"));
  private static ByteBuffer tableBname = ByteBuffer.wrap(Bytes.toBytes("tableB"));
  private static ByteBuffer familyAname = ByteBuffer.wrap(Bytes.toBytes("FamilyA"));
  private static ByteBuffer qualifierAname = ByteBuffer.wrap(Bytes.toBytes("QualifierA"));
  private static ByteBuffer rowAname = ByteBuffer.wrap(Bytes.toBytes("RowA"));
  private static ByteBuffer valueA = ByteBuffer.wrap(Bytes.toBytes("ValueA"));

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  /**
   * Tests for creating, enabling, disabling, modifying, and deleting tables.
   *
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testTableAdminAndMetadata() throws Exception {
    AvroServer.HBaseImpl impl =
      new AvroServer.HBaseImpl(TEST_UTIL.getConfiguration());

    assertEquals(impl.listTables().size(), 0);

    ATableDescriptor tableA = new ATableDescriptor();
    tableA.name = tableAname;
    impl.createTable(tableA);
    assertEquals(impl.listTables().size(), 1);
    assertTrue(impl.isTableEnabled(tableAname));
    assertTrue(impl.tableExists(tableAname));

    ATableDescriptor tableB = new ATableDescriptor();
    tableB.name = tableBname;
    impl.createTable(tableB);
    assertEquals(impl.listTables().size(), 2);

    impl.disableTable(tableBname);
    assertFalse(impl.isTableEnabled(tableBname));

    impl.deleteTable(tableBname);
    assertEquals(impl.listTables().size(), 1);

    impl.disableTable(tableAname);
    assertFalse(impl.isTableEnabled(tableAname));

    tableA.maxFileSize = 123456L;
    impl.modifyTable(tableAname, tableA);
    // It can take a while for the change to take effect.  Wait here a while.
    while(impl.describeTable(tableAname) == null) {
      Threads.sleep(100);
    }

    assertTrue(impl.describeTable(tableAname).maxFileSize == 123456L);
    assertEquals(123456L, (long) impl.describeTable(tableAname).maxFileSize);
/* DISABLED FOR NOW TILL WE HAVE BETTER DISABLE/ENABLE
    impl.enableTable(tableAname);
    assertTrue(impl.isTableEnabled(tableAname));
    
    impl.disableTable(tableAname);
    */
    impl.deleteTable(tableAname);
  }

  /**
   * Tests for creating, modifying, and deleting column families.
   *
   * @throws Exception
   */
  @Test
  public void testFamilyAdminAndMetadata() throws Exception {
    AvroServer.HBaseImpl impl =
      new AvroServer.HBaseImpl(TEST_UTIL.getConfiguration());

    ATableDescriptor tableA = new ATableDescriptor();
    tableA.name = tableAname;
    AFamilyDescriptor familyA = new AFamilyDescriptor();
    familyA.name = familyAname;
    Schema familyArraySchema = Schema.createArray(AFamilyDescriptor.SCHEMA$);
    GenericArray<AFamilyDescriptor> families = new GenericData.Array<AFamilyDescriptor>(1, familyArraySchema);
    families.add(familyA);
    tableA.families = families;
    impl.createTable(tableA);
    assertEquals(impl.describeTable(tableAname).families.size(), 1);

    impl.disableTable(tableAname);
    assertFalse(impl.isTableEnabled(tableAname));

    familyA.maxVersions = 123456;
    impl.modifyFamily(tableAname, familyAname, familyA);
    assertEquals((int) impl.describeFamily(tableAname, familyAname).maxVersions, 123456);

    impl.deleteFamily(tableAname, familyAname);
    assertEquals(impl.describeTable(tableAname).families.size(), 0);

    impl.deleteTable(tableAname);
  }

  /**
   * Tests for adding, reading, and deleting data.
   *
   * @throws Exception
   */
  @Test
  public void testDML() throws Exception {
    AvroServer.HBaseImpl impl =
      new AvroServer.HBaseImpl(TEST_UTIL.getConfiguration());

    ATableDescriptor tableA = new ATableDescriptor();
    tableA.name = tableAname;
    AFamilyDescriptor familyA = new AFamilyDescriptor();
    familyA.name = familyAname;
    Schema familyArraySchema = Schema.createArray(AFamilyDescriptor.SCHEMA$);
    GenericArray<AFamilyDescriptor> families = new GenericData.Array<AFamilyDescriptor>(1, familyArraySchema);
    families.add(familyA);
    tableA.families = families;
    impl.createTable(tableA);
    assertEquals(impl.describeTable(tableAname).families.size(), 1);

    AGet getA = new AGet();
    getA.row = rowAname;
    Schema columnsSchema = Schema.createArray(AColumn.SCHEMA$);
    GenericArray<AColumn> columns = new GenericData.Array<AColumn>(1, columnsSchema);
    AColumn column = new AColumn();
    column.family = familyAname;
    column.qualifier = qualifierAname;
    columns.add(column);
    getA.columns = columns;
   
    assertFalse(impl.exists(tableAname, getA));

    APut putA = new APut();
    putA.row = rowAname;
    Schema columnValuesSchema = Schema.createArray(AColumnValue.SCHEMA$);
    GenericArray<AColumnValue> columnValues = new GenericData.Array<AColumnValue>(1, columnValuesSchema);
    AColumnValue acv = new AColumnValue();
    acv.family = familyAname;
    acv.qualifier = qualifierAname;
    acv.value = valueA;
    columnValues.add(acv);
    putA.columnValues = columnValues;

    impl.put(tableAname, putA);
    assertTrue(impl.exists(tableAname, getA));

    assertEquals(impl.get(tableAname, getA).entries.size(), 1);

    impl.disableTable(tableAname);
    impl.deleteTable(tableAname);
  }
}
