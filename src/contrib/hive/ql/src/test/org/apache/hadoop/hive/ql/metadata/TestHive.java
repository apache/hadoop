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

package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DB;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde2.thrift.test.Complex;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.ThriftDeserializer;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;

import com.facebook.thrift.protocol.TBinaryProtocol;

public class TestHive extends TestCase {
  private Hive hm;
  private HiveConf hiveConf;

  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf(this.getClass());
    try {
      this.hm = Hive.get(hiveConf);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("Unable to initialize Hive Metastore using configruation: \n " + hiveConf);
      throw e;
    }
  }

  protected void tearDown() throws Exception {
    try {
      super.tearDown();
      Hive.closeCurrent();
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("Unable to close Hive Metastore using configruation: \n " + hiveConf);
      throw e;
    }
  }

  public void testTable() throws Throwable {
    try {
      // create a simple table and test create, drop, get
      String tableName = "table_for_testtable";
      try {
        this.hm.dropTable(tableName);
      } catch (HiveException e1) {
        e1.printStackTrace();
        assertTrue("Unable to drop table", false);
      }
      Table tbl = new Table(tableName);
      List<FieldSchema>  fields = tbl.getCols();

      fields.add(new FieldSchema("col1", Constants.INT_TYPE_NAME, "int -- first column"));
      fields.add(new FieldSchema("col2", Constants.STRING_TYPE_NAME, "string -- second column"));
      fields.add(new FieldSchema("col3", Constants.DOUBLE_TYPE_NAME, "double -- thrift column"));
      tbl.setFields(fields);

      tbl.setOutputFormatClass(IgnoreKeyTextOutputFormat.class);
      tbl.setInputFormatClass(SequenceFileInputFormat.class);

      tbl.setProperty("comment", "this is a test table created as part junit tests");

      List<String> bucketCols = tbl.getBucketCols();
      bucketCols.add("col1");
      try {
        tbl.setBucketCols(bucketCols);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to set bucket column for table: " + tableName, false);
      }

      List<FieldSchema>  partCols = new ArrayList<FieldSchema>();
      partCols.add(new FieldSchema("ds", Constants.STRING_TYPE_NAME, 
          "partition column, date but in string format as date type is not yet supported in QL"));
      tbl.setPartCols(partCols);

      tbl.setNumBuckets((short) 512);
      tbl.setOwner("pchakka");
      tbl.setRetention(10);

      // set output format parameters (these are not supported by QL but only for demo purposes)
      tbl.setSerdeParam(Constants.FIELD_DELIM, "1");
      tbl.setSerdeParam(Constants.LINE_DELIM, "\n");
      tbl.setSerdeParam(Constants.MAPKEY_DELIM, "3");
      tbl.setSerdeParam(Constants.COLLECTION_DELIM, "2");

      tbl.setSerdeParam(Constants.FIELD_DELIM, "1");
      tbl.setSerializationLib(MetadataTypedColumnsetSerDe.class.getName());

      // create table
      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to create table: " + tableName, false);
      }

      // get table
      Table ft = null;
      try {
        ft = hm.getTable(tableName);
        ft.checkValidity();
        assertEquals("Table names didn't match for table: " + tableName, tbl.getName(), ft.getName());
        assertEquals("Table owners didn't match for table: " + tableName, tbl.getOwner(), ft.getOwner());
        assertEquals("Table retention didn't match for table: " + tableName, tbl.getRetention(), ft.getRetention());
        assertEquals("Data location is not set correctly", DB.getDefaultTablePath(tableName, this.hiveConf).toString(), ft.getDataLocation().toString());
        // now that URI is set correctly, set the original table's uri and then compare the two tables
        tbl.setDataLocation(ft.getDataLocation());
        assertTrue("Tables  doesn't match: " + tableName, ft.getTTable().equals(tbl.getTTable()));
        assertEquals("Serde is not set correctly", tbl.getDeserializer().getClass().getName(), ft.getDeserializer().getClass().getName());
        assertEquals("SerializationLib is not set correctly", tbl.getSerializationLib(), MetadataTypedColumnsetSerDe.class.getName());
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to fetch table correctly: " + tableName, false);
      }

      try {
        hm.dropTable(tableName, true, false);
        Table ft2 = hm.getTable(tableName, false);
        assertNull("Unable to drop table ", ft2);
      } catch (HiveException e) {
        assertTrue("Unable to drop table: " + tableName, false);
      }
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTable failed");
      throw e;
    }
  }

  /**
   * Tests create and fetch of a thrift based table
   * @throws Throwable 
   */
  public void testThriftTable() throws Throwable {
    String tableName = "table_for_test_thrifttable";
    try {
      try {
        this.hm.dropTable(tableName);
      } catch (HiveException e1) {
        System.err.println(StringUtils.stringifyException(e1));
        assertTrue("Unable to drop table", false);
      }
      Table tbl = new Table(tableName);
      tbl.setInputFormatClass(SequenceFileInputFormat.class.getName());
      tbl.setOutputFormatClass(SequenceFileOutputFormat.class.getName());
      tbl.setSerializationLib(ThriftDeserializer.class.getName());
      tbl.setSerdeParam(Constants.SERIALIZATION_CLASS, Complex.class.getName());
      tbl.setSerdeParam(Constants.SERIALIZATION_FORMAT, TBinaryProtocol.class.getName());
      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create table: " + tableName, false);
      }
      // get table
      Table ft = null;
      try {
        ft = hm.getTable(tableName);
        assertNotNull("Unable to fetch table", ft);
        ft.checkValidity();
        assertEquals("Table names didn't match for table: " + tableName, tbl.getName(), ft.getName());
        assertEquals("Table owners didn't match for table: " + tableName, tbl.getOwner(), ft.getOwner());
        assertEquals("Table retention didn't match for table: " + tableName, tbl.getRetention(), ft.getRetention());
        assertEquals("Data location is not set correctly", DB.getDefaultTablePath(tableName, this.hiveConf).toString(), ft.getDataLocation().toString());
        // now that URI is set correctly, set the original table's uri and then compare the two tables
        tbl.setDataLocation(ft.getDataLocation());
        assertTrue("Tables  doesn't match: " + tableName, ft.getTTable().equals(tbl.getTTable()));
        assertEquals("SerializationLib is not set correctly", tbl.getSerializationLib(), ThriftDeserializer.class.getName());
        assertEquals("Serde is not set correctly", tbl.getDeserializer().getClass().getName(), ft.getDeserializer().getClass().getName());
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to fetch table correctly: " + tableName, false);
      }
      hm.dropTable(tableName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testThriftTable() failed");
      throw e;
    }
  }

  private static Table createTestTable(String dbName, String tableName) throws HiveException {
    Table tbl = new Table(tableName);
    tbl.getTTable().setDbName(dbName);
    tbl.setInputFormatClass(SequenceFileInputFormat.class.getName());
    tbl.setOutputFormatClass(SequenceFileOutputFormat.class.getName());
    tbl.setSerializationLib(ThriftDeserializer.class.getName());
    tbl.setSerdeParam(Constants.SERIALIZATION_CLASS, Complex.class.getName());
    tbl.setSerdeParam(Constants.SERIALIZATION_FORMAT, TBinaryProtocol.class.getName());
    return tbl;
  }

  public void testGetTables() throws Throwable {
    try {
      String dbName = "db_for_testgettables";
      hm.dropDatabase(dbName);
      hm.createDatabase(dbName, "");

      List<String> ts = new ArrayList<String>(2);
      ts.add("table1");
      ts.add("table2");
      Table tbl1 = createTestTable(dbName, ts.get(0));
      hm.createTable(tbl1);

      Table tbl2 = createTestTable(dbName, ts.get(1));
      hm.createTable(tbl2);

      List<String> fts = hm.getTablesForDb(dbName, ".*");
      assertEquals(ts, fts);
      assertEquals(2, fts.size());

      fts = hm.getTablesForDb(dbName, ".*1");
      assertEquals(1, fts.size());
      assertEquals(ts.get(0), fts.get(0));
      hm.dropDatabase(dbName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testGetTables() failed");
      throw e;
    }
  }

  public void testPartition() throws Throwable {
    try {
      String tableName = "table_for_testpartition";
      try {
        hm.dropTable(tableName);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to drop table: " + tableName, false);
      }
      LinkedList<String> cols = new LinkedList<String>();
      cols.add("key");
      cols.add("value");

      LinkedList<String> part_cols = new LinkedList<String>();
      part_cols.add("ds");
      part_cols.add("hr");
      try {
        hm.createTable(tableName, cols, part_cols, TextInputFormat.class, IgnoreKeyTextOutputFormat.class);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create table: " + tableName, false);
      }
      Table tbl = null;
      try {
        tbl = hm.getTable(tableName);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to fetch table: " + tableName, false);
      }
      HashMap<String, String> part_spec = new HashMap<String, String>();
      part_spec.clear();
      part_spec.put("ds", "2008-04-08");
      part_spec.put("hr", "12");
      try {
        hm.createPartition(tbl, part_spec);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create parition for table: " + tableName, false);
      }
      hm.dropTable(tableName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testPartition() failed");
      throw e;
    }
  }
}
