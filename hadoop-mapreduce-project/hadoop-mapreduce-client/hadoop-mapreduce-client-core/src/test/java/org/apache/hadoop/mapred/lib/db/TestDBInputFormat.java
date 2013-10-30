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

package org.apache.hadoop.mapred.lib.db;

import java.sql.DriverManager;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBInputFormat.DBInputSplit;
import org.apache.hadoop.mapred.lib.db.DBInputFormat.DBRecordReader;
import org.apache.hadoop.mapred.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DriverForTest;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestDBInputFormat {

  /**
   * test DBInputFormat class. Class should split result for chunks
   * @throws Exception
   */
  @Test(timeout = 10000)
  public void testDBInputFormat() throws Exception {
    JobConf configuration = new JobConf();
    setupDriver(configuration);
    
    DBInputFormat<NullDBWritable> format = new DBInputFormat<NullDBWritable>();
    format.setConf(configuration);
    format.setConf(configuration);
    DBInputFormat.DBInputSplit splitter = new DBInputFormat.DBInputSplit(1, 10);
    Reporter reporter = mock(Reporter.class);
    RecordReader<LongWritable, NullDBWritable> reader = format.getRecordReader(
        splitter, configuration, reporter);

    configuration.setInt(MRJobConfig.NUM_MAPS, 3);
    InputSplit[] lSplits = format.getSplits(configuration, 3);
    assertEquals(5, lSplits[0].getLength());
    assertEquals(3, lSplits.length);

    // test reader .Some simple tests
    assertEquals(LongWritable.class, reader.createKey().getClass());
    assertEquals(0, reader.getPos());
    assertEquals(0, reader.getProgress(), 0.001);
    reader.close();
  }
  
  /** 
   * test configuration for db. should works DBConfiguration.* parameters. 
   */
  @Test (timeout = 5000)
  public void testSetInput() {
    JobConf configuration = new JobConf();

    String[] fieldNames = { "field1", "field2" };
    DBInputFormat.setInput(configuration, NullDBWritable.class, "table",
        "conditions", "orderBy", fieldNames);
    assertEquals(
        "org.apache.hadoop.mapred.lib.db.DBInputFormat$NullDBWritable",
        configuration.getClass(DBConfiguration.INPUT_CLASS_PROPERTY, null)
            .getName());
    assertEquals("table",
        configuration.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, null));

    String[] fields = configuration
        .getStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
    assertEquals("field1", fields[0]);
    assertEquals("field2", fields[1]);

    assertEquals("conditions",
        configuration.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY, null));
    assertEquals("orderBy",
        configuration.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY, null));

    configuration = new JobConf();

    DBInputFormat.setInput(configuration, NullDBWritable.class, "query",
        "countQuery");
    assertEquals("query", configuration.get(DBConfiguration.INPUT_QUERY, null));
    assertEquals("countQuery",
        configuration.get(DBConfiguration.INPUT_COUNT_QUERY, null));
    
    JobConf jConfiguration = new JobConf();
    DBConfiguration.configureDB(jConfiguration, "driverClass", "dbUrl", "user",
        "password");
    assertEquals("driverClass",
        jConfiguration.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
    assertEquals("dbUrl", jConfiguration.get(DBConfiguration.URL_PROPERTY));
    assertEquals("user", jConfiguration.get(DBConfiguration.USERNAME_PROPERTY));
    assertEquals("password",
        jConfiguration.get(DBConfiguration.PASSWORD_PROPERTY));
    jConfiguration = new JobConf();
    DBConfiguration.configureDB(jConfiguration, "driverClass", "dbUrl");
    assertEquals("driverClass",
        jConfiguration.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
    assertEquals("dbUrl", jConfiguration.get(DBConfiguration.URL_PROPERTY));
    assertNull(jConfiguration.get(DBConfiguration.USERNAME_PROPERTY));
    assertNull(jConfiguration.get(DBConfiguration.PASSWORD_PROPERTY));
  }

  /**
   * 
   * test DBRecordReader. This reader should creates keys, values, know about position.. 
   */
  @SuppressWarnings("unchecked")
  @Test (timeout = 5000)
  public void testDBRecordReader() throws Exception {

    JobConf job = mock(JobConf.class);
    DBConfiguration dbConfig = mock(DBConfiguration.class);
    String[] fields = { "field1", "filed2" };

    @SuppressWarnings("rawtypes")
    DBRecordReader reader = new DBInputFormat<NullDBWritable>().new DBRecordReader(
        new DBInputSplit(),  NullDBWritable.class, job,
        DriverForTest.getConnection(), dbConfig, "condition", fields, "table");
    LongWritable key = reader.createKey();
    assertEquals(0, key.get());
    DBWritable value = reader.createValue();
    assertEquals(
        "org.apache.hadoop.mapred.lib.db.DBInputFormat$NullDBWritable", value
            .getClass().getName());
    assertEquals(0, reader.getPos());
    assertFalse(reader.next(key, value));

  }

  private void setupDriver(JobConf configuration) throws Exception {
    configuration.set(DBConfiguration.URL_PROPERTY, "testUrl");
    DriverManager.registerDriver(new DriverForTest());
    configuration.set(DBConfiguration.DRIVER_CLASS_PROPERTY,
        DriverForTest.class.getCanonicalName());
  }

}
