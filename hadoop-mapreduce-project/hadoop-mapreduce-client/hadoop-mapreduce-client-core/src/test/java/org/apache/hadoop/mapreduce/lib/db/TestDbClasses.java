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
package org.apache.hadoop.mapreduce.lib.db;

import java.sql.Connection;
import java.sql.Types;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestDbClasses {
  /**
   * test splitters from DataDrivenDBInputFormat. For different data types may
   * be different splitter
   */
  @Test(timeout = 10000)
  public void testDataDrivenDBInputFormatSplitter() {
    DataDrivenDBInputFormat<NullDBWritable> format = new DataDrivenDBInputFormat<NullDBWritable>();
    testCommonSplitterTypes(format);
    assertEquals(DateSplitter.class, format.getSplitter(Types.TIMESTAMP)
        .getClass());
    assertEquals(DateSplitter.class, format.getSplitter(Types.DATE).getClass());
    assertEquals(DateSplitter.class, format.getSplitter(Types.TIME).getClass());
  }

  @Test(timeout = 10000)
  public void testDataDrivenDBInputFormat() throws Exception {
    JobContext jobContext = mock(JobContext.class);
    Configuration configuration = new Configuration();
    configuration.setInt(MRJobConfig.NUM_MAPS, 1);

    when(jobContext.getConfiguration()).thenReturn(configuration);
    DataDrivenDBInputFormat<NullDBWritable> format = new DataDrivenDBInputFormat<NullDBWritable>();
    List<InputSplit> splits = format.getSplits(jobContext);
    assertEquals(1, splits.size());
    DataDrivenDBInputSplit split = (DataDrivenDBInputSplit) splits.get(0);
    assertEquals("1=1", split.getLowerClause());
    assertEquals("1=1", split.getUpperClause());

    // 2
    configuration.setInt(MRJobConfig.NUM_MAPS, 2);

    DataDrivenDBInputFormat.setBoundingQuery(configuration, "query");
    assertEquals("query",
        configuration.get(DBConfiguration.INPUT_BOUNDING_QUERY));

    Job job = mock(Job.class);
    when(job.getConfiguration()).thenReturn(configuration);
    DataDrivenDBInputFormat.setInput(job, NullDBWritable.class, "query",
        "Bounding Query");

    assertEquals("Bounding Query",
        configuration.get(DBConfiguration.INPUT_BOUNDING_QUERY));
  }

  @Test(timeout = 10000)
  public void testOracleDataDrivenDBInputFormat() throws Exception {
    OracleDataDrivenDBInputFormat<NullDBWritable> format = 
        new OracleDataDrivenDBInputFormatForTest();
    testCommonSplitterTypes(format);
    assertEquals(OracleDateSplitter.class, format.getSplitter(Types.TIMESTAMP)
        .getClass());
    assertEquals(OracleDateSplitter.class, 
        format.getSplitter(Types.DATE).getClass());
    assertEquals(OracleDateSplitter.class, 
        format.getSplitter(Types.TIME).getClass());
  }

  /**
   * test generate sql script for OracleDBRecordReader.
   */

  @Test(timeout = 20000)
  public void testOracleDBRecordReader() throws Exception {
    DBInputSplit splitter = new DBInputSplit(1, 10);
    Configuration configuration = new Configuration();
    Connection connect = DriverForTest.getConnection();

    DBConfiguration dbConfiguration = new DBConfiguration(configuration);
    dbConfiguration.setInputOrderBy("Order");
    String[] fields = { "f1", "f2" };

    OracleDBRecordReader<NullDBWritable> recorder = new OracleDBRecordReader<NullDBWritable>(
        splitter, NullDBWritable.class, configuration, connect,
        dbConfiguration, "condition", fields, "table");
    assertEquals(
        "SELECT * FROM (SELECT a.*,ROWNUM dbif_rno FROM ( SELECT f1, f2 FROM table WHERE condition ORDER BY Order ) a WHERE rownum <= 10 ) WHERE dbif_rno > 1",
        recorder.getSelectQuery());
  }

  private void testCommonSplitterTypes(
      DataDrivenDBInputFormat<NullDBWritable> format) {
    assertEquals(BigDecimalSplitter.class, format.getSplitter(Types.DECIMAL)
        .getClass());
    assertEquals(BigDecimalSplitter.class, format.getSplitter(Types.NUMERIC)
        .getClass());
    assertEquals(BooleanSplitter.class, format.getSplitter(Types.BOOLEAN)
        .getClass());
    assertEquals(BooleanSplitter.class, format.getSplitter(Types.BIT)
        .getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.BIGINT)
        .getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.TINYINT)
        .getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.SMALLINT)
        .getClass());
    assertEquals(IntegerSplitter.class, format.getSplitter(Types.INTEGER)
        .getClass());
    assertEquals(FloatSplitter.class, format.getSplitter(Types.DOUBLE)
        .getClass());
    assertEquals(FloatSplitter.class, format.getSplitter(Types.REAL).getClass());
    assertEquals(FloatSplitter.class, format.getSplitter(Types.FLOAT)
        .getClass());
    assertEquals(TextSplitter.class, format.getSplitter(Types.LONGVARCHAR)
        .getClass());
    assertEquals(TextSplitter.class, format.getSplitter(Types.CHAR).getClass());
    assertEquals(TextSplitter.class, format.getSplitter(Types.VARCHAR)
        .getClass());
    // if unknown data type splitter is null
    assertNull(format.getSplitter(Types.BINARY));
  }

  private class OracleDataDrivenDBInputFormatForTest extends
      OracleDataDrivenDBInputFormat<NullDBWritable> {

    @Override
    public DBConfiguration getDBConf() {

      String[] names = { "field1", "field2" };
      DBConfiguration result = mock(DBConfiguration.class);
      when(result.getInputConditions()).thenReturn("conditions");
      when(result.getInputFieldNames()).thenReturn(names);
      when(result.getInputTableName()).thenReturn("table");
      return result;
    }

    @Override
    public Connection getConnection() {
      return DriverForTest.getConnection();
    }

  }
  
}
