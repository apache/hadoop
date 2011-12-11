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

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.vertica.VerticaConfiguration;
import org.apache.hadoop.vertica.VerticaInputFormat;
import org.apache.hadoop.vertica.VerticaInputSplit;
import org.apache.hadoop.vertica.VerticaOutputFormat;
import org.apache.hadoop.vertica.VerticaRecord;
import org.apache.hadoop.vertica.VerticaRecordReader;
import org.apache.hadoop.vertica.VerticaRecordWriter;

public class TestVertica extends VerticaTestCase {

  public TestVertica(String name) {
    super(name);
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Fake class used to create a job conf
   */
  public class VerticaTestMR extends Configured {
  }

  public Job getVerticaJob() throws IOException {
    Configuration conf = new Configuration(true);
    Cluster cluster = new Cluster(conf);
    Job job = Job.getInstance(cluster);
    job.setJarByClass(VerticaTestMR.class);

    VerticaConfiguration.configureVertica(job.getConfiguration(),
        new String[] { AllTests.getHostname() }, AllTests.getDatabase(),
        AllTests.getUsername(), AllTests.getPassword());
    return job;
  }

  public VerticaInputSplit getVerticaSplit(boolean fake) throws Exception {
    List<Object> segment_params = new ArrayList<Object>();
    long start = 0;
    long end = 0;
    String input_query = "SELECT value FROM mrsource WHERE key = ?";

    segment_params.add(3);
    if (fake) {
      segment_params.add(Calendar.getInstance().getTime());
      segment_params.add("foobar");
      start = 5;
      end = 10;
    }

    VerticaInputSplit input = new VerticaInputSplit(input_query,
        segment_params, start, end);
    input.configure(getVerticaJob().getConfiguration());

    return input;
  }

  public void testVerticaRecord() throws ParseException, IOException {
    if(!AllTests.isSetup()) {
      return;
    }

    List<Integer> types = new ArrayList<Integer>();
    List<Object> values = new ArrayList<Object>();
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();
    DateFormat datefmt = new SimpleDateFormat("yyyy-MM-dd");
    DateFormat timefmt = new SimpleDateFormat("HH:mm:ss");
    DateFormat tmstmpfmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    types.add(Types.BIGINT);
    values.add(209348039485345L); // BIGINT
    types.add(Types.INTEGER);
    values.add(2342345); // INTGER
    types.add(Types.TINYINT);
    values.add((short) 564); // TINYINT
    types.add(Types.SMALLINT);
    values.add((short) 4); // SMALLINT
    types.add(Types.REAL);
    values.add(new BigDecimal(15234342345.532637)); // REAL
    types.add(Types.DECIMAL);
    values.add(new BigDecimal(346223093.4256)); // DECIMAL
    types.add(Types.NUMERIC);
    values.add(new BigDecimal(209232301132.4203)); // NUMERIC
    types.add(Types.DOUBLE);
    values.add(934029342.234); // DOUBLE
    types.add(Types.FLOAT);
    values.add((float) 62304.235); // FLOAT
    types.add(Types.BINARY);
    values.add(new byte[10]); // BINARY
    types.add(Types.LONGVARBINARY);
    values.add(new byte[10]); // LONGVARBINARY
    types.add(Types.VARBINARY);
    values.add(new byte[10]); // VARBINARY
    types.add(Types.BOOLEAN);
    values.add(new Boolean(true)); // BOOLEAN
    types.add(Types.CHAR);
    values.add('x'); // CHAR
    types.add(Types.LONGNVARCHAR);
    values.add("2ialnnnnsfm9.3;olainlekf nasl f'\\4\r\n"); // LONGNVARCHAR
    types.add(Types.LONGVARCHAR);
    values.add("3jflin4f'\\4\r\n'"); // LONGVARCHAR
    types.add(Types.NCHAR);
    values.add("jf|ls4\\4\r\nf44sf"); // NCHAR
    types.add(Types.VARCHAR);
    values.add("4filjsf!@#$^&)*()"); // VARCHAR
    types.add(Types.DATE);
    values.add(new Date(datefmt.parse("2009-06-07").getTime())); // DATE
    types.add(Types.TIME);
    values.add(new Time(timefmt.parse("16:17:18.90").getTime())); // TIME
    types.add(Types.TIMESTAMP);
    values
        .add(new Timestamp(tmstmpfmt.parse("2007-08-09 6:07:05.06").getTime())); // TIMESTAMP

    types.add(Types.BIGINT);
    values.add(null); // BIGINT
    types.add(Types.INTEGER);
    values.add(null); // INTGER
    types.add(Types.TINYINT);
    values.add(null); // TINYINT
    types.add(Types.SMALLINT);
    values.add(null); // SMALLINT
    types.add(Types.REAL);
    values.add(null); // REAL
    types.add(Types.DECIMAL);
    values.add(null); // DECIMAL
    types.add(Types.NUMERIC);
    values.add(null); // NUMERIC
    types.add(Types.DOUBLE);
    values.add(null); // DOUBLE
    types.add(Types.FLOAT);
    values.add(null); // FLOAT
    types.add(Types.BINARY);
    values.add(null); // BINARY
    types.add(Types.LONGVARBINARY);
    values.add(null); // LONGVARBINARY
    types.add(Types.VARBINARY);
    values.add(null); // VARBINARY
    types.add(Types.BOOLEAN);
    values.add(null); // BOOLEAN
    types.add(Types.CHAR);
    values.add(null); // CHAR
    types.add(Types.LONGNVARCHAR);
    values.add(null); // LONGNVARCHAR
    types.add(Types.LONGVARCHAR);
    values.add(null); // LONGVARCHAR
    types.add(Types.NCHAR);
    values.add(null); // NCHAR
    types.add(Types.VARCHAR);
    values.add(null); // VARCHAR
    types.add(Types.DATE);
    values.add(null); // DATE
    types.add(Types.TIME);
    values.add(null); // TIME
    types.add(Types.TIMESTAMP);
    values
        .add(null); // TIMESTAMP
    
    
    String sql1 = null;
    sql1 = recordTest(types, values, out, in, true);
    
    out = new DataOutputBuffer();
    in = new DataInputBuffer();
    String sql2 = null;
    sql2 = recordTest(types, values, out, in, true);
    
    assertEquals("SQL Serialization test failed", sql1, sql2);
  }

  private String recordTest(List<Integer> types, List<Object> values,
      DataOutputBuffer out, DataInputBuffer in, boolean date_string)
      throws IOException {
    VerticaRecord record = new VerticaRecord(null, types, values, date_string);

    // TODO: test values as hashmap of column names

    // write values into an output buffer
    record.write(out);

    // copy to an input buffer
    in.reset(out.getData(), out.getLength());

    // create a new record with new values
    List<Object> new_values = new ArrayList<Object>();
    record = new VerticaRecord(null, types, new_values, date_string);

    // read back into values
    record.readFields(in);

    // compare values
    for(int i = 0; i < values.size(); i++)
      if(values.get(i) == null) assertSame("Vertica Record serialized value " + i + " is null", values.get(i), new_values.get(i));
      else if(values.get(i).getClass().isArray()) {
        Object a = values.get(i);
        Object b = new_values.get(i);
        for(int j = 0; j < Array.getLength(a); j++)
          assertEquals("Vertica Record serialized value " + i + "[" + j + "] does not match", Array.get(a, j), Array.get(b, j));
      }
      else {
        assertEquals("Vertica Record serialized value " + i + " does not match", values.get(i), new_values.get(i));
      }

    // data in sql form
    return record.toSQLString();
  }

  public void testVerticaSplit() throws Exception {
    if(!AllTests.isSetup()) {
      return;
    }

    VerticaInputSplit input = getVerticaSplit(true);
    VerticaInputSplit rem_input = new VerticaInputSplit();

    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();

    input.write(out);

    in.reset(out.getData(), out.getLength());

    rem_input.readFields(in);
    assertEquals("Serialized segment params do not match", rem_input.getSegmentParams(), input.getSegmentParams());
    assertEquals("Serialized start does not match", rem_input.getStart(), input.getStart());
    assertEquals("Serialized length does not match", rem_input.getLength(), input.getLength());
  }

  public void testVerticaReader() throws Exception {
    if(!AllTests.isSetup()) {
      return;
    }

    VerticaInputSplit input = getVerticaSplit(false);
    VerticaRecordReader reader = new VerticaRecordReader(input, input
        .getConfiguration());
    TaskAttemptContext context = new TaskAttemptContextImpl(input
        .getConfiguration(), new TaskAttemptID());
    reader.initialize(input, context);

    boolean hasValue = reader.nextKeyValue();
    assertEquals("There should be a record in the database", hasValue, true);
    
    LongWritable key = reader.getCurrentKey();
    VerticaRecord value = reader.getCurrentValue();

    assertEquals("Key should be 1 for first record", key.get(), 1);
    assertEquals("Result type should be VARCHAR", ((Integer)value.getTypes().get(0)).intValue(), Types.VARCHAR);
    assertEquals("Result value should be three", value.getValues().get(0), "three");
    reader.close();
  }

  public void validateInput(Job job) throws IOException {
    VerticaInputFormat input = new VerticaInputFormat();
    List<InputSplit> splits = null;

    Configuration conf = job.getConfiguration();
    conf.setInt("mapreduce.job.maps", 1);
    JobContext context = new JobContextImpl(conf, new JobID());

    splits = input.getSplits(context);
    assert splits.size() == 1;

    conf.setInt("mapreduce.job.maps", 3);
    splits = input.getSplits(context);
    assert splits.size() == 3;

    conf.setInt("mapreduce.job.maps", 10);
    splits = input.getSplits(context);
    assert splits.size() == 10;
  }

  public void testVerticaInput() throws IOException {
    if(!AllTests.isSetup()) {
      return;
    }

    String input_query1 = "SELECT value FROM mrsource";
    String input_query2 = "SELECT value FROM mrsource WHERE key = ?";
    String segment_query = "SELECT y FROM bar";
    List<List<Object>> segment_params = new ArrayList<List<Object>>();
    for (int i = 0; i < 4; i++) {
      ArrayList<Object> params = new ArrayList<Object>();
      params.add(i);
      segment_params.add(params);
    }

    Job job = getVerticaJob();
    VerticaInputFormat.setInput(job, input_query1);
    validateInput(job);

    job = getVerticaJob();
    VerticaInputFormat.setInput(job, input_query2, segment_query);
    validateInput(job);

    VerticaInputFormat.setInput(job, input_query2, segment_params);
    validateInput(job);
  }

  public void testVerticaOutput() throws Exception {
    if(!AllTests.isSetup()) {
      return;
    }

    // TODO: test create schema
    // TODO: test writable variants of data types
    VerticaOutputFormat output = new VerticaOutputFormat();
    Job job = getVerticaJob();
    VerticaOutputFormat.setOutput(job, "mrtarget", true, "a int", "b boolean",
        "c char(1)", "d date", "f float", "t timestamp", "v varchar",
        "z varbinary");
    output.checkOutputSpecs(job, true);
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(),
        new TaskAttemptID());
    VerticaRecordWriter writer = (VerticaRecordWriter) output
        .getRecordWriter(context);

    Text table = new Text();
    table.set("mrtarget");

    VerticaRecord record = VerticaOutputFormat.getValue(job.getConfiguration());
    record.set(0, 125, true);
    record.set(1, true, true);
    record.set(2, 'c', true);
    record.set(3, Calendar.getInstance().getTime(), true);
    record.set(4, 234.526, true);
    record.set(5, Calendar.getInstance().getTime(), true);
    record.set(6, "foobar string", true);
    record.set(7, new byte[10], true);

    writer.write(table, record);
    writer.close(null);
  }
}
