/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests various scan start and stop row scenarios. This is set in a scan and
 * tested in a MapReduce job to see if that is handed over and done properly
 * too.
 */
public class TestTableInputFormatScan {

  static final Log LOG = LogFactory.getLog(TestTableInputFormatScan.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  static final byte[] TABLE_NAME = Bytes.toBytes("scantest");
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
  static final String KEY_STARTROW = "startRow";
  static final String KEY_LASTROW = "stpRow";

  private static HTable table = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(TableInputFormat.class);
    TEST_UTIL.enableDebug(TableInputFormatBase.class);
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    table = TEST_UTIL.createTable(TABLE_NAME, INPUT_FAMILY);
    TEST_UTIL.createMultiRegions(table, INPUT_FAMILY);
    TEST_UTIL.loadTable(table, INPUT_FAMILY);
    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Pass the key and value to reduce.
   */
  public static class ScanMapper
  extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

    /**
     * Pass the key and value to reduce.
     *
     * @param key  The key, here "aaa", "aab" etc.
     * @param value  The value is the same as the key.
     * @param context  The task context.
     * @throws IOException When reading the rows fails.
     */
    @Override
    public void map(ImmutableBytesWritable key, Result value,
      Context context)
    throws IOException, InterruptedException {
      if (value.size() != 1) {
        throw new IOException("There should only be one input column");
      }
      Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
        cf = value.getMap();
      if(!cf.containsKey(INPUT_FAMILY)) {
        throw new IOException("Wrong input columns. Missing: '" +
          Bytes.toString(INPUT_FAMILY) + "'.");
      }
      String val = Bytes.toStringBinary(value.getValue(INPUT_FAMILY, null));
      LOG.info("map: key -> " + Bytes.toStringBinary(key.get()) +
        ", value -> " + val);
      context.write(key, key);
    }

  }

  /**
   * Checks the last and first key seen against the scanner boundaries.
   */
  public static class ScanReducer
  extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable,
                  NullWritable, NullWritable> {

    private String first = null;
    private String last = null;

    protected void reduce(ImmutableBytesWritable key,
        Iterable<ImmutableBytesWritable> values, Context context)
    throws IOException ,InterruptedException {
      int count = 0;
      for (ImmutableBytesWritable value : values) {
        String val = Bytes.toStringBinary(value.get());
        LOG.info("reduce: key[" + count + "] -> " +
          Bytes.toStringBinary(key.get()) + ", value -> " + val);
        if (first == null) first = val;
        last = val;
        count++;
      }
    }

    protected void cleanup(Context context)
    throws IOException, InterruptedException {
      Configuration c = context.getConfiguration();
      String startRow = c.get(KEY_STARTROW);
      String lastRow = c.get(KEY_LASTROW);
      LOG.info("cleanup: first -> \"" + first + "\", start row -> \"" + startRow + "\"");
      LOG.info("cleanup: last -> \"" + last + "\", last row -> \"" + lastRow + "\"");
      if (startRow != null && startRow.length() > 0) {
        assertEquals(startRow, first);
      }
      if (lastRow != null && lastRow.length() > 0) {
        assertEquals(lastRow, last);
      }
    }

  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, null, null);
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToAPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "app", "apo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToBBA()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "bba", "baz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToBBB()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "bbb", "bba");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToOPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "opp", "opo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanOBBToOPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("obb", "opp", "opo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanOBBToQPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("obb", "qpp", "qpo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanOPPToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("opp", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanYYXToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("yyx", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanYYYToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("yyy", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanYZYToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("yzy", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private void testScan(String start, String stop, String last)
  throws IOException, InterruptedException, ClassNotFoundException {
    String jobName = "Scan" + (start != null ? start.toUpperCase() : "Empty") +
    "To" + (stop != null ? stop.toUpperCase() : "Empty");
    LOG.info("Before map/reduce startup - job " + jobName);
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILY);
    if (start != null) {
      scan.setStartRow(Bytes.toBytes(start));
    }
    c.set(KEY_STARTROW, start != null ? start : "");
    if (stop != null) {
      scan.setStopRow(Bytes.toBytes(stop));
    }
    c.set(KEY_LASTROW, last != null ? last : "");
    LOG.info("scan before: " + scan);
    Job job = new Job(c, jobName);
    TableMapReduceUtil.initTableMapperJob(
      Bytes.toString(TABLE_NAME), scan, ScanMapper.class,
      ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
    job.setReducerClass(ScanReducer.class);
    job.setNumReduceTasks(1); // one to get final "first" and "last" key
    FileOutputFormat.setOutputPath(job, new Path(job.getJobName()));
    LOG.info("Started " + job.getJobName());
    job.waitForCompletion(true);
    assertTrue(job.isComplete());
    LOG.info("After map/reduce completion - job " + jobName);
  }
}