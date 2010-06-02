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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MultiRegionTable;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Test Map/Reduce job over HBase tables. The map/reduce process we're testing
 * on our tables is simple - take every row in the table, reverse the value of
 * a particular cell, and write it back to the table.
 */
public class TestTableMapReduce extends MultiRegionTable {

  private static final Log LOG = LogFactory.getLog(TestTableMapReduce.class);

  static final String MULTI_REGION_TABLE_NAME = "mrtest";
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
  static final byte[] OUTPUT_FAMILY = Bytes.toBytes("text");

  /** constructor */
  public TestTableMapReduce() {
    super(Bytes.toString(INPUT_FAMILY));
    desc = new HTableDescriptor(MULTI_REGION_TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(INPUT_FAMILY));
    desc.addFamily(new HColumnDescriptor(OUTPUT_FAMILY));
  }

  /**
   * Pass the given key and processed record reduce
   */
  public static class ProcessContentsMapper
  extends TableMapper<ImmutableBytesWritable, Put> {

    /**
     * Pass the key, and reversed value to reduce
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     */
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

      // Get the original value and reverse it
      String originalValue = new String(value.getValue(INPUT_FAMILY, null),
        HConstants.UTF8_ENCODING);
      StringBuilder newValue = new StringBuilder(originalValue);
      newValue.reverse();
      // Now set the value to be collected
      Put outval = new Put(key.get());
      outval.add(OUTPUT_FAMILY, null, Bytes.toBytes(newValue.toString()));
      context.write(key, outval);
    }
  }

  /**
   * Test a map/reduce against a multi-region table
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public void testMultiRegionTable()
  throws IOException, InterruptedException, ClassNotFoundException {
    runTestOnTable(new HTable(conf, MULTI_REGION_TABLE_NAME));
  }

  private void runTestOnTable(HTable table)
  throws IOException, InterruptedException, ClassNotFoundException {
    MiniMRCluster mrCluster = new MiniMRCluster(2, fs.getUri().toString(), 1);

    Job job = null;
    try {
      LOG.info("Before map/reduce startup");
      job = new Job(conf, "process column contents");
      job.setNumReduceTasks(1);
      Scan scan = new Scan();
      scan.addFamily(INPUT_FAMILY);
      TableMapReduceUtil.initTableMapperJob(
        Bytes.toString(table.getTableName()), scan,
        ProcessContentsMapper.class, ImmutableBytesWritable.class,
        Put.class, job);
      TableMapReduceUtil.initTableReducerJob(
        Bytes.toString(table.getTableName()),
        IdentityTableReducer.class, job);
      FileOutputFormat.setOutputPath(job, new Path("test"));
      LOG.info("Started " + Bytes.toString(table.getTableName()));
      job.waitForCompletion(true);
      LOG.info("After map/reduce completion");

      // verify map-reduce results
      verify(Bytes.toString(table.getTableName()));
    } finally {
      mrCluster.shutdown();
      if (job != null) {
        FileUtil.fullyDelete(
          new File(job.getConfiguration().get("hadoop.tmp.dir")));
      }
    }
  }

  private void verify(String tableName) throws IOException {
    HTable table = new HTable(conf, tableName);
    boolean verified = false;
    long pause = conf.getLong("hbase.client.pause", 5 * 1000);
    int numRetries = conf.getInt("hbase.client.retries.number", 5);
    for (int i = 0; i < numRetries; i++) {
      try {
        LOG.info("Verification attempt #" + i);
        verifyAttempt(table);
        verified = true;
        break;
      } catch (NullPointerException e) {
        // If here, a cell was empty.  Presume its because updates came in
        // after the scanner had been opened.  Wait a while and retry.
        LOG.debug("Verification attempt failed: " + e.getMessage());
      }
      try {
        Thread.sleep(pause);
      } catch (InterruptedException e) {
        // continue
      }
    }
    assertTrue(verified);
  }

  /**
   * Looks at every value of the mapreduce output and verifies that indeed
   * the values have been reversed.
   *
   * @param table Table to scan.
   * @throws IOException
   * @throws NullPointerException if we failed to find a cell value
   */
  private void verifyAttempt(final HTable table) throws IOException, NullPointerException {
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILY);
    scan.addFamily(OUTPUT_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    try {
      for (Result r : scanner) {
        if (LOG.isDebugEnabled()) {
          if (r.size() > 2 ) {
            throw new IOException("Too many results, expected 2 got " +
              r.size());
          }
        }
        byte[] firstValue = null;
        byte[] secondValue = null;
        int count = 0;
        for(KeyValue kv : r.list()) {
          if (count == 0) {
            firstValue = kv.getValue();
          }
          if (count == 1) {
            secondValue = kv.getValue();
          }
          count++;
          if (count == 2) {
            break;
          }
        }

        String first = "";
        if (firstValue == null) {
          throw new NullPointerException(Bytes.toString(r.getRow()) +
            ": first value is null");
        }
        first = new String(firstValue, HConstants.UTF8_ENCODING);

        String second = "";
        if (secondValue == null) {
          throw new NullPointerException(Bytes.toString(r.getRow()) +
            ": second value is null");
        }
        byte[] secondReversed = new byte[secondValue.length];
        for (int i = 0, j = secondValue.length - 1; j >= 0; j--, i++) {
          secondReversed[i] = secondValue[j];
        }
        second = new String(secondReversed, HConstants.UTF8_ENCODING);

        if (first.compareTo(second) != 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("second key is not the reverse of first. row=" +
                Bytes.toStringBinary(r.getRow()) + ", first value=" + first +
                ", second value=" + second);
          }
          fail();
        }
      }
    } finally {
      scanner.close();
    }
  }

  /**
   * Test that we add tmpjars correctly including the ZK jar.
   */
  public void testAddDependencyJars() throws Exception {
    Job job = new Job();
    TableMapReduceUtil.addDependencyJars(job);
    String tmpjars = job.getConfiguration().get("tmpjars");

    System.err.println("tmpjars: " + tmpjars);
    assertTrue(tmpjars.contains("zookeeper"));
    assertTrue(tmpjars.contains("guava"));
  }
}
