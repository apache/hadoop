/**
 * Copyright 2009 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Simple test for {@link KeyValueSortReducer} and {@link HFileOutputFormat}.
 * Sets up and runs a mapreduce job that writes hfile output.
 * Creates a few inner classes to implement splits and an inputformat that
 * emits keys and values like those of {@link PerformanceEvaluation}.  Makes
 * as many splits as "mapred.map.tasks" maps.
 */
public class TestHFileOutputFormat  {
  private final static int ROWSPERSPLIT = 1024;

  private static final byte[] FAMILY_NAME = PerformanceEvaluation.FAMILY_NAME;
  private static final byte[] TABLE_NAME = Bytes.toBytes("TestTable");
  
  private HBaseTestingUtility util = new HBaseTestingUtility();
  
  private static Log LOG = LogFactory.getLog(TestHFileOutputFormat.class);
  
  /**
   * Simple mapper that makes KeyValue output.
   */
  static class RandomKVGeneratingMapper
  extends Mapper<NullWritable, NullWritable,
                 ImmutableBytesWritable, KeyValue> {
    
    private int keyLength;
    private static final int KEYLEN_DEFAULT=10;
    private static final String KEYLEN_CONF="randomkv.key.length";

    private int valLength;
    private static final int VALLEN_DEFAULT=10;
    private static final String VALLEN_CONF="randomkv.val.length";
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);
      
      Configuration conf = context.getConfiguration();
      keyLength = conf.getInt(KEYLEN_CONF, KEYLEN_DEFAULT);
      valLength = conf.getInt(VALLEN_CONF, VALLEN_DEFAULT);
    }

    protected void map(
        NullWritable n1, NullWritable n2,
        Mapper<NullWritable, NullWritable,
               ImmutableBytesWritable,KeyValue>.Context context)
        throws java.io.IOException ,InterruptedException
    {

      byte keyBytes[] = new byte[keyLength];
      byte valBytes[] = new byte[valLength];
      
      Random random = new Random(System.currentTimeMillis());
      for (int i = 0; i < ROWSPERSPLIT; i++) {

        random.nextBytes(keyBytes);
        random.nextBytes(valBytes);
        ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);

        KeyValue kv = new KeyValue(keyBytes, PerformanceEvaluation.FAMILY_NAME,
            PerformanceEvaluation.QUALIFIER_NAME, valBytes);
        context.write(key, kv);
      }
    }
  }

  @Before
  public void cleanupDir() throws IOException {
    util.cleanupTestDir();
  }
  
  
  private void setupRandomGeneratorMapper(Job job) {
    job.setInputFormatClass(NMapInputFormat.class);
    job.setMapperClass(RandomKVGeneratingMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
  }

  /**
   * Test that {@link HFileOutputFormat} RecordWriter amends timestamps if
   * passed a keyvalue whose timestamp is {@link HConstants#LATEST_TIMESTAMP}.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2615">HBASE-2615</a>
   */
  @Test
  public void test_LATEST_TIMESTAMP_isReplaced()
  throws IOException, InterruptedException {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;
    TaskAttemptContext context = null;
    Path dir =
      HBaseTestingUtility.getTestDir("test_LATEST_TIMESTAMP_isReplaced");
    try {
      Job job = new Job(conf);
      FileOutputFormat.setOutputPath(job, dir);
      context = new TaskAttemptContext(job.getConfiguration(),
        new TaskAttemptID());
      HFileOutputFormat hof = new HFileOutputFormat();
      writer = hof.getRecordWriter(context);
      final byte [] b = Bytes.toBytes("b");

      // Test 1.  Pass a KV that has a ts of LATEST_TIMESTAMP.  It should be
      // changed by call to write.  Check all in kv is same but ts.
      KeyValue kv = new KeyValue(b, b, b);
      KeyValue original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertFalse(original.equals(kv));
      assertTrue(Bytes.equals(original.getRow(), kv.getRow()));
      assertTrue(original.matchingColumn(kv.getFamily(), kv.getQualifier()));
      assertNotSame(original.getTimestamp(), kv.getTimestamp());
      assertNotSame(HConstants.LATEST_TIMESTAMP, kv.getTimestamp());

      // Test 2. Now test passing a kv that has explicit ts.  It should not be
      // changed by call to record write.
      kv = new KeyValue(b, b, b, kv.getTimestamp() - 1, b);
      original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertTrue(original.equals(kv));
    } finally {
      if (writer != null && context != null) writer.close(context);
      dir.getFileSystem(conf).delete(dir, true);
    }
  }

  /**
   * Run small MR job.
   */
  @Test
  public void testWritingPEData() throws Exception {
    Configuration conf = util.getConfiguration();
    Path testDir = HBaseTestingUtility.getTestDir("testWritingPEData");
    FileSystem fs = testDir.getFileSystem(conf);
    
    // Set down this value or we OOME in eclipse.
    conf.setInt("io.sort.mb", 20);
    // Write a few files.
    conf.setLong("hbase.hregion.max.filesize", 64 * 1024);
    
    Job job = new Job(conf, "testWritingPEData");
    setupRandomGeneratorMapper(job);
    // This partitioner doesn't work well for number keys but using it anyways
    // just to demonstrate how to configure it.
    byte[] startKey = new byte[RandomKVGeneratingMapper.KEYLEN_DEFAULT];
    byte[] endKey = new byte[RandomKVGeneratingMapper.KEYLEN_DEFAULT];
    
    Arrays.fill(startKey, (byte)0);
    Arrays.fill(endKey, (byte)0xff);
    
    job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
    // Set start and end rows for partitioner.
    SimpleTotalOrderPartitioner.setStartKey(job.getConfiguration(), startKey);
    SimpleTotalOrderPartitioner.setEndKey(job.getConfiguration(), endKey);
    job.setReducerClass(KeyValueSortReducer.class);
    job.setOutputFormatClass(HFileOutputFormat.class);
    job.setNumReduceTasks(4);
    
    FileOutputFormat.setOutputPath(job, testDir);
    assertTrue(job.waitForCompletion(false));
    FileStatus [] files = fs.listStatus(testDir);
    assertTrue(files.length > 0);
  }
  
  @Test
  public void testJobConfiguration() throws Exception {
    Job job = new Job();
    HTable table = Mockito.mock(HTable.class);
    byte[][] mockKeys = new byte[][] {
        HConstants.EMPTY_BYTE_ARRAY,
        Bytes.toBytes("aaa"),
        Bytes.toBytes("ggg"),
        Bytes.toBytes("zzz")
    };
    Mockito.doReturn(mockKeys).when(table).getStartKeys();
    
    HFileOutputFormat.configureIncrementalLoad(job, table);
    assertEquals(job.getNumReduceTasks(), 4);
  }
  
  private byte [][] generateRandomStartKeys(int numKeys) {
    Random random = new Random();
    byte[][] ret = new byte[numKeys][];
    // first region start key is always empty
    ret[0] = HConstants.EMPTY_BYTE_ARRAY;
    for (int i = 1; i < numKeys; i++) {
      ret[i] = PerformanceEvaluation.generateValue(random);
    }
    return ret;
  }
  
  @Test
  public void testMRIncrementalLoad() throws Exception {
    doIncrementalLoadTest(false);
  }
  
  @Test
  public void testMRIncrementalLoadWithSplit() throws Exception {
    doIncrementalLoadTest(true);
  }
  
  private void doIncrementalLoadTest(
      boolean shouldChangeRegions) throws Exception {
    Configuration conf = util.getConfiguration();
    Path testDir = HBaseTestingUtility.getTestDir("testLocalMRIncrementalLoad");
    byte[][] startKeys = generateRandomStartKeys(5);
    
    try {
      util.startMiniCluster();
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTable table = util.createTable(TABLE_NAME, FAMILY_NAME);
      int numRegions = util.createMultiRegions(
          util.getConfiguration(), table, FAMILY_NAME,
          startKeys);
      assertEquals("Should make 5 regions",
          numRegions, 5);
      assertEquals("Should start with empty table",
          0, util.countRows(table));

      // Generate the bulk load files
      util.startMiniMapReduceCluster();
      runIncrementalPELoad(conf, table, testDir);
      // This doesn't write into the table, just makes files
      assertEquals("HFOF should not touch actual table",
          0, util.countRows(table));
  
      if (shouldChangeRegions) {
        LOG.info("Changing regions in table");
        admin.disableTable(table.getTableName());
        byte[][] newStartKeys = generateRandomStartKeys(15);
        util.createMultiRegions(util.getConfiguration(),
            table, FAMILY_NAME, newStartKeys);
        admin.enableTable(table.getTableName());
        while (table.getRegionsInfo().size() != 15 ||
            !admin.isTableAvailable(table.getTableName())) {
          Thread.sleep(1000);
          LOG.info("Waiting for new region assignment to happen");
        }
      }
      
      // Perform the actual load
      new LoadIncrementalHFiles(conf).doBulkLoad(testDir, table);
      
      // Ensure data shows up
      int expectedRows = conf.getInt("mapred.map.tasks", 1) * ROWSPERSPLIT;
      assertEquals("LoadIncrementalHFiles should put expected data in table",
          expectedRows, util.countRows(table));
      String tableDigestBefore = util.checksumRows(table);
            
      // Cause regions to reopen
      admin.disableTable(TABLE_NAME);
      while (table.getRegionsInfo().size() != 0) {
        Thread.sleep(1000);
        LOG.info("Waiting for table to disable"); 
      }
      admin.enableTable(TABLE_NAME);
      util.waitTableAvailable(TABLE_NAME, 30000);
      
      assertEquals("Data should remain after reopening of regions",
          tableDigestBefore, util.checksumRows(table));
    } finally {
      util.shutdownMiniMapReduceCluster();
      util.shutdownMiniCluster();
    }
  }
  
  
  
  private void runIncrementalPELoad(
      Configuration conf, HTable table, Path outDir)
  throws Exception {
    Job job = new Job(conf, "testLocalMRIncrementalLoad");
    setupRandomGeneratorMapper(job);
    HFileOutputFormat.configureIncrementalLoad(job, table);
    FileOutputFormat.setOutputPath(job, outDir);
    
    assertEquals(table.getRegionsInfo().size(),
        job.getNumReduceTasks());
    
    assertTrue(job.waitForCompletion(true));
  }
  
  public static void main(String args[]) throws Exception {
    new TestHFileOutputFormat().manualTest(args);
  }
  
  public void manualTest(String args[]) throws Exception {
    Configuration conf = HBaseConfiguration.create();    
    util = new HBaseTestingUtility(conf);
    if ("newtable".equals(args[0])) {
      byte[] tname = args[1].getBytes();
      HTable table = util.createTable(tname, FAMILY_NAME);
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.disableTable(tname);
      util.createMultiRegions(conf, table, FAMILY_NAME,
          generateRandomStartKeys(5));
      admin.enableTable(tname);
    } else if ("incremental".equals(args[0])) {
      byte[] tname = args[1].getBytes();
      HTable table = new HTable(conf, tname);
      Path outDir = new Path("incremental-out");
      runIncrementalPELoad(conf, table, outDir);
    } else {
      throw new RuntimeException(
          "usage: TestHFileOutputFormat newtable | incremental");
    }
  }
}
