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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
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

import com.google.common.collect.Lists;

/**
 * Simple test for {@link KeyValueSortReducer} and {@link HFileOutputFormat}.
 * Sets up and runs a mapreduce job that writes hfile output.
 * Creates a few inner classes to implement splits and an inputformat that
 * emits keys and values like those of {@link PerformanceEvaluation}.
 */
public class TestHFileOutputFormat  {
  private final static int ROWSPERSPLIT = 1024;

  private static final byte[][] FAMILIES
    = { Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-A"))
      , Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-B"))};
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
      
      int taskId = context.getTaskAttemptID().getTaskID().getId();
      assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";

      Random random = new Random();
      for (int i = 0; i < ROWSPERSPLIT; i++) {

        random.nextBytes(keyBytes);
        // Ensure that unique tasks generate unique keys
        keyBytes[keyLength - 1] = (byte)(taskId & 0xFF);
        random.nextBytes(valBytes);
        ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);

        for (byte[] family : TestHFileOutputFormat.FAMILIES) {
          KeyValue kv = new KeyValue(keyBytes, family,
              PerformanceEvaluation.QUALIFIER_NAME, valBytes);
          context.write(key, kv);
        }
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
  throws Exception {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;
    TaskAttemptContext context = null;
    Path dir =
      util.getDataTestDir("test_LATEST_TIMESTAMP_isReplaced");
    try {
      Job job = new Job(conf);
      FileOutputFormat.setOutputPath(job, dir);
      context = getTestTaskAttemptContext(job);
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
   * @return True if the available mapreduce is post-0.20.
   */
  private static boolean isPost020MapReduce() {
    // Here is a coarse test for post 0.20 hadoop; TAC became an interface.
    return TaskAttemptContext.class.isInterface();
  }

  private TaskAttemptContext getTestTaskAttemptContext(final Job job)
  throws IOException, Exception {
    TaskAttemptContext context;
    if (isPost020MapReduce()) {
      TaskAttemptID id =
        TaskAttemptID.forName("attempt_200707121733_0001_m_000000_0");
      Class<?> clazz =
        Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
      Constructor<?> c = clazz.
          getConstructor(job.getConfiguration().getClass(), TaskAttemptID.class);
      context = (TaskAttemptContext)c.newInstance(job.getConfiguration(), id);
    } else {
      context = org.apache.hadoop.hbase.mapreduce.hadoopbackport.InputSampler.
        getTaskAttemptContext(job);
    }
    return context;
  }

  /*
   * Test that {@link HFileOutputFormat} creates an HFile with TIMERANGE
   * metadata used by time-restricted scans.
   */
  @Test
  public void test_TIMERANGE() throws Exception { 
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;
    TaskAttemptContext context = null;
    Path dir =
      util.getDataTestDir("test_TIMERANGE_present");
    LOG.info("Timerange dir writing to dir: "+ dir);
    try {
      // build a record writer using HFileOutputFormat
      Job job = new Job(conf);
      FileOutputFormat.setOutputPath(job, dir);
      context = getTestTaskAttemptContext(job);
      HFileOutputFormat hof = new HFileOutputFormat();
      writer = hof.getRecordWriter(context);

      // Pass two key values with explicit times stamps
      final byte [] b = Bytes.toBytes("b");

      // value 1 with timestamp 2000
      KeyValue kv = new KeyValue(b, b, b, 2000, b);
      KeyValue original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertEquals(original,kv);

      // value 2 with timestamp 1000
      kv = new KeyValue(b, b, b, 1000, b);
      original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertEquals(original, kv);

      // verify that the file has the proper FileInfo.
      writer.close(context);

      // the generated file lives 3 directories down and is the only file,
      // so we traverse the dirs to get to the file
      // ./_temporary/_attempt__0000_r_000000_0/b/1979617994050536795
      FileSystem fs = FileSystem.get(conf);
      Path path = HFileOutputFormat.getOutputPath(job);
      FileStatus[] sub1 = fs.listStatus(path);
      FileStatus[] sub2 = fs.listStatus(sub1[0].getPath());
      FileStatus[] sub3 = fs.listStatus(sub2[0].getPath());
      FileStatus[] file = fs.listStatus(sub3[0].getPath());

      // open as HFile Reader and pull out TIMERANGE FileInfo.
      HFile.Reader rd = HFile.createReader(fs, file[0].getPath(),
          new CacheConfig(conf));
      Map<byte[],byte[]> finfo = rd.loadFileInfo();
      byte[] range = finfo.get("TIMERANGE".getBytes());
      assertNotNull(range);

      // unmarshall and check values.
      TimeRangeTracker timeRangeTracker = new TimeRangeTracker();
      Writables.copyWritable(range, timeRangeTracker);
      LOG.info(timeRangeTracker.getMinimumTimestamp() +
          "...." + timeRangeTracker.getMaximumTimestamp());
      assertEquals(1000, timeRangeTracker.getMinimumTimestamp());
      assertEquals(2000, timeRangeTracker.getMaximumTimestamp());
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
    Path testDir = util.getDataTestDir("testWritingPEData");
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
    setupMockStartKeys(table);
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
    Path testDir = util.getDataTestDir("testLocalMRIncrementalLoad");
    byte[][] startKeys = generateRandomStartKeys(5);
    
    try {
      util.startMiniCluster();
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTable table = util.createTable(TABLE_NAME, FAMILIES);
      assertEquals("Should start with empty table",
          0, util.countRows(table));
      int numRegions = util.createMultiRegions(
          util.getConfiguration(), table, FAMILIES[0], startKeys);
      assertEquals("Should make 5 regions", numRegions, 5);

      // Generate the bulk load files
      util.startMiniMapReduceCluster();
      runIncrementalPELoad(conf, table, testDir);
      // This doesn't write into the table, just makes files
      assertEquals("HFOF should not touch actual table",
          0, util.countRows(table));
  

      // Make sure that a directory was created for every CF
      int dir = 0;
      for (FileStatus f : testDir.getFileSystem(conf).listStatus(testDir)) {
        for (byte[] family : FAMILIES) {
          if (Bytes.toString(family).equals(f.getPath().getName())) {
            ++dir;
          }
        }
      }
      assertEquals("Column family not found in FS.", FAMILIES.length, dir);

      // handle the split case
      if (shouldChangeRegions) {
        LOG.info("Changing regions in table");
        admin.disableTable(table.getTableName());
        while(util.getMiniHBaseCluster().getMaster().getAssignmentManager().
            isRegionsInTransition()) {
          Threads.sleep(200);
          LOG.info("Waiting on table to finish disabling");
        }
        byte[][] newStartKeys = generateRandomStartKeys(15);
        util.createMultiRegions(
            util.getConfiguration(), table, FAMILIES[0], newStartKeys);
        admin.enableTable(table.getTableName());
        while (table.getRegionsInfo().size() != 15 ||
            !admin.isTableAvailable(table.getTableName())) {
          Thread.sleep(200);
          LOG.info("Waiting for new region assignment to happen");
        }
      }
      
      // Perform the actual load
      new LoadIncrementalHFiles(conf).doBulkLoad(testDir, table);
      
      // Ensure data shows up
      int expectedRows = NMapInputFormat.getNumMapTasks(conf) * ROWSPERSPLIT;
      assertEquals("LoadIncrementalHFiles should put expected data in table",
          expectedRows, util.countRows(table));
      Scan scan = new Scan();
      ResultScanner results = table.getScanner(scan);
      int count = 0;
      for (Result res : results) {
        count++;
        assertEquals(FAMILIES.length, res.raw().length);
        KeyValue first = res.raw()[0];
        for (KeyValue kv : res.raw()) {
          assertTrue(KeyValue.COMPARATOR.matchingRows(first, kv));
          assertTrue(Bytes.equals(first.getValue(), kv.getValue()));
        }
      }
      results.close();
      String tableDigestBefore = util.checksumRows(table);
            
      // Cause regions to reopen
      admin.disableTable(TABLE_NAME);
      while (!admin.isTableDisabled(TABLE_NAME)) {
        Thread.sleep(200);
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

    Assert.assertFalse( util.getTestFileSystem().exists(outDir)) ;

    assertEquals(table.getRegionsInfo().size(),
      job.getNumReduceTasks());
    
    assertTrue(job.waitForCompletion(true));
  }
  
  /**
   * Test for
   * {@link HFileOutputFormat#createFamilyCompressionMap(Configuration)}. Tests
   * that the compression map is correctly deserialized from configuration
   * 
   * @throws IOException
   */
  @Test
  public void testCreateFamilyCompressionMap() throws IOException {
    for (int numCfs = 0; numCfs <= 3; numCfs++) {
      Configuration conf = new Configuration(this.util.getConfiguration());
      Map<String, Compression.Algorithm> familyToCompression = getMockColumnFamilies(numCfs);
      HTable table = Mockito.mock(HTable.class);
      setupMockColumnFamilies(table, familyToCompression);
      HFileOutputFormat.configureCompression(table, conf);

      // read back family specific compression setting from the configuration
      Map<byte[], String> retrievedFamilyToCompressionMap = HFileOutputFormat.createFamilyCompressionMap(conf);

      // test that we have a value for all column families that matches with the
      // used mock values
      for (Entry<String, Algorithm> entry : familyToCompression.entrySet()) {
        assertEquals("Compression configuration incorrect for column family:" + entry.getKey(), entry.getValue()
                     .getName(), retrievedFamilyToCompressionMap.get(entry.getKey().getBytes()));
      }
    }
  }

  private void setupMockColumnFamilies(HTable table,
    Map<String, Compression.Algorithm> familyToCompression) throws IOException
  {
    HTableDescriptor mockTableDescriptor = new HTableDescriptor(TABLE_NAME);
    for (Entry<String, Compression.Algorithm> entry : familyToCompression.entrySet()) {
      mockTableDescriptor.addFamily(new HColumnDescriptor(entry.getKey().getBytes(), 1, entry.getValue().getName(),
                                                          false, false, 0, "none"));
    }
    Mockito.doReturn(mockTableDescriptor).when(table).getTableDescriptor();
  }

  private void setupMockStartKeys(HTable table) throws IOException {
    byte[][] mockKeys = new byte[][] {
        HConstants.EMPTY_BYTE_ARRAY,
        Bytes.toBytes("aaa"),
        Bytes.toBytes("ggg"),
        Bytes.toBytes("zzz")
    };
    Mockito.doReturn(mockKeys).when(table).getStartKeys();
  }

  /**
   * @return a map from column family names to compression algorithms for
   *         testing column family compression. Column family names have special characters
   */
  private Map<String, Compression.Algorithm> getMockColumnFamilies(int numCfs) {
    Map<String, Compression.Algorithm> familyToCompression = new HashMap<String, Compression.Algorithm>();
    // use column family names having special characters
    if (numCfs-- > 0) {
      familyToCompression.put("Family1!@#!@#&", Compression.Algorithm.LZO);
    }
    if (numCfs-- > 0) {
      familyToCompression.put("Family2=asdads&!AASD", Compression.Algorithm.SNAPPY);
    }
    if (numCfs-- > 0) {
      familyToCompression.put("Family2=asdads&!AASD", Compression.Algorithm.GZ);
    }
    if (numCfs-- > 0) {
      familyToCompression.put("Family3", Compression.Algorithm.NONE);
    }
    return familyToCompression;
  }
  
  /**
   * Test that {@link HFileOutputFormat} RecordWriter uses compression settings
   * from the column family descriptor
   */
  @Test
  public void testColumnFamilyCompression() throws Exception {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;
    TaskAttemptContext context = null;
    Path dir =
        util.getDataTestDir("testColumnFamilyCompression");

    HTable table = Mockito.mock(HTable.class);

    Map<String, Compression.Algorithm> configuredCompression =
      new HashMap<String, Compression.Algorithm>();
    Compression.Algorithm[] supportedAlgos = getSupportedCompressionAlgorithms();

    int familyIndex = 0;
    for (byte[] family : FAMILIES) {
      configuredCompression.put(Bytes.toString(family),
                                supportedAlgos[familyIndex++ % supportedAlgos.length]);
    }
    setupMockColumnFamilies(table, configuredCompression);

    // set up the table to return some mock keys
    setupMockStartKeys(table);

    try {
      // partial map red setup to get an operational writer for testing
      // We turn off the sequence file compression, because DefaultCodec
      // pollutes the GZip codec pool with an incompatible compressor.
      conf.set("io.seqfile.compression.type", "NONE");
      Job job = new Job(conf, "testLocalMRIncrementalLoad");
      setupRandomGeneratorMapper(job);
      HFileOutputFormat.configureIncrementalLoad(job, table);
      FileOutputFormat.setOutputPath(job, dir);
      context = getTestTaskAttemptContext(job);
      HFileOutputFormat hof = new HFileOutputFormat();
      writer = hof.getRecordWriter(context);

      // write out random rows
      writeRandomKeyValues(writer, context, ROWSPERSPLIT);
      writer.close(context);

      // Make sure that a directory was created for every CF
      FileSystem fileSystem = dir.getFileSystem(conf);
      
      // commit so that the filesystem has one directory per column family
      hof.getOutputCommitter(context).commitTask(context);
      for (byte[] family : FAMILIES) {
        String familyStr = new String(family);
        boolean found = false;
        for (FileStatus f : fileSystem.listStatus(dir)) {

          if (Bytes.toString(family).equals(f.getPath().getName())) {
            // we found a matching directory
            found = true;

            // verify that the compression on this file matches the configured
            // compression
            Path dataFilePath = fileSystem.listStatus(f.getPath())[0].getPath();
            Reader reader = HFile.createReader(fileSystem, dataFilePath,
                new CacheConfig(conf));
            reader.loadFileInfo();
            assertEquals("Incorrect compression used for column family " + familyStr
                         + "(reader: " + reader + ")",
                         configuredCompression.get(familyStr), reader.getCompressionAlgorithm());
            break;
          }
        }

        if (!found) {
          fail("HFile for column family " + familyStr + " not found");
        }
      }

    } finally {
      dir.getFileSystem(conf).delete(dir, true);
    }
  }


  /**
   * @return
   */
  private Compression.Algorithm[] getSupportedCompressionAlgorithms() {
    String[] allAlgos = HFile.getSupportedCompressionAlgorithms();
    List<Compression.Algorithm> supportedAlgos = Lists.newArrayList();

    for (String algoName : allAlgos) {
      try {
        Compression.Algorithm algo = Compression.getCompressionAlgorithmByName(algoName);
        algo.getCompressor();
        supportedAlgos.add(algo);
      }catch (Exception e) {
        // this algo is not available
      }
    }

    return supportedAlgos.toArray(new Compression.Algorithm[0]);
  }


  /**
   * Write random values to the writer assuming a table created using
   * {@link #FAMILIES} as column family descriptors
   */
  private void writeRandomKeyValues(RecordWriter<ImmutableBytesWritable, KeyValue> writer, TaskAttemptContext context,
      int numRows)
      throws IOException, InterruptedException {
    byte keyBytes[] = new byte[Bytes.SIZEOF_INT];
    int valLength = 10;
    byte valBytes[] = new byte[valLength];

    int taskId = context.getTaskAttemptID().getTaskID().getId();
    assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";

    Random random = new Random();
    for (int i = 0; i < numRows; i++) {

      Bytes.putInt(keyBytes, 0, i);
      random.nextBytes(valBytes);
      ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);

      for (byte[] family : TestHFileOutputFormat.FAMILIES) {
        KeyValue kv = new KeyValue(keyBytes, family,
            PerformanceEvaluation.QUALIFIER_NAME, valBytes);
        writer.write(key, kv);
      }
    }
  }
  
  public static void main(String args[]) throws Exception {
    new TestHFileOutputFormat().manualTest(args);
  }
  
  public void manualTest(String args[]) throws Exception {
    Configuration conf = HBaseConfiguration.create();    
    util = new HBaseTestingUtility(conf);
    if ("newtable".equals(args[0])) {
      byte[] tname = args[1].getBytes();
      HTable table = util.createTable(tname, FAMILIES);
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.disableTable(tname);
      byte[][] startKeys = generateRandomStartKeys(5);
      util.createMultiRegions(conf, table, FAMILIES[0], startKeys);
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
