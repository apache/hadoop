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
package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat.OneBlockInfo;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat.OneFileInfo;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import com.google.common.collect.HashMultiset;

public class TestCombineFileInputFormat extends TestCase {

  private static final String rack1[] = new String[] {
    "/r1"
  };
  private static final String hosts1[] = new String[] {
    "host1.rack1.com"
  };
  private static final String rack2[] = new String[] {
    "/r2"
  };
  private static final String hosts2[] = new String[] {
    "host2.rack2.com"
  };
  private static final String rack3[] = new String[] {
    "/r3"
  };
  private static final String hosts3[] = new String[] {
    "host3.rack3.com"
  };
  final Path inDir = new Path("/racktesting");
  final Path outputPath = new Path("/output");
  final Path dir1 = new Path(inDir, "/dir1");
  final Path dir2 = new Path(inDir, "/dir2");
  final Path dir3 = new Path(inDir, "/dir3");
  final Path dir4 = new Path(inDir, "/dir4");
  final Path dir5 = new Path(inDir, "/dir5");

  static final int BLOCKSIZE = 1024;
  static final byte[] databuf = new byte[BLOCKSIZE];

  private static final String DUMMY_FS_URI = "dummyfs:///";

  /** Dummy class to extend CombineFileInputFormat*/
  private class DummyInputFormat extends CombineFileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text,Text> createRecordReader(InputSplit split, 
        TaskAttemptContext context) throws IOException {
      return null;
    }
  }

  /** Dummy class to extend CombineFileInputFormat. It allows 
   * non-existent files to be passed into the CombineFileInputFormat, allows
   * for easy testing without having to create real files.
   */
  private class DummyInputFormat1 extends DummyInputFormat {
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
      Path[] files = getInputPaths(job);
      List<FileStatus> results = new ArrayList<FileStatus>();
      for (int i = 0; i < files.length; i++) {
        Path p = files[i];
        FileSystem fs = p.getFileSystem(job.getConfiguration());
        results.add(fs.getFileStatus(p));
      }
      return results;
    }
  }

  /** Dummy class to extend CombineFileInputFormat. It allows
   * testing with files having missing blocks without actually removing replicas.
   */
  public static class MissingBlockFileSystem extends DistributedFileSystem {
    String fileWithMissingBlocks;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
      fileWithMissingBlocks = "";
      super.initialize(name, conf);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(
        FileStatus stat, long start, long len) throws IOException {
      if (stat.isDir()) {
        return null;
      }
      System.out.println("File " + stat.getPath());
      String name = stat.getPath().toUri().getPath();
      BlockLocation[] locs =
        super.getFileBlockLocations(stat, start, len);
      if (name.equals(fileWithMissingBlocks)) {
        System.out.println("Returning missing blocks for " + fileWithMissingBlocks);
        locs[0] = new HdfsBlockLocation(new BlockLocation(new String[0],
            new String[0], locs[0].getOffset(), locs[0].getLength()), null);
      }
      return locs;
    }

    public void setFileWithMissingBlocks(String f) {
      fileWithMissingBlocks = f;
    }
  }

  private static final String DUMMY_KEY = "dummy.rr.key";

  private static class DummyRecordReader extends RecordReader<Text, Text> {
    private TaskAttemptContext context;
    private CombineFileSplit s;
    private int idx;
    private boolean used;

    public DummyRecordReader(CombineFileSplit split, TaskAttemptContext context,
        Integer i) {
      this.context = context;
      this.idx = i;
      this.s = split;
      this.used = true;
    }

    /** @return a value specified in the context to check whether the
     * context is properly updated by the initialize() method.
     */
    public String getDummyConfVal() {
      return this.context.getConfiguration().get(DUMMY_KEY);
    }

    public void initialize(InputSplit split, TaskAttemptContext context) {
      this.context = context;
      this.s = (CombineFileSplit) split;

      // By setting used to true in the c'tor, but false in initialize,
      // we can check that initialize() is always called before use
      // (e.g., in testReinit()).
      this.used = false;
    }

    public boolean nextKeyValue() {
      boolean ret = !used;
      this.used = true;
      return ret;
    }

    public Text getCurrentKey() {
      return new Text(this.context.getConfiguration().get(DUMMY_KEY));
    }

    public Text getCurrentValue() {
      return new Text(this.s.getPath(idx).toString());
    }

    public float getProgress() {
      return used ? 1.0f : 0.0f;
    }

    public void close() {
    }
  }

  /** Extend CFIF to use CFRR with DummyRecordReader */
  private class ChildRRInputFormat extends CombineFileInputFormat<Text, Text> {
    @SuppressWarnings("unchecked")
    @Override
    public RecordReader<Text,Text> createRecordReader(InputSplit split, 
        TaskAttemptContext context) throws IOException {
      return new CombineFileRecordReader((CombineFileSplit) split, context,
          (Class) DummyRecordReader.class);
    }
  }

  public void testRecordReaderInit() throws InterruptedException, IOException {
    // Test that we properly initialize the child recordreader when
    // CombineFileInputFormat and CombineFileRecordReader are used.

    TaskAttemptID taskId = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 0);
    Configuration conf1 = new Configuration();
    conf1.set(DUMMY_KEY, "STATE1");
    TaskAttemptContext context1 = new TaskAttemptContextImpl(conf1, taskId);

    // This will create a CombineFileRecordReader that itself contains a
    // DummyRecordReader.
    InputFormat inputFormat = new ChildRRInputFormat();

    Path [] files = { new Path("file1") };
    long [] lengths = { 1 };

    CombineFileSplit split = new CombineFileSplit(files, lengths);

    RecordReader rr = inputFormat.createRecordReader(split, context1);
    assertTrue("Unexpected RR type!", rr instanceof CombineFileRecordReader);

    // Verify that the initial configuration is the one being used.
    // Right after construction the dummy key should have value "STATE1"
    assertEquals("Invalid initial dummy key value", "STATE1",
      rr.getCurrentKey().toString());

    // Switch the active context for the RecordReader...
    Configuration conf2 = new Configuration();
    conf2.set(DUMMY_KEY, "STATE2");
    TaskAttemptContext context2 = new TaskAttemptContextImpl(conf2, taskId);
    rr.initialize(split, context2);

    // And verify that the new context is updated into the child record reader.
    assertEquals("Invalid secondary dummy key value", "STATE2",
      rr.getCurrentKey().toString());
  }

  public void testReinit() throws Exception {
    // Test that a split containing multiple files works correctly,
    // with the child RecordReader getting its initialize() method
    // called a second time.
    TaskAttemptID taskId = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 0);
    Configuration conf = new Configuration();
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskId);

    // This will create a CombineFileRecordReader that itself contains a
    // DummyRecordReader.
    InputFormat inputFormat = new ChildRRInputFormat();

    Path [] files = { new Path("file1"), new Path("file2") };
    long [] lengths = { 1, 1 };

    CombineFileSplit split = new CombineFileSplit(files, lengths);
    RecordReader rr = inputFormat.createRecordReader(split, context);
    assertTrue("Unexpected RR type!", rr instanceof CombineFileRecordReader);

    // first initialize() call comes from MapTask. We'll do it here.
    rr.initialize(split, context);

    // First value is first filename.
    assertTrue(rr.nextKeyValue());
    assertEquals("file1", rr.getCurrentValue().toString());

    // The inner RR will return false, because it only emits one (k, v) pair.
    // But there's another sub-split to process. This returns true to us.
    assertTrue(rr.nextKeyValue());
    
    // And the 2nd rr will have its initialize method called correctly.
    assertEquals("file2", rr.getCurrentValue().toString());
    
    // But after both child RR's have returned their singleton (k, v), this
    // should also return false.
    assertFalse(rr.nextKeyValue());
  }

  public void testSplitPlacement() throws Exception {
    MiniDFSCluster dfs = null;
    FileSystem fileSys = null;
    try {
      /* Start 3 datanodes, one each in rack r1, r2, r3. Create five files
       * 1) file1 and file5, just after starting the datanode on r1, with 
       *    a repl factor of 1, and,
       * 2) file2, just after starting the datanode on r2, with 
       *    a repl factor of 2, and,
       * 3) file3, file4 after starting the all three datanodes, with a repl 
       *    factor of 3.
       * At the end, file1, file5 will be present on only datanode1, file2 will 
       * be present on datanode 1 and datanode2 and 
       * file3, file4 will be present on all datanodes. 
       */
      Configuration conf = new Configuration();
      conf.setBoolean("dfs.replication.considerLoad", false);
      dfs = new MiniDFSCluster(conf, 1, true, rack1, hosts1);
      dfs.waitActive();

      fileSys = dfs.getFileSystem();
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }
      Path file1 = new Path(dir1 + "/file1");
      writeFile(conf, file1, (short)1, 1);
      // create another file on the same datanode
      Path file5 = new Path(dir5 + "/file5");
      writeFile(conf, file5, (short)1, 1);
      // split it using a CombinedFile input format
      DummyInputFormat inFormat = new DummyInputFormat();
      Job job = Job.getInstance(conf);
      FileInputFormat.setInputPaths(job, dir1 + "," + dir5);
      List<InputSplit> splits = inFormat.getSplits(job);
      System.out.println("Made splits(Test0): " + splits.size());
      for (InputSplit split : splits) {
        System.out.println("File split(Test0): " + split);
      }
      assertEquals(1, splits.size());
      CombineFileSplit fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file5.getName(), fileSplit.getPath(1).getName());
      assertEquals(0, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]);
      
      dfs.startDataNodes(conf, 1, true, null, rack2, hosts2, null);
      dfs.waitActive();

      // create file on two datanodes.
      Path file2 = new Path(dir2 + "/file2");
      writeFile(conf, file2, (short)2, 2);

      // split it using a CombinedFile input format
      inFormat = new DummyInputFormat();
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2);
      inFormat.setMinSplitSizeRack(BLOCKSIZE);
      splits = inFormat.getSplits(job);
      System.out.println("Made splits(Test1): " + splits.size());

      // make sure that each split has different locations
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }
      assertEquals(2, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file2.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1

      // create another file on 3 datanodes and 3 racks.
      dfs.startDataNodes(conf, 1, true, null, rack3, hosts3, null);
      dfs.waitActive();
      Path file3 = new Path(dir3 + "/file3");
      writeFile(conf, new Path(dir3 + "/file3"), (short)3, 3);
      inFormat = new DummyInputFormat();
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2 + "," + dir3);
      inFormat.setMinSplitSizeRack(BLOCKSIZE);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test2): " + split);
      }
      assertEquals(3, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(3, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file3.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(file3.getName(), fileSplit.getPath(2).getName());
      assertEquals(2 * BLOCKSIZE, fileSplit.getOffset(2));
      assertEquals(BLOCKSIZE, fileSplit.getLength(2));
      assertEquals(hosts3[0], fileSplit.getLocations()[0]); // should be on r3
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file2.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1

      // create file4 on all three racks
      Path file4 = new Path(dir4 + "/file4");
      writeFile(conf, file4, (short)3, 3);
      inFormat = new DummyInputFormat();
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      inFormat.setMinSplitSizeRack(BLOCKSIZE);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test3): " + split);
      }
      assertEquals(3, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(6, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file3.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(file3.getName(), fileSplit.getPath(2).getName());
      assertEquals(2 * BLOCKSIZE, fileSplit.getOffset(2));
      assertEquals(BLOCKSIZE, fileSplit.getLength(2));
      assertEquals(hosts3[0], fileSplit.getLocations()[0]); // should be on r3
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file2.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1

      // maximum split size is 2 blocks 
      inFormat = new DummyInputFormat();
      inFormat.setMinSplitSizeNode(BLOCKSIZE);
      inFormat.setMaxSplitSize(2*BLOCKSIZE);
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test4): " + split);
      }
      assertEquals(5, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file3.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals("host3.rack3.com", fileSplit.getLocations()[0]);
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file2.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals("host2.rack2.com", fileSplit.getLocations()[0]);
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file3.getName(), fileSplit.getPath(1).getName());
      assertEquals(2 * BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals("host1.rack1.com", fileSplit.getLocations()[0]);

      // maximum split size is 3 blocks 
      inFormat = new DummyInputFormat();
      inFormat.setMinSplitSizeNode(BLOCKSIZE);
      inFormat.setMaxSplitSize(3*BLOCKSIZE);
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test5): " + split);
      }
      assertEquals(3, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(3, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file3.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(file3.getName(), fileSplit.getPath(2).getName());
      assertEquals(2 * BLOCKSIZE, fileSplit.getOffset(2));
      assertEquals(BLOCKSIZE, fileSplit.getLength(2));
      assertEquals("host3.rack3.com", fileSplit.getLocations()[0]);
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file2.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(file4.getName(), fileSplit.getPath(2).getName());
      assertEquals(0, fileSplit.getOffset(2));
      assertEquals(BLOCKSIZE, fileSplit.getLength(2));
      assertEquals("host2.rack2.com", fileSplit.getLocations()[0]);
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(3, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file4.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(file4.getName(), fileSplit.getPath(2).getName());
      assertEquals(2*BLOCKSIZE, fileSplit.getOffset(2));
      assertEquals(BLOCKSIZE, fileSplit.getLength(2));
      assertEquals("host1.rack1.com", fileSplit.getLocations()[0]);

      // maximum split size is 4 blocks 
      inFormat = new DummyInputFormat();
      inFormat.setMaxSplitSize(4*BLOCKSIZE);
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test6): " + split);
      }
      assertEquals(3, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(4, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file3.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(file3.getName(), fileSplit.getPath(2).getName());
      assertEquals(2 * BLOCKSIZE, fileSplit.getOffset(2));
      assertEquals(BLOCKSIZE, fileSplit.getLength(2));
      assertEquals("host3.rack3.com", fileSplit.getLocations()[0]);
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(4, fileSplit.getNumPaths());
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file2.getName(), fileSplit.getPath(1).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(file4.getName(), fileSplit.getPath(2).getName());
      assertEquals(BLOCKSIZE, fileSplit.getOffset(2));
      assertEquals(BLOCKSIZE, fileSplit.getLength(2));
      assertEquals(file4.getName(), fileSplit.getPath(3).getName());
      assertEquals( 2 * BLOCKSIZE, fileSplit.getOffset(3));
      assertEquals(BLOCKSIZE, fileSplit.getLength(3));
      assertEquals("host2.rack2.com", fileSplit.getLocations()[0]);
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1

      // maximum split size is 7 blocks and min is 3 blocks
      inFormat = new DummyInputFormat();
      inFormat.setMaxSplitSize(7*BLOCKSIZE);
      inFormat.setMinSplitSizeNode(3*BLOCKSIZE);
      inFormat.setMinSplitSizeRack(3*BLOCKSIZE);
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test7): " + split);
      }
      assertEquals(2, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(6, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals("host3.rack3.com", fileSplit.getLocations()[0]);
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(3, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals("host1.rack1.com", fileSplit.getLocations()[0]);

      // Rack 1 has file1, file2 and file3 and file4
      // Rack 2 has file2 and file3 and file4
      // Rack 3 has file3 and file4
      // setup a filter so that only file1 and file2 can be combined
      inFormat = new DummyInputFormat();
      FileInputFormat.addInputPath(job, inDir);
      inFormat.setMinSplitSizeRack(1); // everything is at least rack local
      inFormat.createPool(new TestFilter(dir1), 
                          new TestFilter(dir2));
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }
      assertEquals(3, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(6, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(hosts3[0], fileSplit.getLocations()[0]); // should be on r3

      // measure performance when there are multiple pools and
      // many files in each pool.
      int numPools = 100;
      int numFiles = 1000;
      DummyInputFormat1 inFormat1 = new DummyInputFormat1();
      for (int i = 0; i < numFiles; i++) {
        FileInputFormat.setInputPaths(job, file1);
      }
      inFormat1.setMinSplitSizeRack(1); // everything is at least rack local
      final Path dirNoMatch1 = new Path(inDir, "/dirxx");
      final Path dirNoMatch2 = new Path(inDir, "/diryy");
      for (int i = 0; i < numPools; i++) {
        inFormat1.createPool(new TestFilter(dirNoMatch1), 
                            new TestFilter(dirNoMatch2));
      }
      long start = System.currentTimeMillis();
      splits = inFormat1.getSplits(job);
      long end = System.currentTimeMillis();
      System.out.println("Elapsed time for " + numPools + " pools " +
                         " and " + numFiles + " files is " + 
                         ((end - start)/1000) + " seconds.");

      // This file has three whole blocks. If the maxsplit size is
      // half the block size, then there should be six splits.
      inFormat = new DummyInputFormat();
      inFormat.setMaxSplitSize(BLOCKSIZE/2);
      FileInputFormat.setInputPaths(job, dir3);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test8): " + split);
      }
      assertEquals(splits.size(), 6);

    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }

  static void writeFile(Configuration conf, Path name,
                        short replication, int numBlocks)
      throws IOException, TimeoutException, InterruptedException {
    FileSystem fileSys = FileSystem.get(conf);

    FSDataOutputStream stm = fileSys.create(name, true,
                                            conf.getInt("io.file.buffer.size", 4096),
                                            replication, (long)BLOCKSIZE);
    writeDataAndSetReplication(fileSys, name, stm, replication, numBlocks);
  }

  // Creates the gzip file and return the FileStatus
  static FileStatus writeGzipFile(Configuration conf, Path name,
      short replication, int numBlocks)
      throws IOException, TimeoutException, InterruptedException {
    FileSystem fileSys = FileSystem.get(conf);

    GZIPOutputStream out = new GZIPOutputStream(fileSys.create(name, true, conf
        .getInt("io.file.buffer.size", 4096), replication, (long) BLOCKSIZE));
    writeDataAndSetReplication(fileSys, name, out, replication, numBlocks);
    return fileSys.getFileStatus(name);
  }

  private static void writeDataAndSetReplication(FileSystem fileSys, Path name,
        OutputStream out, short replication, int numBlocks)
      throws IOException, TimeoutException, InterruptedException {
    for (int i = 0; i < numBlocks; i++) {
      out.write(databuf);
    }
    out.close();
    DFSTestUtil.waitReplication(fileSys, name, replication);
  }
  
  public void testNodeInputSplit() throws IOException, InterruptedException {
    // Regression test for MAPREDUCE-4892. There are 2 nodes with all blocks on 
    // both nodes. The grouping ensures that both nodes get splits instead of 
    // just the first node
    DummyInputFormat inFormat = new DummyInputFormat();
    int numBlocks = 12;
    long totLength = 0;
    long blockSize = 100;
    long maxSize = 200;
    long minSizeNode = 50;
    long minSizeRack = 50;
    String[] locations = { "h1", "h2" };
    String[] racks = new String[0];
    Path path = new Path("hdfs://file");
    
    OneBlockInfo[] blocks = new OneBlockInfo[numBlocks];
    for(int i=0; i<numBlocks; ++i) {
      blocks[i] = new OneBlockInfo(path, i*blockSize, blockSize, locations, racks);
      totLength += blockSize;
    }
    
    List<InputSplit> splits = new ArrayList<InputSplit>();
    HashMap<String, Set<String>> rackToNodes = 
                              new HashMap<String, Set<String>>();
    HashMap<String, List<OneBlockInfo>> rackToBlocks = 
                              new HashMap<String, List<OneBlockInfo>>();
    HashMap<OneBlockInfo, String[]> blockToNodes = 
                              new HashMap<OneBlockInfo, String[]>();
    HashMap<String, List<OneBlockInfo>> nodeToBlocks = 
                              new HashMap<String, List<OneBlockInfo>>();
    
    OneFileInfo.populateBlockInfo(blocks, rackToBlocks, blockToNodes, 
                             nodeToBlocks, rackToNodes);
    
    inFormat.createSplits(nodeToBlocks, blockToNodes, rackToBlocks, totLength,  
                          maxSize, minSizeNode, minSizeRack, splits);
    
    int expectedSplitCount = (int)(totLength/maxSize);
    Assert.assertEquals(expectedSplitCount, splits.size());
    HashMultiset<String> nodeSplits = HashMultiset.create();
    for(int i=0; i<expectedSplitCount; ++i) {
      InputSplit inSplit = splits.get(i);
      Assert.assertEquals(maxSize, inSplit.getLength());
      Assert.assertEquals(1, inSplit.getLocations().length);
      nodeSplits.add(inSplit.getLocations()[0]);
    }
    Assert.assertEquals(3, nodeSplits.count(locations[0]));
    Assert.assertEquals(3, nodeSplits.count(locations[1]));
  }
  
  public void testSplitPlacementForCompressedFiles() throws Exception {
    MiniDFSCluster dfs = null;
    FileSystem fileSys = null;
    try {
      /* Start 3 datanodes, one each in rack r1, r2, r3. Create five gzipped
       *  files
       * 1) file1 and file5, just after starting the datanode on r1, with 
       *    a repl factor of 1, and,
       * 2) file2, just after starting the datanode on r2, with 
       *    a repl factor of 2, and,
       * 3) file3, file4 after starting the all three datanodes, with a repl 
       *    factor of 3.
       * At the end, file1, file5 will be present on only datanode1, file2 will 
       * be present on datanode 1 and datanode2 and 
       * file3, file4 will be present on all datanodes. 
       */
      Configuration conf = new Configuration();
      conf.setBoolean("dfs.replication.considerLoad", false);
      dfs = new MiniDFSCluster(conf, 1, true, rack1, hosts1);
      dfs.waitActive();

      fileSys = dfs.getFileSystem();
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }
      Path file1 = new Path(dir1 + "/file1.gz");
      FileStatus f1 = writeGzipFile(conf, file1, (short)1, 1);
      // create another file on the same datanode
      Path file5 = new Path(dir5 + "/file5.gz");
      FileStatus f5 = writeGzipFile(conf, file5, (short)1, 1);
      // split it using a CombinedFile input format
      DummyInputFormat inFormat = new DummyInputFormat();
      Job job = Job.getInstance(conf);
      FileInputFormat.setInputPaths(job, dir1 + "," + dir5);
      List<InputSplit> splits = inFormat.getSplits(job);
      System.out.println("Made splits(Test0): " + splits.size());
      for (InputSplit split : splits) {
        System.out.println("File split(Test0): " + split);
      }
      assertEquals(1, splits.size());
      CombineFileSplit fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f1.getLen(), fileSplit.getLength(0));
      assertEquals(file5.getName(), fileSplit.getPath(1).getName());
      assertEquals(0, fileSplit.getOffset(1));
      assertEquals(f5.getLen(), fileSplit.getLength(1));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]);
      
      dfs.startDataNodes(conf, 1, true, null, rack2, hosts2, null);
      dfs.waitActive();

      // create file on two datanodes.
      Path file2 = new Path(dir2 + "/file2.gz");
      FileStatus f2 = writeGzipFile(conf, file2, (short)2, 2);

      // split it using a CombinedFile input format
      inFormat = new DummyInputFormat();
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2);
      inFormat.setMinSplitSizeRack(f1.getLen());
      splits = inFormat.getSplits(job);
      System.out.println("Made splits(Test1): " + splits.size());

      // make sure that each split has different locations
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }
      assertEquals(2, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f2.getLen(), fileSplit.getLength(0));
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f1.getLen(), fileSplit.getLength(0));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1

      // create another file on 3 datanodes and 3 racks.
      dfs.startDataNodes(conf, 1, true, null, rack3, hosts3, null);
      dfs.waitActive();
      Path file3 = new Path(dir3 + "/file3.gz");
      FileStatus f3 = writeGzipFile(conf, file3, (short)3, 3);
      inFormat = new DummyInputFormat();
      FileInputFormat.setInputPaths(job, dir1 + "," + dir2 + "," + dir3);
      inFormat.setMinSplitSizeRack(f1.getLen());
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test2): " + split);
      }
      assertEquals(3, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f3.getLen(), fileSplit.getLength(0));
      assertEquals(hosts3[0], fileSplit.getLocations()[0]); // should be on r3
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f2.getLen(), fileSplit.getLength(0));
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f1.getLen(), fileSplit.getLength(0));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1

      // create file4 on all three racks
      Path file4 = new Path(dir4 + "/file4.gz");
      FileStatus f4 = writeGzipFile(conf, file4, (short)3, 3);
      inFormat = new DummyInputFormat();
      FileInputFormat.setInputPaths(job,
          dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      inFormat.setMinSplitSizeRack(f1.getLen());
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test3): " + split);
      }
      assertEquals(3, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f3.getLen(), fileSplit.getLength(0));
      assertEquals(file4.getName(), fileSplit.getPath(1).getName());
      assertEquals(0, fileSplit.getOffset(1));
      assertEquals(f4.getLen(), fileSplit.getLength(1));
      assertEquals(hosts3[0], fileSplit.getLocations()[0]); // should be on r3
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f2.getLen(), fileSplit.getLength(0));
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f1.getLen(), fileSplit.getLength(0));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1

      // maximum split size is file1's length
      inFormat = new DummyInputFormat();
      inFormat.setMinSplitSizeNode(f1.getLen());
      inFormat.setMaxSplitSize(f1.getLen());
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test4): " + split);
      }
      assertEquals(4, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f3.getLen(), fileSplit.getLength(0));
      assertEquals(hosts3[0], fileSplit.getLocations()[0]); // should be on r3
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f2.getLen(), fileSplit.getLength(0));
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r3
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f1.getLen(), fileSplit.getLength(0));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(3);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file4.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f4.getLen(), fileSplit.getLength(0));
      assertEquals(hosts3[0], fileSplit.getLocations()[0]); // should be on r1

      // maximum split size is twice file1's length
      inFormat = new DummyInputFormat();
      inFormat.setMinSplitSizeNode(f1.getLen());
      inFormat.setMaxSplitSize(2 * f1.getLen());
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test5): " + split);
      }
      assertEquals(3, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f3.getLen(), fileSplit.getLength(0));
      assertEquals(file4.getName(), fileSplit.getPath(1).getName());
      assertEquals(0, fileSplit.getOffset(1));
      assertEquals(f4.getLen(), fileSplit.getLength(1));
      assertEquals(hosts3[0], fileSplit.getLocations()[0]);
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file2.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f2.getLen(), fileSplit.getLength(0));
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f1.getLen(), fileSplit.getLength(0));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1

      // maximum split size is 4 times file1's length 
      inFormat = new DummyInputFormat();
      inFormat.setMinSplitSizeNode(2 * f1.getLen());
      inFormat.setMaxSplitSize(4 * f1.getLen());
      FileInputFormat.setInputPaths(job,
          dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test6): " + split);
      }
      assertEquals(2, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file3.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f3.getLen(), fileSplit.getLength(0));
      assertEquals(file4.getName(), fileSplit.getPath(1).getName());
      assertEquals(0, fileSplit.getOffset(1));
      assertEquals(f4.getLen(), fileSplit.getLength(1));
      assertEquals(hosts3[0], fileSplit.getLocations()[0]);
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(f1.getLen(), fileSplit.getLength(0));
      assertEquals(file2.getName(), fileSplit.getPath(1).getName());
      assertEquals(0, fileSplit.getOffset(1), BLOCKSIZE);
      assertEquals(f2.getLen(), fileSplit.getLength(1));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1

      // maximum split size and min-split-size per rack is 4 times file1's length
      inFormat = new DummyInputFormat();
      inFormat.setMaxSplitSize(4 * f1.getLen());
      inFormat.setMinSplitSizeRack(4 * f1.getLen());
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test7): " + split);
      }
      assertEquals(1, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(4, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(hosts1[0], fileSplit.getLocations()[0]);

      // minimum split size per node is 4 times file1's length
      inFormat = new DummyInputFormat();
      inFormat.setMinSplitSizeNode(4 * f1.getLen());
      FileInputFormat.setInputPaths(job, 
        dir1 + "," + dir2 + "," + dir3 + "," + dir4);
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test8): " + split);
      }
      assertEquals(1, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(4, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(hosts1[0], fileSplit.getLocations()[0]);

      // Rack 1 has file1, file2 and file3 and file4
      // Rack 2 has file2 and file3 and file4
      // Rack 3 has file3 and file4
      // setup a filter so that only file1 and file2 can be combined
      inFormat = new DummyInputFormat();
      FileInputFormat.addInputPath(job, inDir);
      inFormat.setMinSplitSizeRack(1); // everything is at least rack local
      inFormat.createPool(new TestFilter(dir1), 
                          new TestFilter(dir2));
      splits = inFormat.getSplits(job);
      for (InputSplit split : splits) {
        System.out.println("File split(Test9): " + split);
      }
      assertEquals(3, splits.size());
      fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(hosts2[0], fileSplit.getLocations()[0]); // should be on r2
      fileSplit = (CombineFileSplit) splits.get(1);
      assertEquals(1, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(hosts1[0], fileSplit.getLocations()[0]); // should be on r1
      fileSplit = (CombineFileSplit) splits.get(2);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(hosts3[0], fileSplit.getLocations()[0]); // should be on r3

      // measure performance when there are multiple pools and
      // many files in each pool.
      int numPools = 100;
      int numFiles = 1000;
      DummyInputFormat1 inFormat1 = new DummyInputFormat1();
      for (int i = 0; i < numFiles; i++) {
        FileInputFormat.setInputPaths(job, file1);
      }
      inFormat1.setMinSplitSizeRack(1); // everything is at least rack local
      final Path dirNoMatch1 = new Path(inDir, "/dirxx");
      final Path dirNoMatch2 = new Path(inDir, "/diryy");
      for (int i = 0; i < numPools; i++) {
        inFormat1.createPool(new TestFilter(dirNoMatch1), 
                            new TestFilter(dirNoMatch2));
      }
      long start = System.currentTimeMillis();
      splits = inFormat1.getSplits(job);
      long end = System.currentTimeMillis();
      System.out.println("Elapsed time for " + numPools + " pools " +
                         " and " + numFiles + " files is " + 
                         ((end - start)) + " milli seconds.");
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }

  /**
   * Test that CFIF can handle missing blocks.
   */
  public void testMissingBlocks() throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    FileSystem fileSys = null;
    String testName = "testMissingBlocks";
    try {
      Configuration conf = new Configuration();
      conf.set("fs.hdfs.impl", MissingBlockFileSystem.class.getName());
      conf.setBoolean("dfs.replication.considerLoad", false);
      dfs = new MiniDFSCluster(conf, 1, true, rack1, hosts1);
      dfs.waitActive();

      namenode = (dfs.getFileSystem()).getUri().getHost() + ":" +
                 (dfs.getFileSystem()).getUri().getPort();

      fileSys = dfs.getFileSystem();
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }

      Path file1 = new Path(dir1 + "/file1");
      writeFile(conf, file1, (short)1, 1);
      // create another file on the same datanode
      Path file5 = new Path(dir5 + "/file5");
      writeFile(conf, file5, (short)1, 1);

      ((MissingBlockFileSystem)fileSys).setFileWithMissingBlocks(file1.toUri().getPath());
      // split it using a CombinedFile input format
      DummyInputFormat inFormat = new DummyInputFormat();
      Job job = Job.getInstance(conf);
      FileInputFormat.setInputPaths(job, dir1 + "," + dir5);
      List<InputSplit> splits = inFormat.getSplits(job);
      System.out.println("Made splits(Test0): " + splits.size());
      for (InputSplit split : splits) {
        System.out.println("File split(Test0): " + split);
      }
      assertEquals(splits.size(), 1);
      CombineFileSplit fileSplit = (CombineFileSplit) splits.get(0);
      assertEquals(2, fileSplit.getNumPaths());
      assertEquals(1, fileSplit.getLocations().length);
      assertEquals(file1.getName(), fileSplit.getPath(0).getName());
      assertEquals(0, fileSplit.getOffset(0));
      assertEquals(BLOCKSIZE, fileSplit.getLength(0));
      assertEquals(file5.getName(), fileSplit.getPath(1).getName());
      assertEquals(0, fileSplit.getOffset(1));
      assertEquals(BLOCKSIZE, fileSplit.getLength(1));
      assertEquals(hosts1[0], fileSplit.getLocations()[0]);

    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }
  
  /**
   * Test when the input file's length is 0.
   */
  @Test
  public void testForEmptyFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fileSys = FileSystem.get(conf);
    Path file = new Path("test" + "/file");
    FSDataOutputStream out = fileSys.create(file, true,
        conf.getInt("io.file.buffer.size", 4096), (short) 1, (long) BLOCKSIZE);
    out.write(new byte[0]);
    out.close();

    // split it using a CombinedFile input format
    DummyInputFormat inFormat = new DummyInputFormat();
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, "test");
    List<InputSplit> splits = inFormat.getSplits(job);
    assertEquals(1, splits.size());
    CombineFileSplit fileSplit = (CombineFileSplit) splits.get(0);
    assertEquals(1, fileSplit.getNumPaths());
    assertEquals(file.getName(), fileSplit.getPath(0).getName());
    assertEquals(0, fileSplit.getOffset(0));
    assertEquals(0, fileSplit.getLength(0));

    fileSys.delete(file.getParent(), true);
  }

  /**
   * Test when input files are from non-default file systems
   */
  @Test
  public void testForNonDefaultFileSystem() throws Throwable {
    Configuration conf = new Configuration();

    // use a fake file system scheme as default
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, DUMMY_FS_URI);

    // default fs path
    assertEquals(DUMMY_FS_URI, FileSystem.getDefaultUri(conf).toString());
    // add a local file
    Path localPath = new Path("testFile1");
    FileSystem lfs = FileSystem.getLocal(conf);
    FSDataOutputStream dos = lfs.create(localPath);
    dos.writeChars("Local file for CFIF");
    dos.close();

    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, lfs.makeQualified(localPath));
    DummyInputFormat inFormat = new DummyInputFormat();
    List<InputSplit> splits = inFormat.getSplits(job);
    assertTrue(splits.size() > 0);
    for (InputSplit s : splits) {
      CombineFileSplit cfs = (CombineFileSplit)s;
      for (Path p : cfs.getPaths()) {
        assertEquals(p.toUri().getScheme(), "file");
      }
    }
  }

  static class TestFilter implements PathFilter {
    private Path p;

    // store a path prefix in this TestFilter
    public TestFilter(Path p) {
      this.p = p;
    }

    // returns true if the specified path matches the prefix stored
    // in this TestFilter.
    public boolean accept(Path path) {
      if (path.toUri().getPath().indexOf(p.toString()) == 0) {
        return true;
      }
      return false;
    }

    public String toString() {
      return "PathFilter:" + p;
    }
  }

  /*
   * Prints out the input splits for the specified files
   */
  private void splitRealFiles(String[] args) throws IOException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance();
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException("Wrong file system: " + fs.getClass().getName());
    }
    long blockSize = fs.getDefaultBlockSize();

    DummyInputFormat inFormat = new DummyInputFormat();
    for (int i = 0; i < args.length; i++) {
      FileInputFormat.addInputPaths(job, args[i]);
    }
    inFormat.setMinSplitSizeRack(blockSize);
    inFormat.setMaxSplitSize(10 * blockSize);

    List<InputSplit> splits = inFormat.getSplits(job);
    System.out.println("Total number of splits " + splits.size());
    for (int i = 0; i < splits.size(); ++i) {
      CombineFileSplit fileSplit = (CombineFileSplit) splits.get(i);
      System.out.println("Split[" + i + "] " + fileSplit);
    }
  }

  public static void main(String[] args) throws Exception{

    // if there are some parameters specified, then use those paths
    if (args.length != 0) {
      TestCombineFileInputFormat test = new TestCombineFileInputFormat();
      test.splitRealFiles(args);
    } else {
      TestCombineFileInputFormat test = new TestCombineFileInputFormat();
      test.testSplitPlacement();
    }
  }
}
