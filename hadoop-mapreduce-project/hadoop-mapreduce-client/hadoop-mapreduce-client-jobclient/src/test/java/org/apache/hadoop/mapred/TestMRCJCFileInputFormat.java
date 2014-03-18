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
package org.apache.hadoop.mapred;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;

@SuppressWarnings("deprecation")
public class TestMRCJCFileInputFormat extends TestCase {

  Configuration conf = new Configuration();
  MiniDFSCluster dfs = null;

  private MiniDFSCluster newDFSCluster(JobConf conf) throws Exception {
    return new MiniDFSCluster.Builder(conf).numDataNodes(4)
        .racks(new String[]{"/rack0", "/rack0", "/rack1", "/rack1"})
        .hosts(new String[]{"host0", "host1", "host2", "host3"})
        .build();
  }

  public void testLocality() throws Exception {
    JobConf job = new JobConf(conf);
    dfs = newDFSCluster(job);
    FileSystem fs = dfs.getFileSystem();
    System.out.println("FileSystem " + fs.getUri());

    Path inputDir = new Path("/foo/");
    String fileName = "part-0000";
    createInputs(fs, inputDir, fileName);

    // split it using a file input format
    TextInputFormat.addInputPath(job, inputDir);
    TextInputFormat inFormat = new TextInputFormat();
    inFormat.configure(job);
    InputSplit[] splits = inFormat.getSplits(job, 1);
    FileStatus fileStatus = fs.getFileStatus(new Path(inputDir, fileName));
    BlockLocation[] locations =
      fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    System.out.println("Made splits");

    // make sure that each split is a block and the locations match
    for(int i=0; i < splits.length; ++i) {
      FileSplit fileSplit = (FileSplit) splits[i];
      System.out.println("File split: " + fileSplit);
      for (String h: fileSplit.getLocations()) {
        System.out.println("Location: " + h);
      }
      System.out.println("Block: " + locations[i]);
      assertEquals(locations[i].getOffset(), fileSplit.getStart());
      assertEquals(locations[i].getLength(), fileSplit.getLength());
      String[] blockLocs = locations[i].getHosts();
      String[] splitLocs = fileSplit.getLocations();
      assertEquals(2, blockLocs.length);
      assertEquals(2, splitLocs.length);
      assertTrue((blockLocs[0].equals(splitLocs[0]) &&
                  blockLocs[1].equals(splitLocs[1])) ||
                 (blockLocs[1].equals(splitLocs[0]) &&
                  blockLocs[0].equals(splitLocs[1])));
    }

    assertEquals("Expected value of " + FileInputFormat.NUM_INPUT_FILES,
                 1, job.getLong(FileInputFormat.NUM_INPUT_FILES, 0));
  }

  private void createInputs(FileSystem fs, Path inDir, String fileName)
      throws IOException, TimeoutException, InterruptedException {
    // create a multi-block file on hdfs
    Path path = new Path(inDir, fileName);
    final short replication = 2;
    DataOutputStream out = fs.create(path, true, 4096,
                                     replication, 512, null);
    for(int i=0; i < 1000; ++i) {
      out.writeChars("Hello\n");
    }
    out.close();
    System.out.println("Wrote file");
    DFSTestUtil.waitReplication(fs, path, replication);
  }

  public void testNumInputs() throws Exception {
    JobConf job = new JobConf(conf);
    dfs = newDFSCluster(job);
    FileSystem fs = dfs.getFileSystem();
    System.out.println("FileSystem " + fs.getUri());

    Path inputDir = new Path("/foo/");
    final int numFiles = 10;
    String fileNameBase = "part-0000";
    for (int i=0; i < numFiles; ++i) {
      createInputs(fs, inputDir, fileNameBase + String.valueOf(i));
    }
    createInputs(fs, inputDir, "_meta");
    createInputs(fs, inputDir, "_temp");

    // split it using a file input format
    TextInputFormat.addInputPath(job, inputDir);
    TextInputFormat inFormat = new TextInputFormat();
    inFormat.configure(job);
    InputSplit[] splits = inFormat.getSplits(job, 1);

    assertEquals("Expected value of " + FileInputFormat.NUM_INPUT_FILES,
                 numFiles, job.getLong(FileInputFormat.NUM_INPUT_FILES, 0));
  }
  
  final Path root = new Path("/TestFileInputFormat");
  final Path file1 = new Path(root, "file1");
  final Path dir1 = new Path(root, "dir1");
  final Path file2 = new Path(dir1, "file2");

  static final int BLOCKSIZE = 1024;
  static final byte[] databuf = new byte[BLOCKSIZE];

  private static final String rack1[] = new String[] {
    "/r1"
  };
  private static final String hosts1[] = new String[] {
    "host1.rack1.com"
  };
  
  private class DummyFileInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return null;
    }
  }

  public void testMultiLevelInput() throws Exception {
    JobConf job = new JobConf(conf);

    job.setBoolean("dfs.replication.considerLoad", false);
    dfs = new MiniDFSCluster.Builder(job).racks(rack1).hosts(hosts1).build();
    dfs.waitActive();

    String namenode = (dfs.getFileSystem()).getUri().getHost() + ":" +
                      (dfs.getFileSystem()).getUri().getPort();

    FileSystem fileSys = dfs.getFileSystem();
    if (!fileSys.mkdirs(dir1)) {
      throw new IOException("Mkdirs failed to create " + root.toString());
    }
    writeFile(job, file1, (short)1, 1);
    writeFile(job, file2, (short)1, 1);

    // split it using a CombinedFile input format
    DummyFileInputFormat inFormat = new DummyFileInputFormat();
    inFormat.setInputPaths(job, root);

    // By default, we don't allow multi-level/recursive inputs
    boolean exceptionThrown = false;
    try {
      InputSplit[] splits = inFormat.getSplits(job, 1);
    } catch (Exception e) {
      exceptionThrown = true;
    }
    assertTrue("Exception should be thrown by default for scanning a "
        + "directory with directories inside.", exceptionThrown);

    // Enable multi-level/recursive inputs
    job.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true);
    InputSplit[] splits = inFormat.getSplits(job, 1);
    assertEquals(splits.length, 2);
  }

  @SuppressWarnings("rawtypes")
  public void testLastInputSplitAtSplitBoundary() throws Exception {
    FileInputFormat fif = new FileInputFormatForTest(1024l * 1024 * 1024,
        128l * 1024 * 1024);
    JobConf job = new JobConf();
    InputSplit[] splits = fif.getSplits(job, 8);
    assertEquals(8, splits.length);
    for (int i = 0; i < splits.length; i++) {
      InputSplit split = splits[i];
      assertEquals(("host" + i), split.getLocations()[0]);
    }
  }

  @SuppressWarnings("rawtypes")
  public void testLastInputSplitExceedingSplitBoundary() throws Exception {
    FileInputFormat fif = new FileInputFormatForTest(1027l * 1024 * 1024,
        128l * 1024 * 1024);
    JobConf job = new JobConf();
    InputSplit[] splits = fif.getSplits(job, 8);
    assertEquals(8, splits.length);
    for (int i = 0; i < splits.length; i++) {
      InputSplit split = splits[i];
      assertEquals(("host" + i), split.getLocations()[0]);
    }
  }

  @SuppressWarnings("rawtypes")
  public void testLastInputSplitSingleSplit() throws Exception {
    FileInputFormat fif = new FileInputFormatForTest(100l * 1024 * 1024,
        128l * 1024 * 1024);
    JobConf job = new JobConf();
    InputSplit[] splits = fif.getSplits(job, 1);
    assertEquals(1, splits.length);
    for (int i = 0; i < splits.length; i++) {
      InputSplit split = splits[i];
      assertEquals(("host" + i), split.getLocations()[0]);
    }
  }

  private class FileInputFormatForTest<K, V> extends FileInputFormat<K, V> {

    long splitSize;
    long length;

    FileInputFormatForTest(long length, long splitSize) {
      this.length = length;
      this.splitSize = splitSize;
    }

    @Override
    public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
        Reporter reporter) throws IOException {
      return null;
    }

    @Override
    protected FileStatus[] listStatus(JobConf job) throws IOException {
      FileStatus mockFileStatus = mock(FileStatus.class);
      when(mockFileStatus.getBlockSize()).thenReturn(splitSize);
      when(mockFileStatus.isDirectory()).thenReturn(false);
      Path mockPath = mock(Path.class);
      FileSystem mockFs = mock(FileSystem.class);

      BlockLocation[] blockLocations = mockBlockLocations(length, splitSize);
      when(mockFs.getFileBlockLocations(mockFileStatus, 0, length)).thenReturn(
          blockLocations);
      when(mockPath.getFileSystem(any(Configuration.class))).thenReturn(mockFs);

      when(mockFileStatus.getPath()).thenReturn(mockPath);
      when(mockFileStatus.getLen()).thenReturn(length);

      FileStatus[] fs = new FileStatus[1];
      fs[0] = mockFileStatus;
      return fs;
    }

    @Override
    protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
      return splitSize;
    }

    private BlockLocation[] mockBlockLocations(long size, long splitSize) {
      int numLocations = (int) (size / splitSize);
      if (size % splitSize != 0)
        numLocations++;
      BlockLocation[] blockLocations = new BlockLocation[numLocations];
      for (int i = 0; i < numLocations; i++) {
        String[] names = new String[] { "b" + i };
        String[] hosts = new String[] { "host" + i };
        blockLocations[i] = new BlockLocation(names, hosts, i * splitSize,
            Math.min(splitSize, size - (splitSize * i)));
      }
      return blockLocations;
    }
  }

  static void writeFile(Configuration conf, Path name,
      short replication, int numBlocks)
      throws IOException, TimeoutException, InterruptedException {
    FileSystem fileSys = FileSystem.get(conf);

    FSDataOutputStream stm = fileSys.create(name, true,
                                            conf.getInt("io.file.buffer.size", 4096),
                                            replication, (long)BLOCKSIZE);
    for (int i = 0; i < numBlocks; i++) {
      stm.write(databuf);
    }
    stm.close();
    DFSTestUtil.waitReplication(fileSys, name, replication);
  }

  @Override
  public void tearDown() throws Exception {
    if (dfs != null) {
      dfs.shutdown();
      dfs = null;
    }
  }
}
