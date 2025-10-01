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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestFileInputFormat {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFileInputFormat.class);
  
  private static String testTmpDir = System.getProperty("test.build.data", "/tmp");
  private static final Path TEST_ROOT_DIR = new Path(testTmpDir, "TestFIF");
  
  private static FileSystem localFs;
  
  private int numThreads;
  
  public void initTestFileInputFormat(int pNumThreads) {
    this.numThreads = pNumThreads;
    LOG.info("Running with numThreads: " + pNumThreads);
  }

  public static Collection<Object[]> data() {
    Object[][] data = new Object[][]{{1}, {5}};
    return Arrays.asList(data);
  }
  
  @BeforeEach
  public void setup() throws IOException {
    LOG.info("Using Test Dir: " + TEST_ROOT_DIR);
    localFs = FileSystem.getLocal(new Configuration());
    localFs.delete(TEST_ROOT_DIR, true);
    localFs.mkdirs(TEST_ROOT_DIR);
  }
  
  @AfterEach
  public void cleanup() throws IOException {
    localFs.delete(TEST_ROOT_DIR, true);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testNumInputFilesRecursively(int pNumThreads) throws Exception {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = getConfiguration();
    conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    assertEquals(3, splits.size(), "Input splits are not correct");
    verifySplits(Lists.newArrayList("test:/a1/a2/file2", "test:/a1/a2/file3",
        "test:/a1/file1"), splits);

    // Using the deprecated configuration
    conf = getConfiguration();
    conf.set("mapred.input.dir.recursive", "true");
    job = Job.getInstance(conf);
    splits = fileInputFormat.getSplits(job);
    verifySplits(Lists.newArrayList("test:/a1/a2/file2", "test:/a1/a2/file3",
        "test:/a1/file1"), splits);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testNumInputFilesWithoutRecursively(int pNumThreads) throws Exception {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = getConfiguration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    assertEquals(2, splits.size(), "Input splits are not correct");
    verifySplits(Lists.newArrayList("test:/a1/a2", "test:/a1/file1"), splits);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testNumInputFilesIgnoreDirs(int pNumThreads) throws Exception {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = getConfiguration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    conf.setBoolean(FileInputFormat.INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, true);
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    assertEquals(1, splits.size(), "Input splits are not correct");
    verifySplits(Lists.newArrayList("test:/a1/file1"), splits);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testListLocatedStatus(int pNumThreads) throws Exception {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = getConfiguration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    conf.setBoolean("fs.test.impl.disable.cache", false);
    conf.set(FileInputFormat.INPUT_DIR, "test:///a1/a2");
    MockFileSystem mockFs =
        (MockFileSystem) new Path("test:///").getFileSystem(conf);
    assertEquals(0, mockFs.numListLocatedStatusCalls,
        "listLocatedStatus already called");
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    assertEquals(2, splits.size(), "Input splits are not correct");
    assertEquals(1, mockFs.numListLocatedStatusCalls,
        "listLocatedStatus calls");
    FileSystem.closeAll();
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testSplitLocationInfo(int pNumThreads) throws Exception {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = getConfiguration();
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        "test:///a1/a2");
    Job job = Job.getInstance(conf);
    TextInputFormat fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    String[] locations = splits.get(0).getLocations();
    assertEquals(2, locations.length);
    SplitLocationInfo[] locationInfo = splits.get(0).getLocationInfo();
    assertEquals(2, locationInfo.length);
    SplitLocationInfo localhostInfo = locations[0].equals("localhost") ?
        locationInfo[0] : locationInfo[1];
    SplitLocationInfo otherhostInfo = locations[0].equals("otherhost") ?
        locationInfo[0] : locationInfo[1];
    assertTrue(localhostInfo.isOnDisk());
    assertTrue(localhostInfo.isInMemory());
    assertTrue(otherhostInfo.isOnDisk());
    assertFalse(otherhostInfo.isInMemory());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testListStatusSimple(int pNumThreads) throws IOException {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    List<Path> expectedPaths = configureTestSimple(conf, localFs);
    
    Job job  = Job.getInstance(conf);
    FileInputFormat<?, ?> fif = new TextInputFormat();
    List<FileStatus> statuses = fif.listStatus(job);

    verifyFileStatuses(expectedPaths, statuses, localFs);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testListStatusNestedRecursive(int pNumThreads) throws IOException {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    List<Path> expectedPaths = configureTestNestedRecursive(conf, localFs);
    Job job  = Job.getInstance(conf);
    FileInputFormat<?, ?> fif = new TextInputFormat();
    List<FileStatus> statuses = fif.listStatus(job);

    verifyFileStatuses(expectedPaths, statuses, localFs);
  }


  @MethodSource("data")
  @ParameterizedTest
  public void testListStatusNestedNonRecursive(int pNumThreads) throws IOException {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    List<Path> expectedPaths = configureTestNestedNonRecursive(conf, localFs);
    Job job  = Job.getInstance(conf);
    FileInputFormat<?, ?> fif = new TextInputFormat();
    List<FileStatus> statuses = fif.listStatus(job);

    verifyFileStatuses(expectedPaths, statuses, localFs);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testListStatusErrorOnNonExistantDir(int pNumThreads) throws IOException {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    configureTestErrorOnNonExistantDir(conf, localFs);
    Job job  = Job.getInstance(conf);
    FileInputFormat<?, ?> fif = new TextInputFormat();
    try {
      fif.listStatus(job);
      fail("Expecting an IOException for a missing Input path");
    } catch (IOException e) {
      Path expectedExceptionPath = new Path(TEST_ROOT_DIR, "input2");
      expectedExceptionPath = localFs.makeQualified(expectedExceptionPath);
      assertInstanceOf(InvalidInputException.class, e);
      assertEquals(
          "Input path does not exist: " + expectedExceptionPath.toString(),
          e.getMessage());
    }
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testShrinkStatus(int pNumThreads) throws IOException {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = getConfiguration();
    MockFileSystem mockFs =
        (MockFileSystem) new Path("test:///").getFileSystem(conf);
    Path dir1  = new Path("test:/a1");
    RemoteIterator<LocatedFileStatus> statuses = mockFs.listLocatedStatus(dir1);
    boolean verified = false;
    while (statuses.hasNext()) {
      LocatedFileStatus orig = statuses.next();
      LocatedFileStatus shrink =
          (LocatedFileStatus)FileInputFormat.shrinkStatus(orig);
      assertTrue(orig.equals(shrink));
      if (shrink.getBlockLocations() != null) {
        assertEquals(orig.getBlockLocations().length,
            shrink.getBlockLocations().length);
        for (int i = 0; i < shrink.getBlockLocations().length; i++) {
          verified = true;
          BlockLocation location = shrink.getBlockLocations()[i];
          BlockLocation actual = orig.getBlockLocations()[i];
          assertNotNull(((HdfsBlockLocation)actual).getLocatedBlock());
          assertEquals(BlockLocation.class.getName(),
              location.getClass().getName());
          assertArrayEquals(actual.getHosts(), location.getHosts());
          assertArrayEquals(actual.getCachedHosts(),
              location.getCachedHosts());
          assertArrayEquals(actual.getStorageIds(),
              location.getStorageIds());
          assertArrayEquals(actual.getStorageTypes(),
              location.getStorageTypes());
          assertArrayEquals(actual.getTopologyPaths(),
              location.getTopologyPaths());
          assertArrayEquals(actual.getNames(), location.getNames());
          assertEquals(actual.getLength(), location.getLength());
          assertEquals(actual.getOffset(), location.getOffset());
          assertEquals(actual.isCorrupt(), location.isCorrupt());
        }
      } else {
        assertTrue(orig.getBlockLocations() == null);
      }
    }
    assertTrue(verified);
  }

  public static List<Path> configureTestSimple(Configuration conf, FileSystem localFs)
      throws IOException {
    Path base1 = new Path(TEST_ROOT_DIR, "input1");
    Path base2 = new Path(TEST_ROOT_DIR, "input2");
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        localFs.makeQualified(base1) + "," + localFs.makeQualified(base2));
    localFs.mkdirs(base1);
    localFs.mkdirs(base2);

    Path in1File1 = new Path(base1, "file1");
    Path in1File2 = new Path(base1, "file2");
    localFs.createNewFile(in1File1);
    localFs.createNewFile(in1File2);

    Path in2File1 = new Path(base2, "file1");
    Path in2File2 = new Path(base2, "file2");
    localFs.createNewFile(in2File1);
    localFs.createNewFile(in2File2);
    List<Path> expectedPaths = Lists.newArrayList(in1File1, in1File2, in2File1,
        in2File2);
    return expectedPaths;
  }

  public static List<Path> configureTestNestedRecursive(Configuration conf,
      FileSystem localFs) throws IOException {
    Path base1 = new Path(TEST_ROOT_DIR, "input1");
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        localFs.makeQualified(base1).toString());
    conf.setBoolean(
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE,
        true);
    localFs.mkdirs(base1);

    Path inDir1 = new Path(base1, "dir1");
    Path inDir2 = new Path(base1, "dir2");
    Path inFile1 = new Path(base1, "file1");

    Path dir1File1 = new Path(inDir1, "file1");
    Path dir1File2 = new Path(inDir1, "file2");

    Path dir2File1 = new Path(inDir2, "file1");
    Path dir2File2 = new Path(inDir2, "file2");

    localFs.mkdirs(inDir1);
    localFs.mkdirs(inDir2);

    localFs.createNewFile(inFile1);
    localFs.createNewFile(dir1File1);
    localFs.createNewFile(dir1File2);
    localFs.createNewFile(dir2File1);
    localFs.createNewFile(dir2File2);

    List<Path> expectedPaths = Lists.newArrayList(inFile1, dir1File1,
        dir1File2, dir2File1, dir2File2);
    return expectedPaths;
  }

  public static List<Path> configureTestNestedNonRecursive(Configuration conf,
      FileSystem localFs) throws IOException {
    Path base1 = new Path(TEST_ROOT_DIR, "input1");
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        localFs.makeQualified(base1).toString());
    conf.setBoolean(
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE,
        false);
    localFs.mkdirs(base1);

    Path inDir1 = new Path(base1, "dir1");
    Path inDir2 = new Path(base1, "dir2");
    Path inFile1 = new Path(base1, "file1");

    Path dir1File1 = new Path(inDir1, "file1");
    Path dir1File2 = new Path(inDir1, "file2");

    Path dir2File1 = new Path(inDir2, "file1");
    Path dir2File2 = new Path(inDir2, "file2");

    localFs.mkdirs(inDir1);
    localFs.mkdirs(inDir2);

    localFs.createNewFile(inFile1);
    localFs.createNewFile(dir1File1);
    localFs.createNewFile(dir1File2);
    localFs.createNewFile(dir2File1);
    localFs.createNewFile(dir2File2);

    List<Path> expectedPaths = Lists.newArrayList(inFile1, inDir1, inDir2);
    return expectedPaths;
  }

  public static List<Path> configureTestErrorOnNonExistantDir(Configuration conf,
      FileSystem localFs) throws IOException {
    Path base1 = new Path(TEST_ROOT_DIR, "input1");
    Path base2 = new Path(TEST_ROOT_DIR, "input2");
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        localFs.makeQualified(base1) + "," + localFs.makeQualified(base2));
    conf.setBoolean(
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE,
        true);
    localFs.mkdirs(base1);

    Path inFile1 = new Path(base1, "file1");
    Path inFile2 = new Path(base1, "file2");

    localFs.createNewFile(inFile1);
    localFs.createNewFile(inFile2);

    List<Path> expectedPaths = Lists.newArrayList();
    return expectedPaths;
  }

  public static void verifyFileStatuses(List<Path> expectedPaths,
      List<FileStatus> fetchedStatuses, final FileSystem localFs) {
    assertEquals(expectedPaths.size(), fetchedStatuses.size());

    Iterable<Path> fqExpectedPaths =
        expectedPaths.stream().map(
            input -> localFs.makeQualified(input)).collect(Collectors.toList());


    Set<Path> expectedPathSet = Sets.newHashSet(fqExpectedPaths);
    for (FileStatus fileStatus : fetchedStatuses) {
      if (!expectedPathSet.remove(localFs.makeQualified(fileStatus.getPath()))) {
        fail("Found extra fetched status: " + fileStatus.getPath());
      }
    }
    assertEquals(0, expectedPathSet.size(),
        "Not all expectedPaths matched: " + expectedPathSet);
  }


  private void verifySplits(List<String> expected, List<InputSplit> splits) {
    Iterable<String> pathsFromSplits =
        splits.stream().map(
            input-> ((FileSplit) input).getPath().toString())
            .collect(Collectors.toList());

    Set<String> expectedSet = Sets.newHashSet(expected);
    for (String splitPathString : pathsFromSplits) {
      if (!expectedSet.remove(splitPathString)) {
        fail("Found extra split: " + splitPathString);
      }
    }
    assertEquals(0, expectedSet.size(),
        "Not all expectedPaths matched: " + expectedSet);
  }
  
  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.test.impl.disable.cache", "true");
    conf.setClass("fs.test.impl", MockFileSystem.class, FileSystem.class);
    conf.set(FileInputFormat.INPUT_DIR, "test:///a1");
    return conf;
  }

  static class MockFileSystem extends RawLocalFileSystem {
    int numListLocatedStatusCalls = 0;

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
        IOException {
      if (f.toString().equals("test:/a1")) {
        return new FileStatus[] {
            new FileStatus(0, true, 1, 150, 150, new Path("test:/a1/a2")),
            new FileStatus(10, false, 1, 150, 150, new Path("test:/a1/file1")) };
      } else if (f.toString().equals("test:/a1/a2")) {
        return new FileStatus[] {
            new FileStatus(10, false, 1, 150, 150,
                new Path("test:/a1/a2/file2")),
            new FileStatus(10, false, 1, 151, 150,
                new Path("test:/a1/a2/file3")) };
      }
      return new FileStatus[0];
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
        throws IOException {
      return new FileStatus[] { new FileStatus(10, true, 1, 150, 150,
          pathPattern) };
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter)
        throws FileNotFoundException, IOException {
      return this.listStatus(f);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
        throws IOException {
      DatanodeInfo[] ds = new DatanodeInfo[2];
      ds[0] = new DatanodeDescriptor(
          new DatanodeID("127.0.0.1", "localhost", "abcd",
              9866, 9867, 9868, 9869));
      ds[1] = new DatanodeDescriptor(
          new DatanodeID("1.0.0.1", "otherhost", "efgh",
              9866, 9867, 9868, 9869));
      long blockLen = len / 3;
      ExtendedBlock b1 = new ExtendedBlock("bpid", 0, blockLen, 0);
      ExtendedBlock b2 = new ExtendedBlock("bpid", 1, blockLen, 1);
      ExtendedBlock b3 = new ExtendedBlock("bpid", 2, len - 2 * blockLen, 2);
      String[] names = new String[]{ "localhost:9866", "otherhost:9866" };
      String[] hosts = new String[]{ "localhost", "otherhost" };
      String[] cachedHosts = {"localhost"};
      BlockLocation loc1 = new BlockLocation(names, hosts, cachedHosts,
          new String[0], 0, blockLen, false);
      BlockLocation loc2 = new BlockLocation(names, hosts, cachedHosts,
          new String[0], blockLen, blockLen, false);
      BlockLocation loc3 = new BlockLocation(names, hosts, cachedHosts,
          new String[0], 2 * blockLen, len - 2 * blockLen, false);
      return new BlockLocation[]{
          new HdfsBlockLocation(loc1, new LocatedBlock(b1, ds)),
          new HdfsBlockLocation(loc2, new LocatedBlock(b2, ds)),
          new HdfsBlockLocation(loc3, new LocatedBlock(b3, ds)) };
    }

    @Override
    protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f,
        PathFilter filter) throws FileNotFoundException, IOException {
      ++numListLocatedStatusCalls;
      return super.listLocatedStatus(f, filter);
    }
  }
}
