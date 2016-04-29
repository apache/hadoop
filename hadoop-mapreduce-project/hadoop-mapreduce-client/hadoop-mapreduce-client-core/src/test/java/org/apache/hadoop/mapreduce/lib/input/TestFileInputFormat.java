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

import javax.annotation.Nullable;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(value = Parameterized.class)
public class TestFileInputFormat {
  
  private static final Log LOG = LogFactory.getLog(TestFileInputFormat.class);
  
  private static String testTmpDir = System.getProperty("test.build.data", "/tmp");
  private static final Path TEST_ROOT_DIR = new Path(testTmpDir, "TestFIF");
  
  private static FileSystem localFs;
  
  private int numThreads;
  
  public TestFileInputFormat(int numThreads) {
    this.numThreads = numThreads;
    LOG.info("Running with numThreads: " + numThreads);
  }
  
  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { 1 }, { 5 }};
    return Arrays.asList(data);
  }
  
  @Before
  public void setup() throws IOException {
    LOG.info("Using Test Dir: " + TEST_ROOT_DIR);
    localFs = FileSystem.getLocal(new Configuration());
    localFs.delete(TEST_ROOT_DIR, true);
    localFs.mkdirs(TEST_ROOT_DIR);
  }
  
  @After
  public void cleanup() throws IOException {
    localFs.delete(TEST_ROOT_DIR, true);
  }

  @Test
  public void testNumInputFilesRecursively() throws Exception {
    Configuration conf = getConfiguration();
    conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    Assert.assertEquals("Input splits are not correct", 3, splits.size());
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

  @Test
  public void testNumInputFilesWithoutRecursively() throws Exception {
    Configuration conf = getConfiguration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    Assert.assertEquals("Input splits are not correct", 2, splits.size());
    verifySplits(Lists.newArrayList("test:/a1/a2", "test:/a1/file1"), splits);
  }

  @Test
  public void testListLocatedStatus() throws Exception {
    Configuration conf = getConfiguration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    conf.setBoolean("fs.test.impl.disable.cache", false);
    conf.set(FileInputFormat.INPUT_DIR, "test:///a1/a2");
    MockFileSystem mockFs =
        (MockFileSystem) new Path("test:///").getFileSystem(conf);
    Assert.assertEquals("listLocatedStatus already called",
        0, mockFs.numListLocatedStatusCalls);
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    Assert.assertEquals("Input splits are not correct", 2, splits.size());
    Assert.assertEquals("listLocatedStatuss calls",
        1, mockFs.numListLocatedStatusCalls);
    FileSystem.closeAll();
  }
  
  @Test
  public void testSplitLocationInfo() throws Exception {
    Configuration conf = getConfiguration();
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        "test:///a1/a2");
    Job job = Job.getInstance(conf);
    TextInputFormat fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    String[] locations = splits.get(0).getLocations();
    Assert.assertEquals(2, locations.length);
    SplitLocationInfo[] locationInfo = splits.get(0).getLocationInfo();
    Assert.assertEquals(2, locationInfo.length);
    SplitLocationInfo localhostInfo = locations[0].equals("localhost") ?
        locationInfo[0] : locationInfo[1];
    SplitLocationInfo otherhostInfo = locations[0].equals("otherhost") ?
        locationInfo[0] : locationInfo[1];
    Assert.assertTrue(localhostInfo.isOnDisk());
    Assert.assertTrue(localhostInfo.isInMemory());
    Assert.assertTrue(otherhostInfo.isOnDisk());
    Assert.assertFalse(otherhostInfo.isInMemory());
  }

  @Test
  public void testListStatusSimple() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    List<Path> expectedPaths = configureTestSimple(conf, localFs);
    
    Job job  = Job.getInstance(conf);
    FileInputFormat<?, ?> fif = new TextInputFormat();
    List<FileStatus> statuses = fif.listStatus(job);

    verifyFileStatuses(expectedPaths, statuses, localFs);
  }

  @Test
  public void testListStatusNestedRecursive() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    List<Path> expectedPaths = configureTestNestedRecursive(conf, localFs);
    Job job  = Job.getInstance(conf);
    FileInputFormat<?, ?> fif = new TextInputFormat();
    List<FileStatus> statuses = fif.listStatus(job);

    verifyFileStatuses(expectedPaths, statuses, localFs);
  }


  @Test
  public void testListStatusNestedNonRecursive() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    List<Path> expectedPaths = configureTestNestedNonRecursive(conf, localFs);
    Job job  = Job.getInstance(conf);
    FileInputFormat<?, ?> fif = new TextInputFormat();
    List<FileStatus> statuses = fif.listStatus(job);

    verifyFileStatuses(expectedPaths, statuses, localFs);
  }

  @Test
  public void testListStatusErrorOnNonExistantDir() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    configureTestErrorOnNonExistantDir(conf, localFs);
    Job job  = Job.getInstance(conf);
    FileInputFormat<?, ?> fif = new TextInputFormat();
    try {
      fif.listStatus(job);
      Assert.fail("Expecting an IOException for a missing Input path");
    } catch (IOException e) {
      Path expectedExceptionPath = new Path(TEST_ROOT_DIR, "input2");
      expectedExceptionPath = localFs.makeQualified(expectedExceptionPath);
      Assert.assertTrue(e instanceof InvalidInputException);
      Assert.assertEquals(
          "Input path does not exist: " + expectedExceptionPath.toString(),
          e.getMessage());
    }
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
    Assert.assertEquals(expectedPaths.size(), fetchedStatuses.size());

    Iterable<Path> fqExpectedPaths = Iterables.transform(expectedPaths,
        new Function<Path, Path>() {
          @Override
          public Path apply(Path input) {
            return localFs.makeQualified(input);
          }
        });

    Set<Path> expectedPathSet = Sets.newHashSet(fqExpectedPaths);
    for (FileStatus fileStatus : fetchedStatuses) {
      if (!expectedPathSet.remove(localFs.makeQualified(fileStatus.getPath()))) {
        Assert.fail("Found extra fetched status: " + fileStatus.getPath());
      }
    }
    Assert.assertEquals(
        "Not all expectedPaths matched: " + expectedPathSet.toString(), 0,
        expectedPathSet.size());
  }


  private void verifySplits(List<String> expected, List<InputSplit> splits) {
    Iterable<String> pathsFromSplits = Iterables.transform(splits,
        new Function<InputSplit, String>() {
          @Override
          public String apply(@Nullable InputSplit input) {
            return ((FileSplit) input).getPath().toString();
          }
        });

    Set<String> expectedSet = Sets.newHashSet(expected);
    for (String splitPathString : pathsFromSplits) {
      if (!expectedSet.remove(splitPathString)) {
        Assert.fail("Found extra split: " + splitPathString);
      }
    }
    Assert.assertEquals(
        "Not all expectedPaths matched: " + expectedSet.toString(), 0,
        expectedSet.size());
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
      return new BlockLocation[] {
          new BlockLocation(new String[] { "localhost:9866", "otherhost:9866" },
              new String[] { "localhost", "otherhost" }, new String[] { "localhost" },
              new String[0], 0, len, false) };    }

    @Override
    protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f,
        PathFilter filter) throws FileNotFoundException, IOException {
      ++numListLocatedStatusCalls;
      return super.listLocatedStatus(f, filter);
    }
  }
}
