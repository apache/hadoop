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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
  public void testListLocatedStatus(int pNumThreads) throws Exception {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = getConfiguration();
    conf.setBoolean("fs.test.impl.disable.cache", false);
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        "test:///a1/a2");
    MockFileSystem mockFs =
        (MockFileSystem) new Path("test:///").getFileSystem(conf);
    assertEquals(0, mockFs.numListLocatedStatusCalls,
        "listLocatedStatus already called");
    JobConf job = new JobConf(conf);
    TextInputFormat fileInputFormat = new TextInputFormat();
    fileInputFormat.configure(job);
    InputSplit[] splits = fileInputFormat.getSplits(job, 1);
    assertEquals(2, splits.length, "Input splits are not correct");
    assertEquals(1, mockFs.numListLocatedStatusCalls, "listLocatedStatus calls");
    FileSystem.closeAll();
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testIgnoreDirs(int pNumThreads) throws Exception {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = getConfiguration();
    conf.setBoolean(FileInputFormat.INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, true);
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, "test:///a1");
    MockFileSystem mockFs = (MockFileSystem) new Path("test:///").getFileSystem(conf);
    JobConf job = new JobConf(conf);
    TextInputFormat fileInputFormat = new TextInputFormat();
    fileInputFormat.configure(job);
    InputSplit[] splits = fileInputFormat.getSplits(job, 1);
    assertEquals(1, splits.length, "Input splits are not correct");
    FileSystem.closeAll();
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testSplitLocationInfo(int pNumThreads) throws Exception {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = getConfiguration();
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        "test:///a1/a2");
    JobConf job = new JobConf(conf);
    TextInputFormat fileInputFormat = new TextInputFormat();
    fileInputFormat.configure(job);
    FileSplit[] splits = (FileSplit[]) fileInputFormat.getSplits(job, 1);
    String[] locations = splits[0].getLocations();
    assertEquals(2, locations.length);
    SplitLocationInfo[] locationInfo = splits[0].getLocationInfo();
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

    List<Path> expectedPaths = org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .configureTestSimple(conf, localFs);

    JobConf jobConf = new JobConf(conf);
    TextInputFormat fif = new TextInputFormat();
    fif.configure(jobConf);
    FileStatus[] statuses = fif.listStatus(jobConf);

    org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .verifyFileStatuses(expectedPaths, Lists.newArrayList(statuses), localFs);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testListStatusNestedRecursive(int pNumThreads) throws IOException {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    List<Path> expectedPaths = org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .configureTestNestedRecursive(conf, localFs);
    JobConf jobConf = new JobConf(conf);
    TextInputFormat fif = new TextInputFormat();
    fif.configure(jobConf);
    FileStatus[] statuses = fif.listStatus(jobConf);

    org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .verifyFileStatuses(expectedPaths, Lists.newArrayList(statuses),
            localFs);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testListStatusNestedNonRecursive(int pNumThreads) throws IOException {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    List<Path> expectedPaths = org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .configureTestNestedNonRecursive(conf, localFs);
    JobConf jobConf = new JobConf(conf);
    TextInputFormat fif = new TextInputFormat();
    fif.configure(jobConf);
    FileStatus[] statuses = fif.listStatus(jobConf);

    org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .verifyFileStatuses(expectedPaths, Lists.newArrayList(statuses),
        localFs);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testListStatusErrorOnNonExistantDir(int pNumThreads) throws IOException {
    initTestFileInputFormat(pNumThreads);
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .configureTestErrorOnNonExistantDir(conf, localFs);
    JobConf jobConf = new JobConf(conf);
    TextInputFormat fif = new TextInputFormat();
    fif.configure(jobConf);
    try {
      fif.listStatus(jobConf);
      fail("Expecting an IOException for a missing Input path");
    } catch (IOException e) {
      Path expectedExceptionPath = new Path(TEST_ROOT_DIR, "input2");
      expectedExceptionPath = localFs.makeQualified(expectedExceptionPath);
      assertInstanceOf(InvalidInputException.class, e);
      assertEquals("Input path does not exist: " + expectedExceptionPath.toString(),
          e.getMessage());
    }
  }

  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.test.impl.disable.cache", "true");
    conf.setClass("fs.test.impl", MockFileSystem.class, FileSystem.class);
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        "test:///a1");
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
              new String[0], 0, len, false) };
    }

    @Override
    protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f,
        PathFilter filter) throws FileNotFoundException, IOException {
      ++numListLocatedStatusCalls;
      return super.listLocatedStatus(f, filter);
    }
  }
}
