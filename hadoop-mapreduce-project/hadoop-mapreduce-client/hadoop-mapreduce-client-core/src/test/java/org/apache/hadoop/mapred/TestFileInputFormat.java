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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

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
  public void testListLocatedStatus() throws Exception {
    Configuration conf = getConfiguration();
    conf.setBoolean("fs.test.impl.disable.cache", false);
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        "test:///a1/a2");
    MockFileSystem mockFs =
        (MockFileSystem) new Path("test:///").getFileSystem(conf);
    Assert.assertEquals("listLocatedStatus already called",
        0, mockFs.numListLocatedStatusCalls);
    JobConf job = new JobConf(conf);
    TextInputFormat fileInputFormat = new TextInputFormat();
    fileInputFormat.configure(job);
    InputSplit[] splits = fileInputFormat.getSplits(job, 1);
    Assert.assertEquals("Input splits are not correct", 2, splits.length);
    Assert.assertEquals("listLocatedStatuss calls",
        1, mockFs.numListLocatedStatusCalls);
    FileSystem.closeAll();
  }
  
  @Test
  public void testSplitLocationInfo() throws Exception {
    Configuration conf = getConfiguration();
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
        "test:///a1/a2");
    JobConf job = new JobConf(conf);
    TextInputFormat fileInputFormat = new TextInputFormat();
    fileInputFormat.configure(job);
    FileSplit[] splits = (FileSplit[]) fileInputFormat.getSplits(job, 1);
    String[] locations = splits[0].getLocations();
    Assert.assertEquals(2, locations.length);
    SplitLocationInfo[] locationInfo = splits[0].getLocationInfo();
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

    List<Path> expectedPaths = org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .configureTestSimple(conf, localFs);

    JobConf jobConf = new JobConf(conf);
    TextInputFormat fif = new TextInputFormat();
    fif.configure(jobConf);
    FileStatus[] statuses = fif.listStatus(jobConf);

    org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .verifyFileStatuses(expectedPaths, Lists.newArrayList(statuses),
            localFs);
  }

  @Test
  public void testListStatusNestedRecursive() throws IOException {
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

  @Test
  public void testListStatusNestedNonRecursive() throws IOException {
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

  @Test
  public void testListStatusErrorOnNonExistantDir() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(FileInputFormat.LIST_STATUS_NUM_THREADS, numThreads);

    org.apache.hadoop.mapreduce.lib.input.TestFileInputFormat
        .configureTestErrorOnNonExistantDir(conf, localFs);
    JobConf jobConf = new JobConf(conf);
    TextInputFormat fif = new TextInputFormat();
    fif.configure(jobConf);
    try {
      fif.listStatus(jobConf);
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
          new BlockLocation(new String[] { "localhost:50010", "otherhost:50010" },
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
