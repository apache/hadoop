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
package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CopyCommands.CopyToLocal;

import static org.apache.hadoop.fs.shell.CopyCommandWithMultiThread.DEFAULT_QUEUE_SIZE;
import static org.apache.hadoop.fs.shell.CopyCommandWithMultiThread.MAX_THREAD_COUNT;
import static org.junit.Assert.assertEquals;

public class TestCopyToLocal {

  private static final String FROM_DIR_NAME = "fromDir";
  private static final String TO_DIR_NAME = "toDir";

  private static FileSystem fs;
  private static Path testDir;
  private static Configuration conf;

  private static int initialize(Path dir) throws Exception {
    fs.mkdirs(dir);
    Path fromDirPath = new Path(dir, FROM_DIR_NAME);
    fs.mkdirs(fromDirPath);
    Path toDirPath = new Path(dir, TO_DIR_NAME);
    fs.mkdirs(toDirPath);

    int numTotalFiles = 0;
    int numDirs = RandomUtils.nextInt(0, 5);
    for (int dirCount = 0; dirCount < numDirs; ++dirCount) {
      Path subDirPath = new Path(fromDirPath, "subdir" + dirCount);
      fs.mkdirs(subDirPath);
      int numFiles = RandomUtils.nextInt(0, 10);
      for (int fileCount = 0; fileCount < numFiles; ++fileCount) {
        numTotalFiles++;
        Path subFile = new Path(subDirPath, "file" + fileCount);
        fs.createNewFile(subFile);
        FSDataOutputStream output = fs.create(subFile, true);
        for (int i = 0; i < 100; ++i) {
          output.writeInt(i);
          output.writeChar('\n');
        }
        output.close();
      }
    }

    return numTotalFiles;
  }

  @BeforeClass
  public static void init() throws Exception {
    conf = new Configuration(false);
    conf.set("fs.file.impl", LocalFileSystem.class.getName());
    fs = FileSystem.getLocal(conf);
    testDir = new FileSystemTestHelper().getTestRootPath(fs);
    // don't want scheme on the path, just an absolute path
    testDir = new Path(fs.makeQualified(testDir).toUri().getPath());

    FileSystem.setDefaultUri(conf, fs.getUri());
    fs.setWorkingDirectory(testDir);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    fs.delete(testDir, true);
    fs.close();
  }

  private void run(CopyCommandWithMultiThread cmd, String... args) {
    cmd.setConf(conf);
    assertEquals(0, cmd.run(args));
  }

  @org.junit.Test(timeout = 10000)
  public void testCopy() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    initialize(dir);
    MultiThreadedCopy copy = new MultiThreadedCopy(1, DEFAULT_QUEUE_SIZE, 0);
    run(new MultiThreadedCopy(1, DEFAULT_QUEUE_SIZE, 0),
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
    assert copy.getExecutor() == null;
  }

  @org.junit.Test(timeout = 10000)
  public void testCopyWithThreads() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    int numFiles = initialize(dir);
    int randThreads = RandomUtils.nextInt(0, MAX_THREAD_COUNT - 1) + 1;
    run(new MultiThreadedCopy(randThreads, DEFAULT_QUEUE_SIZE,
            (randThreads == 1 ? 0 : numFiles)), "-t", Integer.toString(randThreads),
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
  }

  @org.junit.Test(timeout = 10000)
  public void testCopyWithThreadWrong() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    int numFiles = initialize(dir);
    run(new MultiThreadedCopy(MAX_THREAD_COUNT, DEFAULT_QUEUE_SIZE, numFiles),
        "-t", Integer.toString(MAX_THREAD_COUNT * 2),
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
  }

  @Test(timeout = 10000)
  public void testCopyWithZeroThreads() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    initialize(dir);
    run(new MultiThreadedCopy(1, DEFAULT_QUEUE_SIZE, 0), "-t", "0",
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
  }

  @org.junit.Test(timeout = 10000)
  public void testCopyWithThreadsAndQueueSize() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    int numFiles = initialize(dir);
    int randThreads = RandomUtils.nextInt(0, MAX_THREAD_COUNT - 1) + 1;
    int queueSize = 256;
    run(new MultiThreadedCopy(randThreads, queueSize, numFiles), "-t",
        Integer.toString(randThreads), "-q", Integer.toString(queueSize),
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
  }

  @org.junit.Test(timeout = 10000)
  public void testCopyWithThreadsAndQueueSizeWrong() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    int numFiles = initialize(dir);
    int randThreads = RandomUtils.nextInt(0, MAX_THREAD_COUNT - 1) + 1;
    int queueSize = 0;
    run(new MultiThreadedCopy(randThreads, DEFAULT_QUEUE_SIZE, numFiles), "-t",
        String.valueOf(randThreads), "-q", Integer.toString(queueSize),
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
  }

  @org.junit.Test(timeout = 10000)
  public void testCopySingleFile() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    fs.mkdirs(dir);
    Path fromDirPath = new Path(dir, FROM_DIR_NAME);
    fs.mkdirs(fromDirPath);
    Path subFile = new Path(fromDirPath, "file0");
    fs.createNewFile(subFile);
    FSDataOutputStream output = fs.create(subFile, true);
    for (int i = 0; i < 100; ++i) {
      output.writeInt(i);
      output.writeChar('\n');
    }
    output.close();

    MultiThreadedCopy copy =
        new MultiThreadedCopy(MAX_THREAD_COUNT, DEFAULT_QUEUE_SIZE, 0);
    run(copy, "-t", String.valueOf(MAX_THREAD_COUNT), subFile.toString(),
        new Path(dir, TO_DIR_NAME).toString());
    assert copy.getExecutor() == null;
  }

  private static class MultiThreadedCopy extends CopyToLocal {
    public static final String NAME = "multiThreadCopy";
    private final int expectedThreads;
    private final int expectedQueuePoolSize;
    private final int expectedCompletedTaskCount;

    MultiThreadedCopy(int expectedThreads, int expectedQueuePoolSize,
        int expectedCompletedTaskCount) {
      this.expectedThreads = expectedThreads;
      this.expectedQueuePoolSize = expectedQueuePoolSize;
      this.expectedCompletedTaskCount = expectedCompletedTaskCount;
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
        throws IOException {
      // Check if the number of threads are same as expected
      Assert.assertEquals(expectedThreads, getThreadCount());
      // Check if the queue pool size of executor is same as expected
      Assert.assertEquals(expectedQueuePoolSize, getThreadPoolQueueSize());

      super.processArguments(args);

      if (isMultiThreadNecessary(args)) {
        // Once the copy is complete, check following
        // 1) number of completed tasks are same as expected
        // 2) There are no active tasks in the executor
        // 3) Executor has shutdown correctly
        ThreadPoolExecutor executor = getExecutor();
        Assert.assertEquals(expectedCompletedTaskCount,
            executor.getCompletedTaskCount());
        Assert.assertEquals(0, executor.getActiveCount());
        Assert.assertTrue(executor.isTerminated());
      } else {
        assert getExecutor() == null;
      }
    }
  }
}
