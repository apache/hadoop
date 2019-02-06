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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.shell.CopyCommands.CopyFromLocal;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;

/**
 * Test for copyFromLocal.
 */
public class TestCopyFromLocal {
  private static final String FROM_DIR_NAME = "fromDir";
  private static final String TO_DIR_NAME = "toDir";

  private static FileSystem fs;
  private static Path testDir;
  private static Configuration conf;

  public static int initialize(Path dir) throws Exception {
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
        for(int i = 0; i < 100; ++i) {
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

  private void run(CommandWithDestination cmd, String... args) {
    cmd.setConf(conf);
    assertEquals(0, cmd.run(args));
  }

  @Test(timeout = 10000)
  public void testCopyFromLocal() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    TestCopyFromLocal.initialize(dir);
    run(new TestMultiThreadedCopy(1, 0),
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
  }

  @Test(timeout = 10000)
  public void testCopyFromLocalWithThreads() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    int numFiles = TestCopyFromLocal.initialize(dir);
    int maxThreads = Runtime.getRuntime().availableProcessors() * 2;
    int randThreads = RandomUtils.nextInt(0, maxThreads - 1) + 1;
    String numThreads = Integer.toString(randThreads);
    run(new TestMultiThreadedCopy(randThreads,
        randThreads == 1 ? 0 : numFiles), "-t", numThreads,
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
  }

  @Test(timeout = 10000)
  public void testCopyFromLocalWithThreadWrong() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    int numFiles = TestCopyFromLocal.initialize(dir);
    int maxThreads = Runtime.getRuntime().availableProcessors() * 2;
    String numThreads = Integer.toString(maxThreads * 2);
    run(new TestMultiThreadedCopy(maxThreads, numFiles), "-t", numThreads,
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
  }

  @Test(timeout = 10000)
  public void testCopyFromLocalWithZeroThreads() throws Exception {
    Path dir = new Path("dir" + RandomStringUtils.randomNumeric(4));
    TestCopyFromLocal.initialize(dir);
    run(new TestMultiThreadedCopy(1, 0), "-t", "0",
        new Path(dir, FROM_DIR_NAME).toString(),
        new Path(dir, TO_DIR_NAME).toString());
  }

  private class TestMultiThreadedCopy extends CopyFromLocal {
    public static final String NAME = "testCopyFromLocal";
    private int expectedThreads;
    private int expectedCompletedTaskCount;

    TestMultiThreadedCopy(int expectedThreads,
                          int expectedCompletedTaskCount) {
      this.expectedThreads = expectedThreads;
      this.expectedCompletedTaskCount = expectedCompletedTaskCount;
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
        throws IOException {
      // Check if the correct number of threads are spawned
      Assert.assertEquals(expectedThreads, getNumThreads());
      super.processArguments(args);
      // Once the copy is complete, check following
      // 1) number of completed tasks are same as expected
      // 2) There are no active tasks in the executor
      // 3) Executor has shutdown correctly
      ThreadPoolExecutor executor = getExecutor();
      Assert.assertEquals(expectedCompletedTaskCount,
          executor.getCompletedTaskCount());
      Assert.assertEquals(0, executor.getActiveCount());
      Assert.assertTrue(executor.isTerminated());
    }
  }
}
