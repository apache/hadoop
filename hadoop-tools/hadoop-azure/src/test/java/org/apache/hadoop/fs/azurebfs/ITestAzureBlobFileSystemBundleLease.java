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

package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assume.assumeTrue;

/**
 * Test lease operations.
 */
public class ITestAzureBlobFileSystemBundleLease extends
    AbstractAbfsIntegrationTest {
  private static final int BASE_SIZE = 1024;
  private static final int ONE_THOUSAND = 1000;
  private static final int ONE_MB = 1024 * 1024;
  private static final int FLUSH_TIMES = 20;
  private static final int TEST_BUFFER_SIZE = 3 * ONE_THOUSAND * BASE_SIZE;
  private static final int THREAD_SLEEP_TIME = 1000;
  private static final Path TEST_FILE_PATH = new Path("testfile");

  public ITestAzureBlobFileSystemBundleLease() throws Exception {
    super();
    Configuration conf = getRawConfiguration();
    conf.set(ConfigurationKeys.FS_AZURE_WRITE_ENFORCE_LEASE, "true");
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeTrue(getFileSystem().getIsNamespaceEnabled());
  }

  @Test
  public void testAppendWithLength0() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    try(FSDataOutputStream stream = fs.create(TEST_FILE_PATH)) {
      final byte[] b = new byte[1024];
      new Random().nextBytes(b);
      stream.write(b, 1000, 0);
    }

    assertEquals(0, fs.getFileStatus(TEST_FILE_PATH).getLen());
    //Try deletion. It should succeed as lease has been released.
    fs.delete(TEST_FILE_PATH, false);
  }

  @Test
  public void testAppendAfterCreate() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(TEST_FILE_PATH).close();
    try(FSDataOutputStream stream = fs.append(TEST_FILE_PATH)) {
      final byte[] b = new byte[1024];
      new Random().nextBytes(b);
      stream.write(b, 0, 1024);
    }

    assertEquals(1024, fs.getFileStatus(TEST_FILE_PATH).getLen());
    //Try deletion. It should succeed as lease has been released.
    fs.delete(TEST_FILE_PATH, false);
  }

  @Test
  public void testMultipleWriter() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final byte[] b = new byte[1024];
    new Random().nextBytes(b);
    try(FSDataOutputStream stream = fs.create(TEST_FILE_PATH)) {
      stream.write(b, 0, 1024);
      intercept(IOException.class,
          () -> fs.create(TEST_FILE_PATH, true));
      FSDataOutputStream stream2 = fs.append(TEST_FILE_PATH);
      intercept(IOException.class,
          () -> {
        stream2.write(b, 1000, 0);
        stream2.close(); });
    }

    //Retry Create and append after close
    fs.create(TEST_FILE_PATH, true).close();
    FSDataOutputStream stream2 = fs.append(TEST_FILE_PATH);
    stream2.write(b, 0, 1024);
    stream2.hflush();
    //Try deletion. It should fail as lease has not been released.
    intercept(IOException.class,
            () -> fs.delete(TEST_FILE_PATH, false));
    stream2.close();
    assertEquals(1024, fs.getFileStatus(TEST_FILE_PATH).getLen());
    fs.delete(TEST_FILE_PATH, false);
  }

  @Test
  public void testAbfsOutputStreamSyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());

    final byte[] b;
    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      b = new byte[TEST_BUFFER_SIZE];
      new Random().nextBytes(b);
      stream.write(b);

      for (int i = 0; i < FLUSH_TIMES; i++) {
        stream.hsync();
        Thread.sleep(10);
      }
    }

    final byte[] r = new byte[TEST_BUFFER_SIZE];
    try (FSDataInputStream inputStream = fs.open(testFilePath, 4 * ONE_MB)) {
      int result = inputStream.read(r);

      assertNotEquals(-1, result);
      assertArrayEquals(r, b);
    }

    //Try deletion. It should succeed as lease has been released.
    fs.delete(testFilePath, false);
  }

  @Test
  public void testAbfsOutputStreamAsyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());
    final byte[] b;
    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      b = new byte[TEST_BUFFER_SIZE];
      new Random().nextBytes(b);

      for (int i = 0; i < 2; i++) {
        stream.write(b);

        for (int j = 0; j < FLUSH_TIMES; j++) {
          stream.flush();
          Thread.sleep(10);
        }
      }
    }

    final byte[] r = new byte[TEST_BUFFER_SIZE];
    try (FSDataInputStream inputStream = fs.open(testFilePath, 4 * ONE_MB)) {
      while (inputStream.available() != 0) {
        int result = inputStream.read(r);

        assertNotEquals("read returned -1", -1, result);
        assertArrayEquals("buffer read from stream", r, b);
      }
    }

    //Try deletion. It should succeed as lease has been released.
    fs.delete(testFilePath, false);
  }

  @Test
  public void testHflush() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    byte[] buffer = new byte[TEST_BUFFER_SIZE];
    new Random().nextBytes(buffer);
    String fileName = UUID.randomUUID().toString();
    final Path testFilePath = path(fileName);

    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      stream.write(buffer);
      stream.hflush();
      byte[] readBuffer = new byte[buffer.length];
      fs.open(testFilePath).read(readBuffer, 0, readBuffer.length);
      assertArrayEquals(
          "Bytes read do not match bytes written.",
          buffer,
          readBuffer);
    }

    //Try deletion. It should succeed as lease has been released.
    fs.delete(testFilePath, false);
  }

  @Test
  public void testWriteHeavyBytesToFileSyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());
    ExecutorService es;
    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      es = Executors.newFixedThreadPool(10);

      final byte[] b = new byte[TEST_BUFFER_SIZE];
      new Random().nextBytes(b);

      List<Future<Void>> tasks = new ArrayList<>();
      for (int i = 0; i < FLUSH_TIMES; i++) {
        Callable<Void> callable = new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            stream.write(b);
            return null;
          }
        };

        tasks.add(es.submit(callable));
      }

      boolean shouldStop = false;
      while (!shouldStop) {
        shouldStop = true;
        for (Future<Void> task : tasks) {
          if (!task.isDone()) {
            stream.hsync();
            shouldStop = false;
            Thread.sleep(THREAD_SLEEP_TIME);
          }
        }
      }

      tasks.clear();
    }

    es.shutdownNow();
    FileStatus fileStatus = fs.getFileStatus(testFilePath);
    long expectedWrites = (long) TEST_BUFFER_SIZE * FLUSH_TIMES;
    assertEquals("Wrong file length in " + testFilePath, expectedWrites, fileStatus.getLen());

    //Try deletion. It should succeed as lease has been released.
    fs.delete(testFilePath, false);
  }

  @Test
  public void testWriteHeavyBytesToFileAsyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    ExecutorService es = Executors.newFixedThreadPool(10);

    final Path testFilePath = path(methodName.getMethodName());
    try (FSDataOutputStream stream = fs.create(testFilePath)) {

      final byte[] b = new byte[TEST_BUFFER_SIZE];
      new Random().nextBytes(b);

      List<Future<Void>> tasks = new ArrayList<>();
      for (int i = 0; i < FLUSH_TIMES; i++) {
        Callable<Void> callable = new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            stream.write(b);
            return null;
          }
        };

        tasks.add(es.submit(callable));
      }

      boolean shouldStop = false;
      while (!shouldStop) {
        shouldStop = true;
        for (Future<Void> task : tasks) {
          if (!task.isDone()) {
            stream.flush();
            shouldStop = false;
          }
        }
      }
      Thread.sleep(THREAD_SLEEP_TIME);
      tasks.clear();
    }

    es.shutdownNow();
    FileStatus fileStatus = fs.getFileStatus(testFilePath);
    assertEquals((long) TEST_BUFFER_SIZE * FLUSH_TIMES, fileStatus.getLen());

    //Try deletion. It should succeed as lease has been released.
    fs.delete(testFilePath, false);
  }

  /**
   * Tests
   * 1. create overwrite=false of a file that doesnt pre-exist
   * 2. create overwrite=false of a file that pre-exists
   * 3. create overwrite=true of a file that doesnt pre-exist
   * 4. create overwrite=true of a file that pre-exists
   * matches the expectation when run against both combinations of
   * fs.azure.enable.conditional.create.overwrite=true and
   * fs.azure.enable.conditional.create.overwrite=false
   * @throws Throwable
   */
  @Test
  public void testDefaultCreateOverwriteFileTest() throws Throwable {
    testCreateFileOverwrite(true);
    testCreateFileOverwrite(false);
  }

  public void testCreateFileOverwrite(boolean enableConditionalCreateOverwrite)
      throws Throwable {
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(enableConditionalCreateOverwrite));

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    final Path nonOverwriteFile = new Path("/NonOverwriteTest_FileName_"
        + UUID.randomUUID().toString());

    // Case 1: Not Overwrite - File does not pre-exist
    // create should be successful
    fs.create(nonOverwriteFile, false).close();

    // Case 2: Not Overwrite - File pre-exists
    intercept(FileAlreadyExistsException.class,
        () -> fs.create(nonOverwriteFile, false).close());

    final Path overwriteFilePath = new Path("/OverwriteTest_FileName_"
        + UUID.randomUUID().toString());

    // Case 3: Overwrite - File does not pre-exist
    // create should be successful
    fs.create(overwriteFilePath, true).close();

    // Case 4: Overwrite - File pre-exists
    fs.create(overwriteFilePath, true).close();

    //Try deletion. It should succeed as lease has been released.
    fs.delete(overwriteFilePath, false);
  }
}
