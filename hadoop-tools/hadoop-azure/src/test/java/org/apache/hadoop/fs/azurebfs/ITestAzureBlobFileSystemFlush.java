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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Test flush operation.
 */
public class ITestAzureBlobFileSystemFlush extends AbstractAbfsScaleTest {
  private static final int BASE_SIZE = 1024;
  private static final int ONE_THOUSAND = 1000;
  private static final int TEST_BUFFER_SIZE = 5 * ONE_THOUSAND * BASE_SIZE;
  private static final int ONE_MB = 1024 * 1024;
  private static final int FLUSH_TIMES = 200;
  private static final int THREAD_SLEEP_TIME = 6000;

  private static final Path TEST_FILE_PATH = new Path("/testfile");

  public ITestAzureBlobFileSystemFlush() {
    super();
  }

  @Test
  public void testAbfsOutputStreamAsyncFlushWithRetainUncommittedData() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final byte[] b;
    try(final FSDataOutputStream stream = fs.create(TEST_FILE_PATH)) {
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
    try(FSDataInputStream inputStream = fs.open(TEST_FILE_PATH, 4 * ONE_MB)) {
      while (inputStream.available() != 0) {
        int result = inputStream.read(r);

        assertNotEquals("read returned -1", -1, result);
        assertArrayEquals("buffer read from stream", r, b);
      }
    }
  }

  @Test
  public void testAbfsOutputStreamSyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final byte[] b;
    try(final FSDataOutputStream stream = fs.create(TEST_FILE_PATH)) {
      b = new byte[TEST_BUFFER_SIZE];
      new Random().nextBytes(b);
      stream.write(b);

      for (int i = 0; i < FLUSH_TIMES; i++) {
        stream.hsync();
        stream.hflush();
        Thread.sleep(10);
      }
    }

    final byte[] r = new byte[TEST_BUFFER_SIZE];
    try(FSDataInputStream inputStream = fs.open(TEST_FILE_PATH, 4 * ONE_MB)) {
      int result = inputStream.read(r);

      assertNotEquals(-1, result);
      assertArrayEquals(r, b);
    }
  }


  @Test
  public void testWriteHeavyBytesToFileSyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final FileSystem.Statistics abfsStatistics;
    ExecutorService es;
    try(final FSDataOutputStream stream = fs.create(TEST_FILE_PATH)) {
      abfsStatistics = fs.getFsStatistics();
      abfsStatistics.reset();

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
    FileStatus fileStatus = fs.getFileStatus(TEST_FILE_PATH);
    long expectedWrites = (long) TEST_BUFFER_SIZE * FLUSH_TIMES;
    assertEquals("Wrong file length in " + fileStatus, expectedWrites, fileStatus.getLen());
    assertEquals("wrong bytes Written count in " + abfsStatistics,
        expectedWrites, abfsStatistics.getBytesWritten());
  }

  @Test
  public void testWriteHeavyBytesToFileAsyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    ExecutorService es = Executors.newFixedThreadPool(10);
    try(final FSDataOutputStream stream = fs.create(TEST_FILE_PATH)) {

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
    FileStatus fileStatus = fs.getFileStatus(TEST_FILE_PATH);
    assertEquals((long) TEST_BUFFER_SIZE * FLUSH_TIMES, fileStatus.getLen());
  }
}
