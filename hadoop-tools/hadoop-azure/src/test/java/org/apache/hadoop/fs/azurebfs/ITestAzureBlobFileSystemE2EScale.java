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
 * Test end to end between ABFS client and ABFS server with heavy traffic.
 */
public class ITestAzureBlobFileSystemE2EScale extends
    AbstractAbfsScaleTest {
  private static final int TEN = 10;
  private static final int ONE_THOUSAND = 1000;
  private static final int BASE_SIZE = 1024;
  private static final int ONE_MB = 1024 * 1024;
  private static final int DEFAULT_WRITE_TIMES = 100;
  private static final Path TEST_FILE = new Path("ITestAzureBlobFileSystemE2EScale");

  public ITestAzureBlobFileSystemE2EScale() {
  }

  @Test
  public void testWriteHeavyBytesToFileAcrossThreads() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final FSDataOutputStream stream = fs.create(TEST_FILE);
    ExecutorService es = Executors.newFixedThreadPool(TEN);

    int testWriteBufferSize = 2 * TEN * ONE_THOUSAND * BASE_SIZE;
    final byte[] b = new byte[testWriteBufferSize];
    new Random().nextBytes(b);
    List<Future<Void>> tasks = new ArrayList<>();

    int operationCount = DEFAULT_WRITE_TIMES;
    for (int i = 0; i < operationCount; i++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          stream.write(b);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    tasks.clear();
    stream.close();

    es.shutdownNow();
    FileStatus fileStatus = fs.getFileStatus(TEST_FILE);
    assertEquals(testWriteBufferSize * operationCount, fileStatus.getLen());
  }

  @Test
  public void testReadWriteHeavyBytesToFileWithStatistics() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final FileSystem.Statistics abfsStatistics;
    int testBufferSize;
    final byte[] sourceData;
    try (FSDataOutputStream stream = fs.create(TEST_FILE)) {
      abfsStatistics = fs.getFsStatistics();
      abfsStatistics.reset();

      testBufferSize = 5 * TEN * ONE_THOUSAND * BASE_SIZE;
      sourceData = new byte[testBufferSize];
      new Random().nextBytes(sourceData);
      stream.write(sourceData);
    }

    final byte[] remoteData = new byte[testBufferSize];
    int bytesRead;
    try (FSDataInputStream inputStream = fs.open(TEST_FILE, 4 * ONE_MB)) {
      bytesRead = inputStream.read(remoteData);
    }

    String stats = abfsStatistics.toString();
    assertEquals("Bytes read in " + stats,
        remoteData.length, abfsStatistics.getBytesRead());
    assertEquals("bytes written in " + stats,
        sourceData.length, abfsStatistics.getBytesWritten());
    assertEquals("bytesRead from read() call", testBufferSize, bytesRead);
    assertArrayEquals("round tripped data", sourceData, remoteData);

  }
}
