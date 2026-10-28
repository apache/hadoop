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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that concurrent vectored reads on a
 * {@link RawLocalFileSystem.LocalFSFileInputStream} lazily open exactly one
 * {@link AsynchronousFileChannel}, and that close() releases it.
 *
 * The test lives in this package so it can access the package-private
 * getAsyncChannel() method directly (no reflection).
 */
@Timeout(120)
public class TestRawLocalFileSystemAsyncChannel {

  private static final int THREADS = 8;
  private static final int ATTEMPTS = 3;
  private static final int FILE_SIZE = 8 * 1024;

  private final FileContextTestHelper helper = new FileContextTestHelper();

  private RawLocalFileSystem rfs;
  private byte[] fileData;
  private Path testFile;

  @BeforeEach
  public void setUp() throws IOException {
    rfs = new RawLocalFileSystem();
    rfs.initialize(rfs.getUri(), new Configuration());
    fileData = FileContextTestHelper.getFileData(FILE_SIZE / 1024, 1024);
    testFile = helper.getTestRootPath(
        FileContext.getFileContext(new Configuration()),
        "async-channel-concurrency");
    try (OutputStream out = rfs.create(testFile, true)) {
      out.write(fileData);
    }
  }

  @AfterEach
  public void tearDown() throws IOException {
    rfs.delete(testFile, true);
  }

  /**
   * N threads race on a fresh stream at a barrier, each taking the
   * readVectored code path that lazily opens the async channel. Exactly one
   * channel instance must be shared by all of them; any additional channel
   * opened by a race would be leaked, since close() only closes the field's
   * final value. Each attempt uses a fresh stream; the post-condition is
   * asserted deterministically for every attempt.
   */
  @Test
  public void testConcurrentReadVectoredOpensSingleChannel() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
        runSingleInstanceRace(pool, attempt);
      }
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private void runSingleInstanceRace(ExecutorService pool, int attempt)
      throws Exception {
    RawLocalFileSystem.LocalFSFileInputStream stream =
        rfs.new LocalFSFileInputStream(testFile);
    try {
      CyclicBarrier barrier = new CyclicBarrier(THREADS);
      List<Future<AsynchronousFileChannel>> channels = new ArrayList<>();
      for (int i = 0; i < THREADS; i++) {
        channels.add(pool.submit(() -> {
          barrier.await();
          // The lazy-open path readVectored uses; its return value is the
          // channel this invocation observed.
          return stream.getAsyncChannel();
        }));
      }

      Set<Integer> identities = new HashSet<>();
      for (Future<AsynchronousFileChannel> future : channels) {
        AsynchronousFileChannel channel = future.get(30, TimeUnit.SECONDS);
        assertNotNull(channel, "channel must not be null");
        assertTrue(channel.isOpen(),
            "channel handed out must still be open");
        identities.add(System.identityHashCode(channel));
      }
      assertEquals(1, identities.size(),
          "attempt " + attempt + ": " + THREADS + " concurrent readVectored"
              + " callers must all observe exactly one channel instance;"
              + " observed " + identities.size());

      // The single shared channel must serve a real vectored read.
      FileRange range = FileRange.createFileRange(0, fileData.length);
      stream.readVectored(Collections.singletonList(range),
          ByteBuffer::allocate);
      ByteBuffer buffer = range.getData().get(30, TimeUnit.SECONDS);
      assertEquals(ByteBuffer.wrap(fileData), buffer,
          "vectored read must return the file contents");

      // close() must not throw and must close the one channel in use;
      // a leaked second channel would stay open (invisible here), but the
      // shared one being closed proves close() saw the same instance.
      AsynchronousFileChannel shared = channels.get(0).get();
      stream.close();
      assertFalse(shared.isOpen(),
          "close() must close the channel it handed out");
    } finally {
      stream.close();
    }
  }
}
