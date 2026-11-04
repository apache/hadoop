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
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.List;
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests that concurrent callers of {@code getAsyncChannel()} on a
 * {@link RawLocalFileSystem.LocalFSFileInputStream}, which readVectored
 * uses to open its channel lazily, share one {@link AsynchronousFileChannel},
 * and that close() closes it.
 */
@Timeout(120)
public class TestRawLocalFileSystemAsyncChannel {

  private static final int THREADS = 8;
  private static final int ATTEMPTS = 3;

  private final FileContextTestHelper helper = new FileContextTestHelper();

  private RawLocalFileSystem rfs;
  private Path testFile;

  @BeforeEach
  public void setUp() throws IOException {
    rfs = new RawLocalFileSystem();
    rfs.initialize(rfs.getUri(), new Configuration());
    testFile = helper.getTestRootPath(
        FileContext.getFileContext(new Configuration()), "async-channel");
    try (OutputStream out = rfs.create(testFile, true)) {
      out.write(new byte[1024]);
    }
  }

  @AfterEach
  public void tearDown() throws IOException {
    rfs.delete(testFile, true);
  }

  /**
   * Threads released together from a barrier open the channel of a fresh
   * stream. All of them must get the same channel, since close() only
   * closes the one it holds. Whether the race shows up without the fix
   * depends on thread scheduling, so the race is run several times.
   */
  @Test
  public void testConcurrentGetAsyncChannelOpensSingleChannel()
      throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
        runRace(pool);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private void runRace(ExecutorService pool) throws Exception {
    AsynchronousFileChannel first;
    try (RawLocalFileSystem.LocalFSFileInputStream stream =
        rfs.new LocalFSFileInputStream(testFile)) {
      CyclicBarrier barrier = new CyclicBarrier(THREADS);
      List<Future<AsynchronousFileChannel>> channels = new ArrayList<>();
      for (int i = 0; i < THREADS; i++) {
        channels.add(pool.submit(() -> {
          barrier.await();
          return stream.getAsyncChannel();
        }));
      }
      first = channels.get(0).get(30, TimeUnit.SECONDS);
      for (Future<AsynchronousFileChannel> channel : channels) {
        assertSame(first, channel.get(30, TimeUnit.SECONDS));
      }
    }
    assertFalse(first.isOpen(), "close() must close the channel");
  }
}
