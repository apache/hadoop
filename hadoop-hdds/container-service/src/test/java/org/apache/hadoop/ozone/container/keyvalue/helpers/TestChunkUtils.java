/*
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
package org.apache.hadoop.ozone.container.keyvalue.helpers;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for {@link ChunkUtils}.
 */
public class TestChunkUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestChunkUtils.class);

  private static final String PREFIX = TestChunkUtils.class.getSimpleName();

  @Test
  public void concurrentReadOfSameFile() throws Exception {
    String s = "Hello World";
    byte[] array = s.getBytes();
    ByteBuffer data = ByteBuffer.wrap(array);
    Path tempFile = Files.createTempFile(PREFIX, "concurrent");
    try {
      ChunkInfo chunkInfo = new ChunkInfo(tempFile.toString(),
          0, data.capacity());
      File file = tempFile.toFile();
      VolumeIOStats stats = new VolumeIOStats();
      ChunkUtils.writeData(file, chunkInfo, data, stats, true);
      int threads = 10;
      ExecutorService executor = new ThreadPoolExecutor(threads, threads,
          0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
      AtomicInteger processed = new AtomicInteger();
      AtomicBoolean failed = new AtomicBoolean();
      for (int i = 0; i < threads; i++) {
        final int threadNumber = i;
        executor.submit(() -> {
          try {
            ByteBuffer readBuffer = ChunkUtils.readData(file, chunkInfo, stats);
            LOG.info("Read data ({}): {}", threadNumber,
                new String(readBuffer.array()));
            if (!Arrays.equals(array, readBuffer.array())) {
              failed.set(true);
            }
          } catch (Exception e) {
            LOG.error("Failed to read data ({})", threadNumber, e);
            failed.set(true);
          }
          processed.incrementAndGet();
        });
      }
      try {
        GenericTestUtils.waitFor(() -> processed.get() == threads,
            100, (int) TimeUnit.SECONDS.toMillis(5));
      } finally {
        executor.shutdownNow();
      }
      assertEquals(threads * stats.getWriteBytes(), stats.getReadBytes());
      assertFalse(failed.get());
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void concurrentProcessing() throws Exception {
    final int perThreadWait = 1000;
    final int maxTotalWait = 5000;
    int threads = 20;
    List<Path> paths = new LinkedList<>();

    try {
      ExecutorService executor = new ThreadPoolExecutor(threads, threads,
          0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
      AtomicInteger processed = new AtomicInteger();
      for (int i = 0; i < threads; i++) {
        Path path = Files.createTempFile(PREFIX, String.valueOf(i));
        paths.add(path);
        executor.submit(() -> {
          ChunkUtils.processFileExclusively(path, () -> {
            try {
              Thread.sleep(perThreadWait);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            processed.incrementAndGet();
            return null;
          });
        });
      }
      try {
        GenericTestUtils.waitFor(() -> processed.get() == threads,
            100, maxTotalWait);
      } finally {
        executor.shutdownNow();
      }
    } finally {
      for (Path path : paths) {
        FileUtils.deleteQuietly(path.toFile());
      }
    }
  }

  @Test
  public void serialRead() throws Exception {
    String s = "Hello World";
    byte[] array = s.getBytes();
    ByteBuffer data = ByteBuffer.wrap(array);
    Path tempFile = Files.createTempFile(PREFIX, "serial");
    try {
      ChunkInfo chunkInfo = new ChunkInfo(tempFile.toString(),
          0, data.capacity());
      File file = tempFile.toFile();
      VolumeIOStats stats = new VolumeIOStats();
      ChunkUtils.writeData(file, chunkInfo, data, stats, true);
      ByteBuffer readBuffer = ChunkUtils.readData(file, chunkInfo, stats);
      assertArrayEquals(array, readBuffer.array());
      assertEquals(stats.getWriteBytes(), stats.getReadBytes());
    } catch (Exception e) {
      LOG.error("Failed to read data", e);
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

}
