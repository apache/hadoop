/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Simple Load generator for testing.
 */
public class MiniOzoneLoadGenerator {

  static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneLoadGenerator.class);

  private ThreadPoolExecutor writeExecutor;
  private int numWriteThreads;
  // number of buffer to be allocated, each is allocated with length which
  // is multiple of 2, each buffer is populated with random data.
  private int numBuffers;
  private List<ByteBuffer> buffers;

  private AtomicBoolean isWriteThreadRunning;

  private final List<OzoneBucket> ozoneBuckets;

  MiniOzoneLoadGenerator(List<OzoneBucket> bucket, int numThreads,
      int numBuffers) {
    this.ozoneBuckets = bucket;
    this.numWriteThreads = numThreads;
    this.numBuffers = numBuffers;
    this.writeExecutor = new ThreadPoolExecutor(numThreads, numThreads, 100,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024),
        new ThreadPoolExecutor.CallerRunsPolicy());
    this.writeExecutor.prestartAllCoreThreads();

    this.isWriteThreadRunning = new AtomicBoolean(false);

    // allocate buffers and populate random data.
    buffers = new ArrayList<>();
    for (int i = 0; i < numBuffers; i++) {
      int size = (int) StorageUnit.KB.toBytes(1 << i);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      buffer.put(RandomUtils.nextBytes(size));
      buffers.add(buffer);
    }
  }

  // Start IO load on an Ozone bucket.
  private void load(long runTimeMillis) {
    long threadID = Thread.currentThread().getId();
    LOG.info("Started IO Thread:{}.", threadID);
    String threadName = Thread.currentThread().getName();
    long startTime = Time.monotonicNow();

    while (isWriteThreadRunning.get() &&
        (Time.monotonicNow() < startTime + runTimeMillis)) {
      // choose a random buffer.
      int index = RandomUtils.nextInt();
      ByteBuffer buffer = buffers.get(index % numBuffers);
      int bufferCapacity = buffer.capacity();

      String keyName = threadName + "-" + index;
      OzoneBucket bucket =
          ozoneBuckets.get((int) (Math.random() * ozoneBuckets.size()));
      try (OzoneOutputStream stream = bucket.createKey(keyName,
          bufferCapacity, ReplicationType.RATIS, ReplicationFactor.THREE,
          new HashMap<>())) {
        stream.write(buffer.array());
      } catch (Exception e) {
        LOG.error("LOADGEN: Create key:{} failed with exception, skipping",
            keyName, e);
        continue;
        // TODO: HDDS-1403.A key write can fail after multiple block writes
        //  to closed container. add a break here once that is fixed.
      }

      try (OzoneInputStream stream = bucket.readKey(keyName)) {
        byte[] readBuffer = new byte[bufferCapacity];
        int readLen = stream.read(readBuffer);

        if (readLen < bufferCapacity) {
          LOG.error("LOADGEN: Read mismatch, key:{} read data length:{} is " +
              "smaller than excepted:{}", keyName, readLen, bufferCapacity);
          break;
        }

        if (!Arrays.equals(readBuffer, buffer.array())) {
          LOG.error("LOADGEN: Read mismatch, key:{} Read data does not match " +
              "the written data", keyName);
          break;
        }

      } catch (Exception e) {
        LOG.error("LOADGEN: Read key:{} failed with exception", keyName, e);
        break;
      }

    }
    // This will terminate other threads too.
    isWriteThreadRunning.set(false);
    LOG.info("Terminating IO thread:{}.", threadID);
  }

  public void startIO(long time, TimeUnit timeUnit) {
    List<CompletableFuture<Void>> writeFutures = new ArrayList<>();
    LOG.info("Starting MiniOzoneLoadGenerator for time {}:{} with {} buffers " +
            "and {} threads", time, timeUnit, numBuffers, numWriteThreads);
    if (isWriteThreadRunning.compareAndSet(false, true)) {
      // Start the IO thread
      for (int i = 0; i < numWriteThreads; i++) {
        writeFutures.add(
            CompletableFuture.runAsync(() -> load(timeUnit.toMillis(time)),
                writeExecutor));
      }

      // Wait for IO to complete
      for (CompletableFuture<Void> f : writeFutures) {
        try {
          f.get();
        } catch (Throwable t) {
          LOG.error("startIO failed with exception", t);
        }
      }
    }
  }

  public void shutdownLoadGenerator() {
    try {
      writeExecutor.shutdown();
      writeExecutor.awaitTermination(1, TimeUnit.DAYS);
    } catch (Exception e) {
      LOG.error("error while closing ", e);
    }
  }
}
