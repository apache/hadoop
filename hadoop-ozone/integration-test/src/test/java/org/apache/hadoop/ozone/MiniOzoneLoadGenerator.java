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
import org.apache.hadoop.ozone.chaos.TestProbability;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Simple Load generator for testing.
 */
public class MiniOzoneLoadGenerator {

  static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneLoadGenerator.class);

  private static String keyNameDelimiter = "_";

  private ThreadPoolExecutor writeExecutor;
  private int numWriteThreads;
  // number of buffer to be allocated, each is allocated with length which
  // is multiple of 2, each buffer is populated with random data.
  private int numBuffers;
  private List<ByteBuffer> buffers;

  private AtomicBoolean isWriteThreadRunning;

  private final List<OzoneBucket> ozoneBuckets;

  private final AtomicInteger agedFileWrittenIndex;
  private final ExecutorService agedFileExecutor;
  private final OzoneBucket agedLoadBucket;
  private final TestProbability agedWriteProbability;

  MiniOzoneLoadGenerator(List<OzoneBucket> bucket,
                         OzoneBucket agedLoadBucket, int numThreads,
      int numBuffers) {
    this.ozoneBuckets = bucket;
    this.numWriteThreads = numThreads;
    this.numBuffers = numBuffers;
    this.writeExecutor = new ThreadPoolExecutor(numThreads, numThreads, 100,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024),
        new ThreadPoolExecutor.CallerRunsPolicy());
    this.writeExecutor.prestartAllCoreThreads();

    this.agedFileWrittenIndex = new AtomicInteger(0);
    this.agedFileExecutor = Executors.newSingleThreadExecutor();
    this.agedLoadBucket = agedLoadBucket;
    this.agedWriteProbability = TestProbability.valueOf(10);

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
      OzoneBucket bucket =
          ozoneBuckets.get((int) (Math.random() * ozoneBuckets.size()));
      try {
        int index = RandomUtils.nextInt();
        String keyName = writeData(index, bucket, threadName);

        readData(bucket, keyName);

        deleteKey(bucket, keyName);
      } catch (Exception e) {
        LOG.error("LOADGEN: Exiting due to exception", e);
        break;
      }
    }
    // This will terminate other threads too.
    isWriteThreadRunning.set(false);
    LOG.info("Terminating IO thread:{}.", threadID);
  }


  private String writeData(int keyIndex, OzoneBucket bucket, String threadName)
      throws Exception {
    // choose a random buffer.
    ByteBuffer buffer = buffers.get(keyIndex % numBuffers);
    int bufferCapacity = buffer.capacity();

    String keyName = threadName + keyNameDelimiter + keyIndex;
    try (OzoneOutputStream stream = bucket.createKey(keyName,
        bufferCapacity, ReplicationType.RATIS, ReplicationFactor.THREE,
        new HashMap<>())) {
      stream.write(buffer.array());
    } catch (Throwable t) {
      LOG.error("LOADGEN: Create key:{} failed with exception, skipping",
          keyName, t);
      throw t;
    }

    return keyName;
  }

  private void readData(OzoneBucket bucket, String keyName) throws Exception {
    int index = Integer.valueOf(keyName.split(keyNameDelimiter)[1]);


    ByteBuffer buffer = buffers.get(index % numBuffers);
    int bufferCapacity = buffer.capacity();

    try (OzoneInputStream stream = bucket.readKey(keyName)) {
      byte[] readBuffer = new byte[bufferCapacity];
      int readLen = stream.read(readBuffer);

      if (readLen < bufferCapacity) {
        throw new IOException("Read mismatch, key:" + keyName +
            " read data length:" + readLen +
            " is smaller than excepted:" + bufferCapacity);
      }

      if (!Arrays.equals(readBuffer, buffer.array())) {
        throw new IOException("Read mismatch, key:" + keyName +
            " read data does not match the written data");
      }
    } catch (Throwable t) {
      LOG.error("LOADGEN: Read key:{} failed with exception", keyName, t);
      throw t;
    }
  }

  private void deleteKey(OzoneBucket bucket, String keyName) throws Exception {
    try {
      bucket.deleteKey(keyName);
    } catch (Throwable t) {
      LOG.error("LOADGEN: Unable to delete key:{}", keyName, t);
      throw t;
    }
  }

  private String getKeyToRead() {
    int currentIndex = agedFileWrittenIndex.get();
    return currentIndex != 0 ?
        String.valueOf(RandomUtils.nextInt(0, currentIndex)): null;
  }

  private void startAgedFilesLoad(long runTimeMillis) {
    long threadID = Thread.currentThread().getId();
    LOG.info("AGED LOADGEN: Started Aged IO Thread:{}.", threadID);
    String threadName = Thread.currentThread().getName();
    long startTime = Time.monotonicNow();

    while (isWriteThreadRunning.get() &&
        (Time.monotonicNow() < startTime + runTimeMillis)) {

      String keyName = null;
      try {
        if (agedWriteProbability.isTrue()) {
          keyName = writeData(agedFileWrittenIndex.incrementAndGet(),
              agedLoadBucket, threadName);
        } else {
          keyName = getKeyToRead();
          if (keyName != null) {
            readData(agedLoadBucket, keyName);
          }
        }
      } catch (Throwable t) {
        LOG.error("AGED LOADGEN: {} Exiting due to exception", keyName, t);
        break;
      }
    }
    // This will terminate other threads too.
    isWriteThreadRunning.set(false);
    LOG.info("Terminating IO thread:{}.", threadID);
  }

  void startIO(long time, TimeUnit timeUnit) {
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

      writeFutures.add(CompletableFuture.runAsync(() ->
              startAgedFilesLoad(timeUnit.toMillis(time)), agedFileExecutor));

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
