/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.prefetch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.prefetch.BlockData;
import org.apache.hadoop.fs.impl.prefetch.BlockManagerParameters;
import org.apache.hadoop.fs.impl.prefetch.BlockManager;
import org.apache.hadoop.fs.impl.prefetch.BufferData;
import org.apache.hadoop.fs.impl.prefetch.ExecutorServiceFuturePool;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_PREFETCH_MAX_BLOCKS_COUNT;
import static org.apache.hadoop.fs.s3a.Constants.HADOOP_TMP_DIR;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_MAX_BLOCKS_COUNT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;

/**
 * Tests to perform read from S3ACachingBlockManager.
 */
public class TestS3ACachingBlockManager extends AbstractHadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestS3ACachingBlockManager.class);

  static final int FILE_SIZE = 15;

  static final int BLOCK_SIZE = 2;

  static final int POOL_SIZE = 3;

  private ExecutorService threadPool;

  private ExecutorServiceFuturePool futurePool;

  /**
   * only used for logging.
   */
  private Path testPath;

  private final S3AInputStreamStatistics streamStatistics =
      new EmptyS3AStatisticsContext().newInputStreamStatistics();

  private final BlockData blockData = new BlockData(FILE_SIZE, BLOCK_SIZE);

  @Before
  public void setup() {
    threadPool = Executors.newFixedThreadPool(4);
    futurePool = new ExecutorServiceFuturePool(threadPool);
    testPath = new Path("/");
  }

  @After
  public void teardown() {
    if (threadPool != null) {
      threadPool.shutdown();
    }
  }

  private static final Configuration CONF =
      S3ATestUtils.prepareTestConfiguration(new Configuration());

  @Test
  public void testFuturePoolNull() throws Exception {
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    Configuration conf = new Configuration();
    BlockManagerParameters blockManagerParams =
        new BlockManagerParameters()
            .withBlockData(blockData)
            .withBufferPoolSize(POOL_SIZE)
            .withConf(conf)
            .withPath(testPath)
            .withPrefetchingStatistics(streamStatistics);
    try (S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File)) {

      intercept(NullPointerException.class,
          () -> new S3ACachingBlockManager(blockManagerParams, reader));
    }
  }

  @Test
  public void testNullReader() throws Exception {
    Configuration conf = new Configuration();
    BlockManagerParameters blockManagerParams =
        new BlockManagerParameters()
            .withBlockData(blockData)
            .withBufferPoolSize(POOL_SIZE)
            .withConf(conf)
            .withFuturePool(futurePool)
            .withMaxBlocksCount(
                conf.getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT))
            .withPath(testPath)
            .withPrefetchingStatistics(streamStatistics);


    intercept(IllegalArgumentException.class, "'reader' must not be null",
        () -> new S3ACachingBlockManager(blockManagerParams, null));
  }

  @Test
  public void testNullBlockData() throws Exception {
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    Configuration conf = new Configuration();
    try (S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File)) {
      BlockManagerParameters blockManagerParams =
          new BlockManagerParameters()
              .withFuturePool(futurePool)
              .withBufferPoolSize(POOL_SIZE)
              .withPrefetchingStatistics(streamStatistics)
              .withConf(conf);

      intercept(IllegalArgumentException.class, "'blockData' must not be null",
          () -> new S3ACachingBlockManager(blockManagerParams, reader));
    }
  }

  @Test
  public void testNonPositiveBufferPoolSize() throws Exception {
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    Configuration conf = new Configuration();
    try (S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File)) {
      BlockManagerParameters blockManagerParams =
          new BlockManagerParameters()
              .withFuturePool(futurePool)
              .withBlockData(blockData)
              .withBufferPoolSize(0)
              .withPrefetchingStatistics(streamStatistics)
              .withConf(conf);

      intercept(IllegalArgumentException.class, "'bufferPoolSize' must be a positive integer",
          () -> new S3ACachingBlockManager(blockManagerParams, reader));

      BlockManagerParameters blockManagerParamsWithNegativeSize =
          new BlockManagerParameters()
              .withFuturePool(futurePool)
              .withBlockData(blockData)
              .withBufferPoolSize(-1)
              .withPrefetchingStatistics(streamStatistics)
              .withConf(conf);

      intercept(IllegalArgumentException.class, "'bufferPoolSize' must be a positive integer",
          () -> new S3ACachingBlockManager(blockManagerParamsWithNegativeSize, reader));
    }
  }

  @Test
  public void testNullPrefetchingStatistics() throws Exception {
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    Configuration conf = new Configuration();
    try (S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File)) {

      BlockManagerParameters blockManagerParamsBuilder7 =
          new BlockManagerParameters()
              .withFuturePool(futurePool)
              .withBlockData(blockData)
              .withBufferPoolSize(POOL_SIZE)
              .withConf(conf);

      intercept(NullPointerException.class,
          () -> new S3ACachingBlockManager(blockManagerParamsBuilder7, reader));
    }
  }

  @Test
  public void testArgChecks() throws Exception {
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File);

    Configuration conf = new Configuration();
    BlockManagerParameters blockManagerParams =
        new BlockManagerParameters()
            .withBlockData(blockData)
            .withBufferPoolSize(POOL_SIZE)
            .withConf(conf)
            .withFuturePool(futurePool)
            .withMaxBlocksCount(
                conf.getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT))
            .withPath(testPath)
            .withPrefetchingStatistics(streamStatistics);


    // Should not throw.
    S3ACachingBlockManager blockManager =
        new S3ACachingBlockManager(blockManagerParams, reader);

    intercept(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> blockManager.get(-1));

    intercept(
        IllegalArgumentException.class,
        "'data' must not be null",
        () -> blockManager.release(null));

    intercept(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> blockManager.requestPrefetch(-1));

    intercept(
        IllegalArgumentException.class,
        "'data' must not be null",
        () -> blockManager.requestCaching(null));
  }

  /**
   * Extends S3ACachingBlockManager so that we can inject asynchronous failures.
   */
  private static final class BlockManagerForTesting
      extends S3ACachingBlockManager {

    BlockManagerForTesting(BlockManagerParameters blockManagerParameters,
        S3ARemoteObjectReader reader) {
      super(blockManagerParameters, reader);
    }

    // If true, forces the next read operation to fail.
    // Resets itself to false after one failure.
    private boolean forceNextReadToFail;

    @Override
    public int read(ByteBuffer buffer, long offset, int size)
        throws IOException {
      if (forceNextReadToFail) {
        forceNextReadToFail = false;
        throw new RuntimeException("foo");
      } else {
        return super.read(buffer, offset, size);
      }
    }

    // If true, forces the next cache-put operation to fail.
    // Resets itself to false after one failure.
    private boolean forceNextCachePutToFail;

    @Override
    protected void cachePut(int blockNumber,
        ByteBuffer buffer) throws IOException {
      if (forceNextCachePutToFail) {
        LOG.debug("Forcing put {} to fail", blockNumber);
        forceNextCachePutToFail = false;
        throw new RuntimeException("bar");
      } else {
        super.cachePut(blockNumber, buffer);
      }
    }

    public Configuration getConf() {
      return CONF;
    }
  }

  // @Ignore
  @Test
  public void testGet() throws Exception {
    testGetHelper(false);
  }

  // @Ignore
  @Test
  public void testGetFailure() throws Exception {
    testGetHelper(true);
  }

  private void testGetHelper(boolean forceReadFailure) throws Exception {
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, true);
    S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File);
    BlockManagerParameters blockManagerParams = getBlockManagerParameters();
    BlockManagerForTesting blockManager =
        new BlockManagerForTesting(blockManagerParams, reader);

    for (int b = 0; b < blockData.getNumBlocks(); b++) {
      // We simulate caching failure for all even numbered blocks.
      boolean forceFailure = forceReadFailure && (b % 2 == 0);

      BufferData data = null;

      if (forceFailure) {
        blockManager.forceNextReadToFail = true;

        intercept(
            RuntimeException.class,
            "foo",
            () -> blockManager.get(3));
      } else {
        data = blockManager.get(b);

        long startOffset = blockData.getStartOffset(b);
        for (int i = 0; i < blockData.getSize(b); i++) {
          assertEquals(startOffset + i, data.getBuffer().get());
        }

        blockManager.release(data);
      }

      assertEquals(POOL_SIZE, blockManager.numAvailable());
    }
  }

  // @Ignore
  @Test
  public void testPrefetch() throws IOException, InterruptedException {
    testPrefetchHelper(false);
  }

  // @Ignore
  @Test
  public void testPrefetchFailure() throws IOException, InterruptedException {
    testPrefetchHelper(true);
  }

  private void testPrefetchHelper(boolean forcePrefetchFailure)
      throws IOException, InterruptedException {
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File);
    BlockManagerParameters blockManagerParams = getBlockManagerParameters();
    BlockManagerForTesting blockManager =
        new BlockManagerForTesting(blockManagerParams, reader);
    assertInitialState(blockManager);

    int expectedNumErrors = 0;
    int expectedNumSuccesses = 0;

    for (int b = 0; b < POOL_SIZE; b++) {
      // We simulate caching failure for all odd numbered blocks.
      boolean forceFailure = forcePrefetchFailure && (b % 2 == 1);
      if (forceFailure) {
        expectedNumErrors++;
        blockManager.forceNextReadToFail = true;
      } else {
        expectedNumSuccesses++;
      }
      blockManager.requestPrefetch(b);
    }

    assertNumCached(blockManager, 0);

    // random IO prefetches will still complete; close/unbuffer will not
    // add the result to the cache.
    blockManager.cancelPrefetches(BlockManager.CancelReason.RandomIO);
    waitForCaching(blockManager, expectedNumSuccesses);
    assertErrorCount(blockManager, expectedNumErrors);
    assertNumCached(blockManager, expectedNumSuccesses);

  }

  private BlockManagerParameters getBlockManagerParameters() {
    return new BlockManagerParameters()
        .withBlockData(blockData)
        .withBufferPoolSize(POOL_SIZE)
        .withConf(CONF)
        .withFuturePool(futurePool)
        .withLocalDirAllocator(new LocalDirAllocator(HADOOP_TMP_DIR))
        .withMaxBlocksCount(
            CONF.getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT))
        .withPath(testPath)
        .withPrefetchingStatistics(streamStatistics);
  }

  // @Ignore
  @Test
  public void testCachingOfPrefetched()
      throws IOException, InterruptedException {
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File);
    Configuration conf = new Configuration();
    BlockManagerParameters blockManagerParamsBuilder =
        new BlockManagerParameters()
            .withBlockData(blockData)
            .withBufferPoolSize(POOL_SIZE)
            .withConf(conf)
            .withFuturePool(futurePool)
            .withLocalDirAllocator(
                new LocalDirAllocator(conf.get(BUFFER_DIR) != null ? BUFFER_DIR : HADOOP_TMP_DIR))
            .withMaxBlocksCount(
                conf.getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT))
            .withPath(testPath)
            .withPrefetchingStatistics(streamStatistics);

    S3ACachingBlockManager blockManager =
        new S3ACachingBlockManager(blockManagerParamsBuilder, reader);
    assertInitialState(blockManager);

    for (int b = 0; b < blockData.getNumBlocks(); b++) {
      blockManager.requestPrefetch(b);
      BufferData data = blockManager.get(b);
      blockManager.requestCaching(data);
    }

    final int expected = Math.min(blockData.getNumBlocks(),
        conf.getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT));
    waitForCaching(blockManager, expected);
    assertNumCached(blockManager, expected);
    assertErrorCount(blockManager, 0);
  }

  private static void assertNumCached(final S3ACachingBlockManager blockManager,
      final int expected) {
    Assertions.assertThat(blockManager.numCached())
        .describedAs("blockManager.numCached() in %s", blockManager)
        .isEqualTo(expected);
  }

  // @Ignore
  @Test
  public void testCachingOfGet() throws IOException, InterruptedException {
    testCachingOfGetHelper(false);
  }

  // @Ignore
  @Test
  public void testCachingFailureOfGet()
      throws IOException, InterruptedException {
    testCachingOfGetHelper(true);
  }

  public void testCachingOfGetHelper(boolean forceCachingFailure)
      throws IOException, InterruptedException {
    MockS3ARemoteObject s3File = new MockS3ARemoteObject(FILE_SIZE, false);
    S3ARemoteObjectReader reader = new S3ARemoteObjectReader(s3File);
    BlockManagerParameters blockManagerParams = getBlockManagerParameters();
    BlockManagerForTesting blockManager =
        new BlockManagerForTesting(blockManagerParams, reader);
    assertInitialState(blockManager);

    int expectedNumErrors = 0;
    int expectedNumSuccesses = 0;

    for (int b = 0; b < blockData.getNumBlocks(); b++) {
      // We simulate caching failure for all odd numbered blocks.
      boolean forceFailure = forceCachingFailure && (b % 2 == 1);
      if (forceFailure) {
        expectedNumErrors++;
      } else {
        expectedNumSuccesses++;
      }

      BufferData data = blockManager.get(b);
      if (forceFailure) {
        blockManager.forceNextCachePutToFail = true;
      }

      blockManager.requestCaching(data);
      waitForCaching(blockManager, Math.min(expectedNumSuccesses, blockManager.getConf()
          .getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT)));
      assertEquals(Math.min(expectedNumSuccesses, blockManager.getConf()
              .getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT)),
          blockManager.numCached());

      if (forceCachingFailure) {
        assertErrorCount(blockManager, expectedNumErrors);
      } else {
        assertErrorCount(blockManager, 0);
      }
    }
  }

  private void assertErrorCount(final S3ACachingBlockManager blockManager, final int expected) {
    Assertions.assertThat(totalErrors(blockManager))
        .describedAs("Total errors of %s", blockManager)
        .isEqualTo(expected);
  }

  private void waitForCaching(
      S3ACachingBlockManager blockManager,
      int expectedCount)
      throws InterruptedException {
    // Wait for async cache operation to be over.
    int numTrys = 0;
    int count;
    do {
      Thread.sleep(100);
      count = blockManager.numCached();
      numTrys++;
      if (numTrys > 600) {
        String message = String.format(
            "waitForCaching: expected: %d, actual: %d, read errors: %d, caching errors: %d in %s",
            expectedCount, count, blockManager.numReadErrors(),
            blockManager.numCachingErrors(),
            blockManager);
        throw new IllegalStateException(message);
      }
    }
    while (count < expectedCount);
  }

  private int totalErrors(S3ACachingBlockManager blockManager) {
    return blockManager.numCachingErrors() + blockManager.numReadErrors();
  }

  private void assertInitialState(S3ACachingBlockManager blockManager) {
    assertNumCached(blockManager, 0);
  }
}
