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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.impl.prefetch.BlockCache;
import org.apache.hadoop.fs.impl.prefetch.BlockData;
import org.apache.hadoop.fs.impl.prefetch.ExecutorServiceFuturePool;
import org.apache.hadoop.fs.impl.prefetch.SingleFilePerBlockCache;
import org.apache.hadoop.fs.impl.prefetch.Validate;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.VectoredIOContext;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.s3a.statistics.impl.CountingChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

import static org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR;
import static org.apache.hadoop.fs.s3a.Constants.HADOOP_TMP_DIR;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.emptyStatisticsStore;

/**
 * Provides 'fake' implementations of S3ARemoteInputStream variants.
 *
 * These implementations avoid accessing the following real resources:
 * -- S3 store
 * -- local filesystem
 *
 * This arrangement allows thorough multi-threaded testing of those
 * implementations without accessing external resources. It also helps
 * avoid test flakiness introduced by external factors.
 */
public final class S3APrefetchFakes {

  private S3APrefetchFakes() {
  }

  public static final String E_TAG = "eTag";

  public static final String OWNER = "owner";

  public static final String VERSION_ID = "v1";

  public static final long MODIFICATION_TIME = 0L;

  private static final Configuration CONF = new Configuration();

  public static final ChangeDetectionPolicy CHANGE_POLICY =
      ChangeDetectionPolicy.createPolicy(
          ChangeDetectionPolicy.Mode.None,
          ChangeDetectionPolicy.Source.None,
          false);

  public static S3AFileStatus createFileStatus(String key, long fileSize) {
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(key);
    long blockSize = fileSize;
    return new S3AFileStatus(
        fileSize, MODIFICATION_TIME, path, blockSize, OWNER, E_TAG, VERSION_ID);
  }

  public static S3ObjectAttributes createObjectAttributes(
      String bucket,
      String key,
      long fileSize) {

    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(key);
    String encryptionKey = "";

    return new S3ObjectAttributes(
        bucket,
        path,
        key,
        S3AEncryptionMethods.NONE,
        encryptionKey,
        E_TAG,
        VERSION_ID,
        fileSize);
  }

  public static S3AReadOpContext createReadContext(
      ExecutorServiceFuturePool futurePool,
      String key,
      int fileSize,
      int prefetchBlockSize,
      int prefetchBlockCount) {

    S3AFileStatus fileStatus = createFileStatus(key, fileSize);
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(key);
    FileSystem.Statistics statistics = new FileSystem.Statistics("s3a");
    S3AStatisticsContext statisticsContext = new EmptyS3AStatisticsContext();
    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(3, 10,
            TimeUnit.MILLISECONDS);

    return new S3AReadOpContext(
        path,
        new Invoker(retryPolicy, Invoker.LOG_EVENT),
        statistics,
        statisticsContext,
        fileStatus,
        new VectoredIOContext()
            .setMinSeekForVectoredReads(1)
            .setMaxReadSizeForVectoredReads(1)
            .build(),
        emptyStatisticsStore(),
        futurePool,
        prefetchBlockSize,
        prefetchBlockCount)
        .withChangeDetectionPolicy(
            ChangeDetectionPolicy.createPolicy(ChangeDetectionPolicy.Mode.None,
                ChangeDetectionPolicy.Source.ETag, false))
        .withInputPolicy(S3AInputPolicy.Normal);
  }

  public static URI createUri(String bucket, String key) {
    return URI.create(String.format("s3a://%s/%s", bucket, key));
  }

  public static ChangeTracker createChangeTracker(
      String bucket,
      String key,
      long fileSize) {

    return new ChangeTracker(
        createUri(bucket, key).toString(),
        CHANGE_POLICY,
        new CountingChangeTracker(),
        createObjectAttributes(bucket, key, fileSize));
  }

  public static S3ObjectInputStream createS3ObjectInputStream(byte[] buffer) {
    return new S3ObjectInputStream(new ByteArrayInputStream(buffer), null);
  }

  public static S3AInputStream.InputStreamCallbacks createInputStreamCallbacks(
      String bucket,
      String key) {

    S3Object object = new S3Object() {
      @Override
      public S3ObjectInputStream getObjectContent() {
        return createS3ObjectInputStream(new byte[8]);
      }

      @Override
      public ObjectMetadata getObjectMetadata() {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader("ETag", E_TAG);
        return metadata;
      }
    };

    return new S3AInputStream.InputStreamCallbacks() {
      @Override
      public S3Object getObject(GetObjectRequest request) {
        return object;
      }

      @Override
      public <T> CompletableFuture<T> submit(CallableRaisingIOE<T> operation) {
        return null;
      }

      @Override
      public GetObjectRequest newGetRequest(String key) {
        return new GetObjectRequest(bucket, key);
      }

      @Override
      public void close() {
      }
    };
  }


  public static S3ARemoteInputStream createInputStream(
      Class<? extends S3ARemoteInputStream> clazz,
      ExecutorServiceFuturePool futurePool,
      String bucket,
      String key,
      int fileSize,
      int prefetchBlockSize,
      int prefetchBlockCount) {

    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(key);

    S3AFileStatus fileStatus = createFileStatus(key, fileSize);
    S3ObjectAttributes s3ObjectAttributes =
        createObjectAttributes(bucket, key, fileSize);
    S3AReadOpContext s3AReadOpContext = createReadContext(
        futurePool,
        key,
        fileSize,
        prefetchBlockSize,
        prefetchBlockCount);

    S3AInputStream.InputStreamCallbacks callbacks =
        createInputStreamCallbacks(bucket, key);
    S3AInputStreamStatistics stats =
        s3AReadOpContext.getS3AStatisticsContext().newInputStreamStatistics();

    if (clazz == FakeS3AInMemoryInputStream.class) {
      return new FakeS3AInMemoryInputStream(s3AReadOpContext,
          s3ObjectAttributes, callbacks, stats);
    } else if (clazz == FakeS3ACachingInputStream.class) {
      return new FakeS3ACachingInputStream(s3AReadOpContext, s3ObjectAttributes,
          callbacks, stats);
    }

    throw new RuntimeException("Unsupported class: " + clazz);
  }

  public static FakeS3AInMemoryInputStream createS3InMemoryInputStream(
      ExecutorServiceFuturePool futurePool,
      String bucket,
      String key,
      int fileSize) {

    return (FakeS3AInMemoryInputStream) createInputStream(
        FakeS3AInMemoryInputStream.class, futurePool, bucket, key, fileSize, 1,
        1);
  }

  public static FakeS3ACachingInputStream createS3CachingInputStream(
      ExecutorServiceFuturePool futurePool,
      String bucket,
      String key,
      int fileSize,
      int prefetchBlockSize,
      int prefetchBlockCount) {

    return (FakeS3ACachingInputStream) createInputStream(
        FakeS3ACachingInputStream.class,
        futurePool,
        bucket,
        key,
        fileSize,
        prefetchBlockSize,
        prefetchBlockCount);
  }

  public static class FakeS3AInMemoryInputStream
      extends S3AInMemoryInputStream {

    public FakeS3AInMemoryInputStream(
        S3AReadOpContext context,
        S3ObjectAttributes s3Attributes,
        S3AInputStream.InputStreamCallbacks client,
        S3AInputStreamStatistics streamStatistics) {
      super(context, s3Attributes, client, streamStatistics);
    }

    @Override
    protected S3ARemoteObject getS3File() {
      randomDelay(200);
      return new MockS3ARemoteObject(
          (int) this.getS3ObjectAttributes().getLen(), false);
    }
  }

  public static class FakeS3FilePerBlockCache extends SingleFilePerBlockCache {

    private final Map<Path, byte[]> files;

    private final int readDelay;

    private final int writeDelay;

    public FakeS3FilePerBlockCache(int readDelay, int writeDelay) {
      super(new EmptyS3AStatisticsContext().newInputStreamStatistics(),
          Constants.DEFAULT_PREFETCH_MAX_BLOCKS_COUNT);
      this.files = new ConcurrentHashMap<>();
      this.readDelay = readDelay;
      this.writeDelay = writeDelay;
    }

    @Override
    protected int readFile(Path path, ByteBuffer buffer) {
      byte[] source = this.files.get(path);
      randomDelay(this.readDelay);
      buffer.put(source);
      return source.length;
    }

    @Override
    protected void writeFile(Path path, ByteBuffer buffer) throws IOException {
      Validate.checkPositiveInteger(buffer.limit(), "buffer.limit()");
      byte[] dest = new byte[buffer.limit()];
      randomDelay(this.writeDelay);
      buffer.rewind();
      buffer.get(dest);
      this.files.put(path, dest);
    }

    private long fileCount = 0;

    @Override
    protected Path getCacheFilePath(final Configuration conf,
        final LocalDirAllocator localDirAllocator)
        throws IOException {
      fileCount++;
      return Paths.get(Long.toString(fileCount));
    }

    @Override
    public void close() throws IOException {
      this.files.clear();
    }
  }

  private static final Random RANDOM = new Random();

  private static void randomDelay(int delay) {
    try {
      Thread.sleep(RANDOM.nextInt(delay));
    } catch (InterruptedException e) {

    }
  }

  public static class FakeS3ACachingBlockManager
      extends S3ACachingBlockManager {

    public FakeS3ACachingBlockManager(
        ExecutorServiceFuturePool futurePool,
        S3ARemoteObjectReader reader,
        BlockData blockData,
        int bufferPoolSize,
        Configuration conf,
        LocalDirAllocator localDirAllocator) {
      super(futurePool, reader, blockData, bufferPoolSize,
          new EmptyS3AStatisticsContext().newInputStreamStatistics(),
          conf, localDirAllocator);
    }

    @Override
    public int read(ByteBuffer buffer, long offset, int size)
        throws IOException {
      randomDelay(100);
      return this.getReader().read(buffer, offset, size);
    }

    @Override
    protected BlockCache createCache(int maxBlocksCount) {
      final int readDelayMs = 50;
      final int writeDelayMs = 200;
      return new FakeS3FilePerBlockCache(readDelayMs, writeDelayMs);
    }
  }

  public static class FakeS3ACachingInputStream extends S3ACachingInputStream {

    public FakeS3ACachingInputStream(
        S3AReadOpContext context,
        S3ObjectAttributes s3Attributes,
        S3AInputStream.InputStreamCallbacks client,
        S3AInputStreamStatistics streamStatistics) {
      super(context, s3Attributes, client, streamStatistics, CONF,
          new LocalDirAllocator(
              CONF.get(BUFFER_DIR) != null ? BUFFER_DIR : HADOOP_TMP_DIR));
    }

    @Override
    protected S3ARemoteObject getS3File() {
      randomDelay(200);
      return new MockS3ARemoteObject(
          (int) this.getS3ObjectAttributes().getLen(), false);
    }

    @Override
    protected S3ACachingBlockManager createBlockManager(
        ExecutorServiceFuturePool futurePool,
        S3ARemoteObjectReader reader,
        BlockData blockData,
        int bufferPoolSize,
        Configuration conf,
        LocalDirAllocator localDirAllocator) {
      return new FakeS3ACachingBlockManager(futurePool, reader, blockData,
          bufferPoolSize, conf, localDirAllocator);
    }
  }
}
