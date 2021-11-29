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

package org.apache.hadoop.fs.s3a.read;

import org.apache.hadoop.fs.common.BlockData;
import org.apache.hadoop.fs.common.BlockCache;
import org.apache.hadoop.fs.common.SingleFilePerBlockCache;
import org.apache.hadoop.fs.common.Validate;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.twitter.util.FuturePool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides 'fake' implementations of S3InputStream variants.
 *
 * These implementations avoid accessing the following real resources:
 * -- S3 store
 * -- local filesystem
 *
 * This arrangement allows thorough multi-threaded testing of those
 * implementations without accessing external resources. It also helps
 * avoid test flakiness introduced by external factors.
 */
public class Fakes {
  public static class TestS3InMemoryInputStream extends S3InMemoryInputStream {
    public TestS3InMemoryInputStream(
        FuturePool futurePool,
        String bucket,
        String key,
        long fileSize,
        AmazonS3 client) {
      super(futurePool, bucket, key, fileSize, client);
    }

    @Override
    protected S3File getS3File(AmazonS3 client, String bucket, String key, long fileSize) {
      randomDelay(200);
      return new TestS3File((int) fileSize, false);
    }
  }

  public static class TestS3FilePerBlockCache extends SingleFilePerBlockCache {
    private final Map<Path, byte[]> files;
    private final int readDelay;
    private final int writeDelay;

    public TestS3FilePerBlockCache(int readDelay, int writeDelay) {
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
    protected Path getCacheFilePath() throws IOException {
      fileCount++;
      return Paths.get(Long.toString(fileCount));
    }

    @Override
    public void close() throws IOException {
      this.files.clear();
    }
  }

  private static final Random random = new Random();

  private static void randomDelay(int delay) {
    try {
      Thread.sleep(random.nextInt(delay));
    } catch (InterruptedException e) {

    }
  }

  public static class TestS3CachingBlockManager extends S3CachingBlockManager {
    public TestS3CachingBlockManager(
        FuturePool futurePool,
        S3Reader reader,
        BlockData blockData,
        int bufferPoolSize) {
      super(futurePool, reader, blockData, bufferPoolSize);
    }

    @Override
    public int read(ByteBuffer buffer, long offset, int size) throws IOException {
      randomDelay(100);
      return this.reader.read(buffer, offset, size);
    }

    @Override
    protected BlockCache createCache() {
      final int readDelayMs = 50;
      final int writeDelayMs = 200;
      return new TestS3FilePerBlockCache(readDelayMs, writeDelayMs);
    }
  }

  public static class TestS3CachingInputStream extends S3CachingInputStream {
    public TestS3CachingInputStream(
        FuturePool futurePool,
        int bufferSize,
        int numBlocksToPrefetch,
        String bucket,
        String key,
        long fileSize,
        AmazonS3 client) {
      super(futurePool, bufferSize, numBlocksToPrefetch, bucket, key, fileSize, client);
    }

    @Override
    protected S3File getS3File(AmazonS3 client, String bucket, String key, long fileSize) {
      return new TestS3File((int) fileSize, false);
    }

    @Override
    protected S3CachingBlockManager createBlockManager(
        FuturePool futurePool,
        S3Reader reader,
        BlockData blockData,
        int bufferPoolSize) {
      return new TestS3CachingBlockManager(futurePool, reader, blockData, bufferPoolSize);
    }
  }
}
