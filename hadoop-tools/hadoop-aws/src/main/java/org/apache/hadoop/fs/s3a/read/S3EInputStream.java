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

import com.amazonaws.services.s3.AmazonS3;
import com.twitter.util.FuturePool;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Enhanced {@link InputStream} for reading from S3.
 *
 * This implementation provides improved read throughput by asynchronously prefetching
 * blocks of configurable size from the underlying S3 file.
 */
public class S3EInputStream extends FSInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3EInputStream.class);

  // Underlying input stream used for reading S3 file.
  private S3InputStream inputStream;

  // S3 access stats.
  private FileSystem.Statistics stats;

  /**
   * Constructs an instance of {@link S3EInputStream}.
   *
   * @param futurePool Future pool used for async reading activity.
   * @param prefetchBlockSize Size of each prefetched block.
   * @param prefetchBlockCount Size of the prefetch queue (in number of blocks).
   * @param bucket Name of S3 bucket from which key is opened.
   * @param key Name of the S3 key to open.
   * @param contentLength length of the file.
   * @param client S3 access client.
   * @param stats {@link FileSystem} stats related to read operation.
   */
  public S3EInputStream(
      FuturePool futurePool,
      int prefetchBlockSize,
      int numBlocksToPrefetch,
      String bucket,
      String key,
      long contentLength,
      AmazonS3 client,
      FileSystem.Statistics stats) {

    this.stats = stats;
    if (contentLength <= prefetchBlockSize) {
      this.inputStream = new S3InMemoryInputStream(futurePool, bucket, key, contentLength, client);
    } else {
      this.inputStream =
          new S3CachingInputStream(
              futurePool,
              prefetchBlockSize,
              numBlocksToPrefetch,
              bucket,
              key,
              contentLength,
              client);
    }
  }

  @Override
  public synchronized int available() throws IOException {
    return this.inputStream.available();
  }

  @Override
  public synchronized long getPos() throws IOException {
    return this.inputStream.getPos();
  }

  @Override
  public synchronized int read() throws IOException {
    int byteRead = this.inputStream.read();
    if (byteRead >= 0) {
      if (stats != null) {
        stats.incrementBytesRead(1);
      }
    }
    return byteRead;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    int bytesRead = this.inputStream.read(buf, off, len);
    if (bytesRead > 0) {
      if (stats != null) {
        stats.incrementBytesRead(bytesRead);
      }
    }
    return bytesRead;
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.inputStream != null) {
      this.inputStream.close();
      this.inputStream = null;
    }
    super.close();
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    this.inputStream.seek(pos);
  }

  // Unsupported functions.

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
