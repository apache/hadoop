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

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.common.Validate;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Enhanced {@code InputStream} for reading from S3.
 *
 * This implementation provides improved read throughput by asynchronously prefetching
 * blocks of configurable size from the underlying S3 file.
 */
public class S3EInputStream extends FSInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3EInputStream.class);

  // Underlying input stream used for reading S3 file.
  private S3InputStream inputStream;

  /**
   * Initializes a new instance of the {@code S3EInputStream} class.
   *
   * @param context .
   * @param s3Attributes .
   * @param client .
   */
  public S3EInputStream(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client) {

    Validate.checkNotNull(context, "context");
    Validate.checkNotNull(s3Attributes, "s3Attributes");
    Validate.checkNotNullAndNotEmpty(s3Attributes.getBucket(), "s3Attributes.getBucket()");
    Validate.checkNotNullAndNotEmpty(s3Attributes.getKey(), "s3Attributes.getKey()");
    Validate.checkNotNegative(s3Attributes.getLen(), "s3Attributes.getLen()");
    Validate.checkNotNull(client, "client");

    long fileSize = s3Attributes.getLen();
    if (fileSize <= context.getPrefetchBlockSize()) {
      this.inputStream = new S3InMemoryInputStream(context, s3Attributes, client);
    } else {
      this.inputStream = new S3CachingInputStream(context, s3Attributes, client);
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
    return this.inputStream.read();
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    return this.inputStream.read(buf, off, len);
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
