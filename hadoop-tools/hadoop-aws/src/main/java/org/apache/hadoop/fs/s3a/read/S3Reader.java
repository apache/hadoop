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

import org.apache.hadoop.fs.common.Io;
import org.apache.hadoop.fs.common.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Provides functionality to read S3 file one block at a time.
 */
public class S3Reader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(S3Reader.class);

  // The S3 file to read.
  private final S3File s3File;

  // Set to true by close().
  private volatile boolean closed;

  /**
   * Constructs an instance of {@link S3Reader}.
   *
   * @param s3File The S3 file to read.
   */
  public S3Reader(S3File s3File) {
    Validate.checkNotNull(s3File, "s3File");

    this.s3File = s3File;
  }

  /**
   * Stars reading at {@code offset} and reads upto {@code size} bytes into {@code buffer}.
   *
   * @param buffer the buffer into which data is returned
   * @param offset the absolute offset into the underlying file where reading starts.
   * @param size the number of bytes to be read.
   *
   * @return number of bytes actually read
   */
  public int read(ByteBuffer buffer, long offset, int size) throws IOException {
    Validate.checkNotNull(buffer, "buffer");
    Validate.checkWithinRange(offset, "offset", 0, this.getFileSize());
    Validate.checkPositiveInteger(size, "size");

    if (this.closed) {
      return -1;
    }

    int reqSize = (int) Math.min(size, this.getFileSize() - offset);
    return readOneBlockWithRetries(buffer, offset, reqSize);
  }

  @Override
  public void close() {
    this.closed = true;
  }

  private long getFileSize() {
    try {
      return this.s3File.size();
    } catch (IOException e) {
      LOG.error("Error getting file size", e);
      throw new RuntimeException(e);
    }
  }

  private int readOneBlockWithRetries(ByteBuffer buffer, long offset, int size)
      throws IOException {
    S3AccessRetryer retryer = new S3AccessRetryer();
    while (true) {
      if (this.closed) {
        return -1;
      }

      try {
        buffer.clear();
        readOneBlock(buffer, offset, size);
        int numBytesRead = buffer.position();
        buffer.limit(numBytesRead);
        return numBytesRead;
      } catch (Exception e) {
        if (!retryer.retry(e)) {
          if (e instanceof EOFException) {
            return -1;
          } else {
            throw e;
          }
        }
      }
    }
  }

  private static final int READ_BUFFER_SIZE = 64 * 1024;

  private void readOneBlock(ByteBuffer buffer, long offset, int size) throws IOException {
    int readSize = Math.min(size, buffer.remaining());
    if (readSize == 0) {
      return;
    }

    InputStream inputStream = s3File.openForRead(offset, readSize);
    int numRemainingBytes = readSize;
    byte[] bytes = new byte[READ_BUFFER_SIZE];

    int numBytesToRead;
    int numBytes;

    try {
      do {
        numBytesToRead = Math.min(READ_BUFFER_SIZE, numRemainingBytes);
        numBytes = inputStream.read(bytes, 0, numBytesToRead);
        if (numBytes < 0) {
          String message = String.format(
              "Unexpected end of stream: buffer[%d], readSize = %d, numRemainingBytes = %d",
              buffer.capacity(), readSize, numRemainingBytes);
          throw new EOFException(message);
        }

        if (numBytes > 0) {
          buffer.put(bytes, 0, numBytes);
          numRemainingBytes -= numBytes;
        }
      }
      while (!this.closed && (numRemainingBytes > 0));
    } finally {
      s3File.close(inputStream);
    }
  }
}
