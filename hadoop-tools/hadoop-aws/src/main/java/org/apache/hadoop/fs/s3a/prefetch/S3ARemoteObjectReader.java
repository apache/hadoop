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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.impl.prefetch.Validate;
import org.apache.hadoop.fs.s3a.HttpChannelEOFException;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.RangeNotSatisfiableEOFException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_REMOTE_BLOCK_READ;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfOperation;

/**
 * Provides functionality to read S3 file one block at a time.
 */
public class S3ARemoteObjectReader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ARemoteObjectReader.class);

  /** We read from the underlying input stream in blocks of this size. */
  private static final int READ_BUFFER_SIZE = 64 * 1024;

  /** The S3 file to read. */
  private final S3ARemoteObject remoteObject;

  /** Set to true by close(). */
  private volatile boolean closed;

  private final S3AInputStreamStatistics streamStatistics;

  /**
   * Constructs an instance of {@link S3ARemoteObjectReader}.
   *
   * @param remoteObject The S3 file to read.
   *
   * @throws IllegalArgumentException if remoteObject is null.
   */
  public S3ARemoteObjectReader(S3ARemoteObject remoteObject) {
    Validate.checkNotNull(remoteObject, "remoteObject");

    this.remoteObject = remoteObject;
    this.streamStatistics = this.remoteObject.getStatistics();
  }

  /**
   * Stars reading at {@code offset} and reads upto {@code size} bytes into {@code buffer}.
   *
   * @param buffer the buffer into which data is returned
   * @param offset the absolute offset into the underlying file where reading starts.
   * @param size the number of bytes to be read.
   *
   * @return number of bytes actually read. -1 if the file is closed.
   * @throws IOException if there is an error reading from the file.
   *
   * @throws IllegalArgumentException if buffer is null.
   * @throws IllegalArgumentException if offset is outside of the range [0, file size].
   * @throws IllegalArgumentException if size is zero or negative.
   */
  @Retries.RetryTranslated
  public int read(ByteBuffer buffer, long offset, int size) throws IOException {
    Validate.checkNotNull(buffer, "buffer");
    Validate.checkWithinRange(offset, "offset", 0, this.remoteObject.size());
    Validate.checkPositiveInteger(size, "size");

    if (this.closed) {
      return -1;
    }

    int reqSize = (int) Math.min(size, this.remoteObject.size() - offset);
    return readOneBlockWithRetries(buffer, offset, reqSize);
  }

  @Override
  public void close() {
    this.closed = true;
  }

  /**
   * Reads one block from S3.
   * <p>
   * There are no retries on base EOFExceptions.
   * {@link HttpChannelEOFException} will be retried.
   * {@link RangeNotSatisfiableEOFException} will be downgraded to
   * partial read, so data may be returned.
   * @param buffer destination.
   * @param offset object offset
   * @param size size to retrieve.
   * @return bytes read.
   * @throws EOFException if this was raised.
   * @throws IOException IO failure.
   */
  @Retries.RetryTranslated
  private int readOneBlockWithRetries(ByteBuffer buffer, long offset, int size)
      throws IOException {

    this.streamStatistics.readOperationStarted(offset, size);
    Invoker invoker = this.remoteObject.getReadInvoker();

    final String path = this.remoteObject.getPath();
    EOFException invokerResponse =
        invoker.retry(String.format("read %s [%d-%d]", path, offset, offset + size),
            path, true,
            trackDurationOfOperation(streamStatistics,
                STREAM_READ_REMOTE_BLOCK_READ, () -> {
                  try {
                    this.readOneBlock(buffer, offset, size);
                  } catch (HttpChannelEOFException e) {
                    // EOF subclasses with are rethrown as errors.
                    this.remoteObject.getStatistics().readException();
                    throw e;
                  } catch (EOFException e) {
                    // the base implementation swallows EOFs.
                    return e;
                  } catch (IOException e) {
                    this.remoteObject.getStatistics().readException();
                    throw e;
                  }
                  return null;
                }));

    int numBytesRead = buffer.position();
    buffer.limit(numBytesRead);
    if (numBytesRead != size) {
      // fewer bytes came back; log at debug
      LOG.debug("Expected to read {} bytes but got {}", size, numBytesRead);
    }
    this.remoteObject.getStatistics()
        .readOperationCompleted(size, numBytesRead);

    if (invokerResponse != null) {
      if (invokerResponse instanceof RangeNotSatisfiableEOFException) {
        // the range wasn't satisfiable, but some may have been read.
        return numBytesRead;
      } else {
        throw invokerResponse;
      }
    }

    // how much was read?
    return numBytesRead;
  }

  /**
   * GET one block from S3.
   * @param buffer buffer to fill up.
   * @param offset offset within the object.
   * @param size size to retrieve.
   * @throws IOException IO failure.
   * @throws HttpChannelEOFException if the channel is closed during the read.
   */
  @Retries.OnceTranslated
  private void readOneBlock(ByteBuffer buffer, long offset, int size)
      throws IOException {
    int readSize = Math.min(size, buffer.remaining());
    if (readSize == 0) {
      return;
    }

    ResponseInputStream<GetObjectResponse> inputStream =
        remoteObject.openForRead(offset, readSize);
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
          throw new HttpChannelEOFException(remoteObject.getPath(), message, null);
        }

        if (numBytes > 0) {
          buffer.put(bytes, 0, numBytes);
          numRemainingBytes -= numBytes;
        }
      }
      while (!this.closed && (numRemainingBytes > 0));
    } finally {
      remoteObject.close(inputStream, numRemainingBytes);
    }
  }
}
