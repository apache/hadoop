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

package org.apache.hadoop.fs.s3a.impl.streams;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class AnalyticsStream extends ObjectInputStream implements StreamCapabilities {

  private S3SeekableInputStream inputStream;
  private long lastReadCurrentPos = 0;
  private volatile boolean closed;

  public static final Logger LOG = LoggerFactory.getLogger(AnalyticsStream.class);

  public AnalyticsStream(final ObjectReadParameters parameters, final S3SeekableInputStreamFactory s3SeekableInputStreamFactory) {
    super(parameters);
    S3ObjectAttributes s3Attributes = parameters.getObjectAttributes();
    this.inputStream = s3SeekableInputStreamFactory.createStream(S3URI.of(s3Attributes.getBucket(), s3Attributes.getKey()));
  }

  /**
   * Indicates whether the given {@code capability} is supported by this stream.
   *
   * @param capability the capability to check.
   * @return true if the given {@code capability} is supported by this stream, false otherwise.
   */
  @Override
  public boolean hasCapability(String capability) {
    return false;
  }

  @Override
  public int read() throws IOException {
    throwIfClosed();
    int bytesRead;
    try {
      bytesRead = inputStream.read();
    } catch (IOException ioe) {
      onReadFailure(ioe);
      throw ioe;
    }
    return bytesRead;
  }

  @Override
  public void seek(long pos) throws IOException {
    throwIfClosed();
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK
          + " " + pos);
    }
    inputStream.seek(pos);
  }


  @Override
  public synchronized long getPos() {
    if (!closed) {
      lastReadCurrentPos = inputStream.getPos();
    }
    return lastReadCurrentPos;
  }


  /**
   * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
   * reached. Leaves the position of the stream unaltered.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len the number of bytes to read; the n-th byte should be the last byte of the stream.
   * @return the total number of bytes read into the buffer
   * @throws IOException if an I/O error occurs
   */
  public int readTail(byte[] buf, int off, int len) throws IOException {
    throwIfClosed();
    int bytesRead;
    try {
      bytesRead = inputStream.readTail(buf, off, len);
    } catch (IOException ioe) {
      onReadFailure(ioe);
      throw ioe;
    }
    return bytesRead;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    throwIfClosed();
    int bytesRead;
    try {
      bytesRead = inputStream.read(buf, off, len);
    } catch (IOException ioe) {
      onReadFailure(ioe);
      throw ioe;
    }
    return bytesRead;
  }


  @Override
  public boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public int available() throws IOException {
    throwIfClosed();
    return super.available();
  }

  @Override
  protected boolean isStreamOpen() {
    return !isClosed();
  }

  protected boolean isClosed() {
    return inputStream == null;
  }

  @Override
  protected void abortInFinalizer() {
    try {
      close();
    } catch (IOException ignored) {

    }
  }

  @Override
  public synchronized void close() throws IOException {
    if(!closed) {
      closed = true;
      try {
        inputStream.close();
        inputStream = null;
        super.close();
      } catch (IOException ioe) {
        LOG.debug("Failure closing stream {}: ", getKey());
        throw ioe;
      }
    }
  }

  /**
   * Close the stream on read failure.
   * No attempt to recover from failure
   *
   * @param ioe exception caught.
   */
  @Retries.OnceTranslated
  private void onReadFailure(IOException ioe) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got exception while trying to read from stream {}, " +
              "not trying to recover:",
              getKey(), ioe);
    } else {
      LOG.info("Got exception while trying to read from stream {}, " +
              "not trying to recover:",
              getKey(), ioe);
    }
    this.close();
  }


  protected void throwIfClosed() throws IOException {
    if (closed) {
      throw new IOException(getKey() + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }
}