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

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.InputPolicy;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;

/**
 * Analytics stream creates a stream using aws-analytics-accelerator-s3. This stream supports
 * parquet specific optimisations such as parquet-aware prefetching. For more details, see
 * https://github.com/awslabs/analytics-accelerator-s3.
 */
public class AnalyticsStream extends ObjectInputStream implements StreamCapabilities {

  private S3SeekableInputStream inputStream;
  private long lastReadCurrentPos = 0;
  private volatile boolean closed;

  public static final Logger LOG = LoggerFactory.getLogger(AnalyticsStream.class);

  public AnalyticsStream(final ObjectReadParameters parameters,
      final S3SeekableInputStreamFactory s3SeekableInputStreamFactory) throws IOException {
    super(InputStreamType.Analytics, parameters);
    S3ObjectAttributes s3Attributes = parameters.getObjectAttributes();
    this.inputStream = s3SeekableInputStreamFactory.createStream(S3URI.of(s3Attributes.getBucket(),
        s3Attributes.getKey()), buildOpenStreamInformation(parameters));
    getS3AStreamStatistics().streamOpened(InputStreamType.Analytics);
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

  private OpenStreamInformation buildOpenStreamInformation(ObjectReadParameters parameters) {
    OpenStreamInformation.OpenStreamInformationBuilder openStreamInformationBuilder =
        OpenStreamInformation.builder()
            .inputPolicy(mapS3AInputPolicyToAAL(parameters.getContext()
            .getInputPolicy()));

    if (parameters.getObjectAttributes().getETag() != null) {
      openStreamInformationBuilder.objectMetadata(ObjectMetadata.builder()
          .contentLength(parameters.getObjectAttributes().getLen())
          .etag(parameters.getObjectAttributes().getETag()).build());
    }

    return openStreamInformationBuilder.build();
  }

  /**
   * If S3A's input policy is Sequential, that is, if the file format to be read is sequential
   * (CSV, JSON), or the file policy passed down is WHOLE_FILE, then AAL's parquet specific
   * optimisations will be turned off, regardless of the file extension. This is to allow for
   * applications like DISTCP that read parquet files, but will read them whole, and so do not
   * follow the typical parquet read patterns of reading footer first etc. and will not benefit
   * from parquet optimisations.
   * Else, AAL will make a decision on which optimisations based on the file extension,
   * if the file ends in .par or .parquet, then parquet specific optimisations are used.
   *
   * @param inputPolicy S3A's input file policy passed down when opening the file
   * @return the AAL read policy
   */
  private InputPolicy mapS3AInputPolicyToAAL(S3AInputPolicy inputPolicy) {
    switch (inputPolicy) {
    case Sequential:
      return InputPolicy.Sequential;
    default:
      return InputPolicy.None;
    }
  }

  protected void throwIfClosed() throws IOException {
    if (closed) {
      throw new IOException(getKey() + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }
}
