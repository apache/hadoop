/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.ozone.web.client.OzoneBucket;
import org.apache.hadoop.ozone.client.rest.OzoneException;

import static org.apache.hadoop.fs.ozone.Constants.BUFFER_TMP_KEY;
import static org.apache.hadoop.fs.ozone.Constants.BUFFER_DIR_KEY;

/**
 * Wraps OzoneInputStream implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class OzoneInputStream extends FSInputStream {
  private static final Log LOG = LogFactory.getLog(OzoneInputStream.class);

  private final RandomAccessFile in;

  /** Closed bit. Volatile so reads are non-blocking. */
  private volatile boolean closed = false;

  /** the ozone bucket client. */
  private final OzoneBucket bucket;

  /** The object key. */
  private final String key;

  /** Object content length. */
  private final long contentLen;

  /** file system stats. */
  private final Statistics stats;

  private final URI keyUri;

  OzoneInputStream(Configuration conf, URI fsUri, OzoneBucket bucket,
      String key, long contentLen, int bufferSize, Statistics statistics)
      throws IOException {
    Objects.requireNonNull(bucket, "bucket can not be null!");
    Objects.requireNonNull(key, "kenName can not be null!");
    this.bucket = bucket;
    this.key = key;
    this.contentLen = contentLen;
    this.stats = statistics;
    this.keyUri = fsUri.resolve(key);

    if (conf.get(BUFFER_DIR_KEY) == null) {
      conf.set(BUFFER_DIR_KEY, conf.get(BUFFER_TMP_KEY) + "/ozone");
    }
    final LocalDirAllocator dirAlloc = new LocalDirAllocator(BUFFER_DIR_KEY);
    final File tmpFile = dirAlloc.createTmpFileForWrite("output-",
        LocalDirAllocator.SIZE_UNKNOWN, conf);
    try {
      LOG.trace("Get Key:" + this.keyUri + " tmp-file:" + tmpFile.toPath());
      bucket.getKey(this.key, tmpFile.toPath());
      in = new RandomAccessFile(tmpFile, "r");
      statistics.incrementReadOps(1);
    } catch (OzoneException oe) {
      final String msg = "Error when getBytes for key = " + key;
      LOG.error(msg, oe);
      throw new IOException(msg, oe);
    }
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    checkNotClosed();
    // Do not allow negative seek
    if (targetPos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + targetPos);
    }

    if (this.contentLen <= 0) {
      return;
    }

    in.seek(targetPos);
  }

  @Override
  public synchronized long getPos() throws IOException {
    checkNotClosed();
    return in.getFilePointer();
  }

  @Override
  public boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    int ch = in.read();
    if (stats != null && ch != -1) {
      stats.incrementBytesRead(1);
    }
    return ch;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    Preconditions.checkArgument(buffer != null, "buffer can not be null");
    int numberOfByteRead = super.read(position, buffer, offset, length);

    if (stats != null && numberOfByteRead > 0) {
      stats.incrementBytesRead(numberOfByteRead);
    }
    return numberOfByteRead;
  }

  @Override
  public synchronized int read(byte[] buffer, int offset, int length)
      throws IOException {
    Preconditions.checkArgument(buffer != null, "buffer can not be null");
    int numberOfByteRead = in.read(buffer, offset, length);
    if (stats != null && numberOfByteRead > 0) {
      stats.incrementBytesRead(numberOfByteRead);
    }
    return numberOfByteRead;
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    final long remainingInWrapped = contentLen - in.getFilePointer();
    return (remainingInWrapped < Integer.MAX_VALUE)
        ? (int)remainingInWrapped
        : Integer.MAX_VALUE;
  }

  @Override
  public synchronized void close() throws IOException {
    in.close();
  }

  @Override
  public synchronized long skip(long pos) throws IOException {
    return in.skipBytes((int) pos);
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(this.keyUri + ": "
          + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

}
