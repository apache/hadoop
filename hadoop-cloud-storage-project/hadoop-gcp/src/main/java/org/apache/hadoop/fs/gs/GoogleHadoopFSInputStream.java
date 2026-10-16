/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.gs;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

final class GoogleHadoopFSInputStream extends FSInputStream {
  public static final Logger LOG = LoggerFactory.getLogger(GoogleHadoopFSInputStream.class);

  // Used for single-byte reads.
  private final byte[] singleReadBuf = new byte[1];

  // Path of the file to read.
  private final URI gcsPath;
  // File Info of gcsPath, will be pre-populated in some cases i.e. when Json client is used and
  // failFast is disabled.

  // All store IO access goes through this.
  private final SeekableByteChannel channel;
  // Number of bytes read through this channel.
  private long totalBytesRead = 0;

  /**
   * Closed bit. Volatile so reads are non-blocking. Updates must be in a synchronized block to
   * guarantee an atomic check and set
   */
  private volatile boolean closed;

  // Statistics tracker provided by the parent GoogleHadoopFileSystem for recording stats
  private final FileSystem.Statistics statistics;

  static GoogleHadoopFSInputStream create(
      GoogleHadoopFileSystem ghfs, URI gcsPath, FileSystem.Statistics statistics)
      throws IOException {
    LOG.trace("create(gcsPath: {})", gcsPath);
    GoogleCloudStorageFileSystem gcsFs = ghfs.getGcsFs();
    FileInfo fileInfo = gcsFs.getFileInfoObject(gcsPath);
    SeekableByteChannel channel = gcsFs.open(fileInfo, ghfs.getFileSystemConfiguration());
    return new GoogleHadoopFSInputStream(gcsPath, channel, statistics);
  }

  private GoogleHadoopFSInputStream(
      URI gcsPath,
      SeekableByteChannel channel,
      FileSystem.Statistics statistics) {
    LOG.trace("GoogleHadoopFSInputStream(gcsPath: {})", gcsPath);
    this.gcsPath = gcsPath;
    this.channel = channel;
    this.statistics = statistics;
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();
    int numRead = read(singleReadBuf, /* offset= */ 0, /* length= */ 1);
    checkState(
        numRead == -1 || numRead == 1,
        "Read %s bytes using single-byte buffer for path %s ending in position %s",
        numRead,
        gcsPath,
        channel.position());
    return numRead > 0 ? singleReadBuf[0] & 0xff : numRead;
  }

  @Override
  public synchronized int read(@Nonnull byte[] buf, int offset, int length) throws IOException {
    checkNotClosed();
    checkNotNull(buf, "buf must not be null");
    if (offset < 0 || length < 0 || length > buf.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    // TODO(user): Wrap this in a while-loop if we ever introduce a non-blocking mode for
    // the underlying channel.
    int numRead = channel.read(ByteBuffer.wrap(buf, offset, length));
    if (numRead > 0) {
      // -1 means we actually read 0 bytes, but requested at least one byte.
      totalBytesRead += numRead;
      statistics.incrementBytesRead(numRead);
      statistics.incrementReadOps(1);
    }
    return numRead;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkNotClosed();
    LOG.trace("seek({})", pos);
    try {
      channel.position(pos);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;

      LOG.trace("close(): {}", gcsPath);
      try {
        if (channel != null) {
          LOG.trace(
              "Closing '{}' file with {} total bytes read", gcsPath, totalBytesRead);
          channel.close();
        }
      } catch (Exception e) {
        LOG.warn("Error while closing underneath read channel resources for path: {}", gcsPath, e);
      }
    }
  }

  /**
   * Gets the current position within the file being read.
   *
   * @return The current position within the file being read.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized long getPos() throws IOException {
    checkNotClosed();
    long pos = channel.position();
    LOG.trace("getPos(): {}", pos);
    return pos;
  }

  /**
   * Seeks a different copy of the data. Not supported.
   *
   * @return true if a new source is found, false otherwise.
   */
  @Override
  public boolean seekToNewSource(long targetPos) {
    LOG.trace("seekToNewSource({}): false", targetPos);
    return false;
  }

  @Override
  public int available() throws IOException {
    if (!channel.isOpen()) {
      throw new ClosedChannelException();
    }
    return super.available();
  }

  /**
   * Verify that the input stream is open. Non-blocking; this gives the last state of the volatile
   * {@link #closed} field.
   *
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(gcsPath + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }
}
