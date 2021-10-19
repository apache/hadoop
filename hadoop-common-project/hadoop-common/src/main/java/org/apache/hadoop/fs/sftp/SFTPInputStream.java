/**
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

package org.apache.hadoop.fs.sftp;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** SFTP FileSystem input stream. */
class SFTPInputStream extends FSInputStream {

  private final ChannelSftp channel;
  private final Path path;
  private InputStream wrappedStream;
  private FileSystem.Statistics stats;
  private boolean closed;
  private long pos;
  private long nextPos;
  private long contentLength;

  SFTPInputStream(ChannelSftp channel, Path path, FileSystem.Statistics stats)
      throws IOException {
    try {
      this.channel = channel;
      this.path = path;
      this.stats = stats;
      this.wrappedStream = channel.get(path.toUri().getPath());
      SftpATTRS stat = channel.lstat(path.toString());
      this.contentLength = stat.getSize();
    } catch (SftpException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void seek(long position) throws IOException {
    checkNotClosed();
    if (position < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    nextPos = position;
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();
    long remaining = contentLength - nextPos;
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) remaining;
  }

  private void seekInternal() throws IOException {
    if (pos == nextPos) {
      return;
    }
    if (nextPos > pos) {
      long skipped = wrappedStream.skip(nextPos - pos);
      pos = pos + skipped;
    }
    if (nextPos < pos) {
      wrappedStream.close();
      try {
        wrappedStream = channel.get(path.toUri().getPath());
        pos = wrappedStream.skip(nextPos);
      } catch (SftpException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return nextPos;
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();
    if (this.contentLength == 0 || (nextPos >= contentLength)) {
      return -1;
    }
    seekInternal();
    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
      nextPos++;
    }
    if (stats != null & byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    super.close();
    wrappedStream.close();
    closed = true;
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(
          path.toUri() + ": " + FSExceptionMessages.STREAM_IS_CLOSED
      );
    }
  }
}
