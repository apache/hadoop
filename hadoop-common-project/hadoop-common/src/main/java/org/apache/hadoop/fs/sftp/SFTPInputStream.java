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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

/** SFTP FileSystem input stream. */
class SFTPInputStream extends FSInputStream {

  public static final String E_SEEK_NOTSUPPORTED = "Seek not supported";
  public static final String E_NULL_INPUTSTREAM = "Null InputStream";
  public static final String E_STREAM_CLOSED = "Stream closed";

  private InputStream wrappedStream;
  private FileSystem.Statistics stats;
  private boolean closed;
  private long pos;

  SFTPInputStream(InputStream stream,  FileSystem.Statistics stats) {

    if (stream == null) {
      throw new IllegalArgumentException(E_NULL_INPUTSTREAM);
    }
    this.wrappedStream = stream;
    this.stats = stats;

    this.pos = 0;
    this.closed = false;
  }

  @Override
  public void seek(long position) throws IOException {
    throw new IOException(E_SEEK_NOTSUPPORTED);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException(E_SEEK_NOTSUPPORTED);
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException(E_STREAM_CLOSED);
    }

    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null & byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  public synchronized int read(byte[] buf, int off, int len)
      throws IOException {
    if (closed) {
      throw new IOException(E_STREAM_CLOSED);
    }

    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null & result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }

  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    super.close();
    wrappedStream.close();
    closed = true;
  }
}
