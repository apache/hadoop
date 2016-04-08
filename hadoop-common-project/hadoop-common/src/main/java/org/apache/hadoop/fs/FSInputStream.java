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
package org.apache.hadoop.fs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/****************************************************************
 * FSInputStream is a generic old InputStream with a little bit
 * of RAF-style seek ability.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class FSInputStream extends InputStream
    implements Seekable, PositionedReadable {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSInputStream.class);

  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't
   * seek past the end of the file.
   */
  @Override
  public abstract void seek(long pos) throws IOException;

  /**
   * Return the current offset from the start of the file
   */
  @Override
  public abstract long getPos() throws IOException;

  /**
   * Seeks a different copy of the data.  Returns true if 
   * found a new source, false otherwise.
   */
  @Override
  public abstract boolean seekToNewSource(long targetPos) throws IOException;

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    validatePositionedReadArgs(position, buffer, offset, length);
    if (length == 0) {
      return 0;
    }
    synchronized (this) {
      long oldPos = getPos();
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } catch (EOFException e) {
        // end of file; this can be raised by some filesystems
        // (often: object stores); it is swallowed here.
        LOG.debug("Downgrading EOFException raised trying to" +
            " read {} bytes at offset {}", length, offset, e);
      } finally {
        seek(oldPos);
      }
      return nread;
    }
  }

  /**
   * Validation code, available for use in subclasses.
   * @param position position: if negative an EOF exception is raised
   * @param buffer destination buffer
   * @param offset offset within the buffer
   * @param length length of bytes to read
   * @throws EOFException if the position is negative
   * @throws IndexOutOfBoundsException if there isn't space for the amount of
   * data requested.
   * @throws IllegalArgumentException other arguments are invalid.
   */
  protected void validatePositionedReadArgs(long position,
      byte[] buffer, int offset, int length) throws EOFException {
    Preconditions.checkArgument(length >= 0, "length is negative");
    if (position < 0) {
      throw new EOFException("position is negative");
    }
    Preconditions.checkArgument(buffer != null, "Null buffer");
    if (buffer.length - offset < length) {
      throw new IndexOutOfBoundsException(
          FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
    validatePositionedReadArgs(position, buffer, offset, length);
    int nread = 0;
    while (nread < length) {
      int nbytes = read(position + nread,
          buffer,
          offset + nread,
          length - nread);
      if (nbytes < 0) {
        throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
      }
      nread += nbytes;
    }
  }

  @Override
  public void readFully(long position, byte[] buffer)
    throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
}
