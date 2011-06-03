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

import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Utility that wraps a {@link FSInputStream} in a {@link DataInputStream}
 * and buffers input through a {@link BufferedInputStream}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataInputStream extends DataInputStream
    implements Seekable, PositionedReadable, Closeable {

  public FSDataInputStream(InputStream in)
    throws IOException {
    super(in);
    if( !(in instanceof Seekable) || !(in instanceof PositionedReadable) ) {
      throw new IllegalArgumentException(
          "In is not an instance of Seekable or PositionedReadable");
    }
  }
  
  /**
   * Seek to the given offset.
   *
   * @param desired offset to seek to
   */
  public synchronized void seek(long desired) throws IOException {
    ((Seekable)in).seek(desired);
  }

  /**
   * Get the current position in the input stream.
   *
   * @return current position in the input stream
   */
  public long getPos() throws IOException {
    return ((Seekable)in).getPos();
  }
  
  /**
   * Read bytes from the given position in the stream to the given buffer.
   *
   * @param position  position in the input stream to seek
   * @param buffer    buffer into which data is read
   * @param offset    offset into the buffer in which data is written
   * @param length    maximum number of bytes to read
   * @return total number of bytes read into the buffer, or <code>-1</code>
   *         if there is no more data because the end of the stream has been
   *         reached
   */
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    return ((PositionedReadable)in).read(position, buffer, offset, length);
  }

  /**
   * Read bytes from the given position in the stream to the given buffer.
   * Continues to read until <code>length</code> bytes have been read.
   *
   * @param position  position in the input stream to seek
   * @param buffer    buffer into which data is read
   * @param offset    offset into the buffer in which data is written
   * @param length    the number of bytes to read
   * @throws EOFException If the end of stream is reached while reading.
   *                      If an exception is thrown an undetermined number
   *                      of bytes in the buffer may have been written. 
   */
  public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
    ((PositionedReadable)in).readFully(position, buffer, offset, length);
  }
  
  /**
   * See {@link #readFully(long, byte[], int, int)}.
   */
  public void readFully(long position, byte[] buffer)
    throws IOException {
    ((PositionedReadable)in).readFully(position, buffer, 0, buffer.length);
  }
  
  /**
   * Seek to the given position on an alternate copy of the data.
   *
   * @param  targetPos  position to seek to
   * @return true if a new source is found, false otherwise
   */
  public boolean seekToNewSource(long targetPos) throws IOException {
    return ((Seekable)in).seekToNewSource(targetPos); 
  }
  
  /**
   * Get a reference to the wrapped input stream. Used by unit tests.
   *
   * @return the underlying input stream
   */
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public InputStream getWrappedStream() {
    return in;
  }
}
