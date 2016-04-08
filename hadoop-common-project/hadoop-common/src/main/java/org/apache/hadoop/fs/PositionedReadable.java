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

/**
 * Stream that permits positional reading.
 *
 * Implementations are required to implement thread-safe operations; this may
 * be supported by concurrent access to the data, or by using a synchronization
 * mechanism to serialize access.
 *
 * Not all implementations meet this requirement. Those that do not cannot
 * be used as a backing store for some applications, such as Apache HBase.
 *
 * Independent of whether or not they are thread safe, some implementations
 * may make the intermediate state of the system, specifically the position
 * obtained in {@code Seekable.getPos()} visible.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PositionedReadable {
  /**
   * Read up to the specified number of bytes, from a given
   * position within a file, and return the number of bytes read. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   * @param position position within file
   * @param buffer destination buffer
   * @param offset offset in the buffer
   * @param length number of bytes to read
   * @return actual number of bytes read; -1 means "none"
   * @throws IOException IO problems.
   */
  int read(long position, byte[] buffer, int offset, int length)
    throws IOException;
  
  /**
   * Read the specified number of bytes, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   * @param position position within file
   * @param buffer destination buffer
   * @param offset offset in the buffer
   * @param length number of bytes to read
   * @throws IOException IO problems.
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   */
  void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException;
  
  /**
   * Read number of bytes equal to the length of the buffer, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   * @param position position within file
   * @param buffer destination buffer
   * @throws IOException IO problems.
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   */
  void readFully(long position, byte[] buffer) throws IOException;
}
