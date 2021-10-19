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
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Implementers of this interface provide a positioned read API that writes to a
 * {@link ByteBuffer} rather than a {@code byte[]}.
 *
 * @see PositionedReadable
 * @see ByteBufferReadable
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ByteBufferPositionedReadable {
  /**
   * Reads up to {@code buf.remaining()} bytes into buf from a given position
   * in the file and returns the number of bytes read. Callers should use
   * {@code buf.limit(...)} to control the size of the desired read and
   * {@code buf.position(...)} to control the offset into the buffer the data
   * should be written to.
   * <p>
   * After a successful call, {@code buf.position()} will be advanced by the
   * number of bytes read and {@code buf.limit()} will be unchanged.
   * <p>
   * In the case of an exception, the state of the buffer (the contents of the
   * buffer, the {@code buf.position()}, the {@code buf.limit()}, etc.) is
   * undefined, and callers should be prepared to recover from this
   * eventuality.
   * <p>
   * Callers should use {@link StreamCapabilities#hasCapability(String)} with
   * {@link StreamCapabilities#PREADBYTEBUFFER} to check if the underlying
   * stream supports this interface, otherwise they might get a
   * {@link UnsupportedOperationException}.
   * <p>
   * Implementations should treat 0-length requests as legitimate, and must not
   * signal an error upon their receipt.
   * <p>
   * This does not change the current offset of a file, and is thread-safe.
   *
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @return the number of bytes read, possibly zero, or -1 if reached
   *         end-of-stream
   * @throws IOException if there is some error performing the read
   */
  int read(long position, ByteBuffer buf) throws IOException;

  /**
   * Reads {@code buf.remaining()} bytes into buf from a given position in
   * the file or until the end of the data was reached before the read
   * operation completed. Callers should use {@code buf.limit(...)} to
   * control the size of the desired read and {@code buf.position(...)} to
   * control the offset into the buffer the data should be written to.
   * <p>
   * This operation provides similar semantics to
   * {@link #read(long, ByteBuffer)}, the difference is that this method is
   * guaranteed to read data until the {@link ByteBuffer} is full, or until
   * the end of the data stream is reached.
   *
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @throws IOException if there is some error performing the read
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   * @see #read(long, ByteBuffer)
   */
  void readFully(long position, ByteBuffer buf) throws IOException;
}
