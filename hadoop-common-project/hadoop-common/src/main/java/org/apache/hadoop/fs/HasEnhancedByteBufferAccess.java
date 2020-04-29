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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ByteBufferPool;

/**
 * FSDataInputStreams implement this interface to provide enhanced
 * byte buffer access.  Usually this takes the form of mmap support.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface HasEnhancedByteBufferAccess {
  /**
   * Get a ByteBuffer containing file data.
   *
   * This ByteBuffer may come from the stream itself, via a call like mmap,
   * or it may come from the ByteBufferFactory which is passed in as an
   * argument.
   *
   * @param factory
   *            If this is non-null, it will be used to create a fallback
   *            ByteBuffer when the stream itself cannot create one.
   * @param maxLength
   *            The maximum length of buffer to return.  We may return a buffer
   *            which is shorter than this.
   * @param opts
   *            Options to use when reading.
   *
   * @return
   *            We will always return an empty buffer if maxLength was 0,
   *            whether or not we are at EOF.
   *            If maxLength &gt; 0, we will return null if the stream
   *            has reached EOF.
   *            Otherwise, we will return a ByteBuffer containing at least one 
   *            byte.  You must free this ByteBuffer when you are done with it 
   *            by calling releaseBuffer on it.  The buffer will continue to be
   *            readable until it is released in this manner.  However, the
   *            input stream's close method may warn about unclosed buffers.
   * @throws    IOException if there was an error reading.
   * @throws    UnsupportedOperationException  if factory was null,
   *             and we needed an external byte buffer.
   * @throws    UnsupportedOperationException  will never be thrown
   *             unless the factory argument is null.
   *
   */
  public ByteBuffer read(ByteBufferPool factory, int maxLength,
      EnumSet<ReadOption> opts)
          throws IOException, UnsupportedOperationException;

  /**
   * Release a ByteBuffer which was created by the enhanced ByteBuffer read
   * function. You must not continue using the ByteBuffer after calling this 
   * function.
   *
   * @param buffer
   *            The ByteBuffer to release.
   */
  public void releaseBuffer(ByteBuffer buffer);
}
