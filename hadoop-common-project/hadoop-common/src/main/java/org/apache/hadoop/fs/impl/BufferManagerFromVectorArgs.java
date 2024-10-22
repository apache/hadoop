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

package org.apache.hadoop.fs.impl;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import org.apache.hadoop.io.ByteBufferPool;

/**
 * A ByteBufferPool implementation that uses a pair of functions to allocate
 * and release ByteBuffers; intended for use implementing the VectorIO API
 * as it makes the pair of functions easier to pass around and use in
 * existing code.
 */
public final class BufferManagerFromVectorArgs implements ByteBufferPool {

  /** The function to allocate a buffer. */
  private final IntFunction<ByteBuffer> allocate;

  /** The function to release a buffer. */
  private final Consumer<ByteBuffer> release;

  /**
   * @param allocate the function to allocate ByteBuffer
   * @param release the function to release a ByteBuffer.
   */
  public BufferManagerFromVectorArgs(IntFunction<ByteBuffer> allocate,
        Consumer<ByteBuffer> release) {
    this.allocate = allocate;
    this.release = release;
  }

  /**
   * Get a ByteBuffer.
   * @param direct     heap/direct flag. Unused.
   * @param length     The minimum length the buffer will have.
   * @return a buffer
   */
  @Override
  public ByteBuffer getBuffer(final boolean direct, final int length) {
    return allocate.apply(length);
  }

  /**
   * Release a buffer.
   * Unlike normal ByteBufferPool implementations
   * a null buffer is accepted and ignored.
   * @param buffer buffer to release; may be null.
   */
  @Override
  public void putBuffer(final ByteBuffer buffer) {
    if (buffer != null) {
      release.accept(buffer);
    }
  }
}
