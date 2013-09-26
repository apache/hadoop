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
package org.apache.hadoop.io;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ByteBufferPool {
  /**
   * Get a new direct ByteBuffer.  The pool can provide this from
   * removing a buffer from its internal cache, or by allocating a 
   * new buffer.
   *
   * @param direct     Whether the buffer should be direct.
   * @param length     The minimum length the buffer will have.
   * @return           A new ByteBuffer.  This ByteBuffer must be direct.
   *                   Its capacity can be less than what was requested, but
   *                   must be at least 1 byte.
   */
  ByteBuffer getBuffer(boolean direct, int length);

  /**
   * Release a buffer back to the pool.
   * The pool may choose to put this buffer into its cache.
   *
   * @param buffer    a direct bytebuffer
   */
  void putBuffer(ByteBuffer buffer);
}
