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

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Buffer pool implementation which uses weak references to store
 * buffers in the pool, such that they are garbage collected when
 * there are no references to the buffer during a gc run. This is
 * important as direct buffers don't get garbage collected automatically
 * during a gc run as they are not stored on heap memory.
 * Also the buffers are stored in a tree map which helps in returning
 * smallest buffer whose size is just greater than requested length.
 * This is a thread safe implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class WeakReferencedElasticByteBufferPool extends ElasticByteBufferPool {

  /**
   * Map to store direct byte buffers of different sizes in the pool.
   * Used tree map such that we can return next greater than capacity
   * buffer if buffer with exact capacity is unavailable.
   * This must be accessed in synchronized blocks.
   */
  private final TreeMap<Key, WeakReference<ByteBuffer>> directBuffers =
          new TreeMap<>();

  /**
   * Map to store heap based byte buffers of different sizes in the pool.
   * Used tree map such that we can return next greater than capacity
   * buffer if buffer with exact capacity is unavailable.
   * This must be accessed in synchronized blocks.
   */
  private final TreeMap<Key, WeakReference<ByteBuffer>> heapBuffers =
          new TreeMap<>();

  /**
   * Method to get desired buffer tree.
   * @param isDirect whether the buffer is heap based or direct.
   * @return corresponding buffer tree.
   */
  private TreeMap<Key, WeakReference<ByteBuffer>> getBufferTree(boolean isDirect) {
    return isDirect
            ? directBuffers
            : heapBuffers;
  }

  /**
   * {@inheritDoc}
   *
   * @param direct whether we want a direct byte buffer or a heap one.
   * @param length length of requested buffer.
   * @return returns equal or next greater than capacity buffer from
   * pool if already available and not garbage collected else creates
   * a new buffer and return it.
   */
  @Override
  public synchronized ByteBuffer getBuffer(boolean direct, int length) {
    TreeMap<Key, WeakReference<ByteBuffer>> buffersTree = getBufferTree(direct);

    // Scan the entire tree and remove all weak null references.
    buffersTree.entrySet().removeIf(next -> next.getValue().get() == null);

    Map.Entry<Key, WeakReference<ByteBuffer>> entry =
            buffersTree.ceilingEntry(new Key(length, 0));
    // If there is no buffer present in the pool with desired size.
    if (entry == null) {
      return direct ? ByteBuffer.allocateDirect(length) :
                      ByteBuffer.allocate(length);
    }
    // buffer is available in the pool and not garbage collected.
    WeakReference<ByteBuffer> bufferInPool = entry.getValue();
    buffersTree.remove(entry.getKey());
    ByteBuffer buffer = bufferInPool.get();
    if (buffer != null) {
      return buffer;
    }
    // buffer was in pool but already got garbage collected.
    return direct
            ? ByteBuffer.allocateDirect(length)
            : ByteBuffer.allocate(length);
  }

  /**
   * Return buffer to the pool.
   * @param buffer buffer to be returned.
   */
  @Override
  public synchronized void putBuffer(ByteBuffer buffer) {
    buffer.clear();
    TreeMap<Key, WeakReference<ByteBuffer>> buffersTree = getBufferTree(buffer.isDirect());
    // Buffers are indexed by (capacity, time).
    // If our key is not unique on the first try, we try again, since the
    // time will be different.  Since we use nanoseconds, it's pretty
    // unlikely that we'll loop even once, unless the system clock has a
    // poor granularity or multi-socket systems have clocks slightly out
    // of sync.
    while (true) {
      Key keyToInsert = new Key(buffer.capacity(), System.nanoTime());
      if (!buffersTree.containsKey(keyToInsert)) {
        buffersTree.put(keyToInsert, new WeakReference<>(buffer));
        return;
      }
    }
  }

  /**
   * Clear the buffer pool thus releasing all the buffers.
   * The caller must remove all references of
   * existing buffers before calling this method to avoid
   * memory leaks.
   */
  @Override
  public synchronized void release() {
    heapBuffers.clear();
    directBuffers.clear();
  }

  /**
   * Get current buffers count in the pool.
   * @param isDirect whether we want to count the heap or direct buffers.
   * @return count of buffers.
   */
  @VisibleForTesting
  public synchronized int getCurrentBuffersCount(boolean isDirect) {
    return isDirect
            ? directBuffers.size()
            : heapBuffers.size();
  }
}
