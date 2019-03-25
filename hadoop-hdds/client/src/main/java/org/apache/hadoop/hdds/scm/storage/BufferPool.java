/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This class creates and manages pool of n buffers.
 */
public class BufferPool {

  private List<ByteBuffer> bufferList;
  private int currentBufferIndex;
  private final int bufferSize;
  private final int capacity;

  public BufferPool(int bufferSize, int capacity) {
    this.capacity = capacity;
    this.bufferSize = bufferSize;
    bufferList = new ArrayList<>(capacity);
    currentBufferIndex = -1;
  }

  public ByteBuffer getCurrentBuffer() {
    return currentBufferIndex == -1 ? null : bufferList.get(currentBufferIndex);
  }

  /**
   * If the currentBufferIndex is less than the buffer size - 1,
   * it means, the next buffer in the list has been freed up for
   * rewriting. Reuse the next available buffer in such cases.
   *
   * In case, the currentBufferIndex == buffer.size and buffer size is still
   * less than the capacity to be allocated, just allocate a buffer of size
   * chunk size.
   *
   */
  public ByteBuffer allocateBufferIfNeeded() {
    ByteBuffer buffer = getCurrentBuffer();
    if (buffer != null && buffer.hasRemaining()) {
      return buffer;
    }
    if (currentBufferIndex < bufferList.size() - 1) {
      buffer = getBuffer(currentBufferIndex + 1);
    } else {
      buffer = ByteBuffer.allocate(bufferSize);
      bufferList.add(buffer);
    }
    Preconditions.checkArgument(bufferList.size() <= capacity);
    currentBufferIndex++;
    // TODO: Turn the below precondition check on when Standalone pipeline
    // is removed in the write path in tests
    // Preconditions.checkArgument(buffer.position() == 0);
    return buffer;
  }

  public void releaseBuffer(ByteBuffer byteBuffer) {
    // always remove from head of the list and append at last
    ByteBuffer buffer = bufferList.remove(0);
    // Ensure the buffer to be removed is always at the head of the list.
    Preconditions.checkArgument(buffer.equals(byteBuffer));
    buffer.clear();
    bufferList.add(buffer);
    Preconditions.checkArgument(currentBufferIndex >= 0);
    currentBufferIndex--;
  }

  public void clearBufferPool() {
    bufferList.clear();
    currentBufferIndex = -1;
  }

  public void checkBufferPoolEmpty() {
    Preconditions.checkArgument(computeBufferData() == 0);
  }

  public long computeBufferData() {
    return bufferList.stream().mapToInt(value -> value.position())
        .sum();
  }

  public int getSize() {
    return bufferList.size();
  }

  public ByteBuffer getBuffer(int index) {
    return bufferList.get(index);
  }

  int getCurrentBufferIndex() {
    return currentBufferIndex;
  }

}
