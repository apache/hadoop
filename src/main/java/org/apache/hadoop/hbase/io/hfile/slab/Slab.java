/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile.slab;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.DirectMemoryUtils;
import com.google.common.base.Preconditions;

/**
 * Slab is a class which is designed to allocate blocks of a certain size.
 * Constructor creates a number of DirectByteBuffers and slices them into the
 * requisite size, then puts them all in a buffer.
 **/

class Slab implements org.apache.hadoop.hbase.io.HeapSize {
  static final Log LOG = LogFactory.getLog(Slab.class);

  /** This is where our items, or blocks of the slab, are stored. */
  private ConcurrentLinkedQueue<ByteBuffer> buffers;

  /** This is where our Slabs are stored */
  private ConcurrentLinkedQueue<ByteBuffer> slabs;

  private final int blockSize;
  private final int numBlocks;
  private long heapSize;

  Slab(int blockSize, int numBlocks) {
    buffers = new ConcurrentLinkedQueue<ByteBuffer>();
    slabs = new ConcurrentLinkedQueue<ByteBuffer>();

    this.blockSize = blockSize;
    this.numBlocks = numBlocks;

    this.heapSize = ClassSize.estimateBase(this.getClass(), false);

    int maxBlocksPerSlab = Integer.MAX_VALUE / blockSize;
    int maxSlabSize = maxBlocksPerSlab * blockSize;

    int numFullSlabs = numBlocks / maxBlocksPerSlab;
    int partialSlabSize = (numBlocks % maxBlocksPerSlab) * blockSize;
    for (int i = 0; i < numFullSlabs; i++) {
      allocateAndSlice(maxSlabSize, blockSize);
    }

    if (partialSlabSize > 0) {
      allocateAndSlice(partialSlabSize, blockSize);
    }
  }

  private void allocateAndSlice(int size, int sliceSize) {
    ByteBuffer newSlab = ByteBuffer.allocateDirect(size);
    slabs.add(newSlab);
    for (int j = 0; j < newSlab.capacity(); j += sliceSize) {
      newSlab.limit(j + sliceSize).position(j);
      ByteBuffer aSlice = newSlab.slice();
      buffers.add(aSlice);
      heapSize += ClassSize.estimateBase(aSlice.getClass(), false);
    }
  }

  /*
   * Shutdown deallocates the memory for all the DirectByteBuffers. Each
   * DirectByteBuffer has a "cleaner" method, which is similar to a
   * deconstructor in C++.
   */
  void shutdown() {
    for (ByteBuffer aSlab : slabs) {
      try {
        DirectMemoryUtils.destroyDirectByteBuffer(aSlab);
      } catch (Exception e) {
        LOG.warn("Unable to deallocate direct memory during shutdown", e);
      }
    }
  }

  int getBlockSize() {
    return this.blockSize;
  }

  int getBlockCapacity() {
    return this.numBlocks;
  }

  int getBlocksRemaining() {
    return this.buffers.size();
  }

  /*
   * This returns null if empty. Throws an exception if you try to allocate a
   * bigger size than the allocator can handle.
   */
  ByteBuffer alloc(int bufferSize) {
    int newCapacity = Preconditions.checkPositionIndex(bufferSize, blockSize);

    ByteBuffer returnedBuffer = buffers.poll();
    if (returnedBuffer == null) {
      return null;
    }

    returnedBuffer.clear().limit(newCapacity);
    return returnedBuffer;
  }

  void free(ByteBuffer toBeFreed) {
    Preconditions.checkArgument(toBeFreed.capacity() == blockSize);
    buffers.add(toBeFreed);
  }

  @Override
  public long heapSize() {
    return heapSize;
  }
}
