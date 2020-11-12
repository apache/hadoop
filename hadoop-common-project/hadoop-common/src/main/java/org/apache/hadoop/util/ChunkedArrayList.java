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
package org.apache.hadoop.util;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * Simplified List implementation which stores elements as a list
 * of chunks, each chunk having a maximum size. This improves over
 * using an ArrayList in that creating a large list will never require
 * a large amount of contiguous heap space -- thus reducing the likelihood
 * of triggering a CMS compaction pause due to heap fragmentation.
 * 
 * The first chunks allocated are small, but each additional chunk is
 * 50% larger than the previous, ramping up to a configurable maximum
 * chunk size. Reasonable defaults are provided which should be a good
 * balance between not making any large allocations while still retaining
 * decent performance.
 *
 * This currently only supports a small subset of List operations --
 * namely addition and iteration.
 */
@InterfaceAudience.Private
public class ChunkedArrayList<T> extends AbstractList<T> {

  /**
   * The chunks which make up the full list.
   */
  private final List<List<T>> chunks = Lists.newArrayList();
  
  /**
   * Cache of the last element in the 'chunks' array above.
   * This speeds up the add operation measurably.
   */
  private List<T> lastChunk = null;

  /**
   * The capacity with which the last chunk was allocated.
   */
  private int lastChunkCapacity;
  
  /**
   * The capacity of the first chunk to allocate in a cleared list.
   */
  private final int initialChunkCapacity;
  
  /**
   * The maximum number of elements for any chunk.
   */
  private final int maxChunkSize;

  /**
   * Total number of elements in the list.
   */
  private int size;
  
  /**
   * Default initial size is 6 elements, since typical minimum object
   * size is 64 bytes, and this leaves enough space for the object
   * header.
   */
  private static final int DEFAULT_INITIAL_CHUNK_CAPACITY = 6;
  
  /**
   * Default max size is 8K elements - which, at 8 bytes per element
   * should be about 64KB -- small enough to easily fit in contiguous
   * free heap space even with a fair amount of fragmentation.
   */
  private static final int DEFAULT_MAX_CHUNK_SIZE = 8*1024;
  

  public ChunkedArrayList() {
    this(DEFAULT_INITIAL_CHUNK_CAPACITY, DEFAULT_MAX_CHUNK_SIZE);
  }

  /**
   * @param initialChunkCapacity the capacity of the first chunk to be
   * allocated
   * @param maxChunkSize the maximum size of any chunk allocated
   */
  public ChunkedArrayList(int initialChunkCapacity, int maxChunkSize) {
    Preconditions.checkArgument(maxChunkSize >= initialChunkCapacity);
    this.initialChunkCapacity = initialChunkCapacity;
    this.maxChunkSize = maxChunkSize;
  }

  @Override
  public Iterator<T> iterator() {
    final Iterator<T> it = Iterables.concat(chunks).iterator();

    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public T next() {
        return it.next();
      }

      @Override
      public void remove() {
        it.remove();
        size--;
      }
    };
  }

  @Override
  public boolean add(T e) {
    if (size == Integer.MAX_VALUE) {
      throw new RuntimeException("Can't add an additional element to the " +
          "list; list already has INT_MAX elements.");
    }
    if (lastChunk == null) {
      addChunk(initialChunkCapacity);
    } else if (lastChunk.size() >= lastChunkCapacity) {
      int newCapacity = lastChunkCapacity + (lastChunkCapacity >> 1);
      addChunk(Math.min(newCapacity, maxChunkSize));
    }
    size++;
    return lastChunk.add(e);
  }

  @Override
  public void clear() {
    chunks.clear();
    lastChunk = null;
    lastChunkCapacity = 0;
    size = 0;
  }
  
  private void addChunk(int capacity) {
    lastChunk = Lists.newArrayListWithCapacity(capacity);
    chunks.add(lastChunk);
    lastChunkCapacity = capacity;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public int size() {
    return size;
  }
  
  @VisibleForTesting
  int getNumChunks() {
    return chunks.size();
  }
  
  @VisibleForTesting
  int getMaxChunkSize() {
    int size = 0;
    for (List<T> chunk : chunks) {
      size = Math.max(size, chunk.size());
    }
    return size;
  }

  @Override
  public T get(int idx) {
    if (idx < 0) {
      throw new IndexOutOfBoundsException();
    }
    int base = 0;
    Iterator<List<T>> it = chunks.iterator();
    while (it.hasNext()) {
      List<T> list = it.next();
      int size = list.size();
      if (idx < base + size) {
        return list.get(idx - base);
      }
      base += size;
    }
    throw new IndexOutOfBoundsException();
  }
}
