/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io.hfile;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Represents an entry in the {@link LruBlockCache}.
 *
 * <p>Makes the block memory-aware with {@link HeapSize} and Comparable
 * to sort by access time for the LRU.  It also takes care of priority by
 * either instantiating as in-memory or handling the transition from single
 * to multiple access.
 */
public class CachedBlock implements HeapSize, Comparable<CachedBlock> {

  public final static long PER_BLOCK_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (3 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG) +
    ClassSize.STRING + ClassSize.BYTE_BUFFER);

  static enum BlockPriority {
    /**
     * Accessed a single time (used for scan-resistance)
     */
    SINGLE,
    /**
     * Accessed multiple times
     */
    MULTI,
    /**
     * Block from in-memory store
     */
    MEMORY
  };

  private final String blockName;
  private final ByteBuffer buf;
  private volatile long accessTime;
  private long size;
  private BlockPriority priority;

  public CachedBlock(String blockName, ByteBuffer buf, long accessTime) {
    this(blockName, buf, accessTime, false);
  }

  public CachedBlock(String blockName, ByteBuffer buf, long accessTime,
      boolean inMemory) {
    this.blockName = blockName;
    this.buf = buf;
    this.accessTime = accessTime;
    this.size = ClassSize.align(blockName.length()) +
    ClassSize.align(buf.capacity()) + PER_BLOCK_OVERHEAD;
    if(inMemory) {
      this.priority = BlockPriority.MEMORY;
    } else {
      this.priority = BlockPriority.SINGLE;
    }
  }

  /**
   * Block has been accessed.  Update its local access time.
   */
  public void access(long accessTime) {
    this.accessTime = accessTime;
    if(this.priority == BlockPriority.SINGLE) {
      this.priority = BlockPriority.MULTI;
    }
  }

  public long heapSize() {
    return size;
  }

  public int compareTo(CachedBlock that) {
    if(this.accessTime == that.accessTime) return 0;
    return this.accessTime < that.accessTime ? 1 : -1;
  }

  public ByteBuffer getBuffer() {
    return this.buf;
  }

  public String getName() {
    return this.blockName;
  }

  public BlockPriority getPriority() {
    return this.priority;
  }
}