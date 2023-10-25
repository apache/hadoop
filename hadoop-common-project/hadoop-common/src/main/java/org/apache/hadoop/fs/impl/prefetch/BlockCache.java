/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;

/**
 * Provides functionality necessary for caching blocks of data read from FileSystem.
 */
public interface BlockCache extends Closeable {

  /**
   * Indicates whether the given block is in this cache.
   *
   * @param blockNumber the id of the given block.
   * @return true if the given block is in this cache, false otherwise.
   */
  boolean containsBlock(int blockNumber);

  /**
   * Gets the blocks in this cache.
   *
   * @return the blocks in this cache.
   */
  Iterable<Integer> blocks();

  /**
   * Gets the number of blocks in this cache.
   *
   * @return the number of blocks in this cache.
   */
  int size();

  /**
   * Gets the block having the given {@code blockNumber}.
   *
   * @param blockNumber the id of the desired block.
   * @param buffer contents of the desired block are copied to this buffer.
   * @throws IOException if there is an error reading the given block.
   */
  void get(int blockNumber, ByteBuffer buffer) throws IOException;

  /**
   * Puts the given block in this cache.
   *
   * @param blockNumber the id of the given block.
   * @param buffer contents of the given block to be added to this cache.
   * @param conf the configuration.
   * @param localDirAllocator the local dir allocator instance.
   * @throws IOException if there is an error writing the given block.
   */
  void put(int blockNumber, ByteBuffer buffer, Configuration conf,
      LocalDirAllocator localDirAllocator) throws IOException;
}
