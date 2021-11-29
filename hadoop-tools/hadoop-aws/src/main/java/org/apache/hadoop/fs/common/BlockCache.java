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

package org.apache.hadoop.fs.common;

import com.twitter.util.ExceptionalFunction0;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Provides functionality necessary for caching blocks of data read from FileSystem.
 */
public interface BlockCache extends Closeable {

  /**
   * Indicates whether the given block is in this cache.
   */
  public boolean containsBlock(Integer blockNumber);

  /**
   * Gets the blocks in this cache.
   */
  public Iterable<Integer> blocks();

  /**
   * Gets the number of blocks in this cache.
   */
  public int size();

  /**
   * Gets the block having the given {@code blockNumber}.
   */
  public void get(Integer blockNumber, ByteBuffer buffer) throws IOException;

  /**
   * Puts the given block in this cache.
   */
  public void put(Integer blockNumber, ByteBuffer buffer) throws IOException;
}
