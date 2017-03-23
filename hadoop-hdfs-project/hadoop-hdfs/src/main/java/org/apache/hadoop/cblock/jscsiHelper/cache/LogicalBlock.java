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
package org.apache.hadoop.cblock.jscsiHelper.cache;

import java.nio.ByteBuffer;

/**
 * Logical Block is the data structure that we write to the cache,
 * the key and data gets written to remote contianers. Rest is used for
 * book keeping for the cache.
 */
public interface LogicalBlock {
  /**
   * Returns the data stream of this block.
   * @return - ByteBuffer
   */
  ByteBuffer getData();

  /**
   * Frees the byte buffer since we don't need it any more.
   */
  void clearData();

  /**
   * Returns the Block ID for this Block.
   * @return long - BlockID
   */
  long getBlockID();

  /**
   * Flag that tells us if this block has been persisted to container.
   * @return whether this block is now persistent
   */
  boolean isPersisted();
}