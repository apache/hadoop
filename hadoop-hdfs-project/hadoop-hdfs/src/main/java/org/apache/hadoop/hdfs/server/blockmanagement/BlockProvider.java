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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedStorageMap.ProvidedBlockList;
import org.apache.hadoop.hdfs.util.RwLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to load provided blocks in the {@link BlockManager}.
 */
public abstract class BlockProvider implements Iterable<Block> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProvidedStorageMap.class);

  private RwLock lock;
  private BlockManager bm;
  private DatanodeStorageInfo storage;
  private boolean hasDNs = false;

  /**
   * @param lock the namesystem lock
   * @param bm block manager
   * @param storage storage for provided blocks
   */
  void init(RwLock lock, BlockManager bm, DatanodeStorageInfo storage) {
    this.bm = bm;
    this.lock = lock;
    this.storage = storage;
  }

  /**
   * start the processing of block report for provided blocks.
   * @throws IOException
   */
  void start() throws IOException {
    assert lock.hasWriteLock() : "Not holding write lock";
    if (hasDNs) {
      return;
    }
    LOG.info("Calling process first blk report from storage: " + storage);
    // first pass; periodic refresh should call bm.processReport
    bm.processFirstBlockReport(storage, new ProvidedBlockList(iterator()));
    hasDNs = true;
  }
}
