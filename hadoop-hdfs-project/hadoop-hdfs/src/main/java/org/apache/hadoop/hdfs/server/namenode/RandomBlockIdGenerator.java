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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.util.IdGenerator;

/**
 * Generator of random block IDs.
 */
@InterfaceAudience.Private
public class RandomBlockIdGenerator implements IdGenerator {
  private final BlockManager blockManager;

  RandomBlockIdGenerator(FSNamesystem namesystem) {
    this.blockManager = namesystem.getBlockManager();
  }

  @Override // NumberGenerator
  public long nextValue() {
    Block b = new Block(DFSUtil.getRandom().nextLong(), 0, 0); 
    while(isValidBlock(b)) {
      b.setBlockId(DFSUtil.getRandom().nextLong());
    }
    return b.getBlockId();
  }

  /**
   * Returns whether the given block is one pointed-to by a file.
   */
  private boolean isValidBlock(Block b) {
    return (blockManager.getBlockCollection(b) != null);
  }
}
