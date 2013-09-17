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
package org.apache.hadoop.hdfs.protocol;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Augments an array of blocks on a datanode with additional information about
 * where the block is stored.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HdfsBlocksMetadata {
  
  /**
   * List of blocks
   */
  private final ExtendedBlock[] blocks;
  
  /**
   * List of volumes
   */
  private final List<byte[]> volumeIds;
  
  /**
   * List of indexes into <code>volumeIds</code>, one per block in
   * <code>blocks</code>. A value of Integer.MAX_VALUE indicates that the
   * block was not found.
   */
  private final List<Integer> volumeIndexes;

  /**
   * Constructs HdfsBlocksMetadata.
   * 
   * @param blocks
   *          List of blocks described
   * @param volumeIds
   *          List of potential volume identifiers, specifying volumes where 
   *          blocks may be stored
   * @param volumeIndexes
   *          Indexes into the list of volume identifiers, one per block
   */
  public HdfsBlocksMetadata(ExtendedBlock[] blocks, List<byte[]> volumeIds, 
      List<Integer> volumeIndexes) {
    this.blocks = blocks;
    this.volumeIds = volumeIds;
    this.volumeIndexes = volumeIndexes;
  }

  /**
   * Get the array of blocks.
   * 
   * @return array of blocks
   */
  public ExtendedBlock[] getBlocks() {
    return blocks;
  }
  
  /**
   * Get the list of volume identifiers in raw byte form.
   * 
   * @return list of ids
   */
  public List<byte[]> getVolumeIds() {
    return volumeIds;
  }

  /**
   * Get a list of indexes into the array of {@link VolumeId}s, one per block.
   * 
   * @return list of indexes
   */
  public List<Integer> getVolumeIndexes() {
    return volumeIndexes;
  }
}
