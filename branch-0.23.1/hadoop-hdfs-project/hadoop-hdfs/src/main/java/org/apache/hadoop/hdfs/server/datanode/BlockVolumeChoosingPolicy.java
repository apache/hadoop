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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;

/**************************************************
 * BlockVolumeChoosingPolicy allows a DataNode to
 * specify what policy is to be used while choosing
 * a volume for a block request.
 * 
 ***************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface BlockVolumeChoosingPolicy {

  /**
   * Returns a specific FSVolume after applying a suitable choice algorithm
   * to place a given block, given a list of FSVolumes and the block
   * size sought for storage.
   * 
   * (Policies that maintain state must be thread-safe.)
   * 
   * @param volumes - the array of FSVolumes that are available.
   * @param blockSize - the size of the block for which a volume is sought.
   * @return the chosen volume to store the block.
   * @throws IOException when disks are unavailable or are full.
   */
  public FSVolume chooseVolume(List<FSVolume> volumes, long blockSize)
    throws IOException;

}
