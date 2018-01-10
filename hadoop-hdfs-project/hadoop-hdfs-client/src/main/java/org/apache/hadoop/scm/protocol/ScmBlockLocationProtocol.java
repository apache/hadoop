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
package org.apache.hadoop.scm.protocol;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationFactor;
import org.apache.hadoop.scm.ScmInfo;

/**
 * ScmBlockLocationProtocol is used by an HDFS node to find the set of nodes
 * to read/write a block.
 */
public interface ScmBlockLocationProtocol {

  /**
   * Find the set of nodes to read/write a block, as
   * identified by the block key.  This method supports batch lookup by
   * passing multiple keys.
   *
   * @param keys batch of block keys to find
   * @return allocated blocks for each block key
   * @throws IOException if there is any failure
   */
  Set<AllocatedBlock> getBlockLocations(Set<String> keys) throws IOException;

  /**
   * Asks SCM where a block should be allocated. SCM responds with the
   * set of datanodes that should be used creating this block.
   * @param size - size of the block.
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  AllocatedBlock allocateBlock(long size, ReplicationType type,
      ReplicationFactor factor, String owner) throws IOException;

  /**
   * Delete blocks for a set of object keys.
   *
   * @param keyBlocksInfoList Map of object key and its blocks.
   * @return list of block deletion results.
   * @throws IOException if there is any failure.
   */
  List<DeleteBlockGroupResult>
      deleteKeyBlocks(List<BlockGroup> keyBlocksInfoList) throws IOException;

  /**
   * Gets the Clusterid and SCM Id from SCM.
   */
  ScmInfo getScmInfo() throws IOException;
}
