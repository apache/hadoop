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
package org.apache.hadoop.hdds.scm.protocol;

import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * ScmBlockLocationProtocol is used by an HDFS node to find the set of nodes
 * to read/write a block.
 */
@KerberosInfo(serverPrincipal = ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
public interface ScmBlockLocationProtocol extends Closeable {

  @SuppressWarnings("checkstyle:ConstantName")
  /**
   * Version 1: Initial version.
   */
  long versionID = 1L;

  /**
   * Asks SCM where a block should be allocated. SCM responds with the
   * set of datanodes that should be used creating this block.
   * @param size - size of the block.
   * @param numBlocks - number of blocks.
   * @param type - replication type of the blocks.
   * @param factor - replication factor of the blocks.
   * @param excludeList List of datanodes/containers to exclude during block
   *                    allocation.
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  List<AllocatedBlock> allocateBlock(long size, int numBlocks,
      ReplicationType type, ReplicationFactor factor, String owner,
      ExcludeList excludeList) throws IOException;

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
