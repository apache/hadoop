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
package org.apache.hadoop.hdfs.server.protocol;

import com.google.common.base.Joiner;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockECRecoveryInfo;

import java.util.Collection;

/**
 * A BlockECRecoveryCommand is an instruction to a DataNode to reconstruct a
 * striped block group with missing blocks.
 *
 * Upon receiving this command, the DataNode pulls data from other DataNodes
 * hosting blocks in this group and reconstructs the lost blocks through codec
 * calculation.
 *
 * After the reconstruction, the DataNode pushes the reconstructed blocks to
 * their final destinations if necessary (e.g., the destination is different
 * from the reconstruction node, or multiple blocks in a group are to be
 * reconstructed).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockECRecoveryCommand extends DatanodeCommand {
  final Collection<BlockECRecoveryInfo> ecTasks;

  /**
   * Create BlockECRecoveryCommand from a collection of
   * {@link BlockECRecoveryInfo}, each representing a recovery task
   */
  public BlockECRecoveryCommand(int action,
      Collection<BlockECRecoveryInfo> blockECRecoveryInfoList) {
    super(action);
    this.ecTasks = blockECRecoveryInfoList;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlockECRecoveryCommand(\n  ");
    Joiner.on("\n  ").appendTo(sb, ecTasks);
    sb.append("\n)");
    return sb.toString();
  }
}
