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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/****************************************************
 * A BlockIdCommand is an instruction to a datanode 
 * regarding some blocks under its control.
 ****************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockIdCommand extends DatanodeCommand {
  final String poolId;
  final long blockIds[];

  /**
   * Create BlockCommand for the given action
   */
  public BlockIdCommand(int action, String poolId, long[] blockIds) {
    super(action);
    this.poolId = poolId;
    this.blockIds= blockIds;
  }
  
  public String getBlockPoolId() {
    return poolId;
  }
  
  public long[] getBlockIds() {
    return blockIds;
  }
}
