/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.common.helpers;

import org.apache.hadoop.hdds.client.BlockID;

import static org.apache.hadoop.hdds.protocol.proto
    .ScmBlockLocationProtocolProtos.DeleteScmBlockResult;

/**
 * Class wraps storage container manager block deletion results.
 */
public class DeleteBlockResult {
  private BlockID blockID;
  private DeleteScmBlockResult.Result result;

  public DeleteBlockResult(final BlockID blockID,
      final DeleteScmBlockResult.Result result) {
    this.blockID = blockID;
    this.result = result;
  }

  /**
   * Get block id deleted.
   * @return block id.
   */
  public BlockID getBlockID() {
    return blockID;
  }

  /**
   * Get key deletion result.
   * @return key deletion result.
   */
  public DeleteScmBlockResult.Result getResult() {
    return result;
  }
}
