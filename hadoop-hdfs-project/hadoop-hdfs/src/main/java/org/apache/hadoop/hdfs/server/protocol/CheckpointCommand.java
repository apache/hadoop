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
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;

/**
 * Checkpoint command.
 * <p>
 * Returned to the backup node by the name-node as a reply to the
 * {@link NamenodeProtocol#startCheckpoint(NamenodeRegistration)}
 * request.<br>
 * Contains:
 * <ul>
 * <li>{@link CheckpointSignature} identifying the particular checkpoint</li>
 * <li>indicator whether the backup image should be discarded before starting 
 * the checkpoint</li>
 * <li>indicator whether the image should be transfered back to the name-node
 * upon completion of the checkpoint.</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CheckpointCommand extends NamenodeCommand {
  private final CheckpointSignature cSig;
  private final boolean needToReturnImage;

  public CheckpointCommand() {
    this(null, false);
  }

  public CheckpointCommand(CheckpointSignature sig,
                           boolean needToReturnImg) {
    super(NamenodeProtocol.ACT_CHECKPOINT);
    this.cSig = sig;
    this.needToReturnImage = needToReturnImg;
  }

  /**
   * Checkpoint signature is used to ensure 
   * that nodes are talking about the same checkpoint.
   */
  public CheckpointSignature getSignature() {
    return cSig;
  }

  /**
   * Indicates whether the new checkpoint image needs to be transfered 
   * back to the name-node after the checkpoint is done.
   * 
   * @return true if the checkpoint should be returned back.
   */
  public boolean needToReturnImage() {
    return needToReturnImage;
  }
}
