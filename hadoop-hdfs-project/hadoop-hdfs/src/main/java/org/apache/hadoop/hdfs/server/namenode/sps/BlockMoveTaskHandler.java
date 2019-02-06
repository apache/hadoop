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

package org.apache.hadoop.hdfs.server.namenode.sps;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;

/**
 * Interface for implementing different ways of block moving approaches. One can
 * connect directly to DN and request block move, and other can talk NN to
 * schedule via heart-beats.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface BlockMoveTaskHandler {

  /**
   * This is an interface method to handle the move tasks. BlockMovingInfo must
   * contain the required info to move the block, that source location,
   * destination location and storage types.
   */
  void submitMoveTask(BlockMovingInfo blkMovingInfo) throws IOException;

}