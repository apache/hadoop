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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.NodeRegistration;

/**
 * This exception is thrown when a node that has not previously 
 * registered is trying to access the name node.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class UnregisteredNodeException extends IOException {
  private static final long serialVersionUID = -5620209396945970810L;

  public UnregisteredNodeException(JournalInfo info) {
    super("Unregistered server: " + info.toString());
  }
  
  public UnregisteredNodeException(NodeRegistration nodeReg) {
    super("Unregistered server: " + nodeReg.toString());
  }

  /**
   * The exception is thrown if a different data-node claims the same
   * storage id as the existing one.
   *  
   * @param nodeID unregistered data-node
   * @param storedNode data-node stored in the system with this storage id
   */
  public UnregisteredNodeException(DatanodeID nodeID, DatanodeInfo storedNode) {
    super("Data node " + nodeID + " is attempting to report storage ID " 
          + nodeID.getDatanodeUuid() + ". Node "
          + storedNode + " is expected to serve this storage.");
  }
}
