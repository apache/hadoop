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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.DatanodeID;


/**
 * This exception is thrown when a datanode tries to register or communicate
 * with the namenode when it does not appear on the list of included nodes, 
 * or has been specifically excluded.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DisallowedDatanodeException extends IOException {
  /** for java.io.Serializable */
  private static final long serialVersionUID = 1L;

  public DisallowedDatanodeException(DatanodeID nodeID, String reason) {
    super("Datanode denied communication with namenode because "
        + reason + ": " + nodeID);
  }

  public DisallowedDatanodeException(DatanodeID nodeID) {
    this(nodeID, "the host is not in the include-list");
  }
}
