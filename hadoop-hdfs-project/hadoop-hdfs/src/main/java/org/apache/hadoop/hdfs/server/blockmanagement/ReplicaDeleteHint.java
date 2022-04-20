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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Provides a hint to excess redundancy processing as to which replica node to delete,
 * and how long of a grace period to provide.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicaDeleteHint {
  /**
   * Represents an empty hint which contains neither a datanode hint nor grace period
   */
  public static ReplicaDeleteHint NONE = new ReplicaDeleteHint(null, 0);

  private final DatanodeDescriptor datanode;
  private final long gracePeriod;

  ReplicaDeleteHint(DatanodeDescriptor datanode, long gracePeriod) {
    this.datanode = datanode;
    this.gracePeriod = gracePeriod;
  }

  /**
   * Return a new ReplicaDeleteHint with the datanode nulled out, but gracePeriod retained.
   */
  ReplicaDeleteHint withNullDataNode() {
    return new ReplicaDeleteHint(null, gracePeriod);
  }

  /**
   * Get the underyling datanode
   */
  DatanodeDescriptor getDatanode() {
    return datanode;
  }

  /**
   * Get the underlying grace period
   */
  long getGracePeriod() {
    return gracePeriod;
  }
}
