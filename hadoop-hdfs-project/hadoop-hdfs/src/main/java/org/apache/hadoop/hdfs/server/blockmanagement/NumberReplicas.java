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

/**
 * A immutable object that stores the number of live replicas and
 * the number of decommissined Replicas.
 */
public class NumberReplicas {
  private int liveReplicas;
  private int decommissionedReplicas;
  private int corruptReplicas;
  private int excessReplicas;
  private int replicasOnStaleNodes;

  NumberReplicas() {
    initialize(0, 0, 0, 0, 0);
  }

  NumberReplicas(int live, int decommissioned, int corrupt, int excess, int stale) {
    initialize(live, decommissioned, corrupt, excess, stale);
  }

  void initialize(int live, int decommissioned, int corrupt, int excess, int stale) {
    liveReplicas = live;
    decommissionedReplicas = decommissioned;
    corruptReplicas = corrupt;
    excessReplicas = excess;
    replicasOnStaleNodes = stale;
  }

  public int liveReplicas() {
    return liveReplicas;
  }
  public int decommissionedReplicas() {
    return decommissionedReplicas;
  }
  public int corruptReplicas() {
    return corruptReplicas;
  }
  public int excessReplicas() {
    return excessReplicas;
  }
  
  /**
   * @return the number of replicas which are on stale nodes.
   * This is not mutually exclusive with the other counts -- ie a
   * replica may count as both "live" and "stale".
   */
  public int replicasOnStaleNodes() {
    return replicasOnStaleNodes;
  }
} 
