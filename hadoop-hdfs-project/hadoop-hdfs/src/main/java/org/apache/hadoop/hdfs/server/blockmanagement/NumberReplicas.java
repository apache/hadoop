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

import org.apache.hadoop.hdfs.util.EnumCounters;

import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.CORRUPT;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.DECOMMISSIONED;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.DECOMMISSIONING;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.EXCESS;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.LIVE;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.MAINTENANCE_FOR_READ;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.MAINTENANCE_NOT_FOR_READ;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.READONLY;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.REDUNDANT;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.STALESTORAGE;

/**
 * A immutable object that stores the number of live replicas and
 * the number of decommissioned Replicas.
 */
public class NumberReplicas extends EnumCounters<NumberReplicas.StoredReplicaState> {

  public enum StoredReplicaState {
    // live replicas. for a striped block, this value excludes redundant
    // replicas for the same internal block
    LIVE,
    READONLY,
    DECOMMISSIONING,
    DECOMMISSIONED,
    // We need live ENTERING_MAINTENANCE nodes to continue
    // to serve read request while it is being transitioned to live
    // IN_MAINTENANCE if these are the only replicas left.
    // MAINTENANCE_NOT_FOR_READ == maintenanceReplicas -
    // Live ENTERING_MAINTENANCE.
    MAINTENANCE_NOT_FOR_READ,
    // Live ENTERING_MAINTENANCE nodes to serve read requests.
    MAINTENANCE_FOR_READ,
    CORRUPT,
    // excess replicas already tracked by blockmanager's excess map
    EXCESS,
    STALESTORAGE,
    // for striped blocks only. number of redundant internal block replicas
    // that have not been tracked by blockmanager yet (i.e., not in excess)
    REDUNDANT
  }

  public NumberReplicas() {
    super(StoredReplicaState.class);
  }

  public int liveReplicas() {
    return (int) get(LIVE);
  }

  public int readOnlyReplicas() {
    return (int) get(READONLY);
  }

  /**
   *
   * @return decommissioned and decommissioning replicas
   */
  public int decommissionedAndDecommissioning() {
    return (int) (get(DECOMMISSIONED) + get(DECOMMISSIONING));
  }

  /**
   *
   * @return decommissioned replicas only
   */
  public int decommissioned() {
    return (int) get(DECOMMISSIONED);
  }

  /**
   *
   * @return decommissioning replicas only
   */
  public int decommissioning() {
    return (int) get(DECOMMISSIONING);
  }

  public int corruptReplicas() {
    return (int) get(CORRUPT);
  }

  public int excessReplicas() {
    return (int) get(EXCESS);
  }
  
  /**
   * @return the number of replicas which are on stale nodes.
   * This is not mutually exclusive with the other counts -- ie a
   * replica may count as both "live" and "stale".
   */
  public int replicasOnStaleNodes() {
    return (int) get(STALESTORAGE);
  }

  public int redundantInternalBlocks() {
    return (int) get(REDUNDANT);
  }

  public int maintenanceNotForReadReplicas() {
    return (int) get(MAINTENANCE_NOT_FOR_READ);
  }

  public int maintenanceReplicas() {
    return (int) (get(MAINTENANCE_NOT_FOR_READ) + get(MAINTENANCE_FOR_READ));
  }

  public int outOfServiceReplicas() {
    return maintenanceReplicas() + decommissionedAndDecommissioning();
  }

  public int liveEnteringMaintenanceReplicas() {
    return (int)get(MAINTENANCE_FOR_READ);
  }
}
