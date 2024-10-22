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
    // live replicas
    LIVE((byte) 1),
    READONLY((byte) 2),
    // decommissioning replicas.
    DECOMMISSIONING((byte) 5),
    DECOMMISSIONED((byte) 11),
    // We need live ENTERING_MAINTENANCE nodes to continue
    // to serve read request while it is being transitioned to live
    // IN_MAINTENANCE if these are the only replicas left.
    // MAINTENANCE_NOT_FOR_READ == maintenanceReplicas -
    // Live ENTERING_MAINTENANCE.
    MAINTENANCE_NOT_FOR_READ((byte) 4),
    // Live ENTERING_MAINTENANCE nodes to serve read requests.
    MAINTENANCE_FOR_READ((byte) 3),
    CORRUPT((byte) 13),
    // excess replicas already tracked by blockmanager's excess map
    EXCESS((byte) 12),
    STALESTORAGE(Byte.MAX_VALUE),
    // for striped blocks only. number of redundant internal block replicas
    // that have not been tracked by blockmanager yet (i.e., not in excess)
    REDUNDANT(Byte.MAX_VALUE);

    /*
     * Priority is used to handle the situation where some block index in
     * an EC block has multiple replicas. The higher the priority, the
     * healthier the storage. 1 is the highest priority.
     * For example, if a block index has two replicas, one in the
     * LIVE state and the other in the CORRUPT state, the state will be
     * store as LIVE.
     *
     * The priorities are classified as follows:
     * (1) States that may be read normally
     *   including LIVE, READONLY, MAINTENANCE_FOR_READ, MAINTENANCE_NOT_FOR_READ,
     *   and DECOMMISSIONING. These states have the highest priority, ranging
     *   from 1 to 5. It is easy to understand that LIVE has the highest
     *   priority, READONLY has the second highest priority, and MAINTENANCE_FOR_READ
     *   must have a higher priority than MAINTENANCE_NOT_FOR_READ. Then
     *   DECOMMISSIONING has a lower priority than MAINTENANCE_FOR_READ and
     *   MAINTENANCE_NOT_FOR_READ, mainly because we can tolerate the temporary loss
     *   of replicas in the MAINTENANCE state.
     * (2) States that may not be read normally
     *   including DECOMMISSIONED, EXCESS, and CORRUPT. Their priorities are the lowest,
     *   ranging from 11 to 13.
     * (3) Some special states, including STALESTORAGE and REDUNDANT, are only used for
     *   statistical calculations.
     * */
    private final byte priority;

    StoredReplicaState(byte priority) {
      this.priority = priority;
    }

    public byte getPriority() {
      return priority;
    }
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
    return decommissioned() + decommissioning();
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

  public void add(final NumberReplicas.StoredReplicaState e, final long value, int blockIndex,
      DatanodeStorageInfo storage, boolean allowReplaceIfExist) {
    super.add(e, value);
  }
}
