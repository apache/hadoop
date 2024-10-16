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

import org.apache.hadoop.thirdparty.com.google.common.base.Objects;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.LIVE;

class NumberReplicasStriped extends NumberReplicas {

  private DatanodeStorageInfo[] storages;
  private StoredReplicaState[] states;
  private boolean considerBusy;
  private boolean[] busys;

  NumberReplicasStriped(int totalBlock, boolean considerBusy) {
    this.storages = new DatanodeStorageInfo[totalBlock];
    this.states = new StoredReplicaState[totalBlock];
    this.considerBusy = considerBusy;
    if (this.considerBusy) {
      this.busys = new boolean[totalBlock];
    }
  }

  public void add(final StoredReplicaState state, final long value, int blockIndex,
      DatanodeStorageInfo storage, boolean busy) {
    if (this.storages[blockIndex] == null) {
      this.storages[blockIndex] = storage;
      this.states[blockIndex] = state;
      if (this.considerBusy) {
        this.busys[blockIndex] = busy;
      }
      this.add(state, 1);
    } else if (state.getPriority() < this.states[blockIndex].getPriority()) {
      this.subtract(this.states[blockIndex], 1);
      this.storages[blockIndex] = storage;
      this.states[blockIndex] = state;
      if (this.considerBusy) {
        this.busys[blockIndex] = busy;
      }
      this.add(state, 1);
    } else if (this.states[blockIndex].getPriority() == state.getPriority()) {
      if (state == LIVE) {
        super.add(StoredReplicaState.REDUNDANT, 1);
      }
      if (!considerBusy || this.busys[blockIndex] == busy) {
        // If we do not consider busy, or both the original and current are busy
        // or not busy, means the same priority, we should replace randomly.
        if (ThreadLocalRandom.current().nextBoolean()) {
          this.storages[blockIndex] = storage;
          this.states[blockIndex] = state;
        }
      } else if (this.busys[blockIndex] && !busy) {
        // If the original storage is busy, the current storage is not busy,
        // means higher priority, we will replace.
        this.storages[blockIndex] = storage;
        this.states[blockIndex] = state;
        this.busys[blockIndex] = busy;
      }
    }
  }

  public DatanodeStorageInfo getStorage(int blockIndex) {
    return storages[blockIndex];
  }

  public StoredReplicaState getState(int blockIndex) {
    return states[blockIndex];
  }

  public boolean isBusy(int blockIndex) {
    assert considerBusy;
    return this.busys[blockIndex];
  }

  public boolean exist(int blockIndex) {
    return this.states[blockIndex] != null;
  }

  public int getSize() {
    return this.storages.length;
  }

  public static boolean isReadableState(StoredReplicaState state) {
    if (state == LIVE || state == StoredReplicaState.READONLY ||
        state == StoredReplicaState.DECOMMISSIONING ||
        state == StoredReplicaState.MAINTENANCE_FOR_READ) {
      return true;
    }
    return false;
  }

  public int readableReplicas() {
    return this.liveReplicas() + this.readOnlyReplicas() + this.decommissioning() +
        this.liveEnteringMaintenanceReplicas();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    NumberReplicasStriped that = (NumberReplicasStriped) o;
    return considerBusy == that.considerBusy &&
        Objects.equal(storages, that.storages) &&
        Objects.equal(states, that.states) &&
        Objects.equal(busys, that.busys);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), storages, states, considerBusy, busys);
  }
}
