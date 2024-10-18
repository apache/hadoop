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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestNumberReplicasStriped {

  @Test
  public void testAddCounter() {
    NumberReplicasStriped replicas = new NumberReplicasStriped(5, false);

    // 1 basic add counter
    DatanodeStorageInfo storage0 = mock(DatanodeStorageInfo.class);
    DatanodeStorageInfo storage1 = mock(DatanodeStorageInfo.class);
    DatanodeStorageInfo storage2 = mock(DatanodeStorageInfo.class);
    DatanodeStorageInfo storage3 = mock(DatanodeStorageInfo.class);
    replicas.add(NumberReplicas.StoredReplicaState.LIVE, 1, 0, storage0, true);
    replicas.add(NumberReplicas.StoredReplicaState.LIVE, 1, 1, storage1, true);
    replicas.add(NumberReplicas.StoredReplicaState.READONLY, 1, 2, storage2, true);
    replicas.add(NumberReplicas.StoredReplicaState.DECOMMISSIONED, 1, 3, storage3, true);
    assertEquals(2, replicas.liveReplicas());
    assertEquals(storage0, replicas.getStorage(0));
    assertEquals(storage1, replicas.getStorage(1));
    assertEquals(1, replicas.readOnlyReplicas());
    assertEquals(storage2, replicas.getStorage(2));
    assertEquals(1, replicas.decommissioned());
    assertEquals(storage3, replicas.getStorage(3));

    // 2 add counter in exist block index
    DatanodeStorageInfo storage4 = mock(DatanodeStorageInfo.class);
    // we can replace with lower priority
    replicas.add(NumberReplicas.StoredReplicaState.READONLY, 1, 0, storage4, false);
    assertEquals(2, replicas.liveReplicas());
    assertEquals(1, replicas.readOnlyReplicas());
    assertEquals(storage0, replicas.getStorage(0));
    // if we replace with same priority, we will replace randomly
    replicas.add(NumberReplicas.StoredReplicaState.LIVE, 1, 0, storage4, false);
    assertEquals(2, replicas.liveReplicas());
    assertEquals(1, replicas.readOnlyReplicas());
    assertTrue(replicas.getStorage(0) == storage0 || replicas.getStorage(0) == storage4);
    // if we replace with higher priority, we must replace
    replicas.add(NumberReplicas.StoredReplicaState.MAINTENANCE_FOR_READ, 1, 3, storage4, false);
    assertEquals(2, replicas.liveReplicas());
    assertEquals(1, replicas.readOnlyReplicas());
    assertEquals(0, replicas.decommissioned());
    assertEquals(1, replicas.maintenanceReplicas());
    assertEquals(storage4, replicas.getStorage(3));
  }

  @Test
  public void testAddCounterConsiderBusy() {
    NumberReplicasStriped replicas = new NumberReplicasStriped(5, true);

    // 1 basic add counter
    DatanodeStorageInfo storage0 = mock(DatanodeStorageInfo.class);
    DatanodeStorageInfo storage1 = mock(DatanodeStorageInfo.class);
    DatanodeStorageInfo storage2 = mock(DatanodeStorageInfo.class);
    DatanodeStorageInfo storage3 = mock(DatanodeStorageInfo.class);
    replicas.add(NumberReplicas.StoredReplicaState.LIVE, 1, 0, storage0, true);
    replicas.add(NumberReplicas.StoredReplicaState.LIVE, 1, 1, storage1, true);
    replicas.add(NumberReplicas.StoredReplicaState.READONLY, 1, 2, storage2, true);
    replicas.add(NumberReplicas.StoredReplicaState.DECOMMISSIONED, 1, 3, storage3, true);
    assertEquals(2, replicas.liveReplicas());
    assertEquals(storage0, replicas.getStorage(0));
    assertEquals(storage1, replicas.getStorage(1));
    assertEquals(1, replicas.readOnlyReplicas());
    assertEquals(storage2, replicas.getStorage(2));
    assertEquals(1, replicas.decommissioned());
    assertEquals(storage3, replicas.getStorage(3));

    // 2 add counter in exist block index
    DatanodeStorageInfo storage4 = mock(DatanodeStorageInfo.class);
    DatanodeStorageInfo storage5 = mock(DatanodeStorageInfo.class);
    DatanodeStorageInfo storage6 = mock(DatanodeStorageInfo.class);
    // we can replace with lower priority
    replicas.add(NumberReplicas.StoredReplicaState.READONLY, 1, 0, storage4, true);
    assertEquals(2, replicas.liveReplicas());
    assertEquals(1, replicas.readOnlyReplicas());
    assertEquals(storage0, replicas.getStorage(0));
    // if we replace with same priority and busy replicas, we will replace randomly
    replicas.add(NumberReplicas.StoredReplicaState.LIVE, 1, 0, storage4, true);
    assertEquals(2, replicas.liveReplicas());
    assertEquals(1, replicas.readOnlyReplicas());
    assertTrue(replicas.getStorage(0) == storage0 || replicas.getStorage(0) == storage4);
    // if we replace with same priority and not busy replicas, because block index 0 is
    // busy before, so we must replace
    replicas.add(NumberReplicas.StoredReplicaState.LIVE, 1, 0, storage5, false);
    assertEquals(storage5, replicas.getStorage(0));
    // if we replace with same priority and busy replicas, because block index 0 is
    // not busy before, so we can not replace
    replicas.add(NumberReplicas.StoredReplicaState.LIVE, 1, 0, storage6, true);
    assertEquals(storage5, replicas.getStorage(0));
    // if we replace with same priority and not busy replicas, because block index 3 is
    // busy before, so we must replace
    replicas.add(NumberReplicas.StoredReplicaState.DECOMMISSIONED, 1, 3, storage4, false);
    assertEquals(2, replicas.liveReplicas());
    assertEquals(1, replicas.readOnlyReplicas());
    assertEquals(1, replicas.decommissioned());
    assertEquals(storage4, replicas.getStorage(3));
    // if we replace with higher priority and busy replicas, even though block index 3 is
    // not busy, we still replace
    replicas.add(NumberReplicas.StoredReplicaState.LIVE, 1, 3, storage5, true);
    assertEquals(3, replicas.liveReplicas());
    assertEquals(1, replicas.readOnlyReplicas());
    assertEquals(0, replicas.decommissioned());
    assertEquals(storage5, replicas.getStorage(3));
  }
}
