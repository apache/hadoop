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
package org.apache.hadoop.hdfs.shortcircuit;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.EndpointShmManager;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.net.unix.DomainSocketWatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Unit tests for {@link DfsClientShmManager}.
 */
public class TestDfsClientShmManager {

  @BeforeEach
  public void before() {
    assumeTrue(null == DomainSocketWatcher.getLoadingFailureReason(),
        "Skipping test: native Unix domain socket support not available");
  }

  /**
   * Regression test for HDFS-17427.
   *
   * When the thread that is loading a new shm segment calls allocSlot() again
   * on the same thread (e.g. because NativeIO.POSIX class initialization
   * triggers an HDFS read via a custom URLClassLoader whose classpath includes
   * remote HDFS JARs), the method must return null instead of deadlocking on
   * {@code finishedLoading.awaitUninterruptibly()}.
   *
   * The test would hang indefinitely without the fix; with the fix it returns
   * null within the @Timeout window.
   */
  @Test
  @Timeout(value = 10)
  public void testReentrantAllocSlotAvoidsDeadlock() throws Exception {
    DfsClientShmManager manager = new DfsClientShmManager(10);
    try {
      DatanodeInfo datanode = new DatanodeInfo.DatanodeInfoBuilder()
          .setIpAddr("127.0.0.1")
          .setHostName("localhost")
          .setDatanodeUuid("test-dn-uuid")
          .setXferPort(9866)
          .build();
      ExtendedBlockId blockId = new ExtendedBlockId(1L, "bp-test");
      MutableBoolean usedPeer = new MutableBoolean(false);

      // Access the manager's internal datanodes map and lock via reflection.
      Field datanodesField =
          DfsClientShmManager.class.getDeclaredField("datanodes");
      datanodesField.setAccessible(true);
      @SuppressWarnings("unchecked")
      HashMap<DatanodeInfo, EndpointShmManager> datanodes =
          (HashMap<DatanodeInfo, EndpointShmManager>) datanodesField.get(manager);

      Field lockField = DfsClientShmManager.class.getDeclaredField("lock");
      lockField.setAccessible(true);
      ReentrantLock lock = (ReentrantLock) lockField.get(manager);

      // Directly instantiate an EndpointShmManager (package-private inner
      // class) and register it for the datanode.
      Constructor<EndpointShmManager> ctor =
          EndpointShmManager.class.getDeclaredConstructor(
              DfsClientShmManager.class, DatanodeInfo.class);
      ctor.setAccessible(true);
      EndpointShmManager endpointManager = ctor.newInstance(manager, datanode);

      lock.lock();
      try {
        datanodes.put(datanode, endpointManager);
      } finally {
        lock.unlock();
      }

      // Simulate the mid-requestNewShm state: the current thread set
      // loading=true (and is responsible for signalling finishedLoading) but
      // then re-enters allocSlot() before requestNewShm() returns — exactly
      // what happens when NativeIO.POSIX.<clinit>() calls new Configuration()
      // which loads resources via a URLClassLoader backed by remote HDFS JARs.
      Field loadingField =
          EndpointShmManager.class.getDeclaredField("loading");
      loadingField.setAccessible(true);
      Field loadingThreadField =
          EndpointShmManager.class.getDeclaredField("loadingThread");
      loadingThreadField.setAccessible(true);

      loadingField.setBoolean(endpointManager, true);
      loadingThreadField.set(endpointManager, Thread.currentThread());

      // Call allocSlot() from the same thread.  Without HDFS-17427 fix this
      // would block forever on finishedLoading.awaitUninterruptibly() because
      // the current thread is the one that must eventually signal the
      // condition.  With the fix it detects the re-entrant call and returns
      // null so the caller can fall back to a non-short-circuit read.
      Slot slot = manager.allocSlot(
          datanode, null, usedPeer, blockId, "test-client");
      assertNull(slot,
          "Re-entrant allocSlot from the loading thread must return null "
              + "to avoid deadlock (HDFS-17427)");
    } finally {
      manager.close();
    }
  }
}
