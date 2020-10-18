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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.EndpointShmManager;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.DomainSocketWatcher;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * DfsClientShm is a subclass of ShortCircuitShm which is used by the
 * DfsClient.
 * When the UNIX domain socket associated with this shared memory segment
 * closes unexpectedly, we mark the slots inside this segment as disconnected.
 * ShortCircuitReplica objects that contain disconnected slots are stale,
 * and will not be used to service new reads or mmap operations.
 * However, in-progress read or mmap operations will continue to proceed.
 * Once the last slot is deallocated, the segment can be safely munmapped.
 *
 * Slots may also become stale because the associated replica has been deleted
 * on the DataNode.  In this case, the DataNode will clear the 'valid' bit.
 * The client will then see these slots as stale (see
 * #{ShortCircuitReplica#isStale}).
 */
public class DfsClientShm extends ShortCircuitShm
    implements DomainSocketWatcher.Handler {
  /**
   * The EndpointShmManager associated with this shared memory segment.
   */
  private final EndpointShmManager manager;

  /**
   * The UNIX domain socket associated with this DfsClientShm.
   * We rely on the DomainSocketWatcher to close the socket associated with
   * this DomainPeer when necessary.
   */
  private final DomainPeer peer;

  /**
   * True if this shared memory segment has lost its connection to the
   * DataNode.
   *
   * {@link DfsClientShm#handle} sets this to true.
   */
  private boolean disconnected = false;

  DfsClientShm(ShmId shmId, FileInputStream stream, EndpointShmManager manager,
      DomainPeer peer) throws IOException {
    super(shmId, stream);
    this.manager = manager;
    this.peer = peer;
  }

  public EndpointShmManager getEndpointShmManager() {
    return manager;
  }

  public DomainPeer getPeer() {
    return peer;
  }

  /**
   * Determine if the shared memory segment is disconnected from the DataNode.
   *
   * This must be called with the DfsClientShmManager lock held.
   *
   * @return   True if the shared memory segment is stale.
   */
  public synchronized boolean isDisconnected() {
    return disconnected;
  }

  /**
   * Handle the closure of the UNIX domain socket associated with this shared
   * memory segment by marking this segment as stale.
   *
   * If there are no slots associated with this shared memory segment, it will
   * be freed immediately in this function.
   */
  @Override
  public boolean handle(DomainSocket sock) {
    manager.unregisterShm(getShmId());
    synchronized (this) {
      Preconditions.checkState(!disconnected);
      disconnected = true;
      boolean hadSlots = false;
      for (Iterator<Slot> iter = slotIterator(); iter.hasNext(); ) {
        Slot slot = iter.next();
        slot.makeInvalid();
        hadSlots = true;
      }
      if (!hadSlots) {
        free();
      }
    }
    return true;
  }
}
