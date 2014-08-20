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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.ShmId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.DomainSocketWatcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Manages short-circuit memory segments for an HDFS client.
 * 
 * Clients are responsible for requesting and releasing shared memory segments used
 * for communicating with the DataNode. The client will try to allocate new slots
 * in the set of existing segments, falling back to getting a new segment from the
 * DataNode via {@link DataTransferProtocol#requestShortCircuitFds}.
 * 
 * The counterpart to this class on the DataNode is {@link ShortCircuitRegistry}.
 * See {@link ShortCircuitRegistry} for more information on the communication protocol.
 */
@InterfaceAudience.Private
public class DfsClientShmManager implements Closeable {
  private static final Log LOG = LogFactory.getLog(DfsClientShmManager.class);

  /**
   * Manages short-circuit memory segments that pertain to a given DataNode.
   */
  class EndpointShmManager {
    /**
     * The datanode we're managing.
     */
    private final DatanodeInfo datanode;

    /**
     * Shared memory segments which have no empty slots.
     *
     * Protected by the manager lock.
     */
    private final TreeMap<ShmId, DfsClientShm> full =
        new TreeMap<ShmId, DfsClientShm>();

    /**
     * Shared memory segments which have at least one empty slot.
     *
     * Protected by the manager lock.
     */
    private final TreeMap<ShmId, DfsClientShm> notFull =
        new TreeMap<ShmId, DfsClientShm>();

    /**
     * True if this datanode doesn't support short-circuit shared memory
     * segments.
     *
     * Protected by the manager lock.
     */
    private boolean disabled = false;

    /**
     * True if we're in the process of loading a shared memory segment from
     * this DataNode.
     *
     * Protected by the manager lock.
     */
    private boolean loading = false;

    EndpointShmManager (DatanodeInfo datanode) {
      this.datanode = datanode;
    }

    /**
     * Pull a slot out of a preexisting shared memory segment.
     *
     * Must be called with the manager lock held.
     *
     * @param blockId     The blockId to put inside the Slot object.
     *
     * @return            null if none of our shared memory segments contain a
     *                      free slot; the slot object otherwise.
     */
    private Slot allocSlotFromExistingShm(ExtendedBlockId blockId) {
      if (notFull.isEmpty()) {
        return null;
      }
      Entry<ShmId, DfsClientShm> entry = notFull.firstEntry();
      DfsClientShm shm = entry.getValue();
      ShmId shmId = shm.getShmId();
      Slot slot = shm.allocAndRegisterSlot(blockId);
      if (shm.isFull()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(this + ": pulled the last slot " + slot.getSlotIdx() +
              " out of " + shm);
        }
        DfsClientShm removedShm = notFull.remove(shmId);
        Preconditions.checkState(removedShm == shm);
        full.put(shmId, shm);
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace(this + ": pulled slot " + slot.getSlotIdx() +
              " out of " + shm);
        }
      }
      return slot;
    }

    /**
     * Ask the DataNode for a new shared memory segment.  This function must be
     * called with the manager lock held.  We will release the lock while
     * communicating with the DataNode.
     *
     * @param clientName    The current client name.
     * @param peer          The peer to use to talk to the DataNode.
     *
     * @return              Null if the DataNode does not support shared memory
     *                        segments, or experienced an error creating the
     *                        shm.  The shared memory segment itself on success.
     * @throws IOException  If there was an error communicating over the socket.
     *                        We will not throw an IOException unless the socket
     *                        itself (or the network) is the problem.
     */
    private DfsClientShm requestNewShm(String clientName, DomainPeer peer)
        throws IOException {
      final DataOutputStream out = 
          new DataOutputStream(
              new BufferedOutputStream(peer.getOutputStream()));
      new Sender(out).requestShortCircuitShm(clientName);
      ShortCircuitShmResponseProto resp = 
          ShortCircuitShmResponseProto.parseFrom(
              PBHelper.vintPrefixed(peer.getInputStream()));
      String error = resp.hasError() ? resp.getError() : "(unknown)";
      switch (resp.getStatus()) {
      case SUCCESS:
        DomainSocket sock = peer.getDomainSocket();
        byte buf[] = new byte[1];
        FileInputStream fis[] = new FileInputStream[1];
        if (sock.recvFileInputStreams(fis, buf, 0, buf.length) < 0) {
          throw new EOFException("got EOF while trying to transfer the " +
              "file descriptor for the shared memory segment.");
        }
        if (fis[0] == null) {
          throw new IOException("the datanode " + datanode + " failed to " +
              "pass a file descriptor for the shared memory segment.");
        }
        try {
          DfsClientShm shm = 
              new DfsClientShm(PBHelper.convert(resp.getId()),
                  fis[0], this, peer);
          if (LOG.isTraceEnabled()) {
            LOG.trace(this + ": createNewShm: created " + shm);
          }
          return shm;
        } finally {
          IOUtils.cleanup(LOG,  fis[0]);
        }
      case ERROR_UNSUPPORTED:
        // The DataNode just does not support short-circuit shared memory
        // access, and we should stop asking.
        LOG.info(this + ": datanode does not support short-circuit " +
            "shared memory access: " + error);
        disabled = true;
        return null;
      default:
        // The datanode experienced some kind of unexpected error when trying to
        // create the short-circuit shared memory segment.
        LOG.warn(this + ": error requesting short-circuit shared memory " +
            "access: " + error);
        return null;
      }
    }

    /**
     * Allocate a new shared memory slot connected to this datanode.
     *
     * Must be called with the EndpointShmManager lock held.
     *
     * @param peer          The peer to use to talk to the DataNode.
     * @param clientName    The client name.
     * @param usedPeer      (out param) Will be set to true if we used the peer.
     *                        When a peer is used
     *
     * @return              null if the DataNode does not support shared memory
     *                        segments, or experienced an error creating the
     *                        shm.  The shared memory segment itself on success.
     * @throws IOException  If there was an error communicating over the socket.
     */
    Slot allocSlot(DomainPeer peer, MutableBoolean usedPeer,
        String clientName, ExtendedBlockId blockId) throws IOException {
      while (true) {
        if (closed) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(this + ": the DfsClientShmManager has been closed.");
          }
          return null;
        }
        if (disabled) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(this + ": shared memory segment access is disabled.");
          }
          return null;
        }
        // Try to use an existing slot.
        Slot slot = allocSlotFromExistingShm(blockId);
        if (slot != null) {
          return slot;
        }
        // There are no free slots.  If someone is loading more slots, wait
        // for that to finish.
        if (loading) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(this + ": waiting for loading to finish...");
          }
          finishedLoading.awaitUninterruptibly();
        } else {
          // Otherwise, load the slot ourselves.
          loading = true;
          lock.unlock();
          DfsClientShm shm;
          try {
            shm = requestNewShm(clientName, peer);
            if (shm == null) continue;
            // See #{DfsClientShmManager#domainSocketWatcher} for details
            // about why we do this before retaking the manager lock.
            domainSocketWatcher.add(peer.getDomainSocket(), shm);
            // The DomainPeer is now our responsibility, and should not be
            // closed by the caller.
            usedPeer.setValue(true);
          } finally {
            lock.lock();
            loading = false;
            finishedLoading.signalAll();
          }
          if (shm.isDisconnected()) {
            // If the peer closed immediately after the shared memory segment
            // was created, the DomainSocketWatcher callback might already have
            // fired and marked the shm as disconnected.  In this case, we
            // obviously don't want to add the SharedMemorySegment to our list
            // of valid not-full segments.
            if (LOG.isDebugEnabled()) {
              LOG.debug(this + ": the UNIX domain socket associated with " +
                  "this short-circuit memory closed before we could make " +
                  "use of the shm.");
            }
          } else {
            notFull.put(shm.getShmId(), shm);
          }
        }
      }
    }
    
    /**
     * Stop tracking a slot.
     *
     * Must be called with the EndpointShmManager lock held.
     *
     * @param slot          The slot to release.
     */
    void freeSlot(Slot slot) {
      DfsClientShm shm = (DfsClientShm)slot.getShm();
      shm.unregisterSlot(slot.getSlotIdx());
      if (shm.isDisconnected()) {
        // Stale shared memory segments should not be tracked here.
        Preconditions.checkState(!full.containsKey(shm.getShmId()));
        Preconditions.checkState(!notFull.containsKey(shm.getShmId()));
        if (shm.isEmpty()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(this + ": freeing empty stale " + shm);
          }
          shm.free();
        }
      } else {
        ShmId shmId = shm.getShmId();
        full.remove(shmId); // The shm can't be full if we just freed a slot.
        if (shm.isEmpty()) {
          notFull.remove(shmId);
  
          // If the shared memory segment is now empty, we call shutdown(2) on
          // the UNIX domain socket associated with it.  The DomainSocketWatcher,
          // which is watching this socket, will call DfsClientShm#handle,
          // cleaning up this shared memory segment.
          //
          // See #{DfsClientShmManager#domainSocketWatcher} for details about why
          // we don't want to call DomainSocketWatcher#remove directly here.
          //
          // Note that we could experience 'fragmentation' here, where the
          // DFSClient allocates a bunch of slots in different shared memory
          // segments, and then frees most of them, but never fully empties out
          // any segment.  We make some attempt to avoid this fragmentation by
          // always allocating new slots out of the shared memory segment with the
          // lowest ID, but it could still occur.  In most workloads,
          // fragmentation should not be a major concern, since it doesn't impact
          // peak file descriptor usage or the speed of allocation.
          if (LOG.isTraceEnabled()) {
            LOG.trace(this + ": shutting down UNIX domain socket for " +
                "empty " + shm);
          }
          shutdown(shm);
        } else {
          notFull.put(shmId, shm);
        }
      }
    }
    
    /**
     * Unregister a shared memory segment.
     *
     * Once a segment is unregistered, we will not allocate any more slots
     * inside that segment.
     *
     * The DomainSocketWatcher calls this while holding the DomainSocketWatcher
     * lock.
     *
     * @param shmId         The ID of the shared memory segment to unregister.
     */
    void unregisterShm(ShmId shmId) {
      lock.lock();
      try {
        full.remove(shmId);
        notFull.remove(shmId);
      } finally {
        lock.unlock();
      }
    }

    @Override
    public String toString() {
      return String.format("EndpointShmManager(%s, parent=%s)",
          datanode, DfsClientShmManager.this);
    }

    PerDatanodeVisitorInfo getVisitorInfo() {
      return new PerDatanodeVisitorInfo(full, notFull, disabled);
    }

    final void shutdown(DfsClientShm shm) {
      try {
        shm.getPeer().getDomainSocket().shutdown();
      } catch (IOException e) {
        LOG.warn(this + ": error shutting down shm: got IOException calling " +
            "shutdown(SHUT_RDWR)", e);
      }
    }
  }

  private boolean closed = false;

  private final ReentrantLock lock = new ReentrantLock();

  /**
   * A condition variable which is signalled when we finish loading a segment
   * from the Datanode.
   */
  private final Condition finishedLoading = lock.newCondition();

  /**
   * Information about each Datanode.
   */
  private final HashMap<DatanodeInfo, EndpointShmManager> datanodes =
      new HashMap<DatanodeInfo, EndpointShmManager>(1);
  
  /**
   * The DomainSocketWatcher which keeps track of the UNIX domain socket
   * associated with each shared memory segment.
   *
   * Note: because the DomainSocketWatcher makes callbacks into this
   * DfsClientShmManager object, you must MUST NOT attempt to take the
   * DomainSocketWatcher lock while holding the DfsClientShmManager lock,
   * or else deadlock might result.   This means that most DomainSocketWatcher
   * methods are off-limits unless you release the manager lock first.
   */
  private final DomainSocketWatcher domainSocketWatcher;
  
  DfsClientShmManager(int interruptCheckPeriodMs) throws IOException {
    this.domainSocketWatcher = new DomainSocketWatcher(interruptCheckPeriodMs);
  }
  
  public Slot allocSlot(DatanodeInfo datanode, DomainPeer peer,
      MutableBoolean usedPeer, ExtendedBlockId blockId,
      String clientName) throws IOException {
    lock.lock();
    try {
      if (closed) {
        LOG.trace(this + ": the DfsClientShmManager isclosed.");
        return null;
      }
      EndpointShmManager shmManager = datanodes.get(datanode);
      if (shmManager == null) {
        shmManager = new EndpointShmManager(datanode);
        datanodes.put(datanode, shmManager);
      }
      return shmManager.allocSlot(peer, usedPeer, clientName, blockId);
    } finally {
      lock.unlock();
    }
  }
  
  public void freeSlot(Slot slot) {
    lock.lock();
    try {
      DfsClientShm shm = (DfsClientShm)slot.getShm();
      shm.getEndpointShmManager().freeSlot(slot);
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  public static class PerDatanodeVisitorInfo {
    public final TreeMap<ShmId, DfsClientShm> full;
    public final TreeMap<ShmId, DfsClientShm> notFull;
    public final boolean disabled;

    PerDatanodeVisitorInfo(TreeMap<ShmId, DfsClientShm> full,
        TreeMap<ShmId, DfsClientShm> notFull, boolean disabled) {
      this.full = full;
      this.notFull = notFull;
      this.disabled = disabled;
    }
  }

  @VisibleForTesting
  public interface Visitor {
    void visit(HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info)
        throws IOException;
  }

  @VisibleForTesting
  public void visit(Visitor visitor) throws IOException {
    lock.lock();
    try {
      HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info = 
          new HashMap<DatanodeInfo, PerDatanodeVisitorInfo>();
      for (Entry<DatanodeInfo, EndpointShmManager> entry :
            datanodes.entrySet()) {
        info.put(entry.getKey(), entry.getValue().getVisitorInfo());
      }
      visitor.visit(info);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Close the DfsClientShmManager.
   */
  @Override
  public void close() throws IOException {
    lock.lock();
    try {
      if (closed) return;
      closed = true;
    } finally {
      lock.unlock();
    }
    // When closed, the domainSocketWatcher will issue callbacks that mark
    // all the outstanding DfsClientShm segments as stale.
    IOUtils.cleanup(LOG, domainSocketWatcher);
  }


  @Override
  public String toString() {
    return String.format("ShortCircuitShmManager(%08x)",
        System.identityHashCode(this));
  }

  @VisibleForTesting
  public DomainSocketWatcher getDomainSocketWatcher() {
    return domainSocketWatcher;
  }
}
