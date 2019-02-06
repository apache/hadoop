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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.ShmId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.DomainSocketWatcher;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;

/**
 * Manages client short-circuit memory segments on the DataNode.
 *
 * DFSClients request shared memory segments from the DataNode.  The 
 * ShortCircuitRegistry generates and manages these segments.  Each segment
 * has a randomly generated 128-bit ID which uniquely identifies it.  The
 * segments each contain several "slots."
 *
 * Before performing a short-circuit read, DFSClients must request a pair of
 * file descriptors from the DataNode via the REQUEST_SHORT_CIRCUIT_FDS
 * operation.  As part of this operation, DFSClients pass the ID of the shared
 * memory segment they would like to use to communicate information about this
 * replica, as well as the slot number within that segment they would like to
 * use.  Slot allocation is always done by the client.
 *
 * Slots are used to track the state of the block on the both the client and
 * datanode. When this DataNode mlocks a block, the corresponding slots for the
 * replicas are marked as "anchorable".  Anchorable blocks can be safely read
 * without verifying the checksum.  This means that BlockReaderLocal objects
 * using these replicas can skip checksumming.  It also means that we can do
 * zero-copy reads on these replicas (the ZCR interface has no way of
 * verifying checksums.)
 * 
 * When a DN needs to munlock a block, it needs to first wait for the block to
 * be unanchored by clients doing a no-checksum read or a zero-copy read. The 
 * DN also marks the block's slots as "unanchorable" to prevent additional 
 * clients from initiating these operations in the future.
 * 
 * The counterpart of this class on the client is {@link DfsClientShmManager}.
 */
public class ShortCircuitRegistry {
  public static final Logger LOG =
      LoggerFactory.getLogger(ShortCircuitRegistry.class);

  private static final int SHM_LENGTH = 8192;

  public static class RegisteredShm extends ShortCircuitShm
      implements DomainSocketWatcher.Handler {
    private final String clientName;
    private final ShortCircuitRegistry registry;

    RegisteredShm(String clientName, ShmId shmId, FileInputStream stream,
        ShortCircuitRegistry registry) throws IOException {
      super(shmId, stream);
      this.clientName = clientName;
      this.registry = registry;
    }

    @Override
    public boolean handle(DomainSocket sock) {
      synchronized (registry) {
        synchronized (this) {
          registry.removeShm(this);
        }
      }
      return true;
    }

    String getClientName() {
      return clientName;
    }
  }

  public synchronized void removeShm(ShortCircuitShm shm) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("removing shm " + shm);
    }
    // Stop tracking the shmId.
    RegisteredShm removedShm = segments.remove(shm.getShmId());
    Preconditions.checkState(removedShm == shm,
        "failed to remove " + shm.getShmId());
    // Stop tracking the slots.
    for (Iterator<Slot> iter = shm.slotIterator(); iter.hasNext(); ) {
      Slot slot = iter.next();
      boolean removed = slots.remove(slot.getBlockId(), slot);
      Preconditions.checkState(removed);
      slot.makeInvalid();
    }
    // De-allocate the memory map and close the shared file. 
    shm.free();
  }

  /**
   * Whether or not the registry is enabled.
   */
  private boolean enabled;

  /**
   * The factory which creates shared file descriptors.
   */
  private final SharedFileDescriptorFactory shmFactory;
  
  /**
   * A watcher which sends out callbacks when the UNIX domain socket
   * associated with a shared memory segment closes.
   */
  private final DomainSocketWatcher watcher;

  private final HashMap<ShmId, RegisteredShm> segments =
      new HashMap<ShmId, RegisteredShm>(0);
  
  private final HashMultimap<ExtendedBlockId, Slot> slots =
      HashMultimap.create(0, 1);
  
  public ShortCircuitRegistry(Configuration conf) throws IOException {
    boolean enabled = false;
    SharedFileDescriptorFactory shmFactory = null;
    DomainSocketWatcher watcher = null;
    try {
      int interruptCheck = conf.getInt(
          DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS,
          DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT);
      if (interruptCheck <= 0) {
        throw new IOException(
            DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS +
            " was set to " + interruptCheck);
      }
      String[] shmPaths =
          conf.getTrimmedStrings(DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS);
      if (shmPaths.length == 0) {
        shmPaths =
            DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS_DEFAULT.split(",");
      }
      shmFactory = SharedFileDescriptorFactory.
          create("HadoopShortCircuitShm_", shmPaths);
      String dswLoadingFailure = DomainSocketWatcher.getLoadingFailureReason();
      if (dswLoadingFailure != null) {
        throw new IOException(dswLoadingFailure);
      }
      watcher = new DomainSocketWatcher(interruptCheck, "datanode");
      enabled = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("created new ShortCircuitRegistry with interruptCheck=" +
                  interruptCheck + ", shmPath=" + shmFactory.getPath());
      }
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Disabling ShortCircuitRegistry", e);
      }
    } finally {
      this.enabled = enabled;
      this.shmFactory = shmFactory;
      this.watcher = watcher;
    }
  }

  /**
   * Process a block mlock event from the FsDatasetCache.
   *
   * @param blockId    The block that was mlocked.
   */
  public synchronized void processBlockMlockEvent(ExtendedBlockId blockId) {
    if (!enabled) return;
    Set<Slot> affectedSlots = slots.get(blockId);
    for (Slot slot : affectedSlots) {
      slot.makeAnchorable();
    }
  }

  /**
   * Mark any slots associated with this blockId as unanchorable.
   *
   * @param blockId        The block ID.
   * @return               True if we should allow the munlock request.
   */
  public synchronized boolean processBlockMunlockRequest(
      ExtendedBlockId blockId) {
    if (!enabled) return true;
    boolean allowMunlock = true;
    Set<Slot> affectedSlots = slots.get(blockId);
    for (Slot slot : affectedSlots) {
      slot.makeUnanchorable();
      if (slot.isAnchored()) {
        allowMunlock = false;
      }
    }
    return allowMunlock;
  }

  /**
   * Invalidate any slot associated with a blockId that we are invalidating
   * (deleting) from this DataNode.  When a slot is invalid, the DFSClient will
   * not use the corresponding replica for new read or mmap operations (although
   * existing, ongoing read or mmap operations will complete.)
   *
   * @param blockId        The block ID.
   */
  public synchronized void processBlockInvalidation(ExtendedBlockId blockId) {
    if (!enabled) return;
    final Set<Slot> affectedSlots = slots.get(blockId);
    if (!affectedSlots.isEmpty()) {
      final StringBuilder bld = new StringBuilder();
      String prefix = "";
      bld.append("Block ").append(blockId).append(" has been invalidated.  ").
          append("Marking short-circuit slots as invalid: ");
      for (Slot slot : affectedSlots) {
        slot.makeInvalid();
        bld.append(prefix).append(slot.toString());
        prefix = ", ";
      }
      LOG.info(bld.toString());
    }
  }

  public synchronized String getClientNames(ExtendedBlockId blockId) {
    if (!enabled) return "";
    final HashSet<String> clientNames = new HashSet<String>();
    final Set<Slot> affectedSlots = slots.get(blockId);
    for (Slot slot : affectedSlots) {
      clientNames.add(((RegisteredShm)slot.getShm()).getClientName());
    }
    return Joiner.on(",").join(clientNames);
  }

  public static class NewShmInfo implements Closeable {
    private final ShmId shmId;
    private final FileInputStream stream;

    NewShmInfo(ShmId shmId, FileInputStream stream) {
      this.shmId = shmId;
      this.stream = stream;
    }

    public ShmId getShmId() {
      return shmId;
    }

    public FileInputStream getFileStream() {
      return stream;
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  /**
   * Handle a DFSClient request to create a new memory segment.
   *
   * @param clientName    Client name as reported by the client.
   * @param sock          The DomainSocket to associate with this memory
   *                        segment.  When this socket is closed, or the
   *                        other side writes anything to the socket, the
   *                        segment will be closed.  This can happen at any
   *                        time, including right after this function returns.
   * @return              A NewShmInfo object.  The caller must close the
   *                        NewShmInfo object once they are done with it.
   * @throws IOException  If the new memory segment could not be created.
   */
  public NewShmInfo createNewMemorySegment(String clientName,
      DomainSocket sock) throws IOException {
    NewShmInfo info = null;
    RegisteredShm shm = null;
    ShmId shmId = null;
    synchronized (this) {
      if (!enabled) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("createNewMemorySegment: ShortCircuitRegistry is " +
              "not enabled.");
        }
        throw new UnsupportedOperationException();
      }
      FileInputStream fis = null;
      try {
        do {
          shmId = ShmId.createRandom();
        } while (segments.containsKey(shmId));
        fis = shmFactory.createDescriptor(clientName, SHM_LENGTH);
        shm = new RegisteredShm(clientName, shmId, fis, this);
      } finally {
        if (shm == null) {
          IOUtils.closeQuietly(fis);
        }
      }
      info = new NewShmInfo(shmId, fis);
      segments.put(shmId, shm);
    }
    // Drop the registry lock to prevent deadlock.
    // After this point, RegisteredShm#handle may be called at any time.
    watcher.add(sock, shm);
    if (LOG.isTraceEnabled()) {
      LOG.trace("createNewMemorySegment: created " + info.shmId);
    }
    return info;
  }
  
  public synchronized void registerSlot(ExtendedBlockId blockId, SlotId slotId,
      boolean isCached) throws InvalidRequestException {
    if (!enabled) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(this + " can't register a slot because the " +
            "ShortCircuitRegistry is not enabled.");
      }
      throw new UnsupportedOperationException();
    }
    ShmId shmId = slotId.getShmId();
    RegisteredShm shm = segments.get(shmId);
    if (shm == null) {
      throw new InvalidRequestException("there is no shared memory segment " +
          "registered with shmId " + shmId);
    }
    Slot slot = shm.registerSlot(slotId.getSlotIdx(), blockId);
    if (isCached) {
      slot.makeAnchorable();
    } else {
      slot.makeUnanchorable();
    }
    boolean added = slots.put(blockId, slot);
    Preconditions.checkState(added);
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + ": registered " + blockId + " with slot " +
        slotId + " (isCached=" + isCached + ")");
    }
  }
  
  public synchronized void unregisterSlot(SlotId slotId)
      throws InvalidRequestException {
    if (!enabled) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("unregisterSlot: ShortCircuitRegistry is " +
            "not enabled.");
      }
      throw new UnsupportedOperationException();
    }
    ShmId shmId = slotId.getShmId();
    RegisteredShm shm = segments.get(shmId);
    if (shm == null) {
      throw new InvalidRequestException("there is no shared memory segment " +
          "registered with shmId " + shmId);
    }
    Slot slot = shm.getSlot(slotId.getSlotIdx());
    slot.makeInvalid();
    shm.unregisterSlot(slotId.getSlotIdx());
    slots.remove(slot.getBlockId(), slot);
  }
  
  public void shutdown() {
    synchronized (this) {
      if (!enabled) return;
      enabled = false;
    }
    IOUtils.closeQuietly(watcher);
  }

  public static interface Visitor {
    boolean accept(HashMap<ShmId, RegisteredShm> segments,
                HashMultimap<ExtendedBlockId, Slot> slots);
  }

  @VisibleForTesting
  public synchronized boolean visit(Visitor visitor) {
    return visitor.accept(segments, slots);
  }
}
