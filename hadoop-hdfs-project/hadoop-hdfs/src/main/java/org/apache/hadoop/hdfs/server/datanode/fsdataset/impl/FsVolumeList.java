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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.Time;

class FsVolumeList {
  private final CopyOnWriteArrayList<FsVolumeImpl> volumes =
      new CopyOnWriteArrayList<>();
  // Tracks volume failures, sorted by volume path.
  // map from volume storageID to the volume failure info
  private final Map<StorageLocation, VolumeFailureInfo> volumeFailureInfos =
      Collections.synchronizedMap(
          new TreeMap<StorageLocation, VolumeFailureInfo>());
  private final ConcurrentLinkedQueue<FsVolumeImpl> volumesBeingRemoved =
      new ConcurrentLinkedQueue<>();
  private final AutoCloseableLock checkDirsLock;
  private final Condition checkDirsLockCondition;

  private final VolumeChoosingPolicy<FsVolumeImpl> blockChooser;
  private final BlockScanner blockScanner;

  FsVolumeList(List<VolumeFailureInfo> initialVolumeFailureInfos,
      BlockScanner blockScanner,
      VolumeChoosingPolicy<FsVolumeImpl> blockChooser) {
    this.blockChooser = blockChooser;
    this.blockScanner = blockScanner;
    this.checkDirsLock = new AutoCloseableLock();
    this.checkDirsLockCondition = checkDirsLock.newCondition();
    for (VolumeFailureInfo volumeFailureInfo: initialVolumeFailureInfos) {
      volumeFailureInfos.put(volumeFailureInfo.getFailedStorageLocation(),
          volumeFailureInfo);
    }
  }

  /**
   * Return an immutable list view of all the volumes.
   */
  List<FsVolumeImpl> getVolumes() {
    return Collections.unmodifiableList(volumes);
  }

  private FsVolumeReference chooseVolume(List<FsVolumeImpl> list,
      long blockSize, String storageId) throws IOException {
    while (true) {
      FsVolumeImpl volume = blockChooser.chooseVolume(list, blockSize,
          storageId);
      try {
        return volume.obtainReference();
      } catch (ClosedChannelException e) {
        FsDatasetImpl.LOG.warn("Chosen a closed volume: " + volume);
        // blockChooser.chooseVolume returns DiskOutOfSpaceException when the list
        // is empty, indicating that all volumes are closed.
        list.remove(volume);
      }
    }
  }

  /** 
   * Get next volume.
   *
   * @param blockSize free space needed on the volume
   * @param storageType the desired {@link StorageType}
   * @param storageId the storage id which may or may not be used by
   *                  the VolumeChoosingPolicy.
   * @return next volume to store the block in.
   */
  FsVolumeReference getNextVolume(StorageType storageType, String storageId,
      long blockSize) throws IOException {
    final List<FsVolumeImpl> list = new ArrayList<>(volumes.size());
    for(FsVolumeImpl v : volumes) {
      if (v.getStorageType() == storageType) {
        list.add(v);
      }
    }
    return chooseVolume(list, blockSize, storageId);
  }

  /**
   * Get next volume.
   *
   * @param blockSize free space needed on the volume
   * @return next volume to store the block in.
   */
  FsVolumeReference getNextTransientVolume(long blockSize) throws IOException {
    // Get a snapshot of currently available volumes.
    final List<FsVolumeImpl> curVolumes = getVolumes();
    final List<FsVolumeImpl> list = new ArrayList<>(curVolumes.size());
    for(FsVolumeImpl v : curVolumes) {
      if (v.isTransientStorage()) {
        list.add(v);
      }
    }
    return chooseVolume(list, blockSize, null);
  }

  long getDfsUsed() throws IOException {
    long dfsUsed = 0L;
    for (FsVolumeImpl v : volumes) {
      try(FsVolumeReference ref = v.obtainReference()) {
        dfsUsed += v.getDfsUsed();
      } catch (ClosedChannelException e) {
        // ignore.
      }
    }
    return dfsUsed;
  }

  long getBlockPoolUsed(String bpid) throws IOException {
    long dfsUsed = 0L;
    for (FsVolumeImpl v : volumes) {
      try (FsVolumeReference ref = v.obtainReference()) {
        dfsUsed += v.getBlockPoolUsed(bpid);
      } catch (ClosedChannelException e) {
        // ignore.
      }
    }
    return dfsUsed;
  }

  long getCapacity() {
    long capacity = 0L;
    for (FsVolumeImpl v : volumes) {
      try (FsVolumeReference ref = v.obtainReference()) {
        capacity += v.getCapacity();
      } catch (IOException e) {
        // ignore.
      }
    }
    return capacity;
  }
    
  long getRemaining() throws IOException {
    long remaining = 0L;
    for (FsVolumeSpi vol : volumes) {
      try (FsVolumeReference ref = vol.obtainReference()) {
        remaining += vol.getAvailable();
      } catch (ClosedChannelException e) {
        // ignore
      }
    }
    return remaining;
  }
  
  void getAllVolumesMap(final String bpid,
                        final ReplicaMap volumeMap,
                        final RamDiskReplicaTracker ramDiskReplicaMap)
      throws IOException {
    long totalStartTime = Time.monotonicNow();
    final List<IOException> exceptions = Collections.synchronizedList(
        new ArrayList<IOException>());
    List<Thread> replicaAddingThreads = new ArrayList<Thread>();
    for (final FsVolumeImpl v : volumes) {
      Thread t = new Thread() {
        public void run() {
          try (FsVolumeReference ref = v.obtainReference()) {
            FsDatasetImpl.LOG.info("Adding replicas to map for block pool " +
                bpid + " on volume " + v + "...");
            long startTime = Time.monotonicNow();
            v.getVolumeMap(bpid, volumeMap, ramDiskReplicaMap);
            long timeTaken = Time.monotonicNow() - startTime;
            FsDatasetImpl.LOG.info("Time to add replicas to map for block pool"
                + " " + bpid + " on volume " + v + ": " + timeTaken + "ms");
          } catch (ClosedChannelException e) {
            FsDatasetImpl.LOG.info("The volume " + v + " is closed while " +
                "adding replicas, ignored.");
          } catch (IOException ioe) {
            FsDatasetImpl.LOG.info("Caught exception while adding replicas " +
                "from " + v + ". Will throw later.", ioe);
            exceptions.add(ioe);
          }
        }
      };
      replicaAddingThreads.add(t);
      t.start();
    }
    for (Thread t : replicaAddingThreads) {
      try {
        t.join();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    if (!exceptions.isEmpty()) {
      throw exceptions.get(0);
    }
    long totalTimeTaken = Time.monotonicNow() - totalStartTime;
    FsDatasetImpl.LOG
        .info("Total time to add all replicas to map for block pool " + bpid
            + ": " + totalTimeTaken + "ms");
  }

  /**
   * Updates the failed volume info in the volumeFailureInfos Map
   * and calls {@link #removeVolume(FsVolumeImpl)} to remove the volume
   * from the volume list for each of the failed volumes.
   *
   * @param failedVolumes set of volumes marked failed.
   */
  void handleVolumeFailures(Set<FsVolumeSpi> failedVolumes) {
    try (AutoCloseableLock lock = checkDirsLock.acquire()) {

      for(FsVolumeSpi vol : failedVolumes) {
        FsVolumeImpl fsv = (FsVolumeImpl) vol;
        try (FsVolumeReference ref = fsv.obtainReference()) {
          addVolumeFailureInfo(fsv);
          removeVolume(fsv);
        } catch (ClosedChannelException e) {
          FsDatasetImpl.LOG.debug("Caught exception when obtaining " +
            "reference count on closed volume", e);
        } catch (IOException e) {
          FsDatasetImpl.LOG.error("Unexpected IOException", e);
        }
      }
      
      waitVolumeRemoved(5000, checkDirsLockCondition);
    }
  }

  /**
   * Wait for the reference of the volume removed from a previous
   * {@link #removeVolume(FsVolumeImpl)} call to be released.
   *
   * @param sleepMillis interval to recheck.
   */
  void waitVolumeRemoved(int sleepMillis, Condition condition) {
    while (!checkVolumesRemoved()) {
      if (FsDatasetImpl.LOG.isDebugEnabled()) {
        FsDatasetImpl.LOG.debug("Waiting for volume reference to be released.");
      }
      try {
        condition.await(sleepMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        FsDatasetImpl.LOG.info("Thread interrupted when waiting for "
            + "volume reference to be released.");
        Thread.currentThread().interrupt();
      }
    }
    FsDatasetImpl.LOG.info("Volume reference is released.");
  }

  @Override
  public String toString() {
    return volumes.toString();
  }

  /**
   * Dynamically add new volumes to the existing volumes that this DN manages.
   *
   * @param ref       a reference to the new FsVolumeImpl instance.
   */
  void addVolume(FsVolumeReference ref) {
    FsVolumeImpl volume = (FsVolumeImpl) ref.getVolume();
    volumes.add(volume);
    if (blockScanner != null) {
      blockScanner.addVolumeScanner(ref);
    } else {
      // If the volume is not put into a volume scanner, it does not need to
      // hold the reference.
      IOUtils.cleanup(null, ref);
    }
    // If the volume is used to replace a failed volume, it needs to reset the
    // volume failure info for this volume.
    removeVolumeFailureInfo(volume.getStorageLocation());
    FsDatasetImpl.LOG.info("Added new volume: " +
        volume.getStorageID());
  }

  /**
   * Dynamically remove a volume in the list.
   * @param target the volume instance to be removed.
   */
  private void removeVolume(FsVolumeImpl target) {
    if (volumes.remove(target)) {
      if (blockScanner != null) {
        blockScanner.removeVolumeScanner(target);
      }
      try {
        target.setClosed();
      } catch (IOException e) {
        FsDatasetImpl.LOG.warn(
            "Error occurs when waiting volume to close: " + target, e);
      }
      target.shutdown();
      volumesBeingRemoved.add(target);
      FsDatasetImpl.LOG.info("Removed volume: " + target);
    } else {
      if (FsDatasetImpl.LOG.isDebugEnabled()) {
        FsDatasetImpl.LOG.debug("Volume " + target +
            " does not exist or is removed by others.");
      }
    }
  }

  /**
   * Dynamically remove volume in the list.
   * @param storageLocation {@link StorageLocation} of the volume to be removed.
   * @param clearFailure set true to remove failure info for this volume.
   */
  void removeVolume(StorageLocation storageLocation, boolean clearFailure) {
    for (FsVolumeImpl fsVolume : volumes) {
      StorageLocation baseLocation = fsVolume.getStorageLocation();
      if (baseLocation.equals(storageLocation)) {
        removeVolume(fsVolume);
      }
    }
    if (clearFailure) {
      removeVolumeFailureInfo(storageLocation);
    }
  }

  VolumeFailureInfo[] getVolumeFailureInfos() {
    Collection<VolumeFailureInfo> infos = volumeFailureInfos.values();
    return infos.toArray(new VolumeFailureInfo[infos.size()]);
  }

  /**
   * Check whether the reference of the volume from a previous
   * {@link #removeVolume(FsVolumeImpl)} call is released.
   *
   * @return Whether the reference is released.
   */
  boolean checkVolumesRemoved() {
    Iterator<FsVolumeImpl> it = volumesBeingRemoved.iterator();
    while (it.hasNext()) {
      FsVolumeImpl volume = it.next();
      if (!volume.checkClosed()) {
        return false;
      }
      it.remove();
    }
    return true;
  }

  void addVolumeFailureInfo(VolumeFailureInfo volumeFailureInfo) {
    // There could be redundant requests for adding the same failed
    // volume because of repeated DataNode reconfigure with same list
    // of volumes. Ignoring update on failed volume so as to preserve
    // old failed capacity details in the map.
    if (!volumeFailureInfos.containsKey(volumeFailureInfo
        .getFailedStorageLocation())) {
      volumeFailureInfos.put(volumeFailureInfo.getFailedStorageLocation(),
          volumeFailureInfo);
    }
  }

  private void addVolumeFailureInfo(FsVolumeImpl vol) {
    addVolumeFailureInfo(new VolumeFailureInfo(
        vol.getStorageLocation(),
        Time.now(),
        vol.getCapacity()));
  }

  void removeVolumeFailureInfo(StorageLocation location) {
    volumeFailureInfos.remove(location);
  }

  void addBlockPool(final String bpid, final Configuration conf) throws IOException {
    long totalStartTime = Time.monotonicNow();
    
    final List<IOException> exceptions = Collections.synchronizedList(
        new ArrayList<IOException>());
    List<Thread> blockPoolAddingThreads = new ArrayList<Thread>();
    for (final FsVolumeImpl v : volumes) {
      Thread t = new Thread() {
        public void run() {
          try (FsVolumeReference ref = v.obtainReference()) {
            FsDatasetImpl.LOG.info("Scanning block pool " + bpid +
                " on volume " + v + "...");
            long startTime = Time.monotonicNow();
            v.addBlockPool(bpid, conf);
            long timeTaken = Time.monotonicNow() - startTime;
            FsDatasetImpl.LOG.info("Time taken to scan block pool " + bpid +
                " on " + v + ": " + timeTaken + "ms");
          } catch (ClosedChannelException e) {
            // ignore.
          } catch (IOException ioe) {
            FsDatasetImpl.LOG.info("Caught exception while scanning " + v +
                ". Will throw later.", ioe);
            exceptions.add(ioe);
          }
        }
      };
      blockPoolAddingThreads.add(t);
      t.start();
    }
    for (Thread t : blockPoolAddingThreads) {
      try {
        t.join();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    if (!exceptions.isEmpty()) {
      throw exceptions.get(0);
    }
    
    long totalTimeTaken = Time.monotonicNow() - totalStartTime;
    FsDatasetImpl.LOG.info("Total time to scan all replicas for block pool " +
        bpid + ": " + totalTimeTaken + "ms");
  }
  
  void removeBlockPool(String bpid, Map<DatanodeStorage, BlockListAsLongs>
      blocksPerVolume) {
    for (FsVolumeImpl v : volumes) {
      v.shutdownBlockPool(bpid, blocksPerVolume.get(v.toDatanodeStorage()));
    }
  }

  void shutdown() {
    for (FsVolumeImpl volume : volumes) {
      if(volume != null) {
        volume.shutdown();
      }
    }
  }
}
