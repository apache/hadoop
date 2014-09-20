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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Time;

class FsVolumeList {
  /**
   * Read access to this unmodifiable list is not synchronized.
   * This list is replaced on modification holding "this" lock.
   */
  volatile List<FsVolumeImpl> volumes = null;

  private final VolumeChoosingPolicy<FsVolumeImpl> blockChooser;
  private volatile int numFailedVolumes;

  FsVolumeList(int failedVols,
      VolumeChoosingPolicy<FsVolumeImpl> blockChooser) {
    this.blockChooser = blockChooser;
    this.numFailedVolumes = failedVols;
  }
  
  int numberOfFailedVolumes() {
    return numFailedVolumes;
  }
  
  /** 
   * Get next volume. Synchronized to ensure {@link #curVolume} is updated
   * by a single thread and next volume is chosen with no concurrent
   * update to {@link #volumes}.
   * @param blockSize free space needed on the volume
   * @param storageType the desired {@link StorageType} 
   * @return next volume to store the block in.
   */
  synchronized FsVolumeImpl getNextVolume(StorageType storageType,
      long blockSize) throws IOException {
    final List<FsVolumeImpl> list = new ArrayList<FsVolumeImpl>(volumes.size());
    for(FsVolumeImpl v : volumes) {
      if (v.getStorageType() == storageType) {
        list.add(v);
      }
    }
    return blockChooser.chooseVolume(list, blockSize);
  }

  /**
   * Get next volume. Synchronized to ensure {@link #curVolume} is updated
   * by a single thread and next volume is chosen with no concurrent
   * update to {@link #volumes}.
   * @param blockSize free space needed on the volume
   * @return next volume to store the block in.
   */
  synchronized FsVolumeImpl getNextTransientVolume(
      long blockSize) throws IOException {
    final List<FsVolumeImpl> list = new ArrayList<FsVolumeImpl>(volumes.size());
    for(FsVolumeImpl v : volumes) {
      if (v.isTransientStorage()) {
        list.add(v);
      }
    }
    return blockChooser.chooseVolume(list, blockSize);
  }

  long getDfsUsed() throws IOException {
    long dfsUsed = 0L;
    for (FsVolumeImpl v : volumes) {
      dfsUsed += v.getDfsUsed();
    }
    return dfsUsed;
  }

  long getBlockPoolUsed(String bpid) throws IOException {
    long dfsUsed = 0L;
    for (FsVolumeImpl v : volumes) {
      dfsUsed += v.getBlockPoolUsed(bpid);
    }
    return dfsUsed;
  }

  long getCapacity() {
    long capacity = 0L;
    for (FsVolumeImpl v : volumes) {
      capacity += v.getCapacity();
    }
    return capacity;
  }
    
  long getRemaining() throws IOException {
    long remaining = 0L;
    for (FsVolumeSpi vol : volumes) {
      remaining += vol.getAvailable();
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
          try {
            FsDatasetImpl.LOG.info("Adding replicas to map for block pool " +
                bpid + " on volume " + v + "...");
            long startTime = Time.monotonicNow();
            v.getVolumeMap(bpid, volumeMap, ramDiskReplicaMap);
            long timeTaken = Time.monotonicNow() - startTime;
            FsDatasetImpl.LOG.info("Time to add replicas to map for block pool"
                + " " + bpid + " on volume " + v + ": " + timeTaken + "ms");
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
    FsDatasetImpl.LOG.info("Total time to add all replicas to map: "
        + totalTimeTaken + "ms");
  }

  /**
   * Calls {@link FsVolumeImpl#checkDirs()} on each volume, removing any
   * volumes from the active list that result in a DiskErrorException.
   * 
   * This method is synchronized to allow only one instance of checkDirs() 
   * call
   * @return list of all the removed volumes.
   */
  synchronized List<FsVolumeImpl> checkDirs() {
    ArrayList<FsVolumeImpl> removedVols = null;
    
    // Make a copy of volumes for performing modification 
    final List<FsVolumeImpl> volumeList = new ArrayList<FsVolumeImpl>(volumes);

    for(Iterator<FsVolumeImpl> i = volumeList.iterator(); i.hasNext(); ) {
      final FsVolumeImpl fsv = i.next();
      try {
        fsv.checkDirs();
      } catch (DiskErrorException e) {
        FsDatasetImpl.LOG.warn("Removing failed volume " + fsv + ": ",e);
        if (removedVols == null) {
          removedVols = new ArrayList<FsVolumeImpl>(1);
        }
        removedVols.add(fsv);
        fsv.shutdown(); 
        i.remove(); // Remove the volume
        numFailedVolumes++;
      }
    }
    
    if (removedVols != null && removedVols.size() > 0) {
      // Replace volume list
      volumes = Collections.unmodifiableList(volumeList);
      FsDatasetImpl.LOG.warn("Completed checkDirs. Removed " + removedVols.size()
          + " volumes. Current volumes: " + this);
    }

    return removedVols;
  }

  @Override
  public String toString() {
    return volumes.toString();
  }

  /**
   * Dynamically add new volumes to the existing volumes that this DN manages.
   * @param newVolume the instance of new FsVolumeImpl.
   */
  synchronized void addVolume(FsVolumeImpl newVolume) {
    // Make a copy of volumes to add new volumes.
    final List<FsVolumeImpl> volumeList = volumes == null ?
        new ArrayList<FsVolumeImpl>() :
        new ArrayList<FsVolumeImpl>(volumes);
    volumeList.add(newVolume);
    volumes = Collections.unmodifiableList(volumeList);
    FsDatasetImpl.LOG.info("Added new volume: " + newVolume.toString());
  }

  /**
   * Dynamically remove volume to the list.
   * @param volume the volume to be removed.
   */
  synchronized void removeVolume(String volume) {
    // Make a copy of volumes to remove one volume.
    final List<FsVolumeImpl> volumeList = new ArrayList<FsVolumeImpl>(volumes);
    for (Iterator<FsVolumeImpl> it = volumeList.iterator(); it.hasNext(); ) {
      FsVolumeImpl fsVolume = it.next();
      if (fsVolume.getBasePath().equals(volume)) {
        fsVolume.shutdown();
        it.remove();
        volumes = Collections.unmodifiableList(volumeList);
        FsDatasetImpl.LOG.info("Removed volume: " + volume);
        break;
      }
    }
  }

  void addBlockPool(final String bpid, final Configuration conf) throws IOException {
    long totalStartTime = Time.monotonicNow();
    
    final List<IOException> exceptions = Collections.synchronizedList(
        new ArrayList<IOException>());
    List<Thread> blockPoolAddingThreads = new ArrayList<Thread>();
    for (final FsVolumeImpl v : volumes) {
      Thread t = new Thread() {
        public void run() {
          try {
            FsDatasetImpl.LOG.info("Scanning block pool " + bpid +
                " on volume " + v + "...");
            long startTime = Time.monotonicNow();
            v.addBlockPool(bpid, conf);
            long timeTaken = Time.monotonicNow() - startTime;
            FsDatasetImpl.LOG.info("Time taken to scan block pool " + bpid +
                " on " + v + ": " + timeTaken + "ms");
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
  
  void removeBlockPool(String bpid) {
    for (FsVolumeImpl v : volumes) {
      v.shutdownBlockPool(bpid);
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
