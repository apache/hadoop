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
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

class FsVolumeList {
  /**
   * Read access to this unmodifiable list is not synchronized.
   * This list is replaced on modification holding "this" lock.
   */
  volatile List<FsVolumeImpl> volumes = null;

  private final VolumeChoosingPolicy<FsVolumeImpl> blockChooser;
  private volatile int numFailedVolumes;

  FsVolumeList(List<FsVolumeImpl> volumes, int failedVols,
      VolumeChoosingPolicy<FsVolumeImpl> blockChooser) {
    this.volumes = Collections.unmodifiableList(volumes);
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
   * @return next volume to store the block in.
   */
  // TODO should choose volume with storage type
  synchronized FsVolumeImpl getNextVolume(long blockSize) throws IOException {
    return blockChooser.chooseVolume(volumes, blockSize);
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
    
  void initializeReplicaMaps(ReplicaMap globalReplicaMap) throws IOException {
    for (FsVolumeImpl v : volumes) {
      v.getVolumeMap(globalReplicaMap);
    }
  }
  
  void getAllVolumesMap(final String bpid, final ReplicaMap volumeMap) throws IOException {
    long totalStartTime = System.currentTimeMillis();
    final List<IOException> exceptions = Collections.synchronizedList(
        new ArrayList<IOException>());
    List<Thread> replicaAddingThreads = new ArrayList<Thread>();
    for (final FsVolumeImpl v : volumes) {
      Thread t = new Thread() {
        public void run() {
          try {
            FsDatasetImpl.LOG.info("Adding replicas to map for block pool " +
                bpid + " on volume " + v + "...");
            long startTime = System.currentTimeMillis();
            v.getVolumeMap(bpid, volumeMap);
            long timeTaken = System.currentTimeMillis() - startTime;
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
    long totalTimeTaken = System.currentTimeMillis() - totalStartTime;
    FsDatasetImpl.LOG.info("Total time to add all replicas to map: "
        + totalTimeTaken + "ms");
  }

  void getVolumeMap(String bpid, FsVolumeImpl volume, ReplicaMap volumeMap)
      throws IOException {
    FsDatasetImpl.LOG.info("Adding replicas to map for block pool " + bpid +
                               " on volume " + volume + "...");
    long startTime = System.currentTimeMillis();
    volume.getVolumeMap(bpid, volumeMap);
    long timeTaken = System.currentTimeMillis() - startTime;
    FsDatasetImpl.LOG.info("Time to add replicas to map for block pool " + bpid +
                               " on volume " + volume + ": " + timeTaken + "ms");
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


  void addBlockPool(final String bpid, final Configuration conf) throws IOException {
    long totalStartTime = System.currentTimeMillis();
    
    final List<IOException> exceptions = Collections.synchronizedList(
        new ArrayList<IOException>());
    List<Thread> blockPoolAddingThreads = new ArrayList<Thread>();
    for (final FsVolumeImpl v : volumes) {
      Thread t = new Thread() {
        public void run() {
          try {
            FsDatasetImpl.LOG.info("Scanning block pool " + bpid +
                " on volume " + v + "...");
            long startTime = System.currentTimeMillis();
            v.addBlockPool(bpid, conf);
            long timeTaken = System.currentTimeMillis() - startTime;
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
    
    long totalTimeTaken = System.currentTimeMillis() - totalStartTime;
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
