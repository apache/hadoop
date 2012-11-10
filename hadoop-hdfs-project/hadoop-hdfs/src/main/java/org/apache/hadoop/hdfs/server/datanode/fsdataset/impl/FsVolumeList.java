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
    
  void getVolumeMap(ReplicaMap volumeMap) throws IOException {
    for (FsVolumeImpl v : volumes) {
      v.getVolumeMap(volumeMap);
    }
  }
  
  void getVolumeMap(String bpid, ReplicaMap volumeMap) throws IOException {
    for (FsVolumeImpl v : volumes) {
      v.getVolumeMap(bpid, volumeMap);
    }
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


  void addBlockPool(String bpid, Configuration conf) throws IOException {
    for (FsVolumeImpl v : volumes) {
      v.addBlockPool(bpid, conf);
    }
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