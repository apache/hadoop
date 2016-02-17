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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * A DN volume choosing policy which takes into account the amount of free
 * space on each of the available volumes when considering where to assign a
 * new replica allocation. By default this policy prefers assigning replicas to
 * those volumes with more available free space, so as to over time balance the
 * available space of all the volumes within a DN.
 * Use fine-grained locks to enable choosing volumes of different storage
 * types concurrently.
 */
public class AvailableSpaceVolumeChoosingPolicy<V extends FsVolumeSpi>
    implements VolumeChoosingPolicy<V>, Configurable {
  
  private static final Log LOG = LogFactory.getLog(AvailableSpaceVolumeChoosingPolicy.class);

  private Object[] syncLocks;
  
  private final Random random;
  
  private long balancedSpaceThreshold = DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_DEFAULT;
  private float balancedPreferencePercent = DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;

  AvailableSpaceVolumeChoosingPolicy(Random random) {
    this.random = random;
    initLocks();
  }

  public AvailableSpaceVolumeChoosingPolicy() {
    this(new Random());
    initLocks();
  }

  private void initLocks() {
    int numStorageTypes = StorageType.values().length;
    syncLocks = new Object[numStorageTypes];
    for (int i = 0; i < numStorageTypes; i++) {
      syncLocks[i] = new Object();
    }
  }

  @Override
  public void setConf(Configuration conf) {
    balancedSpaceThreshold = conf.getLong(
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY,
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_DEFAULT);
    balancedPreferencePercent = conf.getFloat(
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);
    
    LOG.info("Available space volume choosing policy initialized: " +
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY +
        " = " + balancedSpaceThreshold + ", " +
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY +
        " = " + balancedPreferencePercent);

    if (balancedPreferencePercent > 1.0) {
      LOG.warn("The value of " + DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY +
               " is greater than 1.0 but should be in the range 0.0 - 1.0");
    }

    if (balancedPreferencePercent < 0.5) {
      LOG.warn("The value of " + DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY +
               " is less than 0.5 so volumes with less available disk space will receive more block allocations");
    }
  }
  
  @Override
  public Configuration getConf() {
    // Nothing to do. Only added to fulfill the Configurable contract.
    return null;
  }
  
  private final VolumeChoosingPolicy<V> roundRobinPolicyBalanced =
      new RoundRobinVolumeChoosingPolicy<V>();
  private final VolumeChoosingPolicy<V> roundRobinPolicyHighAvailable =
      new RoundRobinVolumeChoosingPolicy<V>();
  private final VolumeChoosingPolicy<V> roundRobinPolicyLowAvailable =
      new RoundRobinVolumeChoosingPolicy<V>();

  @Override
  public V chooseVolume(List<V> volumes,
      long replicaSize) throws IOException {
    if (volumes.size() < 1) {
      throw new DiskOutOfSpaceException("No more available volumes");
    }
    // As all the items in volumes are with the same storage type,
    // so only need to get the storage type index of the first item in volumes
    StorageType storageType = volumes.get(0).getStorageType();
    int index = storageType != null ?
            storageType.ordinal() : StorageType.DEFAULT.ordinal();

    synchronized (syncLocks[index]) {
      return doChooseVolume(volumes, replicaSize);
    }
  }

  private V doChooseVolume(final List<V> volumes,
                         long replicaSize) throws IOException {
    AvailableSpaceVolumeList volumesWithSpaces =
        new AvailableSpaceVolumeList(volumes);
    
    if (volumesWithSpaces.areAllVolumesWithinFreeSpaceThreshold()) {
      // If they're actually not too far out of whack, fall back on pure round
      // robin.
      V volume = roundRobinPolicyBalanced.chooseVolume(volumes, replicaSize);
      if (LOG.isDebugEnabled()) {
        LOG.debug("All volumes are within the configured free space balance " +
            "threshold. Selecting " + volume + " for write of block size " +
            replicaSize);
      }
      return volume;
    } else {
      V volume = null;
      // If none of the volumes with low free space have enough space for the
      // replica, always try to choose a volume with a lot of free space.
      long mostAvailableAmongLowVolumes = volumesWithSpaces
          .getMostAvailableSpaceAmongVolumesWithLowAvailableSpace();
      
      List<V> highAvailableVolumes = extractVolumesFromPairs(
          volumesWithSpaces.getVolumesWithHighAvailableSpace());
      List<V> lowAvailableVolumes = extractVolumesFromPairs(
          volumesWithSpaces.getVolumesWithLowAvailableSpace());
      
      float preferencePercentScaler =
          (highAvailableVolumes.size() * balancedPreferencePercent) +
          (lowAvailableVolumes.size() * (1 - balancedPreferencePercent));
      float scaledPreferencePercent =
          (highAvailableVolumes.size() * balancedPreferencePercent) /
          preferencePercentScaler;
      if (mostAvailableAmongLowVolumes < replicaSize ||
          random.nextFloat() < scaledPreferencePercent) {
        volume = roundRobinPolicyHighAvailable.chooseVolume(
            highAvailableVolumes, replicaSize);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Volumes are imbalanced. Selecting " + volume +
              " from high available space volumes for write of block size "
              + replicaSize);
        }
      } else {
        volume = roundRobinPolicyLowAvailable.chooseVolume(
            lowAvailableVolumes, replicaSize);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Volumes are imbalanced. Selecting " + volume +
              " from low available space volumes for write of block size "
              + replicaSize);
        }
      }
      return volume;
    }
  }
  
  /**
   * Used to keep track of the list of volumes we're choosing from.
   */
  private class AvailableSpaceVolumeList {
    private final List<AvailableSpaceVolumePair> volumes;
    
    public AvailableSpaceVolumeList(List<V> volumes) throws IOException {
      this.volumes = new ArrayList<AvailableSpaceVolumePair>();
      for (V volume : volumes) {
        this.volumes.add(new AvailableSpaceVolumePair(volume));
      }
    }
    
    /**
     * @return true if all volumes' free space is within the
     *         configured threshold, false otherwise.
     */
    public boolean areAllVolumesWithinFreeSpaceThreshold() {
      long leastAvailable = Long.MAX_VALUE;
      long mostAvailable = 0;
      for (AvailableSpaceVolumePair volume : volumes) {
        leastAvailable = Math.min(leastAvailable, volume.getAvailable());
        mostAvailable = Math.max(mostAvailable, volume.getAvailable());
      }
      return (mostAvailable - leastAvailable) < balancedSpaceThreshold;
    }
    
    /**
     * @return the minimum amount of space available on a single volume,
     *         across all volumes.
     */
    private long getLeastAvailableSpace() {
      long leastAvailable = Long.MAX_VALUE;
      for (AvailableSpaceVolumePair volume : volumes) {
        leastAvailable = Math.min(leastAvailable, volume.getAvailable());
      }
      return leastAvailable;
    }
    
    /**
     * @return the maximum amount of space available across volumes with low space.
     */
    public long getMostAvailableSpaceAmongVolumesWithLowAvailableSpace() {
      long mostAvailable = Long.MIN_VALUE;
      for (AvailableSpaceVolumePair volume : getVolumesWithLowAvailableSpace()) {
        mostAvailable = Math.max(mostAvailable, volume.getAvailable());
      }
      return mostAvailable;
    }
    
    /**
     * @return the list of volumes with relatively low available space.
     */
    public List<AvailableSpaceVolumePair> getVolumesWithLowAvailableSpace() {
      long leastAvailable = getLeastAvailableSpace();
      List<AvailableSpaceVolumePair> ret = new ArrayList<AvailableSpaceVolumePair>();
      for (AvailableSpaceVolumePair volume : volumes) {
        if (volume.getAvailable() <= leastAvailable + balancedSpaceThreshold) {
          ret.add(volume);
        }
      }
      return ret;
    }
    
    /**
     * @return the list of volumes with a lot of available space.
     */
    public List<AvailableSpaceVolumePair> getVolumesWithHighAvailableSpace() {
      long leastAvailable = getLeastAvailableSpace();
      List<AvailableSpaceVolumePair> ret = new ArrayList<AvailableSpaceVolumePair>();
      for (AvailableSpaceVolumePair volume : volumes) {
        if (volume.getAvailable() > leastAvailable + balancedSpaceThreshold) {
          ret.add(volume);
        }
      }
      return ret;
    }
    
  }
  
  /**
   * Used so that we only check the available space on a given volume once, at
   * the beginning of {@link AvailableSpaceVolumeChoosingPolicy#chooseVolume(List, long)}.
   */
  private class AvailableSpaceVolumePair {
    private final V volume;
    private final long availableSpace;
    
    public AvailableSpaceVolumePair(V volume) throws IOException {
      this.volume = volume;
      this.availableSpace = volume.getAvailable();
    }
    
    public long getAvailable() {
      return availableSpace;
    }
    
    public V getVolume() {
      return volume;
    }
  }
  
  private List<V> extractVolumesFromPairs(List<AvailableSpaceVolumePair> volumes) {
    List<V> ret = new ArrayList<V>();
    for (AvailableSpaceVolumePair volume : volumes) {
      ret.add(volume.getVolume());
    }
    return ret;
  }

}
