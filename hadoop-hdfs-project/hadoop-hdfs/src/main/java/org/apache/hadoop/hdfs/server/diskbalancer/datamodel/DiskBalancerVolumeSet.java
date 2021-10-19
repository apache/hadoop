/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.datamodel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * DiskBalancerVolumeSet is a collection of storage devices on the
 * data node which are of similar StorageType.
 */
@JsonIgnoreProperties({"sortedQueue", "volumeCount", "idealUsed"})
public class DiskBalancerVolumeSet {
  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerVolumeSet.class);
  private final int maxDisks = 256;

  @JsonProperty("transient")
  private boolean isTransient;
  private Set<DiskBalancerVolume> volumes;

  @JsonIgnore
  private TreeSet<DiskBalancerVolume> sortedQueue;
  private String storageType;
  private String setID;

  private double idealUsed;


  /**
   * Constructs Empty DiskNBalanceVolumeSet.
   * This is needed by jackson
   */
  public DiskBalancerVolumeSet() {
    setID = UUID.randomUUID().toString();
  }

  /**
   * Constructs a DiskBalancerVolumeSet.
   *
   * @param isTransient - boolean
   */
  public DiskBalancerVolumeSet(boolean isTransient) {
    this.isTransient = isTransient;
    volumes = new HashSet<>(maxDisks);
    sortedQueue = new TreeSet<>(new MinHeap());
    this.storageType = null;
    setID = UUID.randomUUID().toString();
  }

  /**
   * Constructs a new DiskBalancerVolumeSet.
   */
  public DiskBalancerVolumeSet(DiskBalancerVolumeSet volumeSet) {
    this.isTransient = volumeSet.isTransient();
    this.storageType = volumeSet.storageType;
    this.volumes = new HashSet<>(volumeSet.volumes);
    sortedQueue = new TreeSet<>(new MinHeap());
    setID = UUID.randomUUID().toString();
  }

  /**
   * Tells us if this volumeSet is transient.
   *
   * @return - true or false
   */
  @JsonProperty("transient")
  public boolean isTransient() {
    return isTransient;
  }

  /**
   * Set the transient properties for this volumeSet.
   *
   * @param transientValue - Boolean
   */
  @JsonProperty("transient")
  public void setTransient(boolean transientValue) {
    this.isTransient = transientValue;
  }

  /**
   * Computes Volume Data Density. Adding a new volume changes
   * the volumeDataDensity for all volumes. So we throw away
   * our priority queue and recompute everything.
   *
   * we discard failed volumes from this computation.
   *
   * totalCapacity = totalCapacity of this volumeSet
   * totalUsed = totalDfsUsed for this volumeSet
   * idealUsed = totalUsed / totalCapacity
   * dfsUsedRatio = dfsUsedOnAVolume / Capacity On that Volume
   * volumeDataDensity = idealUsed - dfsUsedRatio
   */
  public void computeVolumeDataDensity() {
    long totalCapacity = 0;
    long totalUsed = 0;
    sortedQueue.clear();

    // when we plan to re-distribute data we need to make
    // sure that we skip failed volumes.
    for (DiskBalancerVolume volume : volumes) {
      if (!volume.isFailed() && !volume.isSkip()) {

        if (volume.computeEffectiveCapacity() < 0) {
          skipMisConfiguredVolume(volume);
          continue;
        }

        totalCapacity += volume.computeEffectiveCapacity();
        totalUsed += volume.getUsed();
      }
    }

    if (totalCapacity != 0) {
      this.idealUsed = truncateDecimals(totalUsed /
          (double) totalCapacity);
    }

    for (DiskBalancerVolume volume : volumes) {
      if (!volume.isFailed() && !volume.isSkip()) {
        double dfsUsedRatio =
            truncateDecimals(volume.getUsed() /
                (double) volume.computeEffectiveCapacity());

        volume.setVolumeDataDensity(this.idealUsed - dfsUsedRatio);
        sortedQueue.add(volume);
      }
    }
  }

  /**
   * Truncate to 4 digits since uncontrolled precision is some times
   * counter intitive to what users expect.
   * @param value - double.
   * @return double.
   */
  private double truncateDecimals(double value) {
    final int multiplier = 10000;
    return (double) ((long) (value * multiplier)) / multiplier;
  }
  private void skipMisConfiguredVolume(DiskBalancerVolume volume) {
    //probably points to some sort of mis-configuration. Log this and skip
    // processing this volume.
    String errMessage = String.format("Real capacity is negative." +
                                          "This usually points to some " +
                                          "kind of mis-configuration.%n" +
                                          "Capacity : %d Reserved : %d " +
                                          "realCap = capacity - " +
                                          "reserved = %d.%n" +
                                          "Skipping this volume from " +
                                          "all processing. type : %s id" +
                                          " :%s",
                                      volume.getCapacity(),
                                      volume.getReserved(),
                                      volume.computeEffectiveCapacity(),
                                      volume.getStorageType(),
                                      volume.getUuid());

    LOG.error(errMessage);
    volume.setSkip(true);
  }

  /**
   * Returns the number of volumes in the Volume Set.
   *
   * @return int
   */
  @JsonIgnore
  public int getVolumeCount() {
    return volumes.size();
  }

  /**
   * Get Storage Type.
   *
   * @return String
   */
  public String getStorageType() {
    return storageType;
  }

  /**
   * Set Storage Type.
   * @param typeOfStorage -- StorageType
   */
  public void setStorageType(String typeOfStorage) {
    this.storageType = typeOfStorage;
  }

  /**
   * adds a given volume into this volume set.
   *
   * @param volume - volume to add.
   *
   * @throws Exception
   */
  public void addVolume(DiskBalancerVolume volume) throws Exception {
    Preconditions.checkNotNull(volume, "volume cannot be null");
    Preconditions.checkState(isTransient() == volume.isTransient(),
                             "Mismatch in volumeSet and volume's transient " +
                                 "properties.");


    if (this.storageType == null) {
      Preconditions.checkState(volumes.size() == 0L, "Storage Type is Null but"
          + " volume size is " + volumes.size());
      this.storageType = volume.getStorageType();
    } else {
      Preconditions.checkState(this.storageType.equals(volume.getStorageType()),
                               "Adding wrong type of disk to this volume set");
    }
    volumes.add(volume);
    computeVolumeDataDensity();

  }

  /**
   * Returns a list diskVolumes that are part of this volume set.
   *
   * @return List
   */
  public List<DiskBalancerVolume> getVolumes() {
    return new ArrayList<>(volumes);
  }


  @JsonIgnore
  public TreeSet<DiskBalancerVolume> getSortedQueue() {
    return sortedQueue;
  }

  /**
   * Computes whether we need to do any balancing on this volume Set at all.
   * It checks if any disks are out of threshold value
   *
   * @param thresholdPercentage - threshold - in percentage
   *
   * @return true if balancing is needed false otherwise.
   */
  public boolean isBalancingNeeded(double thresholdPercentage) {
    double threshold = thresholdPercentage / 100.0d;

    if(volumes == null || volumes.size() <= 1) {
      // there is nothing we can do with a single volume.
      // so no planning needed.
      return false;
    }

    for (DiskBalancerVolume vol : volumes) {
      boolean notSkip = !vol.isFailed() && !vol.isTransient() && !vol.isSkip();
      Double absDensity =
          truncateDecimals(Math.abs(vol.getVolumeDataDensity()));

      if ((absDensity > threshold) && notSkip) {
        return true;
      }
    }
    return false;
  }

  /**
   * Remove a volume from the current set.
   *
   * This call does not recompute the volumeDataDensity. It has to be
   * done manually after this call.
   *
   * @param volume - Volume to remove
   */
  public void removeVolume(DiskBalancerVolume volume) {
    volumes.remove(volume);
    sortedQueue.remove(volume);
  }

  /**
   * Get Volume Set ID.
   * @return String
   */
  public String getSetID() {
    return setID;
  }

  /**
   * Set VolumeSet ID.
   * @param volID String
   */
  public void setSetID(String volID) {
    this.setID = volID;
  }

  /**
   * Gets the idealUsed for this volume set.
   */

  @JsonIgnore
  public double getIdealUsed() {
    return this.idealUsed;
  }

  static class MinHeap implements Comparator<DiskBalancerVolume>, Serializable {

    /**
     * Compares its two arguments for order.  Returns a negative integer,
     * zero, or a positive integer as the first argument is less than, equal
     * to, or greater than the second.
     */
    @Override
    public int compare(DiskBalancerVolume first, DiskBalancerVolume second) {
      return Double.compare(second.getVolumeDataDensity(),
          first.getVolumeDataDensity());
    }
  }
}
