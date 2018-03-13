/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.datamodel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;

import org.apache.hadoop.hdfs.web.JsonUtil;

import java.io.IOException;

/**
 * DiskBalancerVolume represents a volume in the DataNode.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DiskBalancerVolume {
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(DiskBalancerVolume.class);

  private String path;
  private long capacity;
  private String storageType;
  private long used;
  private long reserved;
  private String uuid;
  private boolean failed;
  private boolean isTransient;
  private double volumeDataDensity;
  private boolean skip = false;
  private boolean isReadOnly;

  /**
   * Constructs DiskBalancerVolume.
   */
  public DiskBalancerVolume() {
  }

  /**
   * Parses a Json string and converts to DiskBalancerVolume.
   *
   * @param json - Json String
   *
   * @return DiskBalancerCluster
   *
   * @throws IOException
   */
  public static DiskBalancerVolume parseJson(String json) throws IOException {
    return READER.readValue(json);
  }

  /**
   * Get this volume Data Density
   * Please see DiskBalancerVolumeSet#computeVolumeDataDensity to see how
   * this is computed.
   *
   * @return float.
   */
  public double getVolumeDataDensity() {
    return volumeDataDensity;
  }

  /**
   * Sets this volume's data density.
   *
   * @param volDataDensity - density
   */
  public void setVolumeDataDensity(double volDataDensity) {
    this.volumeDataDensity = volDataDensity;
  }

  /**
   * Indicates if the volume is Transient in nature.
   *
   * @return true or false.
   */
  public boolean isTransient() {
    return isTransient;
  }

  /**
   * Sets volumes transient nature.
   *
   * @param aTransient - bool
   */
  public void setTransient(boolean aTransient) {
    this.isTransient = aTransient;
  }

  /**
   * Compares two volumes and decides if it is the same volume.
   *
   * @param o Volume Object
   *
   * @return boolean
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DiskBalancerVolume that = (DiskBalancerVolume) o;
    return uuid.equals(that.uuid);
  }

  /**
   * Computes hash code for a diskBalancerVolume.
   *
   * @return int
   */
  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  /**
   * Capacity of this volume.
   *
   * @return long
   */
  public long getCapacity() {
    return capacity;
  }

  /**
   * Get free space of the volume.
   *
   * @return long
   */
  @JsonIgnore
  public long getFreeSpace() {
    return getCapacity() - getUsed();
  }

  /**
   * Get ratio between used space and capacity.
   *
   * @return double
   */
  @JsonIgnore
  public double getUsedRatio() {
    return (1.0 * getUsed()) / getCapacity();
  }

  /**
   * Get ratio between free space and capacity.
   *
   * @return double
   */
  @JsonIgnore
  public double getFreeRatio() {
    return (1.0 * getFreeSpace()) / getCapacity();
  }

  /**
   * Sets the capacity of this volume.
   *
   * @param totalCapacity long
   */
  public void setCapacity(long totalCapacity) {
    this.capacity = totalCapacity;
  }

  /**
   * Indicates if this is a failed volume.
   *
   * @return boolean
   */
  public boolean isFailed() {
    return failed;
  }

  /**
   * Sets the failed flag for this volume.
   *
   * @param fail boolean
   */
  public void setFailed(boolean fail) {
    this.failed = fail;
  }

  /**
   * Returns the path for this volume.
   *
   * @return String
   */
  public String getPath() {
    return path;
  }

  /**
   * Sets the path for this volume.
   *
   * @param volPath Path
   */
  public void setPath(String volPath) {
    this.path = volPath;
  }

  /**
   * Gets the reserved size for this volume.
   *
   * @return Long - Reserved size.
   */
  public long getReserved() {
    return reserved;
  }

  /**
   * Sets the reserved size.
   *
   * @param reservedSize -- Sets the reserved.
   */
  public void setReserved(long reservedSize) {
    this.reserved = reservedSize;
  }

  /**
   * Gets the StorageType.
   *
   * @return String StorageType.
   */
  public String getStorageType() {
    return storageType;
  }

  /**
   * Sets the StorageType.
   *
   * @param typeOfStorage - Storage Type String.
   */
  public void setStorageType(String typeOfStorage) {
    this.storageType = typeOfStorage;
  }

  /**
   * Gets the dfsUsed Size.
   *
   * @return - long - used space
   */
  public long getUsed() {
    return used;
  }

  /**
   * Sets the used Space for Long.
   *
   * @param dfsUsedSpace - dfsUsedSpace for this volume.
   */
  public void setUsed(long dfsUsedSpace) {
    Preconditions.checkArgument(dfsUsedSpace < this.getCapacity(),
        "DiskBalancerVolume.setUsed: dfsUsedSpace(%s) < capacity(%s)",
        dfsUsedSpace, getCapacity());
    this.used = dfsUsedSpace;
  }

  /**
   * Gets the uuid for this volume.
   *
   * @return String - uuid of th volume
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * Sets the uuid for this volume.
   *
   * @param id - String
   */
  public void setUuid(String id) {
    this.uuid = id;
  }

  /**
   * Returns effective capacity of a volume.
   *
   * @return float - fraction that represents used capacity.
   */
  @JsonIgnore
  public long computeEffectiveCapacity() {
    return getCapacity() - getReserved();
  }

  /**
   * returns a Json String.
   *
   * @return String
   *
   * @throws IOException
   */
  public String toJson() throws IOException {
    return JsonUtil.toJsonString(this);
  }

  /**
   * returns if we should skip this volume.
   * @return true / false
   */
  public boolean isSkip() {
    return skip;
  }

  /**
   * Sets the Skip value for this volume.
   * @param skipValue bool
   */
  public void setSkip(boolean skipValue) {
    this.skip = skipValue;
  }

  /**
   * Returns the usedPercentage of a disk.
   * This is useful in debugging disk usage
   * @return float
   */
  public float computeUsedPercentage() {
    return (float) (getUsed()) / (float) (getCapacity());
  }

  /**
   * Tells us if a volume is transient.
   * @param transientValue
   */
  public void setIsTransient(boolean transientValue) {
    this.isTransient = transientValue;
  }

  /**
   * Tells us if this volume is read-only.
   * @return true / false
   */
  public boolean isReadOnly() {
    return isReadOnly;
  }

  /**
   * Sets this volume as read only.
   * @param readOnly - boolean
   */
  public void setReadOnly(boolean readOnly) {
    isReadOnly = readOnly;
  }

}
