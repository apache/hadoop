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

package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.util.StringUtils;

/**
 * Move step is a step that planner can execute that will move data from one
 * volume to another.
 */
public class MoveStep implements Step {
  private DiskBalancerVolume sourceVolume;
  private DiskBalancerVolume destinationVolume;
  private float idealStorage;
  private long bytesToMove;
  private String volumeSetID;

  /**
   * Constructs a MoveStep for the volume set.
   *
   * @param sourceVolume      - Source Disk
   * @param idealStorage      - Ideal Storage Value for this disk set
   * @param destinationVolume - Destination dis
   * @param bytesToMove       - number of bytes to move
   * @param volumeSetID       - a diskBalancer generated id.
   */
  public MoveStep(DiskBalancerVolume sourceVolume, float idealStorage,
                  DiskBalancerVolume destinationVolume, long bytesToMove,
                  String volumeSetID) {
    this.destinationVolume = destinationVolume;
    this.idealStorage = idealStorage;
    this.sourceVolume = sourceVolume;
    this.bytesToMove = bytesToMove;
    this.volumeSetID = volumeSetID;

  }

  /**
   * Empty Constructor for JSON serialization.
   */
  public MoveStep() {
  }

  /**
   * Returns number of bytes to move.
   *
   * @return - long
   */
  @Override
  public long getBytesToMove() {
    return bytesToMove;
  }

  /**
   * Gets the destination volume.
   *
   * @return - volume
   */
  @Override
  public DiskBalancerVolume getDestinationVolume() {
    return destinationVolume;
  }

  /**
   * Gets the IdealStorage.
   *
   * @return float
   */
  @Override
  public float getIdealStorage() {
    return idealStorage;
  }

  /**
   * Gets Source Volume.
   *
   * @return -- Source Volume
   */

  @Override
  public DiskBalancerVolume getSourceVolume() {
    return sourceVolume;
  }

  /**
   * Gets a volume Set ID.
   *
   * @return String
   */
  @Override
  public String getVolumeSetID() {
    return volumeSetID;
  }

  /**
   * Set source volume.
   *
   * @param sourceVolume - volume
   */
  public void setSourceVolume(DiskBalancerVolume sourceVolume) {
    this.sourceVolume = sourceVolume;
  }

  /**
   * Sets destination volume.
   *
   * @param destinationVolume - volume
   */
  public void setDestinationVolume(DiskBalancerVolume destinationVolume) {
    this.destinationVolume = destinationVolume;
  }

  /**
   * Sets Ideal Storage.
   *
   * @param idealStorage - ideal Storage
   */
  public void setIdealStorage(float idealStorage) {
    this.idealStorage = idealStorage;
  }

  /**
   * Sets bytes to move.
   *
   * @param bytesToMove - number of bytes
   */
  public void setBytesToMove(long bytesToMove) {
    this.bytesToMove = bytesToMove;
  }

  /**
   * Sets volume id.
   *
   * @param volumeSetID - volume ID
   */
  public void setVolumeSetID(String volumeSetID) {
    this.volumeSetID = volumeSetID;
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object.
   */
  @Override
  public String toString() {
    return String.format("%s\t %s\t %s\t %s%n",
        this.getSourceVolume().getPath(),
        this.getDestinationVolume().getPath(),
        getSizeString(this.getBytesToMove()),
        this.getDestinationVolume().getStorageType());

  }

  /**
   * Returns human readable move sizes.
   *
   * @param size - bytes being moved.
   * @return String
   */
  @Override
  public String getSizeString(long size) {
    return StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1);
  }
}
