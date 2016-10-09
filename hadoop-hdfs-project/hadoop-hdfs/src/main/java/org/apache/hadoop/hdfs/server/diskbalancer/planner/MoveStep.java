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

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.util.StringUtils;

/**
 * Ignore fields with default values. In most cases Throughtput, diskErrors
 * tolerancePercent and bandwidth will be the system defaults.
 * So we will avoid serializing them into JSON.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
/**
 * Move step is a step that planner can execute that will move data from one
 * volume to another.
 */
public class MoveStep implements Step {
  private DiskBalancerVolume sourceVolume;
  private DiskBalancerVolume destinationVolume;
  private double idealStorage;
  private long bytesToMove;
  private String volumeSetID;

  private long maxDiskErrors;
  private long tolerancePercent;
  private long bandwidth;

  /**
   * Constructs a MoveStep for the volume set.
   *
   * @param sourceVolume      - Source Disk
   * @param idealStorage      - Ideal Storage Value for this disk set
   * @param destinationVolume - Destination dis
   * @param bytesToMove       - number of bytes to move
   * @param volumeSetID       - a diskBalancer generated id.
   */
  public MoveStep(DiskBalancerVolume sourceVolume, double idealStorage,
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
  public double getIdealStorage() {
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
  public void setIdealStorage(double idealStorage) {
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

  /**
   * Gets Maximum numbers of errors to be tolerated before this
   * move operation is aborted.
   * @return  long.
   */
  @Override
  public long getMaxDiskErrors() {
    return maxDiskErrors;
  }

  /**
   * Sets the maximum numbers of Errors to be tolerated before this
   * step is aborted.
   * @param maxDiskErrors - long
   */
  @Override
  public void setMaxDiskErrors(long maxDiskErrors) {
    this.maxDiskErrors = maxDiskErrors;
  }

  /**
   * Tolerance Percentage indicates when a move operation is considered good
   * enough. This is a percentage of deviation from ideal that is considered
   * fine.
   *
   * For example : if the ideal amount on each disk was 1 TB and the
   * tolerance was 10%, then getting to 900 GB on the destination disk is
   * considerd good enough.
   *
   * @return tolerance percentage.
   */
  @Override
  public long getTolerancePercent() {
    return tolerancePercent;
  }

  /**
   * Sets the tolerance percentage.
   * @param tolerancePercent  - long
   */
  @Override
  public void setTolerancePercent(long tolerancePercent) {
    this.tolerancePercent = tolerancePercent;
  }

  /**
   * Gets the disk Bandwidth. That is the MB/Sec to copied. We will max out
   * on this amount of throughput. This is useful to prevent too much I/O on
   * datanode while data node is in use.
   * @return  long.
   */
  @Override
  public long getBandwidth() {
    return bandwidth;
  }

  /**
   * Sets the maximum disk bandwidth per sec to use for this step.
   * @param bandwidth  - Long, MB / Sec of data to be moved between
   *                   source and destinatin volume.
   */
  @Override
  public void setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }
}
