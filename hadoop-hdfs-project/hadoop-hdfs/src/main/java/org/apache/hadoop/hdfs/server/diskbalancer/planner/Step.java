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

/**
 * A step in the plan.
 */
public interface Step {
  /**
   * Return the number of bytes to move.
   *
   * @return bytes
   */
  long getBytesToMove();

  /**
   * Gets the destination volume.
   *
   * @return - volume
   */
  DiskBalancerVolume getDestinationVolume();

  /**
   * Gets the IdealStorage.
   *
   * @return idealStorage
   */
  double getIdealStorage();

  /**
   * Gets Source Volume.
   *
   * @return -- Source Volume
   */
  DiskBalancerVolume getSourceVolume();

  /**
   * Gets a volume Set ID.
   *
   * @return String
   */
  String getVolumeSetID();

  /**
   * Returns a String representation of the Step Size.
   *
   * @return String
   */
  String getSizeString(long size);

  /**
   * Returns maximum number of disk erros tolerated.
   * @return long.
   */
  long getMaxDiskErrors();

  /**
   * Returns tolerance percentage, the good enough value
   * when we move data from one to disk to another.
   * @return long.
   */
  long getTolerancePercent();

  /**
   * Returns max disk bandwidth that disk balancer will use.
   * Expressed in MB/sec. For example, a value like 10
   * indicates that disk balancer will only move 10 MB / sec
   * while it is running.
   * @return long.
   */
  long getBandwidth();

  /**
   * Sets Tolerance percent on a specific step.
   * @param tolerancePercent - tolerance in percentage.
   */
  void setTolerancePercent(long tolerancePercent);

  /**
   * Set Bandwidth on a specific step.
   * @param bandwidth - in MB/s
   */
  void setBandwidth(long bandwidth);

  /**
   * Set maximum errors to tolerate before disk balancer step fails.
   * @param maxDiskErrors - error count.
   */
  void setMaxDiskErrors(long maxDiskErrors);


}
