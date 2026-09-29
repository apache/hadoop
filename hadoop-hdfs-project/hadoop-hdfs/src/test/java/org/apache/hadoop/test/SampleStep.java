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
package org.apache.hadoop.test;

import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;

/**
 * A sample Step implementation used in Serde Tests.
 */
public class SampleStep implements Step {
  private long bytesToMove;
  private long bandwidth;
  private long tolerancePercent;
  private long maxDiskErrors;

  @Override
  public long getBytesToMove() {
    return bytesToMove;
  }

  @Override
  public DiskBalancerVolume getDestinationVolume() {
    return null;
  }

  @Override
  public double getIdealStorage() {
    return 0;
  }

  @Override
  public DiskBalancerVolume getSourceVolume() {
    return null;
  }

  @Override
  public String getVolumeSetID() {
    return "";
  }

  @Override
  public String getSizeString(long size) {
    return Long.toString(size);
  }

  @Override
  public long getMaxDiskErrors() {
    return maxDiskErrors;
  }

  @Override
  public long getTolerancePercent() {
    return tolerancePercent;
  }

  @Override
  public long getBandwidth() {
    return bandwidth;
  }

  @Override
  public void setTolerancePercent(long tolerancePercent) {
    this.tolerancePercent = tolerancePercent;
  }

  @Override
  public void setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  @Override
  public void setMaxDiskErrors(long maxDiskErrors) {
    this.maxDiskErrors = maxDiskErrors;
  }
}
