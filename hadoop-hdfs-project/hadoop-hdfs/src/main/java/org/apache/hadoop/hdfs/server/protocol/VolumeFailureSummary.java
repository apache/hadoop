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
package org.apache.hadoop.hdfs.server.protocol;

/**
 * Summarizes information about data volume failures on a DataNode.
 */
public class VolumeFailureSummary {
  private final String[] failedStorageLocations;
  private final long lastVolumeFailureDate;
  private final long estimatedCapacityLostTotal;

  /**
   * Creates a new VolumeFailureSummary.
   *
   * @param failedStorageLocations storage locations that have failed
   * @param lastVolumeFailureDate date/time of last volume failure in
   *     milliseconds since epoch
   * @param estimatedCapacityLostTotal estimate of capacity lost in bytes
   */
  public VolumeFailureSummary(String[] failedStorageLocations,
      long lastVolumeFailureDate, long estimatedCapacityLostTotal) {
    this.failedStorageLocations = failedStorageLocations;
    this.lastVolumeFailureDate = lastVolumeFailureDate;
    this.estimatedCapacityLostTotal = estimatedCapacityLostTotal;
  }

  /**
   * Returns each storage location that has failed, sorted.
   *
   * @return each storage location that has failed, sorted
   */
  public String[] getFailedStorageLocations() {
    return this.failedStorageLocations;
  }

  /**
   * Returns the date/time of the last volume failure in milliseconds since
   * epoch.
   *
   * @return date/time of last volume failure in milliseconds since epoch
   */
  public long getLastVolumeFailureDate() {
    return this.lastVolumeFailureDate;
  }

  /**
   * Returns estimate of capacity lost.  This is said to be an estimate, because
   * in some cases it's impossible to know the capacity of the volume, such as if
   * we never had a chance to query its capacity before the failure occurred.
   *
   * @return estimate of capacity lost in bytes
   */
  public long getEstimatedCapacityLostTotal() {
    return this.estimatedCapacityLostTotal;
  }
}
