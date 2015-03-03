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

/**
 * Tracks information about failure of a data volume.
 */
final class VolumeFailureInfo {
  private final String failedStorageLocation;
  private final long failureDate;
  private final long estimatedCapacityLost;

  /**
   * Creates a new VolumeFailureInfo, when the capacity lost from this volume
   * failure is unknown.  Typically, this means the volume failed immediately at
   * startup, so there was never a chance to query its capacity.
   *
   * @param failedStorageLocation storage location that has failed
   * @param failureDate date/time of failure in milliseconds since epoch
   */
  public VolumeFailureInfo(String failedStorageLocation, long failureDate) {
    this(failedStorageLocation, failureDate, 0);
  }

  /**
   * Creates a new VolumeFailureInfo.
   *
   * @param failedStorageLocation storage location that has failed
   * @param failureDate date/time of failure in milliseconds since epoch
   * @param estimatedCapacityLost estimate of capacity lost in bytes
   */
  public VolumeFailureInfo(String failedStorageLocation, long failureDate,
      long estimatedCapacityLost) {
    this.failedStorageLocation = failedStorageLocation;
    this.failureDate = failureDate;
    this.estimatedCapacityLost = estimatedCapacityLost;
  }

  /**
   * Returns the storage location that has failed.
   *
   * @return storage location that has failed
   */
  public String getFailedStorageLocation() {
    return this.failedStorageLocation;
  }

  /**
   * Returns date/time of failure
   *
   * @return date/time of failure in milliseconds since epoch
   */
  public long getFailureDate() {
    return this.failureDate;
  }

  /**
   * Returns estimate of capacity lost.  This is said to be an estimate, because
   * in some cases it's impossible to know the capacity of the volume, such as if
   * we never had a chance to query its capacity before the failure occurred.
   *
   * @return estimate of capacity lost in bytes
   */
  public long getEstimatedCapacityLost() {
    return this.estimatedCapacityLost;
  }
}
