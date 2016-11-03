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
 * This class represents, movement status of a set of blocks associated to a
 * track Id.
 */
public class BlocksStorageMovementResult {

  private final long trackId;
  private final Status status;

  /**
   * SUCCESS - If all the blocks associated to track id has moved successfully
   * or maximum possible movements done.
   *
   * <p>
   * FAILURE - If any of its(trackId) blocks movement failed and requires to
   * retry these failed blocks movements. Example selected target node is no
   * more running or no space. So, retrying by selecting new target node might
   * work.
   */
  public static enum Status {
    SUCCESS, FAILURE;
  }

  /**
   * BlocksStorageMovementResult constructor.
   *
   * @param trackId
   *          tracking identifier
   * @param status
   *          block movement status
   */
  public BlocksStorageMovementResult(long trackId, Status status) {
    this.trackId = trackId;
    this.status = status;
  }

  public long getTrackId() {
    return trackId;
  }

  public Status getStatus() {
    return status;
  }

}