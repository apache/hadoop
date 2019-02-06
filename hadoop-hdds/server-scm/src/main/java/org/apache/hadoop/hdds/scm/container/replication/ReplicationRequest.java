/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Wrapper class for hdds replication queue. Implements its natural
 * ordering for priority queue.
 */
public class ReplicationRequest implements Comparable<ReplicationRequest>,
    Serializable {
  private final long containerId;
  private final int replicationCount;
  private final int expecReplicationCount;
  private final long timestamp;

  public ReplicationRequest(long containerId, int replicationCount,
      long timestamp, int expecReplicationCount) {
    this.containerId = containerId;
    this.replicationCount = replicationCount;
    this.timestamp = timestamp;
    this.expecReplicationCount = expecReplicationCount;
  }

  public ReplicationRequest(long containerId, int replicationCount,
      int expecReplicationCount) {
    this(containerId, replicationCount, System.currentTimeMillis(),
        expecReplicationCount);
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object.
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object
   * is less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(ReplicationRequest o) {
    if (o == null) {
      return 1;
    }
    if (this == o) {
      return 0;
    }
    int retVal = Integer
        .compare(getReplicationCount() - getExpecReplicationCount(),
            o.getReplicationCount() - o.getExpecReplicationCount());
    if (retVal != 0) {
      return retVal;
    }
    return Long.compare(getTimestamp(), o.getTimestamp());
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(91, 1011)
        .append(getContainerId())
        .toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReplicationRequest that = (ReplicationRequest) o;
    return new EqualsBuilder().append(getContainerId(), that.getContainerId())
        .isEquals();
  }

  public long getContainerId() {
    return containerId;
  }

  public int getReplicationCount() {
    return replicationCount;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getExpecReplicationCount() {
    return expecReplicationCount;
  }

  @Override
  public String toString() {
    return "ReplicationRequest{" +
        "containerId=" + containerId +
        ", replicationCount=" + replicationCount +
        ", expecReplicationCount=" + expecReplicationCount +
        ", timestamp=" + timestamp +
        '}';
  }
}
