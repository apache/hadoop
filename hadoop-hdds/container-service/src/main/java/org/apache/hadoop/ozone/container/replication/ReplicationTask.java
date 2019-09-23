/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * The task to download a container from the sources.
 */
public class ReplicationTask {

  private volatile Status status = Status.QUEUED;

  private final long containerId;

  private List<DatanodeDetails> sources;

  private final Instant queued = Instant.now();

  public ReplicationTask(long containerId,
      List<DatanodeDetails> sources) {
    this.containerId = containerId;
    this.sources = sources;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReplicationTask that = (ReplicationTask) o;
    return containerId == that.containerId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerId);
  }

  public long getContainerId() {
    return containerId;
  }

  public List<DatanodeDetails> getSources() {
    return sources;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(
      Status status) {
    this.status = status;
  }

  @Override
  public String toString() {
    return "ReplicationTask{" +
        "status=" + status +
        ", containerId=" + containerId +
        ", sources=" + sources +
        ", queued=" + queued +
        '}';
  }

  public Instant getQueued() {
    return queued;
  }

  /**
   * Status of the replication.
   */
  public enum Status {
    QUEUED,
    DOWNLOADING,
    FAILED,
    DONE
  }
}
