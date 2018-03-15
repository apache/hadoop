/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.scm.container.ContainerStates;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;

/**
 * Class that acts as the container state.
 */
public class ContainerState {
  private final HdslProtos.ReplicationType type;
  private final String owner;
  private final HdslProtos.ReplicationFactor replicationFactor;

  /**
   * Constructs a Container Key.
   *
   * @param owner - Container Owners
   * @param type - Replication Type.
   * @param factor - Replication Factors
   */
  public ContainerState(String owner, HdslProtos.ReplicationType type,
      HdslProtos.ReplicationFactor factor) {
    this.type = type;
    this.owner = owner;
    this.replicationFactor = factor;
  }


  public HdslProtos.ReplicationType getType() {
    return type;
  }

  public String getOwner() {
    return owner;
  }

  public HdslProtos.ReplicationFactor getFactor() {
    return replicationFactor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerState that = (ContainerState) o;

    return new EqualsBuilder()
        .append(type, that.type)
        .append(owner, that.owner)
        .append(replicationFactor, that.replicationFactor)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(137, 757)
        .append(type)
        .append(owner)
        .append(replicationFactor)
        .toHashCode();
  }

  @Override
  public String toString() {
    return "ContainerKey{" +
        ", type=" + type +
        ", owner=" + owner +
        ", replicationFactor=" + replicationFactor +
        '}';
  }
}