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

package org.apache.hadoop.hdds.scm.container;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Container ID is an integer that is a value between 1..MAX_CONTAINER ID.
 * <p>
 * We are creating a specific type for this to avoid mixing this with
 * normal integers in code.
 */
public final class ContainerID implements Comparable<ContainerID> {

  private final long id;

  // TODO: make this private.
  /**
   * Constructs ContainerID.
   *
   * @param id int
   */
  public ContainerID(long id) {
    this.id = id;
  }

  /**
   * Factory method for creation of ContainerID.
   * @param containerID  long
   * @return ContainerID.
   */
  public static ContainerID valueof(final long containerID) {
    Preconditions.checkState(containerID > 0,
        "Container ID should be a positive long. "+ containerID);
    return new ContainerID(containerID);
  }

  /**
   * Returns int representation of ID.
   *
   * @return int
   */
  public long getId() {
    return id;
  }

  public byte[] getBytes() {
    return Longs.toByteArray(id);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ContainerID that = (ContainerID) o;

    return new EqualsBuilder()
        .append(getId(), that.getId())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(61, 71)
        .append(getId())
        .toHashCode();
  }

  @Override
  public int compareTo(final ContainerID that) {
    Preconditions.checkNotNull(that);
    return new CompareToBuilder()
        .append(this.getId(), that.getId())
        .build();
  }

  @Override
  public String toString() {
    return "#" + id;
  }
}
