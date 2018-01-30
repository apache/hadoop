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

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.MathUtils;

/**
 * Container ID is an integer that is a value between 1..MAX_CONTAINER ID.
 * <p>
 * We are creating a specific type for this to avoid mixing this with
 * normal integers in code.
 */
public class ContainerID implements Comparable {

  private final long id;

  /**
   * Constructs ContainerID.
   *
   * @param id int
   */
  public ContainerID(long id) {
    Preconditions.checkState(id > 0,
        "Container ID should be a positive int");
    this.id = id;
  }

  /**
   * Factory method for creation of ContainerID.
   * @param containerID  long
   * @return ContainerID.
   */
  public static ContainerID valueof(long containerID) {
    Preconditions.checkState(containerID > 0);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerID that = (ContainerID) o;

    return id == that.id;
  }

  @Override
  public int hashCode() {
    return MathUtils.hash(id);
  }

  @Override
  public int compareTo(Object o) {
    Preconditions.checkNotNull(o);
    if (o instanceof ContainerID) {
      return Long.compare(((ContainerID) o).getId(), this.getId());
    }
    throw new IllegalArgumentException("Object O, should be an instance " +
        "of ContainerID");
  }

  @Override
  public String toString() {
    return "id=" + id;
  }
}
