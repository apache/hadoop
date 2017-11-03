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

package org.apache.hadoop.scm.container.common.helpers;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.util.Time;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Manages Block Information inside a container.
 */
public class BlockContainerInfo extends ContainerInfo
    implements Comparator<BlockContainerInfo>,
    Comparable<BlockContainerInfo>, Serializable {
  private long allocated;
  private long lastUsed; // last used time

  public BlockContainerInfo(ContainerInfo container, long used) {
    super(container);
    this.allocated = used;
    this.lastUsed = Time.monotonicNow();
  }

  public long addAllocated(long size) {
    allocated += size;
    return allocated;
  }

  public long subtractAllocated(long size) {
    allocated -= size;
    return allocated;
  }

  public long getAllocated() {
    return this.allocated;
  }

  /**
   * Gets the last used time from SCM's perspective.
   * @return time in milliseconds.
   */
  public long getLastUsed() {
    return lastUsed;
  }

  /**
   * Sets the last used time from SCM's perspective.
   * @param lastUsed time in milliseconds.
   */
  public void setLastUsed(long lastUsed) {
    this.lastUsed = lastUsed;
  }

  /**
   * Compares its two arguments for order.  Returns a negative integer, zero, or
   * a positive integer as the first argument is less than, equal to, or greater
   * than the second.<p>
   *
   * @param o1 the first object to be compared.
   * @param o2 the second object to be compared.
   * @return a negative integer, zero, or a positive integer as the first
   * argument is less than, equal to, or greater than the second.
   * @throws NullPointerException if an argument is null and this comparator
   *                              does not permit null arguments
   * @throws ClassCastException   if the arguments' types prevent them from
   *                              being compared by this comparator.
   */
  @Override
  public int compare(BlockContainerInfo o1, BlockContainerInfo o2) {
    return Long.compare(o1.getLastUsed(), o2.getLastUsed());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return new EqualsBuilder()
        .appendSuper(super.equals(o))
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 2017)
        .appendSuper(super.hashCode())
        .toHashCode();
  }

  @Override
  public String toString() {

    return "BlockContainerInfo{" +
        "allocated=" + allocated +
        ", lastUsed=" + lastUsed +
        ", ContainerInfo=" + super.toString() + '}';
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less than,
   * equal to, or greater than the specified object.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is
   * less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(BlockContainerInfo o) {
    return this.compare(this, o);
  }

  public boolean canAllocate(long size, long containerSize) {
    //TODO: move container size inside Container Info
    return ((getState() == OzoneProtos.LifeCycleState.ALLOCATED ||
        getState() == OzoneProtos.LifeCycleState.OPEN) &&
        (getAllocated() + size <= containerSize));
  }
}
