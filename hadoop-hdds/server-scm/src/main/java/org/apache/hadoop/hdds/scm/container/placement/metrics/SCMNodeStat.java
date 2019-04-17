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

package org.apache.hadoop.hdds.scm.container.placement.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * This class represents the SCM node stat.
 */
public class SCMNodeStat implements NodeStat {
  private LongMetric capacity;
  private LongMetric scmUsed;
  private LongMetric remaining;

  public SCMNodeStat() {
    this(0L, 0L, 0L);
  }

  public SCMNodeStat(SCMNodeStat other) {
    this(other.capacity.get(), other.scmUsed.get(), other.remaining.get());
  }

  public SCMNodeStat(long capacity, long used, long remaining) {
    Preconditions.checkArgument(capacity >= 0, "Capacity cannot be " +
        "negative.");
    Preconditions.checkArgument(used >= 0, "used space cannot be " +
        "negative.");
    Preconditions.checkArgument(remaining >= 0, "remaining cannot be " +
        "negative");
    this.capacity = new LongMetric(capacity);
    this.scmUsed = new LongMetric(used);
    this.remaining = new LongMetric(remaining);
  }

  /**
   * @return the total configured capacity of the node.
   */
  @Override
  public LongMetric getCapacity() {
    return capacity;
  }

  /**
   * @return the total SCM used space on the node.
   */
  @Override
  public LongMetric getScmUsed() {
    return scmUsed;
  }

  /**
   * @return the total remaining space available on the node.
   */
  @Override
  public LongMetric getRemaining() {
    return remaining;
  }

  /**
   * Set the capacity, used and remaining space on a datanode.
   *
   * @param newCapacity in bytes
   * @param newUsed in bytes
   * @param newRemaining in bytes
   */
  @Override
  @VisibleForTesting
  public void set(long newCapacity, long newUsed, long newRemaining) {
    Preconditions.checkArgument(newCapacity >= 0, "Capacity cannot be " +
        "negative.");
    Preconditions.checkArgument(newUsed >= 0, "used space cannot be " +
        "negative.");
    Preconditions.checkArgument(newRemaining >= 0, "remaining cannot be " +
        "negative");

    this.capacity = new LongMetric(newCapacity);
    this.scmUsed = new LongMetric(newUsed);
    this.remaining = new LongMetric(newRemaining);
  }

  /**
   * Adds a new nodestat to existing values of the node.
   *
   * @param stat Nodestat.
   * @return SCMNodeStat
   */
  @Override
  public SCMNodeStat add(NodeStat stat) {
    this.capacity.set(this.getCapacity().get() + stat.getCapacity().get());
    this.scmUsed.set(this.getScmUsed().get() + stat.getScmUsed().get());
    this.remaining.set(this.getRemaining().get() + stat.getRemaining().get());
    return this;
  }

  /**
   * Subtracts the stat values from the existing NodeStat.
   *
   * @param stat SCMNodeStat.
   * @return Modified SCMNodeStat
   */
  @Override
  public SCMNodeStat subtract(NodeStat stat) {
    this.capacity.set(this.getCapacity().get() - stat.getCapacity().get());
    this.scmUsed.set(this.getScmUsed().get() - stat.getScmUsed().get());
    this.remaining.set(this.getRemaining().get() - stat.getRemaining().get());
    return this;
  }

  @Override
  public boolean equals(Object to) {
    if (to instanceof SCMNodeStat) {
      SCMNodeStat tempStat = (SCMNodeStat) to;
      return capacity.isEqual(tempStat.getCapacity().get()) &&
          scmUsed.isEqual(tempStat.getScmUsed().get()) &&
          remaining.isEqual(tempStat.getRemaining().get());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(capacity.get() ^ scmUsed.get() ^ remaining.get());
  }
}
