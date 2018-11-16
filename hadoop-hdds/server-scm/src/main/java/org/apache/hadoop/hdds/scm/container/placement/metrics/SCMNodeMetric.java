/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.placement.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * SCM Node Metric that is used in the placement classes.
 */
public class SCMNodeMetric  implements DatanodeMetric<SCMNodeStat, Long> {
  private SCMNodeStat stat;

  /**
   * Constructs an SCMNode Metric.
   *
   * @param stat - SCMNodeStat.
   */
  public SCMNodeMetric(SCMNodeStat stat) {
    this.stat = stat;
  }

  /**
   * Set the capacity, used and remaining space on a datanode.
   *
   * @param capacity in bytes
   * @param used in bytes
   * @param remaining in bytes
   */
  @VisibleForTesting
  public SCMNodeMetric(long capacity, long used, long remaining) {
    this.stat = new SCMNodeStat();
    this.stat.set(capacity, used, remaining);
  }

  /**
   *
   * @param o - Other Object
   * @return - True if *this* object is greater than argument.
   */
  @Override
  public boolean isGreater(SCMNodeStat o) {
    Preconditions.checkNotNull(this.stat, "Argument cannot be null");
    Preconditions.checkNotNull(o, "Argument cannot be null");

    // if zero, replace with 1 for the division to work.
    long thisDenominator = (this.stat.getCapacity().get() == 0)
        ? 1 : this.stat.getCapacity().get();
    long otherDenominator = (o.getCapacity().get() == 0)
        ? 1 : o.getCapacity().get();

    float thisNodeWeight =
        stat.getScmUsed().get() / (float) thisDenominator;

    float oNodeWeight =
        o.getScmUsed().get() / (float) otherDenominator;

    if (Math.abs(thisNodeWeight - oNodeWeight) > 0.000001) {
      return thisNodeWeight > oNodeWeight;
    }
    // if these nodes are have similar weight then return the node with more
    // free space as the greater node.
    return stat.getRemaining().isGreater(o.getRemaining().get());
  }

  /**
   * Inverse of isGreater.
   *
   * @param o - other object.
   * @return True if *this* object is Lesser than argument.
   */
  @Override
  public boolean isLess(SCMNodeStat o) {
    Preconditions.checkNotNull(o, "Argument cannot be null");

    // if zero, replace with 1 for the division to work.
    long thisDenominator = (this.stat.getCapacity().get() == 0)
        ? 1 : this.stat.getCapacity().get();
    long otherDenominator = (o.getCapacity().get() == 0)
        ? 1 : o.getCapacity().get();

    float thisNodeWeight =
        stat.getScmUsed().get() / (float) thisDenominator;

    float oNodeWeight =
        o.getScmUsed().get() / (float) otherDenominator;

    if (Math.abs(thisNodeWeight - oNodeWeight) > 0.000001) {
      return thisNodeWeight < oNodeWeight;
    }

    // if these nodes are have similar weight then return the node with less
    // free space as the lesser node.
    return stat.getRemaining().isLess(o.getRemaining().get());
  }

  /**
   * Returns true if the object has same values. Because of issues with
   * equals, and loss of type information this interface supports isEqual.
   *
   * @param o object to compare.
   * @return True, if the values match.
   * TODO : Consider if it makes sense to add remaining to this equation.
   */
  @Override
  public boolean isEqual(SCMNodeStat o) {
    float thisNodeWeight = stat.getScmUsed().get() / (float)
        stat.getCapacity().get();
    float oNodeWeight = o.getScmUsed().get() / (float) o.getCapacity().get();
    return Math.abs(thisNodeWeight - oNodeWeight) < 0.000001;
  }

  /**
   * A resourceCheck, defined by resourceNeeded.
   * For example, S could be bytes required
   * and DatanodeMetric can reply by saying it can be met or not.
   *
   * @param resourceNeeded -  ResourceNeeded in its own metric.
   * @return boolean, True if this resource requirement can be met.
   */
  @Override
  public boolean hasResources(Long resourceNeeded) {
    return false;
  }

  /**
   * Returns the metric.
   *
   * @return T, the object that represents this metric.
   */
  @Override
  public SCMNodeStat get() {
    return stat;
  }

  /**
   * Sets the value of this metric.
   *
   * @param value - value of the metric.
   */
  @Override
  public void set(SCMNodeStat value) {
    stat.set(value.getCapacity().get(), value.getScmUsed().get(),
        value.getRemaining().get());
  }

  /**
   * Adds a value of to the base.
   *
   * @param value - value
   */
  @Override
  public void add(SCMNodeStat value) {
    stat.add(value);
  }

  /**
   * subtract a value.
   *
   * @param value value
   */
  @Override
  public void subtract(SCMNodeStat value) {
    stat.subtract(value);
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is
   * less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  //@Override
  public int compareTo(SCMNodeStat o) {
    if (isEqual(o)) {
      return 0;
    }
    if (isGreater(o)) {
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SCMNodeMetric that = (SCMNodeMetric) o;

    return stat != null ? stat.equals(that.stat) : that.stat == null;
  }

  @Override
  public int hashCode() {
    return stat != null ? stat.hashCode() : 0;
  }
}
