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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

/**
 * An helper class for all metrics based on Longs.
 */
@JsonAutoDetect(fieldVisibility = Visibility.ANY)
public class LongMetric implements DatanodeMetric<Long, Long> {
  private Long value;

  /**
   * Constructs a long Metric.
   *
   * @param value Value for this metric.
   */
  public LongMetric(Long value) {
    this.value = value;
  }

  /**
   * Some syntactic sugar over Comparable interface. This makes code easier to
   * read.
   *
   * @param o - Other Object
   * @return - True if *this* object is greater than argument.
   */
  @Override
  public boolean isGreater(Long o) {
    return compareTo(o) > 0;
  }

  /**
   * Inverse of isGreater.
   *
   * @param o - other object.
   * @return True if *this* object is Lesser than argument.
   */
  @Override
  public boolean isLess(Long o) {
    return compareTo(o) < 0;
  }

  /**
   * Returns true if the object has same values. Because of issues with
   * equals, and loss of type information this interface supports isEqual.
   *
   * @param o object to compare.
   * @return True, if the values match.
   */
  @Override
  public boolean isEqual(Long o) {
    return compareTo(o) == 0;
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
    return isGreater(resourceNeeded);
  }

  /**
   * Returns the metric.
   *
   * @return T, the object that represents this metric.
   */
  @Override
  public Long get() {
    return this.value;
  }

  /**
   * Sets the value of this metric.
   *
   * @param setValue - value of the metric.
   */
  @Override
  public void set(Long setValue) {
    this.value = setValue;

  }

  /**
   * Adds a value of to the base.
   *
   * @param addValue - value
   */
  @Override
  public void add(Long addValue) {
    this.value += addValue;
  }

  /**
   * subtract a value.
   *
   * @param subValue value
   */
  @Override
  public void subtract(Long subValue) {
    this.value -= subValue;
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
  public int compareTo(Long o) {
    return Long.compare(this.value, o);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LongMetric that = (LongMetric) o;

    return value != null ? value.equals(that.value) : that.value == null;
  }

  @Override
  public int hashCode() {
    return value != null ? value.hashCode() : 0;
  }
}
