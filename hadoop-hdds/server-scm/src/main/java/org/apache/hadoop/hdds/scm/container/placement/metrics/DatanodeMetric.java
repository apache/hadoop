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

import org.apache.hadoop.hdds.scm.exceptions.SCMException;

/**
 * DatanodeMetric acts as the basis for all the metric that is used in
 * comparing 2 datanodes.
 */
public interface DatanodeMetric<T, S>  {

  /**
   * Some syntactic sugar over Comparable interface. This makes code easier to
   * read.
   *
   * @param o - Other Object
   * @return - True if *this* object is greater than argument.
   */
  boolean isGreater(T o);

  /**
   * Inverse of isGreater.
   *
   * @param o - other object.
   * @return True if *this* object is Lesser than argument.
   */
  boolean isLess(T o);

  /**
   * Returns true if the object has same values. Because of issues with
   * equals, and loss of type information this interface supports isEqual.
   *
   * @param o object to compare.
   * @return True, if the values match.
   */
  boolean isEqual(T o);

  /**
   * A resourceCheck, defined by resourceNeeded.
   * For example, S could be bytes required
   * and DatanodeMetric can reply by saying it can be met or not.
   *
   * @param resourceNeeded -  ResourceNeeded in its own metric.
   * @return boolean, True if this resource requirement can be met.
   */
  boolean hasResources(S resourceNeeded) throws SCMException;

  /**
   * Returns the metric.
   *
   * @return T, the object that represents this metric.
   */
  T get();

  /**
   * Sets the value of this metric.
   *
   * @param value - value of the metric.
   */
  void set(T value);

  /**
   * Adds a value of to the base.
   * @param value - value
   */
  void add(T value);

  /**
   * subtract a value.
   * @param value value
   */
  void subtract(T value);

}
