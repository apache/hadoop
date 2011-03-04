/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2;

/**
 * The immutable metric
 */
public abstract class Metric {

  public static final String NO_DESCRIPTION = "<<no description>>";
  private final String name;
  private final String description;

  /**
   * Construct the metric with name only
   * @param name  of the metric
   */
  public Metric(String name) {
    this.name = name;
    this.description = NO_DESCRIPTION;
  }

  /**
   * Construct the metric with a name and a description
   * @param name  of the metric
   * @param desc  description of the metric
   */
  public Metric(String name, String desc) {
    this.name = name;
    this.description = desc;
  }

  /**
   * Get the name of the metric
   * @return  the name
   */
  public String name() {
    return name;
  }

  /**
   * Get the description of the metric
   * @return  the description
   */
  public String description() {
    return description;
  }

  /**
   * Get the value of the metric
   * @return  the value of the metric
   */
  public abstract Number value();

  /**
   * Accept a visitor interface
   * @param visitor of the metric
   */
  public abstract void visit(MetricsVisitor visitor);

  // Mostly for testing
  @Override public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Metric other = (Metric) obj;
    if (!this.name.equals(other.name())) {
      return false;
    }
    if (!this.description.equals(other.description())) {
      return false;
    }
    if (!value().equals(other.value())) {
      return false;
    }
    return true;
  }

  @Override public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return "Metric{" + "name='" + name + "' description='" + description +
           "' value="+ value() +'}';
  }

}
