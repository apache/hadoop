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
 * The metrics record builder interface
 */
public abstract class MetricsRecordBuilder {

  /**
   * Add a metrics tag
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @return  self
   */
  public abstract MetricsRecordBuilder tag(String name, String description,
                                           String value);

  /**
   * Add an immutable metrics tag object
   * @param tag a pre-made tag object (potentially save an object construction)
   * @return  self
   */
  public abstract MetricsRecordBuilder add(MetricsTag tag);

  /**
   * Set the context tag
   * @param value of the context
   * @return  self
   */
  public abstract MetricsRecordBuilder setContext(String value);

  /**
   * Add an int counter metric
   * @param name  of the metric
   * @param description of the metric
   * @param value of the metric
   * @return  self
   */
  public abstract MetricsRecordBuilder addCounter(String name,
                                                  String description,
                                                  int value);

  /**
   * Add an long counter metric
   * @param name  of the metric
   * @param description of the metric
   * @param value of the metric
   * @return  self
   */
  public abstract MetricsRecordBuilder addCounter(String name,
                                                  String description,
                                                  long value);

  /**
   * Add a int gauge metric
   * @param name  of the metric
   * @param description of the metric
   * @param value of the metric
   * @return  self
   */
  public abstract MetricsRecordBuilder addGauge(String name,
                                                String description, int value);

  /**
   * Add a long gauge metric
   * @param name  of the metric
   * @param description of the metric
   * @param value of the metric
   * @return  self
   */
  public abstract MetricsRecordBuilder addGauge(String name,
                                                String description, long value);

  /**
   * Add a float gauge metric
   * @param name  of the metric
   * @param description of the metric
   * @param value of the metric
   * @return  self
   */
  public abstract MetricsRecordBuilder addGauge(String name,
                                                String description,
                                                float value);

  /**
   * Add a double gauge metric
   * @param name  of the metric
   * @param description of the metric
   * @param value of the metric
   * @return  self
   */
  public abstract MetricsRecordBuilder addGauge(String name,
                                                String description,
                                                double value);

  /**
   * Add a pre-made immutable metric object
   * @param metric  the pre-made metric to save an object construction
   * @return  self
   */
  public abstract MetricsRecordBuilder add(Metric metric);

}
