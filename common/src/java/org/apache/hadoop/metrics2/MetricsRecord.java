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

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An immutable snapshot of metrics with a timestamp
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MetricsRecord {
  /**
   * Get the timestamp of the metrics
   * @return  the timestamp
   */
  long timestamp();

  /**
   * @return the record name
   */
  String name();

  /**
   * @return the description of the record
   */
  String description();

  /**
   * @return the context name of the record
   */
  String context();

  /**
   * Get the tags of the record
   * Note: returning a collection instead of iterable as we
   * need to use tags as keys (hence Collection#hashCode etc.) in maps
   * @return an unmodifiable collection of tags
   */
  Collection<MetricsTag> tags();

  /**
   * Get the metrics of the record
   * @return an immutable iterable interface for metrics
   */
  Iterable<AbstractMetric> metrics();
}
