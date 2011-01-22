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
 * An immutable snapshot of metrics with a timestamp
 */
public interface MetricsRecord {
  /**
   * Get the timestamp of the metrics
   * @return  the timestamp
   */
  long timestamp();

  /**
   * Get the record name of the metrics
   * @return  the record name
   */
  String name();

  /**
   * Get the context name of the metrics
   * @return  the context name
   */
  String context();

  /**
   * Get the tags of the record
   * @return  the tags
   */
  Iterable<MetricsTag> tags();

  /**
   * Get the metrics of the record
   * @return  the metrics
   */
  Iterable<Metric> metrics();

}
