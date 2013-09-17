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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A visitor interface for metrics
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MetricsVisitor {
  /**
   * Callback for integer value gauges
   * @param info  the metric info
   * @param value of the metric
   */
  public void gauge(MetricsInfo info, int value);

  /**
   * Callback for long value gauges
   * @param info  the metric info
   * @param value of the metric
   */
  public void gauge(MetricsInfo info, long value);

  /**
   * Callback for float value gauges
   * @param info  the metric info
   * @param value of the metric
   */
  public void gauge(MetricsInfo info, float value);

  /**
   * Callback for double value gauges
   * @param info  the metric info
   * @param value of the metric
   */
  public void gauge(MetricsInfo info, double value);

  /**
   * Callback for integer value counters
   * @param info  the metric info
   * @param value of the metric
   */
  public void counter(MetricsInfo info, int value);

  /**
   * Callback for long value counters
   * @param info  the metric info
   * @param value of the metric
   */
  public void counter(MetricsInfo info, long value);
}
