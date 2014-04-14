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

import java.io.Closeable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The metrics sink interface. <p>
 * Implementations of this interface consume the {@link MetricsRecord} generated
 * from {@link MetricsSource}. It registers with {@link MetricsSystem} which
 * periodically pushes the {@link MetricsRecord} to the sink using
 * {@link #putMetrics(MetricsRecord)} method.  If the implementing class also
 * implements {@link Closeable}, then the MetricsSystem will close the sink when
 * it is stopped.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MetricsSink extends MetricsPlugin {
  /**
   * Put a metrics record in the sink
   * @param record  the record to put
   */
  void putMetrics(MetricsRecord record);

  /**
   * Flush any buffered metrics
   */
  void flush();
}
