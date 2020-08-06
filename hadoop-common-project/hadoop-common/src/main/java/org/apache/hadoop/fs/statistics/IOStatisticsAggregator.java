/*
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

package org.apache.hadoop.fs.statistics;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface exported by classes which support
 * aggregation of {@link IOStatistics}.
 * Implementations MAY aggregate all statistics
 * exported by the IOStatistics reference passed in to
 * {@link #aggregate(IOStatistics)}, or they
 * may selectively aggregate specific values/classes
 * of statistics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface IOStatisticsAggregator {

  /**
   * Aggregate the supplied statistics into the current
   * set.
   *
   * @param statistics statistics; may be null
   * @return true if the statistics reference was not null and
   * so aggregated.
   */
  boolean aggregate(@Nullable IOStatistics statistics);
}
