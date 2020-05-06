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

import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;

/**
 * Support for working with IOStatistics.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class IOStatisticsSupport {

  private IOStatisticsSupport() {
  }

  /**
   * Take a snapshot of the current statistics state.
   * This is not an atomic option.
   * The instance can be serialized, and its
   * {@code toString()} method lists all the values.
   * @param statistics statistics
   * @return a snapshot of the current values.
   */
  public static <X extends IOStatistics & Serializable> X snapshot(
      IOStatistics statistics) {
    return IOStatisticsBinding.snapshotStatistics(statistics);
  }


  /**
   * Get the IOStatistics of the source, falling back to
   * null if the source does not implement
   * {@link IOStatisticsSource}, or the return value
   * of {@link IOStatisticsSource#getIOStatistics()} was null.
   * @return an IOStatistics instance or null
   */

  public static IOStatistics retrieveIOStatistics(
      final Object source) {
    return (source instanceof IOStatisticsSource)
        ? ((IOStatisticsSource) source).getIOStatistics()
        : null;
  }
}
