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

import org.apache.hadoop.classification.InterfaceStability;

/**
 * A source of IO statistics.
 * <p>
 * These statistics MUST be instance specific, not thread local.
 * </p>
 */

@InterfaceStability.Unstable
public interface IOStatisticsSource {

  /**
   * Return a statistics instance.
   * <p>
   * It is not a requirement that the same instance is returned every time.
   * {@link IOStatisticsSource}.
   * <p>
   * If the object implementing this is Closeable, this method
   * may return null if invoked on a closed object, even if
   * it returns a valid instance when called earlier.
   * @return an IOStatistics instance or null
   */
  default IOStatistics getIOStatistics() {
    return null;
  }
}
