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

package org.apache.hadoop.yarn.server.timelineservice.reader.filter;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Abstract base class extended to implement timeline filters.
 */
@Private
@Unstable
public abstract class TimelineFilter {

  /**
   * Lists the different filter types.
   */
  @Private
  @Unstable
  public enum TimelineFilterType {
    /**
     * Combines multiple filters.
     */
    LIST,
    /**
     * Filter which is used for key-value comparison.
     */
    COMPARE,
    /**
     * Filter which is used for checking key-value equality.
     */
    KEY_VALUE,
    /**
     * Filter which is used for checking key-multiple values equality.
     */
    KEY_VALUES,
    /**
     * Filter which matches prefix for a config or a metric.
     */
    PREFIX,
    /**
     * Filter which checks existence of a value.
     */
    EXISTS
  }

  public abstract TimelineFilterType getFilterType();

  public String toString() {
    return this.getClass().getSimpleName();
  }
}