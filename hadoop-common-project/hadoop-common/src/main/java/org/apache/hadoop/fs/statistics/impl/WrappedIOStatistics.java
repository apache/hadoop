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

package org.apache.hadoop.fs.statistics.impl;

import java.util.Map;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;

/**
 * Wrap IOStatistics source with another (dynamic) wrapper.
 */
public class WrappedIOStatistics implements IOStatistics {

  private IOStatistics wrapped;

  /**
   * Instantiate.
   * @param wrapped nullable wrapped statistics.
   */
  public WrappedIOStatistics(final IOStatistics wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public Map<String, Long> counters() {
    return getWrapped().counters();
  }

  protected IOStatistics getWrapped() {
    return wrapped;
  }

  protected void setWrapped(final IOStatistics wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public Map<String, Long> gauges() {
    return getWrapped().gauges();
  }

  @Override
  public Map<String, Long> minimums() {
    return getWrapped().minimums();
  }

  @Override
  public Map<String, Long> maximums() {
    return getWrapped().maximums();
  }

  @Override
  public Map<String, MeanStatistic> meanStatistics() {
    return getWrapped().meanStatistics();
  }
}
