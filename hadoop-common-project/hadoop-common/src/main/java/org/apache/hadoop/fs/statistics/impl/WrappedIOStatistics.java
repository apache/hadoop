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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * Wrap IOStatistics source with another (dynamic) wrapper.
 */
public class WrappedIOStatistics implements IOStatistics {

  private IOStatistics source;

  public WrappedIOStatistics(final IOStatistics source) {
    this.source = source;
  }


  protected IOStatistics getSource() {
    return source;
  }

  protected void setSource(final IOStatistics source) {
    this.source = source;
  }

  @Override
  public Long getStatistic(final String key) {
    return source.getStatistic(key);
  }

  @Override
  public boolean isTracked(final String key) {
    return source.isTracked(key);
  }

  @Override
  public Set<String> keys() {
    return source.keys();
  }

  @Override
  public Iterator<Map.Entry<String, Long>> iterator() {
    return source.iterator();
  }
}
