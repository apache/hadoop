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
package org.apache.hadoop.yarn.metrics;

import org.slf4j.Logger;

import org.apache.hadoop.metrics2.MetricsCollector;

/**
 * Used if metric publication should be disabled
 */
public class DispatcherEventMetricsNoOps implements DispatcherEventMetrics {

  private final Logger log;

  public DispatcherEventMetricsNoOps(Logger log) {
    this.log = log;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    log.trace("called getMetrics");
  }

  @Override
  public void init(Class<? extends Enum> typeClass) {
    log.trace("called init");
  }

  @Override
  public void addEvent(Object type) {
    log.trace("called addEvent");
  }

  @Override
  public void removeEvent(Object type) {
    log.trace("called removeEvent");
  }

  @Override
  public void updateRate(Object type, long millisecond) {
    log.trace("called updateRate");
  }
}
