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
package org.apache.hadoop.hbase.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.util.StringUtils;

/**
 * Publishes a rate based on a counter - you increment the counter each
 * time an event occurs (eg: an RPC call) and this publishes a rate.
 */
public class MetricsRate extends MetricsBase {
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.hbase.metrics");

  private int value;
  private float prevRate;
  private long ts;

  public MetricsRate(final String name, final MetricsRegistry registry,
      final String description) {
    super(name, description);
    this.value = 0;
    this.prevRate = 0;
    this.ts = System.currentTimeMillis();
    registry.add(name, this);
  }

  public MetricsRate(final String name, final MetricsRegistry registry) {
    this(name, registry, NO_DESCRIPTION);
  }

  public synchronized void inc(final int incr) {
    value += incr;
  }

  public synchronized void inc() {
    value++;
  }

  public synchronized void intervalHeartBeat() {
    long now = System.currentTimeMillis();
    long diff = (now-ts) / 1000;
    if (diff < 1){
        // To make sure our averages aren't skewed by fast repeated calls,
        // we simply ignore fast repeated calls.
    	return;
    }
    this.prevRate = (float)value / diff;
    this.value = 0;
    this.ts = now;
  }

  @Override
  public synchronized void pushMetric(final MetricsRecord mr) {
    intervalHeartBeat();
    try {
      mr.setMetric(getName(), getPreviousIntervalValue());
    } catch (Exception e) {
      LOG.info("pushMetric failed for " + getName() + "\n" +
          StringUtils.stringifyException(e));
    }
  }

  public synchronized float getPreviousIntervalValue() {
    return this.prevRate;
  }
}
