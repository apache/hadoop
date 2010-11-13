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
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.util.StringUtils;

/**
 * This class extends MetricsTimeVaryingRate to let the metrics
 * persist past a pushMetric() call
 */
public class PersistentMetricsTimeVaryingRate extends MetricsTimeVaryingRate {
  protected static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.hbase.metrics");

  protected boolean reset = false;
  protected long lastOper = 0;
  protected long totalOps = 0;

  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * @param description metrics description
   */
  public PersistentMetricsTimeVaryingRate(final String nam, 
      final MetricsRegistry registry, 
      final String description) {
    super(nam, registry, description);
  }

  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   */
  public PersistentMetricsTimeVaryingRate(final String nam, 
      MetricsRegistry registry) {
    this(nam, registry, NO_DESCRIPTION);
  }

  /**
   * Push updated metrics to the mr.
   * 
   * Note this does NOT push to JMX
   * (JMX gets the info via {@link #getPreviousIntervalAverageTime()} and
   * {@link #getPreviousIntervalNumOps()}
   *
   * @param mr owner of this metric
   */
  @Override
  public synchronized void pushMetric(final MetricsRecord mr) {
    // this will reset the currentInterval & num_ops += prevInterval()
    super.pushMetric(mr);
    // since we're retaining prevInterval(), we don't want to do the incr
    // instead, we want to set that value because we have absolute ops
    try {
      mr.setMetric(getName() + "_num_ops", totalOps);
    } catch (Exception e) {
      LOG.info("pushMetric failed for " + getName() + "\n" +
          StringUtils.stringifyException(e));
    }
    if (reset) {
      // use the previous avg as our starting min/max/avg
      super.inc(getPreviousIntervalAverageTime());
      reset = false;
    } else {
      // maintain the stats that pushMetric() cleared
      maintainStats();
    }
  }
  
  /**
   * Increment the metrics for numOps operations
   * @param numOps - number of operations
   * @param time - time for numOps operations
   */
  @Override
  public synchronized void inc(final int numOps, final long time) {
    super.inc(numOps, time);
    totalOps += numOps;
  }
  
  /**
   * Increment the metrics for numOps operations
   * @param time - time for numOps operations
   */
  @Override
  public synchronized void inc(final long time) {
    super.inc(time);
    ++totalOps;
  }
  
  /**
   * Rollover to a new interval
   * NOTE: does not reset numOps.  this is an absolute value
   */
  public synchronized void resetMinMaxAvg() {
    reset = true;
  }

  /* MetricsTimeVaryingRate will reset every time pushMetric() is called
   * This is annoying for long-running stats that might not get a single 
   * operation in the polling period.  This function ensures that values
   * for those stat entries don't get reset.
   */
  protected void maintainStats() {
    int curOps = this.getPreviousIntervalNumOps();
    if (curOps > 0) {
      long curTime = this.getPreviousIntervalAverageTime();
      long totalTime = curTime * curOps;
      if (totalTime / curTime == curOps) {
        super.inc(curOps, totalTime);
      } else {
        LOG.info("Stats for " + this.getName() + " overflowed! resetting");
      }
    }
  }
}
