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
package org.apache.hadoop.metrics.util;

import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The MetricsTimeVaryingInt class is for a metric that naturally
 * varies over time (e.g. number of files created).
 * The metric is is published at interval heart beat (the interval
 * is set in the metrics config file).
 * Note if one wants a time associated with the metric then use
 * @see org.apache.hadoop.metrics.util.MetricsTimeVaryingRate
 *
 */
public class MetricsTimeVaryingInt {

  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.metrics.util");
  
  private String name;
  private int currentValue;
  private int previousIntervalValue;
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   */
  public MetricsTimeVaryingInt(final String nam) {
    name = nam;
    currentValue = 0;
    previousIntervalValue = 0;
  }
  
  /**
   * Inc metrics for incr vlaue
   * @param incr - number of operations
   */
  public synchronized void inc(final int incr) {
    currentValue += incr;
  }
  
  /**
   * Inc metrics by one
   */
  public synchronized void inc() {
    currentValue++;
  }

  private synchronized void intervalHeartBeat() {
     previousIntervalValue = currentValue;
     currentValue = 0;
  }
  
  /**
   * Push the delta  metrics to the mr.
   * The delta is since the last push/interval.
   * 
   * Note this does NOT push to JMX
   * (JMX gets the info via {@link #previousIntervalValue}
   *
   * @param mr
   */
  public synchronized void pushMetric(final MetricsRecord mr) {
    intervalHeartBeat();
    try {
      mr.incrMetric(name, getPreviousIntervalValue());
    } catch (Exception e) {
      LOG.info("pushMetric failed for " + name + "\n" +
          StringUtils.stringifyException(e));
    }
  }
  
  
  /**
   * The Value at the Previous interval
   * @return prev interval value
   */
  public synchronized int getPreviousIntervalValue() { 
    return previousIntervalValue;
  } 
}
