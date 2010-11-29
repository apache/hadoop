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

/**
 * Publishes a string to the metrics collector
 */
public class MetricsString extends MetricsBase {
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.hbase.metrics");

  private String value;

  public MetricsString(final String name, final MetricsRegistry registry, 
      final String value) {
    super(name, NO_DESCRIPTION);
    this.value = value;
    registry.add(name, this);
  }
  public MetricsString(final String name, final String description, 
      final MetricsRegistry registry, final String value) {
    super(name, description);
    this.value = value;
    registry.add(name, this);
  }
  
  public String getValue() {
    return this.value;
  }

  @Override
  public synchronized void pushMetric(final MetricsRecord mr) {
    // NOOP
    // MetricsMBeanBase.getAttribute is where we actually fill the data
  }
}