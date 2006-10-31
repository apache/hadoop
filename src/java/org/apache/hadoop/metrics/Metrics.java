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
package org.apache.hadoop.metrics;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility class to simplify creation and reporting of hadoop metrics.
 * For examples of usage, see {@link org.apache.hadoop.dfs.DataNode}.
 * @see org.apache.hadoop.metrics.MetricsRecord
 * @see org.apache.hadoop.metrics.MetricsContext
 * @see org.apache.hadoop.metrics.ContextFactory
 * @author Milind Bhandarkar
 */
public class Metrics {
  private static final Log LOG =
      LogFactory.getLog("org.apache.hadoop.util.MetricsUtil");
  
  /**
   * Don't allow creation of a new instance of Metrics
   */
  private Metrics() {}
  
  /**
   * Utility method to create and return
   * a new tagged metrics record instance within the
   * given <code>contextName</code>.
   * If exception is thrown while creating the record for any reason, it is
   * logged, and a null record is returned.
   * @param contextName name of the context
   * @param recordName the name of the record
   * @param tagName name of the tag field of metrics record
   * @param tagValue value of the tag field
   * @return newly created metrics record
   */
  public static MetricsRecord createRecord(String contextName, String recordName,
      String tagName, String tagValue) {
    try {
      MetricsContext metricsContext =
          ContextFactory.getFactory().getContext(contextName);
      if (!metricsContext.isMonitoring()) {metricsContext.startMonitoring();}
      MetricsRecord metricsRecord = metricsContext.createRecord(recordName);
      metricsRecord.setTag(tagName, tagValue);
      return metricsRecord;
    } catch (Throwable ex) {
      LOG.warn("Could not create metrics record with context:"+contextName, ex);
      return null;
    }
  }
  
  /**
   * Utility method to create and return new metrics record instance within the
   * given <code>contextName</code>. This record is tagged with hostname.
   * If exception is thrown while creating the record due to any reason, it is
   * logged, and a null record is returned.
   * @param contextName name of the context
   * @param recordName name of the record
   * @return newly created metrics record
   */
  public static MetricsRecord createRecord(String contextName,
      String recordName) {
    String hostname = null;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
      LOG.info("Cannot get hostname", ex);
      hostname = "unknown";
    }
    return createRecord(contextName, recordName, "hostname", hostname);
  }
  
  /**
   * Sets the named metric to the specified value in the given metrics record.
   * Updates the table of buffered data which is to be sent periodically.
   *
   * @param record record for which the metric is updated
   * @param metricName name of the metric
   * @param metricValue new value of the metric
   */
  public static void report(MetricsRecord record, String metricName,
      long metricValue) {
    if (record != null) {
      record.setMetric(metricName, metricValue);
      record.update();
    }
  }
}
