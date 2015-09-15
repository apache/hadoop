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
package org.apache.hadoop.hdfs.server.common;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;

/**
 * MetricsLoggerTask can be used as utility to dump metrics to log.
 */
public class MetricsLoggerTask implements Runnable {

  public static final Log LOG = LogFactory.getLog(MetricsLoggerTask.class);

  private static ObjectName objectName = null;

  static {
    try {
      objectName = new ObjectName("Hadoop:*");
    } catch (MalformedObjectNameException m) {
      // This should not occur in practice since we pass
      // a valid pattern to the constructor above.
    }
  }

  private Log metricsLog;
  private String nodeName;
  private short maxLogLineLength;

  public MetricsLoggerTask(Log metricsLog, String nodeName,
      short maxLogLineLength) {
    this.metricsLog = metricsLog;
    this.nodeName = nodeName;
    this.maxLogLineLength = maxLogLineLength;
  }

  /**
   * Write metrics to the metrics appender when invoked.
   */
  @Override
  public void run() {
    // Skip querying metrics if there are no known appenders.
    if (!metricsLog.isInfoEnabled() || !hasAppenders(metricsLog)
        || objectName == null) {
      return;
    }

    metricsLog.info(" >> Begin " + nodeName + " metrics dump");
    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

    // Iterate over each MBean.
    for (final ObjectName mbeanName : server.queryNames(objectName, null)) {
      try {
        MBeanInfo mBeanInfo = server.getMBeanInfo(mbeanName);
        final String mBeanNameName = MBeans.getMbeanNameName(mbeanName);
        final Set<String> attributeNames = getFilteredAttributes(mBeanInfo);

        final AttributeList attributes = server.getAttributes(mbeanName,
            attributeNames.toArray(new String[attributeNames.size()]));

        for (Object o : attributes) {
          final Attribute attribute = (Attribute) o;
          final Object value = attribute.getValue();
          final String valueStr = (value != null) ? value.toString() : "null";
          // Truncate the value if it is too long
          metricsLog.info(mBeanNameName + ":" + attribute.getName() + "="
              + trimLine(valueStr));
        }
      } catch (Exception e) {
        metricsLog.error("Failed to get " + nodeName + " metrics for mbean "
            + mbeanName.toString(), e);
      }
    }
    metricsLog.info(" << End " + nodeName + " metrics dump");
  }

  private String trimLine(String valueStr) {
    if (maxLogLineLength <= 0) {
      return valueStr;
    }

    return (valueStr.length() < maxLogLineLength ? valueStr : valueStr
        .substring(0, maxLogLineLength) + "...");
  }

  private static boolean hasAppenders(Log logger) {
    if (!(logger instanceof Log4JLogger)) {
      // Don't bother trying to determine the presence of appenders.
      return true;
    }
    Log4JLogger log4JLogger = ((Log4JLogger) logger);
    return log4JLogger.getLogger().getAllAppenders().hasMoreElements();
  }

  /**
   * Get the list of attributes for the MBean, filtering out a few attribute
   * types.
   */
  private static Set<String> getFilteredAttributes(MBeanInfo mBeanInfo) {
    Set<String> attributeNames = new HashSet<>();
    for (MBeanAttributeInfo attributeInfo : mBeanInfo.getAttributes()) {
      if (!attributeInfo.getType().equals(
          "javax.management.openmbean.TabularData")
          && !attributeInfo.getType().equals(
              "javax.management.openmbean.CompositeData")
          && !attributeInfo.getType().equals(
              "[Ljavax.management.openmbean.CompositeData;")) {
        attributeNames.add(attributeInfo.getName());
      }
    }
    return attributeNames;
  }

  /**
   * Make the metrics logger async and add all pre-existing appenders to the
   * async appender.
   */
  public static void makeMetricsLoggerAsync(Log metricsLog) {
    if (!(metricsLog instanceof Log4JLogger)) {
      LOG.warn("Metrics logging will not be async since "
          + "the logger is not log4j");
      return;
    }
    org.apache.log4j.Logger logger = ((Log4JLogger) metricsLog).getLogger();
    logger.setAdditivity(false); // Don't pollute actual logs with metrics dump

    @SuppressWarnings("unchecked")
    List<Appender> appenders = Collections.list(logger.getAllAppenders());
    // failsafe against trying to async it more than once
    if (!appenders.isEmpty() && !(appenders.get(0) instanceof AsyncAppender)) {
      AsyncAppender asyncAppender = new AsyncAppender();
      // change logger to have an async appender containing all the
      // previously configured appenders
      for (Appender appender : appenders) {
        logger.removeAppender(appender);
        asyncAppender.addAppender(appender);
      }
      logger.addAppender(asyncAppender);
    }
  }
}
