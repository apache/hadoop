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
import java.util.HashSet;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.metrics2.util.MBeans;

/**
 * MetricsLoggerTask can be used as utility to dump metrics to log.
 */
public class MetricsLoggerTask implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(MetricsLoggerTask.class);

  private static ObjectName objectName = null;

  static {
    try {
      objectName = new ObjectName("Hadoop:*");
    } catch (MalformedObjectNameException m) {
      // This should not occur in practice since we pass
      // a valid pattern to the constructor above.
    }
  }

  private Logger metricsLog;
  private String nodeName;
  private short maxLogLineLength;

  public MetricsLoggerTask(String metricsLog, String nodeName, short maxLogLineLength) {
    this.metricsLog = LoggerFactory.getLogger(metricsLog);
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

  // TODO : hadoop-logging module to hide log4j implementation details, this method
  //  can directly call utility from hadoop-logging.
  private static boolean hasAppenders(Logger logger) {
    return org.apache.log4j.Logger.getLogger(logger.getName()).getAllAppenders()
        .hasMoreElements();
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

}
