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
package org.apache.hadoop.metrics2.util;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This util class provides a method to register an MBean using
 * our standard naming convention as described in the doc
 *  for {link {@link #register(String, String, Object)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class MBeans {
  private static final Logger LOG = LoggerFactory.getLogger(MBeans.class);
  private static final String DOMAIN_PREFIX = "Hadoop:";
  private static final String SERVICE_PREFIX = "service=";
  private static final String NAME_PREFIX = "name=";

  private static final Pattern MBEAN_NAME_PATTERN = Pattern.compile(
      "^" + DOMAIN_PREFIX + SERVICE_PREFIX + "([^,]+)," +
      NAME_PREFIX + "(.+)$");

  private MBeans() {
  }

  /**
   * Register the MBean using our standard MBeanName format
   * "hadoop:service=<serviceName>,name=<nameName>"
   * Where the <serviceName> and <nameName> are the supplied parameters.
   *
   * @param serviceName
   * @param nameName
   * @param theMbean - the MBean to register
   * @return the named used to register the MBean
   */
  static public ObjectName register(String serviceName, String nameName,
                                    Object theMbean) {
    return register(serviceName, nameName, new HashMap<String, String>(),
        theMbean);
  }

  /**
   * Register the MBean using our standard MBeanName format
   * "hadoop:service=<serviceName>,name=<nameName>"
   * Where the <serviceName> and <nameName> are the supplied parameters.
   *
   * @param serviceName
   * @param nameName
   * @param properties - Key value pairs to define additional JMX ObjectName
   *                     properties.
   * @param theMbean    - the MBean to register
   * @return the named used to register the MBean
   */
  static public ObjectName register(String serviceName, String nameName,
                                    Map<String, String> properties,
                                    Object theMbean) {
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Preconditions.checkNotNull(properties,
        "JMX bean properties should not be null for "
            + "bean registration.");
    ObjectName name = getMBeanName(serviceName, nameName, properties);
    if (name != null) {
      try {
        mbs.registerMBean(theMbean, name);
        LOG.debug("Registered " + name);
        return name;
      } catch (InstanceAlreadyExistsException iaee) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Failed to register MBean \"" + name + "\"", iaee);
        } else {
          LOG.warn("Failed to register MBean \"" + name
              + "\": Instance already exists.");
        }
      } catch (Exception e) {
        LOG.warn("Failed to register MBean \"" + name + "\"", e);
      }
    }
    return null;
  }

  public static String getMbeanNameService(final ObjectName objectName) {
    Matcher matcher = MBEAN_NAME_PATTERN.matcher(objectName.toString());
    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      throw new IllegalArgumentException(
          objectName + " is not a valid Hadoop mbean");
    }
  }

  public static String getMbeanNameName(final ObjectName objectName) {
    Matcher matcher = MBEAN_NAME_PATTERN.matcher(objectName.toString());
    if (matcher.matches()) {
      return matcher.group(2);
    } else {
      throw new IllegalArgumentException(
          objectName + " is not a valid Hadoop mbean");
    }
  }

  static public void unregister(ObjectName mbeanName) {
    LOG.debug("Unregistering "+ mbeanName);
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (mbeanName == null) {
      LOG.debug("Stacktrace: ", new Throwable());
      return;
    }
    try {
      mbs.unregisterMBean(mbeanName);
    } catch (Exception e) {
      LOG.warn("Error unregistering "+ mbeanName, e);
    }
    DefaultMetricsSystem.removeMBeanName(mbeanName);
  }

  @VisibleForTesting
  static ObjectName getMBeanName(String serviceName, String nameName,
      Map<String, String> additionalParameters) {

    String additionalKeys = additionalParameters.entrySet()
        .stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining(","));

    String nameStr = DOMAIN_PREFIX + SERVICE_PREFIX + serviceName + "," +
        NAME_PREFIX + nameName +
        (additionalKeys.isEmpty() ? "" : "," + additionalKeys);
    try {
      return DefaultMetricsSystem.newMBeanName(nameStr);
    } catch (Exception e) {
      LOG.warn("Error creating MBean object name: "+ nameStr, e);
      return null;
    }
  }
}
