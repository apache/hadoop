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

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.InstanceAlreadyExistsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * This util class provides a method to register an MBean using
 * our standard naming convention as described in the doc
 *  for {link {@link #register(String, String, Object)}
 *
 */
public class MBeans {

  private static final Log LOG = LogFactory.getLog(MBeans.class);

  /**
   * Register the MBean using our standard MBeanName format
   * "hadoop:service=<serviceName>,name=<nameName>"
   * Where the <serviceName> and <nameName> are the supplied parameters
   *
   * @param serviceName
   * @param nameName
   * @param theMbean - the MBean to register
   * @return the named used to register the MBean
   */
  static public ObjectName register(String serviceName, String nameName,
                                    Object theMbean) {
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = getMBeanName(serviceName, nameName);
    try {
      mbs.registerMBean(theMbean, name);
      return name;
    } catch (InstanceAlreadyExistsException ie) {
      LOG.warn(name, ie);
    } catch (Exception e) {
      LOG.warn("Error registering "+ name, e);
    }
    return null;
  }

  static public void unregister(ObjectName mbeanName) {
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (mbeanName == null)
        return;
    try {
      mbs.unregisterMBean(mbeanName);
    } catch (InstanceNotFoundException e ) {
      LOG.warn(mbeanName, e);
    } catch (Exception e) {
      LOG.warn("Error unregistering "+ mbeanName, e);
    }
  }

  static private ObjectName getMBeanName(String serviceName, String nameName) {
    ObjectName name = null;
    String nameStr = "Hadoop:service="+ serviceName +",name="+ nameName;
    try {
      name = new ObjectName(nameStr);
    } catch (MalformedObjectNameException e) {
      LOG.warn("Error creating MBean object name: "+ nameStr, e);
    }
    return name;
  }
}
