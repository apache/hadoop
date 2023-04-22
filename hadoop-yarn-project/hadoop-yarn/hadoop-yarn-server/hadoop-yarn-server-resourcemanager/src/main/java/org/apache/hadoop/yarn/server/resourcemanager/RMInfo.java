/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMX bean for RM info.
 */
public class RMInfo implements RMInfoMXBean {
  private static final Logger LOG = LoggerFactory.getLogger(RMNMInfo.class);
  private ResourceManager resourceManager;
  private ObjectName rmStatusBeanName;

  /**
   * Constructor for RMInfo registers the bean with JMX.
   *
   * @param resourceManager resource manager's context object
   */
  RMInfo(ResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  public void register() {
    StandardMBean bean;
    try {
      bean = new StandardMBean(this, RMInfoMXBean.class);
      rmStatusBeanName = MBeans.register("ResourceManager", "RMInfo", bean);
    } catch (NotCompliantMBeanException e) {
      LOG.warn("Error registering RMInfo MBean", e);
    }
    LOG.info("Registered RMInfo MBean");
  }

  public void unregister() {
    if (rmStatusBeanName != null) {
      MBeans.unregister(rmStatusBeanName);
    }
  }

  @Override public String getState() {
    return this.resourceManager.getRMContext().getHAServiceState().toString();
  }

  @Override public String getHostAndPort() {
    return NetUtils.getHostPortString(ResourceManager.getBindAddress(
        this.resourceManager.getRMContext().getYarnConfiguration()));
  }

  @Override public boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }
}
