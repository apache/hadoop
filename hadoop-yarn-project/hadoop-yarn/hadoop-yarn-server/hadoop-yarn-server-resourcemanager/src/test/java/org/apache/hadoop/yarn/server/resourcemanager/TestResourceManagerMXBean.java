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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Class for testing {@link ResourceManagerMXBean} implementation.
 */
public class TestResourceManagerMXBean {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestResourceManagerMXBean.class);

  @Test
  public void testResourceManagerMXBean() throws Exception {
    try (ResourceManager resourceManager = new ResourceManager()) {
      Configuration conf = new YarnConfiguration();
      UserGroupInformation.setConfiguration(conf);
      resourceManager.init(conf);

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
              "Hadoop:service=ResourceManager,name=ResourceManager");

      // Get attribute "SecurityEnabled"
      boolean securityEnabled = (boolean) mbs.getAttribute(mxbeanName,
              "SecurityEnabled");
      Assert.assertEquals(resourceManager.isSecurityEnabled(), securityEnabled);
    }
  }
}
