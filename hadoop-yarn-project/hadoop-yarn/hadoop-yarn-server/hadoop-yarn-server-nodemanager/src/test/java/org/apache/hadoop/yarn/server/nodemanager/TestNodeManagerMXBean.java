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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Class for testing {@link NodeManagerMXBean} implementation.
 */
public class TestNodeManagerMXBean {
  public static final Log LOG = LogFactory.getLog(
          TestNodeManagerMXBean.class);

  @Test
  public void testNodeManagerMXBean() throws Exception {
    try (NodeManager nodeManager = new NodeManager()) {
      Configuration conf = new YarnConfiguration();
      UserGroupInformation.setConfiguration(conf);
      nodeManager.init(conf);

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
              "Hadoop:service=NodeManager,name=NodeManager");

      // Get attribute "SecurityEnabled"
      boolean securityEnabled = (boolean) mbs.getAttribute(mxbeanName,
              "SecurityEnabled");
      Assert.assertEquals(nodeManager.isSecurityEnabled(), securityEnabled);
    }
  }
}
