/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.util;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestNodeManagerHardwareUtils {

  @Test
  public void testGetContainerCores() {

    YarnConfiguration conf = new YarnConfiguration();
    float ret;
    final int numProcessors = 4;
    ResourceCalculatorPlugin plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numProcessors).when(plugin).getNumProcessors();

    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 0);
    try {
      NodeManagerHardwareUtils.getContainersCores(plugin, conf);
      Assert.fail("getContainerCores should have thrown exception");
    } catch (IllegalArgumentException ie) {
      // expected
    }

    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
      100);
    ret = NodeManagerHardwareUtils.getContainersCores(plugin, conf);
    Assert.assertEquals(4, (int) ret);

    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 50);
    ret = NodeManagerHardwareUtils.getContainersCores(plugin, conf);
    Assert.assertEquals(2, (int) ret);

    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 75);
    ret = NodeManagerHardwareUtils.getContainersCores(plugin, conf);
    Assert.assertEquals(3, (int) ret);

    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 85);
    ret = NodeManagerHardwareUtils.getContainersCores(plugin, conf);
    Assert.assertEquals(3.4, ret, 0.1);

    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
      110);
    ret = NodeManagerHardwareUtils.getContainersCores(plugin, conf);
    Assert.assertEquals(4, (int) ret);
  }
}
