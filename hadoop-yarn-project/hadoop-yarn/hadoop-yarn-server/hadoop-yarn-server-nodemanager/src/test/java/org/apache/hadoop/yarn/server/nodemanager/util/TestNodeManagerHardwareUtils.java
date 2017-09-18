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

/**
 * Test the various functions provided by the NodeManagerHardwareUtils class.
 */
public class TestNodeManagerHardwareUtils {

  static class TestResourceCalculatorPlugin extends ResourceCalculatorPlugin {

    TestResourceCalculatorPlugin() {
      super(null);
    }

    @Override
    public long getVirtualMemorySize() {
      return 0;
    }

    @Override
    public long getPhysicalMemorySize() {
      long ret = Runtime.getRuntime().maxMemory() * 2;
      ret = ret + (4L * 1024 * 1024 * 1024);
      return ret;
    }

    @Override
    public long getAvailableVirtualMemorySize() {
      return 0;
    }

    @Override
    public long getAvailablePhysicalMemorySize() {
      return 0;
    }

    @Override
    public int getNumProcessors() {
      return 8;
    }

    @Override
    public long getCpuFrequency() {
      return 0;
    }

    @Override
    public long getCumulativeCpuTime() {
      return 0;
    }

    @Override
    public float getCpuUsagePercentage() {
      return 0;
    }

    @Override
    public int getNumCores() {
      return 4;
    }
  }

  @Test
  public void testGetContainerCPU() {

    YarnConfiguration conf = new YarnConfiguration();
    float ret;
    final int numProcessors = 8;
    final int numCores = 4;
    ResourceCalculatorPlugin plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numProcessors).when(plugin).getNumProcessors();
    Mockito.doReturn(numCores).when(plugin).getNumCores();

    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 0);
    boolean catchFlag = false;
    try {
      NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
      Assert.fail("getContainerCores should have thrown exception");
    } catch (IllegalArgumentException ie) {
      catchFlag = true;
    }
    Assert.assertTrue(catchFlag);

    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
        100);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(4, (int) ret);

    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 50);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(2, (int) ret);

    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 75);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(3, (int) ret);

    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 85);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(3.4, ret, 0.1);

    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
        110);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(4, (int) ret);
  }

  @Test
  public void testGetVCores() {

    ResourceCalculatorPlugin plugin = new TestResourceCalculatorPlugin();
    YarnConfiguration conf = new YarnConfiguration();

    conf.setFloat(YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER, 1.25f);

    int ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(YarnConfiguration.DEFAULT_NM_VCORES, ret);

    conf.setBoolean(YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        true);
    ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(5, ret);

    conf.setBoolean(YarnConfiguration.NM_COUNT_LOGICAL_PROCESSORS_AS_CORES,
        true);
    ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(10, ret);

    conf.setInt(YarnConfiguration.NM_VCORES, 10);
    ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(10, ret);

    YarnConfiguration conf1 = new YarnConfiguration();
    conf1.setBoolean(YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        false);
    conf.setInt(YarnConfiguration.NM_VCORES, 10);
    ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(10, ret);
  }

  @Test
  public void testGetContainerMemoryMB() throws Exception {

    ResourceCalculatorPlugin plugin = new TestResourceCalculatorPlugin();
    long physicalMemMB = plugin.getPhysicalMemorySize() / (1024 * 1024);
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        true);
    long mem = NodeManagerHardwareUtils.getContainerMemoryMB(null, conf);
    Assert.assertEquals(YarnConfiguration.DEFAULT_NM_PMEM_MB, mem);

    mem = NodeManagerHardwareUtils.getContainerMemoryMB(plugin, conf);
    int hadoopHeapSizeMB =
        (int) (Runtime.getRuntime().maxMemory() / (1024 * 1024));
    int calculatedMemMB =
        (int) (0.8 * (physicalMemMB - (2 * hadoopHeapSizeMB)));
    Assert.assertEquals(calculatedMemMB, mem);

    conf.setInt(YarnConfiguration.NM_PMEM_MB, 1024);
    mem = NodeManagerHardwareUtils.getContainerMemoryMB(conf);
    Assert.assertEquals(1024, mem);

    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        false);
    mem = NodeManagerHardwareUtils.getContainerMemoryMB(conf);
    Assert.assertEquals(YarnConfiguration.DEFAULT_NM_PMEM_MB, mem);
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 10 * 1024);
    mem = NodeManagerHardwareUtils.getContainerMemoryMB(conf);
    Assert.assertEquals(10 * 1024, mem);
  }
}
