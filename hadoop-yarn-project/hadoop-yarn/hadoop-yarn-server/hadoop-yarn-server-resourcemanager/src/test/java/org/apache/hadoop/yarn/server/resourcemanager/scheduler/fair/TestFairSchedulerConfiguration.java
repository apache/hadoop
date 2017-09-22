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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration.parseResourceConfigValue;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Test;

public class TestFairSchedulerConfiguration {
  @Test
  public void testParseResourceConfigValue() throws Exception {
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("2 vcores, 1024 mb").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024 mb, 2 vcores").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("2vcores,1024mb").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024mb,2vcores").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024   mb, 2    vcores").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024 Mb, 2 vCores").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("  1024 mb, 2 vcores  ").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("  1024.3 mb, 2.35 vcores  ").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("  1024. mb, 2. vcores  ").getResource());

    Resource clusterResource = BuilderUtils.newResource(2048, 4);
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50% memory, 50% cpu").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50% Memory, 50% CpU").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50%").getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 4),
        parseResourceConfigValue("50% memory, 100% cpu").
        getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 4),
        parseResourceConfigValue(" 100% cpu, 50% memory").
        getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 0),
        parseResourceConfigValue("50% memory, 0% cpu").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50 % memory, 50 % cpu").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50%memory,50%cpu").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("  50  %  memory,  50  %  cpu  ").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50.% memory, 50.% cpu").
            getResource(clusterResource));

    clusterResource =  BuilderUtils.newResource(1024 * 10, 4);
    assertEquals(BuilderUtils.newResource((int)(1024 * 10 * 0.109), 2),
        parseResourceConfigValue("10.9% memory, 50.6% cpu").
            getResource(clusterResource));
  }
  
  @Test(expected = AllocationConfigurationException.class)
  public void testNoUnits() throws Exception {
    parseResourceConfigValue("1024");
  }
  
  @Test(expected = AllocationConfigurationException.class)
  public void testOnlyMemory() throws Exception {
    parseResourceConfigValue("1024mb");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testOnlyCPU() throws Exception {
    parseResourceConfigValue("1024vcores");
  }
  
  @Test(expected = AllocationConfigurationException.class)
  public void testGibberish() throws Exception {
    parseResourceConfigValue("1o24vc0res");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testNoUnitsPercentage() throws Exception {
    parseResourceConfigValue("95%, 50% memory");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testInvalidNumPercentage() throws Exception {
    parseResourceConfigValue("95A% cpu, 50% memory");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testCpuPercentageMemoryAbsolute() throws Exception {
    parseResourceConfigValue("50% cpu, 1024 mb");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testMemoryPercentageCpuAbsolute() throws Exception {
    parseResourceConfigValue("50% memory, 2 vcores");
  }
}
