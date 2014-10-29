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

import java.io.File;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Test;

public class TestFairSchedulerConfiguration {
  @Test
  public void testParseResourceConfigValue() throws Exception {
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("2 vcores, 1024 mb"));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024 mb, 2 vcores"));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("2vcores,1024mb"));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024mb,2vcores"));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024   mb, 2    vcores"));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024 Mb, 2 vCores"));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("  1024 mb, 2 vcores  "));
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
  
}
