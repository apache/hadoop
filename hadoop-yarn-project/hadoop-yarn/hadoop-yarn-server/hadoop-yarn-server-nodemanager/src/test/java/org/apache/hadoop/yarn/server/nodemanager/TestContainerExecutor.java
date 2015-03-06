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

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class TestContainerExecutor {
  
  private ContainerExecutor containerExecutor = new DefaultContainerExecutor();

  @Test (timeout = 5000)
  public void testRunCommandNoPriority() throws Exception {
    Configuration conf = new Configuration();
    String[] command = containerExecutor.getRunCommand("echo", "group1", "user", null, conf);
    assertTrue("first command should be the run command for the platform", 
               command[0].equals(Shell.WINUTILS) || command[0].equals("bash"));  
  }

  @Test (timeout = 5000)
  public void testRunCommandwithPriority() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY, 2);
    String[] command = containerExecutor.getRunCommand("echo", "group1", "user", null, conf);
    if (Shell.WINDOWS) {
      // windows doesn't currently support
      assertEquals("first command should be the run command for the platform", 
               Shell.WINUTILS, command[0]); 
    } else {
      assertEquals("first command should be nice", "nice", command[0]); 
      assertEquals("second command should be -n", "-n", command[1]); 
      assertEquals("third command should be the priority", Integer.toString(2),
                   command[2]); 
    }

    // test with negative number
    conf.setInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY, -5);
    command = containerExecutor.getRunCommand("echo", "group1", "user", null, conf);
    if (Shell.WINDOWS) {
      // windows doesn't currently support
      assertEquals("first command should be the run command for the platform", 
               Shell.WINUTILS, command[0]); 
    } else {
      assertEquals("first command should be nice", "nice", command[0]); 
      assertEquals("second command should be -n", "-n", command[1]); 
      assertEquals("third command should be the priority", Integer.toString(-5),
                    command[2]); 
    }
  }

  @Test (timeout = 5000)
  public void testRunCommandWithNoResources() {
    // Windows only test
    assumeTrue(Shell.WINDOWS);
    Configuration conf = new Configuration();
    String[] command = containerExecutor.getRunCommand("echo", "group1", null, null,
        conf, Resource.newInstance(1024, 1));
    // Assert the cpu and memory limits are set correctly in the command
    String[] expected = { Shell.WINUTILS, "task", "create", "-m", "-1", "-c",
        "-1", "group1", "cmd /c " + "echo" };
    Assert.assertTrue(Arrays.equals(expected, command));
  }

  @Test (timeout = 5000)
  public void testRunCommandWithMemoryOnlyResources() {
    // Windows only test
    assumeTrue(Shell.WINDOWS);
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED, "true");
    String[] command = containerExecutor.getRunCommand("echo", "group1", null, null,
        conf, Resource.newInstance(1024, 1));
    // Assert the cpu and memory limits are set correctly in the command
    String[] expected = { Shell.WINUTILS, "task", "create", "-m", "1024", "-c",
        "-1", "group1", "cmd /c " + "echo" };
    Assert.assertTrue(Arrays.equals(expected, command));
  }

  @Test (timeout = 5000)
  public void testRunCommandWithCpuAndMemoryResources() {
    // Windows only test
    assumeTrue(Shell.WINDOWS);
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED, "true");
    conf.set(YarnConfiguration.NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED, "true");
    String[] command = containerExecutor.getRunCommand("echo", "group1", null, null,
        conf, Resource.newInstance(1024, 1));
    float yarnProcessors = NodeManagerHardwareUtils.getContainersCores(
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf),
        conf);
    int cpuRate = Math.min(10000, (int) ((1 * 10000) / yarnProcessors));
    // Assert the cpu and memory limits are set correctly in the command
    String[] expected = { Shell.WINUTILS, "task", "create", "-m", "1024", "-c",
        String.valueOf(cpuRate), "group1", "cmd /c " + "echo" };
    Assert.assertTrue(Arrays.equals(expected, command));
  }
}
