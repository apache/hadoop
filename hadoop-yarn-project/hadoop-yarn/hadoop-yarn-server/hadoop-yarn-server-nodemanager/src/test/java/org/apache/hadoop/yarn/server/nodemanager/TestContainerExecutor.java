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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


import org.junit.Test;
import static org.junit.Assert.*;

public class TestContainerExecutor {

  @Test (timeout = 5000)
  public void testRunCommandNoPriority() throws Exception {
    Configuration conf = new Configuration();
    String[] command = ContainerExecutor.getRunCommand("echo", "group1", conf);
    assertTrue("first command should be the run command for the platform", 
               command[0].equals(Shell.WINUTILS) || command[0].equals("bash"));  
  }

  @Test (timeout = 5000)
  public void testRunCommandwithPriority() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY, 2);
    String[] command = ContainerExecutor.getRunCommand("echo", "group1", conf);
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
    command = ContainerExecutor.getRunCommand("echo", "group1", conf);
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

}
