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
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;

import junit.framework.TestCase;

public class TestLinuxTaskController extends TestCase {
  private static int INVALID_TASKCONTROLLER_PERMISSIONS = 22;
  private static File testDir = new File(System.getProperty("test.build.data",
      "/tmp"), TestLinuxTaskController.class.getName());
  private static String taskControllerPath = System
      .getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_PATH);

  protected void setUp() throws Exception {
    testDir.mkdirs();
  }

  protected void tearDown() throws Exception {
    FileUtil.fullyDelete(testDir);
  }

  public static class MyLinuxTaskController extends LinuxTaskController {
    String taskControllerExePath = taskControllerPath + "/task-controller";
  }

  private void validateTaskControllerSetup(TaskController controller,
      boolean shouldFail) throws IOException {
    if (shouldFail) {
      // task controller setup should fail validating permissions.
      Throwable th = null;
      try {
        controller.setup(new LocalDirAllocator("mapred.local.dir"));
      } catch (IOException ie) {
        th = ie;
      }
      assertNotNull("No exception during setup", th);
      assertTrue("Exception message does not contain exit code"
          + INVALID_TASKCONTROLLER_PERMISSIONS, th.getMessage().contains(
          "with exit code " + INVALID_TASKCONTROLLER_PERMISSIONS));
    } else {
      controller.setup(new LocalDirAllocator("mapred.local.dir"));
    }

  }

  public void testTaskControllerGroup() throws Exception {
    if (!ClusterWithLinuxTaskController.isTaskExecPathPassed()) {
      return;
    }
    // cleanup configuration file.
    ClusterWithLinuxTaskController
        .getTaskControllerConfFile(taskControllerPath).delete();
    Configuration conf = new Configuration();
    // create local dirs and set in the conf.
    File mapredLocal = new File(testDir, "mapred/local");
    mapredLocal.mkdirs();
    conf.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY, mapredLocal.toString());

    // setup task-controller without setting any group name
    TaskController controller = new MyLinuxTaskController();
    controller.setConf(conf);
    validateTaskControllerSetup(controller, true);

    // set an invalid group name for the task controller group
    conf.set(ClusterWithLinuxTaskController.TT_GROUP, "invalid");
    // write the task-controller's conf file
    ClusterWithLinuxTaskController.createTaskControllerConf(taskControllerPath,
        conf);
    validateTaskControllerSetup(controller, true);

    conf.set(ClusterWithLinuxTaskController.TT_GROUP,
        ClusterWithLinuxTaskController.taskTrackerSpecialGroup);
    // write the task-controller's conf file
    ClusterWithLinuxTaskController.createTaskControllerConf(taskControllerPath,
        conf);
    validateTaskControllerSetup(controller, false);
  }
}
