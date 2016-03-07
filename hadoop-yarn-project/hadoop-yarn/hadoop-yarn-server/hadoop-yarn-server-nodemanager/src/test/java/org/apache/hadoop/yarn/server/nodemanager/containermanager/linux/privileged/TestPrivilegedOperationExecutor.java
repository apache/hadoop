/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestPrivilegedOperationExecutor {
  private static final Log LOG = LogFactory
      .getLog(TestPrivilegedOperationExecutor.class);
  private String localDataDir;
  private String customExecutorPath;
  private Configuration nullConf = null;
  private Configuration emptyConf;
  private Configuration confWithExecutorPath;

  private String cGroupTasksNone;
  private String cGroupTasksInvalid;
  private String cGroupTasks1;
  private String cGroupTasks2;
  private String cGroupTasks3;
  private PrivilegedOperation opDisallowed;
  private PrivilegedOperation opTasksNone;
  private PrivilegedOperation opTasksInvalid;
  private PrivilegedOperation opTasks1;
  private PrivilegedOperation opTasks2;
  private PrivilegedOperation opTasks3;

  @Before
  public void setup() {
    localDataDir = System.getProperty("test.build.data");
    customExecutorPath = localDataDir + "/bin/container-executor";
    emptyConf = new YarnConfiguration();
    confWithExecutorPath = new YarnConfiguration();
    confWithExecutorPath.set(YarnConfiguration
        .NM_LINUX_CONTAINER_EXECUTOR_PATH, customExecutorPath);

    cGroupTasksNone = "none";
    cGroupTasksInvalid = "invalid_string";
    cGroupTasks1 = "cpu/hadoop_yarn/container_01/tasks";
    cGroupTasks2 = "net_cls/hadoop_yarn/container_01/tasks";
    cGroupTasks3 = "blkio/hadoop_yarn/container_01/tasks";
    opDisallowed = new PrivilegedOperation
        (PrivilegedOperation.OperationType.DELETE_AS_USER);
    opTasksNone = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupTasksNone);
    opTasksInvalid = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            cGroupTasksInvalid);
    opTasks1 = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupTasks1);
    opTasks2 = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupTasks2);
    opTasks3 = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupTasks3);
  }

  @Test
  public void testExecutorPath() {
    String containerExePath = PrivilegedOperationExecutor
        .getContainerExecutorExecutablePath(nullConf);

    //In case HADOOP_YARN_HOME isn't set, CWD is used. If conf is null or
    //NM_LINUX_CONTAINER_EXECUTOR_PATH is not set, then a defaultPath is
    //constructed.
    String yarnHomeEnvVar = System.getenv("HADOOP_YARN_HOME");
    String yarnHome = yarnHomeEnvVar != null ? yarnHomeEnvVar
        : new File("").getAbsolutePath();
    String expectedPath = yarnHome + "/bin/container-executor";

    Assert.assertEquals(expectedPath, containerExePath);

    containerExePath = PrivilegedOperationExecutor
        .getContainerExecutorExecutablePath(emptyConf);
    Assert.assertEquals(expectedPath, containerExePath);

    //if NM_LINUX_CONTAINER_EXECUTOR_PATH is set, this must be returned
    expectedPath = customExecutorPath;
    containerExePath = PrivilegedOperationExecutor
        .getContainerExecutorExecutablePath(confWithExecutorPath);
    Assert.assertEquals(expectedPath, containerExePath);
  }

  @Test
  public void testExecutionCommand() {
    PrivilegedOperationExecutor exec = PrivilegedOperationExecutor
        .getInstance(confWithExecutorPath);
    PrivilegedOperation op = new PrivilegedOperation(PrivilegedOperation
        .OperationType.TC_MODIFY_STATE);
    String[] cmdArray = exec.getPrivilegedOperationExecutionCommand(null, op);

    //No arguments added - so the resulting array should consist of
    //1)full path to executor 2) cli switch
    Assert.assertEquals(2, cmdArray.length);
    Assert.assertEquals(customExecutorPath, cmdArray[0]);
    Assert.assertEquals(op.getOperationType().getOption(), cmdArray[1]);

    //other (dummy) arguments to tc modify state
    String[] additionalArgs = { "cmd_file_1", "cmd_file_2", "cmd_file_3"};

    op.appendArgs(additionalArgs);
    cmdArray = exec.getPrivilegedOperationExecutionCommand(null, op);

    //Resulting array should be of length 2 greater than the number of
    //additional arguments added.

    Assert.assertEquals(2 + additionalArgs.length, cmdArray.length);
    Assert.assertEquals(customExecutorPath, cmdArray[0]);
    Assert.assertEquals(op.getOperationType().getOption(), cmdArray[1]);

    //Rest of args should be same as additional args.
    for (int i = 0; i < additionalArgs.length; ++i) {
      Assert.assertEquals(additionalArgs[i], cmdArray[2 + i]);
    }

    //Now test prefix commands
    List<String> prefixCommands = Arrays.asList("nice", "-10");
    cmdArray = exec.getPrivilegedOperationExecutionCommand(prefixCommands, op);
    int prefixLength = prefixCommands.size();
    //Resulting array should be of length of prefix command args + 2 (exec
    // path + switch) + length of additional args.
    Assert.assertEquals(prefixLength + 2 + additionalArgs.length,
        cmdArray.length);

    //Prefix command array comes first
    for (int i = 0; i < prefixLength; ++i) {
      Assert.assertEquals(prefixCommands.get(i), cmdArray[i]);
    }

    //Followed by the container executor path and the cli switch
    Assert.assertEquals(customExecutorPath, cmdArray[prefixLength]);
    Assert.assertEquals(op.getOperationType().getOption(),
        cmdArray[prefixLength + 1]);

    //Followed by the rest of the args
    //Rest of args should be same as additional args.
    for (int i = 0; i < additionalArgs.length; ++i) {
      Assert.assertEquals(additionalArgs[i], cmdArray[prefixLength + 2 + i]);
    }
  }

  @Test
  public void testSquashCGroupOperationsWithInvalidOperations() {
    List<PrivilegedOperation> ops = new ArrayList<>();

    //Ensure that disallowed ops are rejected
    ops.add(opTasksNone);
    ops.add(opDisallowed);

    try {
      PrivilegedOperationExecutor.squashCGroupOperations(ops);
      Assert.fail("Expected squash operation to fail with an exception!");
    } catch (PrivilegedOperationException e) {
      LOG.info("Caught expected exception : " + e);
    }

    //Ensure that invalid strings are rejected
    ops.clear();
    ops.add(opTasksNone);
    ops.add(opTasksInvalid);

    try {
      PrivilegedOperationExecutor.squashCGroupOperations(ops);
      Assert.fail("Expected squash operation to fail with an exception!");
    } catch (PrivilegedOperationException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testSquashCGroupOperationsWithValidOperations() {
    List<PrivilegedOperation> ops = new ArrayList<>();
    //Test squashing, including 'none'
    ops.clear();
    ops.add(opTasks1);
    //this is expected to be ignored
    ops.add(opTasksNone);
    ops.add(opTasks2);
    ops.add(opTasks3);

    try {
      PrivilegedOperation op = PrivilegedOperationExecutor
          .squashCGroupOperations(ops);
      String expected = new StringBuffer
          (PrivilegedOperation.CGROUP_ARG_PREFIX)
          .append(cGroupTasks1).append(PrivilegedOperation
              .LINUX_FILE_PATH_SEPARATOR)
          .append(cGroupTasks2).append(PrivilegedOperation
              .LINUX_FILE_PATH_SEPARATOR)
          .append(cGroupTasks3).toString();

      //We expect axactly one argument
      Assert.assertEquals(1, op.getArguments().size());
      //Squashed list of tasks files
      Assert.assertEquals(expected, op.getArguments().get(0));
    } catch (PrivilegedOperationException e) {
      LOG.info("Caught unexpected exception : " + e);
      Assert.fail("Caught unexpected exception: " + e);
    }
  }
}