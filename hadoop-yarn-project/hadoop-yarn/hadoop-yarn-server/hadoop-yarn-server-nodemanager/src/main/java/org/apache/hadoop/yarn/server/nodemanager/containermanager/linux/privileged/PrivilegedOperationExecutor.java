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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * provides mechanisms to execute PrivilegedContainerOperations *
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrivilegedOperationExecutor {
  private static final Log LOG = LogFactory.getLog(PrivilegedOperationExecutor
      .class);
  private volatile static PrivilegedOperationExecutor instance;

  private String containerExecutorExe;

  public static String getContainerExecutorExecutablePath(Configuration conf) {
    String yarnHomeEnvVar =
        System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
    File hadoopBin = new File(yarnHomeEnvVar, "bin");
    String defaultPath =
        new File(hadoopBin, "container-executor").getAbsolutePath();
    return null == conf
        ? defaultPath
        : conf.get(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH,
        defaultPath);
  }

  private void init(Configuration conf) {
    containerExecutorExe = getContainerExecutorExecutablePath(conf);
  }

  private PrivilegedOperationExecutor(Configuration conf) {
    init(conf);
  }

  public static PrivilegedOperationExecutor getInstance(Configuration conf) {
    if (instance == null) {
      synchronized (PrivilegedOperationExecutor.class) {
        if (instance == null) {
          instance = new PrivilegedOperationExecutor(conf);
        }
      }
    }

    return instance;
  }

  /**
   * @param prefixCommands in some cases ( e.g priorities using nice ),
   *                       prefix commands are necessary
   * @param operation      the type and arguments for the operation to be
   *                       executed
   * @return execution string array for priviledged operation
   */

  public String[] getPrivilegedOperationExecutionCommand(List<String>
      prefixCommands,
      PrivilegedOperation operation) {
    List<String> fullCommand = new ArrayList<String>();

    if (prefixCommands != null && !prefixCommands.isEmpty()) {
      fullCommand.addAll(prefixCommands);
    }

    fullCommand.add(containerExecutorExe);
    fullCommand.add(operation.getOperationType().getOption());
    fullCommand.addAll(operation.getArguments());

    String[] fullCommandArray =
        fullCommand.toArray(new String[fullCommand.size()]);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Privileged Execution Command Array: " +
          Arrays.toString(fullCommandArray));
    }

    return fullCommandArray;
  }

  /**
   * Executes a privileged operation. It is up to the callers to ensure that
   * each privileged operation's parameters are constructed correctly. The
   * parameters are passed verbatim to the container-executor binary.
   *
   * @param prefixCommands in some cases ( e.g priorities using nice ),
   *                       prefix commands are necessary
   * @param operation      the type and arguments for the operation to be executed
   * @param workingDir     (optional) working directory for execution
   * @param env            (optional) env of the command will include specified vars
   * @param grabOutput     return (possibly large) shell command output
   * @return stdout contents from shell executor - useful for some privileged
   * operations - e.g --tc_read
   * @throws org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException
   */
  public String executePrivilegedOperation(List<String> prefixCommands,
      PrivilegedOperation operation, File workingDir,
      Map<String, String> env, boolean grabOutput)
      throws PrivilegedOperationException {
    String[] fullCommandArray = getPrivilegedOperationExecutionCommand
        (prefixCommands, operation);
    ShellCommandExecutor exec = new ShellCommandExecutor(fullCommandArray,
        workingDir, env);

    try {
      exec.execute();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Privileged Execution Operation Output:");
        LOG.debug(exec.getOutput());
      }
    } catch (ExitCodeException e) {
      String logLine = new StringBuffer("Shell execution returned exit code: ")
          .append(exec.getExitCode())
          .append(". Privileged Execution Operation Output: ")
          .append(System.lineSeparator()).append(exec.getOutput()).toString();

      LOG.warn(logLine);
      throw new PrivilegedOperationException(e);
    } catch (IOException e) {
      LOG.warn("IOException executing command: ", e);
      throw new PrivilegedOperationException(e);
    }

    if (grabOutput) {
      return exec.getOutput();
    }

    return null;
  }

  /**
   * Executes a privileged operation. It is up to the callers to ensure that
   * each privileged operation's parameters are constructed correctly. The
   * parameters are passed verbatim to the container-executor binary.
   *
   * @param operation  the type and arguments for the operation to be executed
   * @param grabOutput return (possibly large) shell command output
   * @return stdout contents from shell executor - useful for some privileged
   * operations - e.g --tc_read
   * @throws org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException
   */
  public String executePrivilegedOperation(PrivilegedOperation operation,
      boolean grabOutput) throws PrivilegedOperationException {
    return executePrivilegedOperation(null, operation, null, null, grabOutput);
  }

  //Utility functions for squashing together operations in supported ways
  //At some point, we need to create a generalized mechanism that uses a set
  //of squashing 'rules' to squash an set of PrivilegedOperations of varying
  //types - e.g Launch Container + Add Pid to CGroup(s) + TC rules

  /**
   * Squash operations for cgroups - e.g mount, add pid to cgroup etc .,
   * For now, we only implement squashing for 'add pid to cgroup' since this
   * is the only optimization relevant to launching containers
   *
   * @return single squashed cgroup operation. Null on failure.
   */

  public static PrivilegedOperation squashCGroupOperations
  (List<PrivilegedOperation> ops) throws PrivilegedOperationException {
    if (ops.size() == 0) {
      return null;
    }

    StringBuffer finalOpArg = new StringBuffer(PrivilegedOperation
        .CGROUP_ARG_PREFIX);
    boolean noneArgsOnly = true;

    for (PrivilegedOperation op : ops) {
      if (!op.getOperationType()
          .equals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP)) {
        LOG.warn("Unsupported operation type: " + op.getOperationType());
        throw new PrivilegedOperationException("Unsupported operation type:"
            + op.getOperationType());
      }

      List<String> args = op.getArguments();
      if (args.size() != 1) {
        LOG.warn("Invalid number of args: " + args.size());
        throw new PrivilegedOperationException("Invalid number of args: "
            + args.size());
      }

      String arg = args.get(0);
      String tasksFile = StringUtils.substringAfter(arg,
          PrivilegedOperation.CGROUP_ARG_PREFIX);
      if (tasksFile == null || tasksFile.isEmpty()) {
        LOG.warn("Invalid argument: " + arg);
        throw new PrivilegedOperationException("Invalid argument: " + arg);
      }

      if (tasksFile.equals("none")) {
        //Don't append to finalOpArg
        continue;
      }

      if (noneArgsOnly == false) {
        //We have already appended at least one tasks file.
        finalOpArg.append(",");
        finalOpArg.append(tasksFile);
      } else {
        finalOpArg.append(tasksFile);
        noneArgsOnly = false;
      }
    }

    if (noneArgsOnly) {
      finalOpArg.append("none"); //there were no tasks file to append
    }

    PrivilegedOperation finalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP, finalOpArg
        .toString());

    return finalOp;
  }
}