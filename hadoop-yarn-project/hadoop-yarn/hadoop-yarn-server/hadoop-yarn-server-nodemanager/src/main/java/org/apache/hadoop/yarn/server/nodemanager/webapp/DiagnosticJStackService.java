/** * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.yarn.server.nodemanager.webapp;


import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Arrays;
import java.util.stream.Stream;

public class DiagnosticJStackService {

  private static final Logger LOG = LoggerFactory.getLogger(DiagnosticJStackService.class);
  private final Context context;

  public DiagnosticJStackService(Context context) {
    this.context = context;
  }

  public String collectNodeThreadDump(int numberOfJStack) throws IOException {
    checkShellNotWindows();

    long nodeManagerPid = ProcessHandle.current().pid();

    return runJStack(nodeManagerPid, numberOfJStack);
  }

  public String collectApplicationThreadDump(String appId, int numberOfJStack) throws IOException {
    checkShellNotWindows();

    ApplicationId applicationId = ApplicationId.fromString(appId);
    List<Long> applicationPids = getApplicationPids(applicationId);

    return runJStack(applicationPids, numberOfJStack);
  }

  private void checkShellNotWindows() {
    if (Shell.WINDOWS) {
      throw new UnsupportedOperationException("Not implemented for Windows.");
    }
  }

  protected List<Long> getApplicationPids(ApplicationId appId){
    List<Long> pids = new ArrayList<>();

    Application app = context.getApplications().get(appId);
    if (app == null){
      throw new YarnRuntimeException("Application " + appId + " does not exist");
    }

    for (ContainerId containerId : app.getContainers().keySet()){
      String pidForContainerIdStr = context.getContainerExecutor().getProcessId(containerId);
      long pidForContainerId = Long.parseLong(pidForContainerIdStr);

      ProcessHandle.of(pidForContainerId).ifPresent(handle ->
        handle.descendants() // Get only the java processId of containerId's children
          .filter(childProcess -> childProcess.info().command().orElse("").contains("java"))
          .map(ProcessHandle::pid)
          .forEach(pids::add)
      );

    }

    LOG.info("Application PIDs: {}", pids);

    return pids;
  }

  private String runJStack(List<Long> pids, int numJStacks) throws IOException {
    StringBuilder result = new StringBuilder();

    for(Long pid : pids){
      result.append(runJStack(pid, numJStacks));
    }

    return result.toString();
  }

  private String runJStack(long pid, int numJStacks) throws IOException {
    Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);

    if (processHandle.isEmpty()){
      throw new IOException("Process with PID " + pid + " is no longer exists");
    }

    String nmUser = System.getProperty("user.name");

    String processOwner = processHandle.get().info().user().orElse(nmUser);
    Configuration conf = context.getConf();

    String yarnHomeEnvVar = System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
    File hadoopBin = new File(yarnHomeEnvVar, "bin");
    String defaultPath = new File(hadoopBin, "container-executor").getAbsolutePath();
    String containerExecutorPath = conf.get(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, defaultPath);

    String javaHome = System.getProperty("java.home");
    String jstackPath = javaHome + "/bin/jstack";
    String[] jstackCommand = {
            containerExecutorPath, "--run-jstack", processOwner, String.valueOf(pid), jstackPath
    };

    LOG.info("Running JStack command: {}", Arrays.toString(jstackCommand));

    Shell.ShellCommandExecutor cmd =
      new Shell.ShellCommandExecutor(jstackCommand, null, null, 60_000);

    StringBuilder result = new StringBuilder();

    for (int i = 0; i < numJStacks; i++) {
      cmd.execute();
      result.append(String.format(
        "--- JStack iteration %d for PID: %d ---%n%s%n", i, pid, cmd.getOutput()));
    }

    return result.toString();
  }


}
