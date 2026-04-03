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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Optional;
import java.util.Arrays;

public class DiagnosticJStackService {

  private static final Logger LOG = LoggerFactory.getLogger(DiagnosticJStackService.class);

  private static final String NM_USER = System.getProperty("user.name");
  private static final String JSTACK_PATH = System.getProperty("java.home") + "/bin/jstack";
  private final Context context;
  private final Configuration conf;

  public DiagnosticJStackService(Context context) {
    this.context = context;
    this.conf = context.getConf();
  }

  public String collectNodeThreadDump(int numberOfJStack, HttpServletRequest req)
      throws IOException {
    checkShellNotWindows();

    long nodeManagerPid = ProcessHandle.current().pid();

    checkAdminACL(req);

    return runJStack(nodeManagerPid, numberOfJStack);
  }

  private void checkAdminACL(HttpServletRequest req) throws IOException {
    UserGroupInformation callerUGI = getUserGroupInformation(req);

    boolean isAdmin = context.getApplicationACLsManager().isAdmin(callerUGI);

    if (!isAdmin) {
      throw new YarnRuntimeException("User " + callerUGI.getShortUserName() +
              " is not authorized to run jstack on NodeManager ");
    }
  }

  public String collectApplicationThreadDump(
      String appId, int numberOfJStack, HttpServletRequest req)
      throws IOException {
    checkShellNotWindows();

    ApplicationId applicationId = ApplicationId.fromString(appId);

    Application app = context.getApplications().get(applicationId);
    if (app == null){
      throw new YarnRuntimeException("Application " + applicationId + " does not exist");
    }

    checkApplicationACL(req, app);

    Map<ContainerId, List<Long>> containerPids = getApplicationContainerPids(app);

    return runJStack(containerPids, numberOfJStack);
  }

  private void checkApplicationACL(HttpServletRequest req, Application app) throws IOException {
    UserGroupInformation callerUGI = getUserGroupInformation(req);

    boolean isAuthorized = context.getApplicationACLsManager().checkAccess(
        callerUGI, ApplicationAccessType.VIEW_APP, app.getUser(), app.getAppId()
    );

    if(!isAuthorized){
      throw new YarnRuntimeException("User " + callerUGI.getShortUserName() +
        " is not authorized to view application " + app.getAppId());
    }

  }

  private void checkShellNotWindows() {
    if (Shell.WINDOWS) {
      throw new UnsupportedOperationException("Not implemented for Windows.");
    }
  }

  protected Map<ContainerId, List<Long>> getApplicationContainerPids(Application app){
    Map<ContainerId, List<Long>> containerPids = new HashMap<>();

    for (ContainerId containerId : app.getContainers().keySet()){
      String pidForContainerIdStr = context.getContainerExecutor().getProcessId(containerId);
      long parentPid = Long.parseLong(pidForContainerIdStr);

      List<Long> javaContainerPids = ProcessHandle.of(parentPid).stream()
          .flatMap(ProcessHandle::descendants)
          .filter(childProcess -> {
            String cmdLine = childProcess.info().commandLine().orElse("").trim();
            // Command Line: /usr/lib/jvm/jdk1.17.0.11.0-openjdk/bin/java
            // -Djava.net.preferIPv4Stack=true
            if (cmdLine.isEmpty()){
              return false;
            }

            String executable = cmdLine.split("\\s+")[0];
            // The first token is always the executable binary
            return executable.equals("java") || executable.endsWith("/java");
          })
          .map(ProcessHandle::pid)
          .toList();

      containerPids.put(containerId, javaContainerPids);

    }

    LOG.info("Application PIDs by ContainerId: {}", containerPids);

    return containerPids;
  }

  private String runJStack(Map<ContainerId, List<Long>> containerPids, int numJStacks){
    StringBuilder result = new StringBuilder();

    for(Map.Entry<ContainerId, List<Long>> entry : containerPids.entrySet()){
      ContainerId containerId = entry.getKey();
      List<Long> javaContainerPids = entry.getValue();

      if (javaContainerPids.isEmpty()){
        result.append(String.format("=== Thread Dumps for ContainerId: %s%n is skipped " +
            "because no Java Process ID exist ===", containerId.toString()));
      } else {
        for (Long pid : javaContainerPids) {
          result.append(String.format(
              "=== Thread Dumps for ContainerId: %s, PID: %d ===%n%s%n",
              containerId.toString(), pid, runJStack(pid, numJStacks)));
        }
      }

    }

    return result.toString();
  }

  private String runJStack(long pid, int numJStacks) {
    Optional<ProcessHandle> processHandleOpt = ProcessHandle.of(pid);

    if (processHandleOpt.isEmpty()){
      String msg = String.format("Process with PID " + pid + " is no longer exists");
      LOG.warn(msg);
      return "Status: Skipped Process with PID " + msg;
    }

    ProcessHandle processHandle = processHandleOpt.get();

    String runningUser = processHandle.info().user().orElse(NM_USER);
    String containerExecutorPath =
        PrivilegedOperationExecutor.getContainerExecutorExecutablePath(conf);

    String[] jstackCommand = {
        containerExecutorPath, "--run-jstack", runningUser, String.valueOf(pid), JSTACK_PATH
    };

    LOG.info("Running JStack command: {}", Arrays.toString(jstackCommand));

    StringBuilder result = new StringBuilder();

    for (int i = 0; i < numJStacks; i++) {
      Shell.ShellCommandExecutor cmd =
          new Shell.ShellCommandExecutor(jstackCommand, null, null, 60_000);

      try {
        cmd.execute();
        result.append(String.format(
            "--- JStack iteration %d for PID: %d ---%n%s%n", i, pid, cmd.getOutput()));
      } catch (IOException e) {
        result.append(String.format(
            "Failed to run jstack on PID: " + pid + " at iteration: " + i +
            " (Process likely exited before/during running jstack): " + e.getMessage()));
        break;
      }
    }

    return result.toString();
  }

  private UserGroupInformation getUserGroupInformation(HttpServletRequest req) throws IOException {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI;

    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    } else {
      callerUGI = UserGroupInformation.getCurrentUser(); // Fallback to current OS user
    }

    LOG.info("Checking ACL for Caller UGI: {}", callerUGI.toString());

    return callerUGI;

  }

}
