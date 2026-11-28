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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/**
 * Collects thread dumps of the ResourceManager JVM via REST diagnostics.
 */
public class RMDiagnosticJStackService {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMDiagnosticJStackService.class);

  private static final String JSTACK_PATH =
      System.getProperty("java.home") + "/bin/jstack";

  private final ResourceManager rm;

  public RMDiagnosticJStackService(ResourceManager rm) {
    this.rm = rm;
  }

  public String collectResourceManagerThreadDump(int numberOfJStack,
      HttpServletRequest req) throws IOException {
    checkShellNotWindows();
    checkAdminACL(req);
    long rmPid = ProcessHandle.current().pid();
    return runJStack(rmPid, numberOfJStack);
  }

  private void checkAdminACL(HttpServletRequest req) throws IOException {
    UserGroupInformation callerUGI = getUserGroupInformation(req);
    boolean isAdmin = rm.getApplicationACLsManager().isAdmin(callerUGI);
    if (!isAdmin) {
      throw new YarnRuntimeException("User " + callerUGI.getShortUserName()
          + " is not authorized to run jstack on ResourceManager ");
    }
  }

  private void checkShellNotWindows() {
    if (Shell.WINDOWS) {
      throw new UnsupportedOperationException("Not implemented for Windows.");
    }
  }

  private String runJStack(long pid, int numJStacks) {
    Optional<ProcessHandle> processHandleOpt = ProcessHandle.of(pid);
    if (processHandleOpt.isEmpty()) {
      String msg = "Process with PID " + pid + " is no longer exists";
      LOG.warn(msg);
      return "Status: Skipped Process with PID " + msg;
    }

    String[] jstackCommand = {JSTACK_PATH, String.valueOf(pid)};
    LOG.info("Running JStack command: {}", Arrays.toString(jstackCommand));

    StringBuilder result = new StringBuilder();
    for (int i = 0; i < numJStacks; i++) {
      Shell.ShellCommandExecutor cmd =
          new Shell.ShellCommandExecutor(jstackCommand, null, null, 60_000);
      try {
        cmd.execute();
        result.append(String.format(
            "--- JStack iteration %d for PID: %d ---%n%s%n", i, pid,
            cmd.getOutput()));
      } catch (IOException e) {
        result.append(String.format(
            "Failed to run jstack on PID: %d at iteration: %d "
                + "(Process likely exited before/during running jstack): %s",
            pid, i, e.getMessage()));
        break;
      }
    }
    return result.toString();
  }

  private UserGroupInformation getUserGroupInformation(HttpServletRequest req)
      throws IOException {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    } else {
      callerUGI = UserGroupInformation.getCurrentUser();
    }
    LOG.info("Checking ACL for Caller UGI: {}", callerUGI);
    return callerUGI;
  }
}
