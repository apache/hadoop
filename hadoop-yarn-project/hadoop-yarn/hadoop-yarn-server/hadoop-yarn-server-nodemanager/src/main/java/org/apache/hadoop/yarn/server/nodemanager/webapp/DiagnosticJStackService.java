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


import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Arrays;

public class DiagnosticJStackService {

    private final Context context;
    private static final Logger LOG = LoggerFactory
            .getLogger(DiagnosticJStackService.class);

    public DiagnosticJStackService(Context context) {
        this.context = context;
    }


    public static String collectNodeThreadDump(int numberOfJStack)
            throws IOException {
        if (Shell.WINDOWS) {
            throw new UnsupportedOperationException("Not implemented for Windows");
        }

        long nodeManagerPid = getNodeManagerPid();

        return runJStack(nodeManagerPid, numberOfJStack);

    }

    public String collectApplicationThreadDump(String appId, int numberOfJStack)
            throws IOException {
        if(!appId.matches("application_\\d{13}_\\d{4}")) {
            throw new RuntimeException("Invalid application id: " + appId);
        }

        if (Shell.WINDOWS) {
            throw new UnsupportedOperationException("Not implemented for Windows.");
        }

        List<Long> applicationPids = getApplicationPids(appId);

        return runJStack(applicationPids, numberOfJStack);
    }


    public static long getNodeManagerPid() {
        return ProcessHandle.current().pid();
    }

    public List<Long> getApplicationPids(String appId){
        List<Long> pids = new ArrayList<>();

        ApplicationId appIdObj = ApplicationId.fromString(appId);
        Application app = context.getApplications().get(appIdObj);
        if (app != null) {
            Map<ContainerId, Container> containers = app.getContainers();
            for (ContainerId containerId : containers.keySet()){
                long pidForContainerId = Long.parseLong(context.getContainerExecutor().getProcessId(containerId));

                ProcessHandle.of(pidForContainerId).ifPresent(parentProcess ->
                        parentProcess.descendants()
                                .filter(childProcess ->
                                        childProcess.info().command().orElse("").contains("java"))
                                .map(ProcessHandle::pid)
                                .forEach(pids::add)
                );

            }
        }

        return pids;

    }


    public static String runJStack(List<Long> pids, int numJStacks) throws IOException {
        StringBuilder result = new StringBuilder();

        for(Long pid : pids){
            result.append(runJStack(pid, numJStacks));
        }

        return result.toString();
    }

    public static String runJStack(long pid, int numJStacks) throws IOException {
        Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);

        if (processHandle.isEmpty()){
            throw new IOException("Process with PID " + pid + " is no longer exists");
        }

        String processOwner = processHandle.get().info().user().orElse("root");
        String[] jstackCommand = {"sudo", "-u", processOwner, "jstack", String.valueOf(pid)};

        LOG.info("Running JStack command: {}", Arrays.toString(jstackCommand));

        Shell.ShellCommandExecutor cmd =
                new Shell.ShellCommandExecutor(jstackCommand, null, null, 60_000);

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < numJStacks; i++) {
            cmd.execute();
            result.append(String.format(
                    "--- JStack iteration %d for PID: %d ---\n%s\n", i, pid, cmd.getOutput()
            ));
        }

        return result.toString();
    }


}
