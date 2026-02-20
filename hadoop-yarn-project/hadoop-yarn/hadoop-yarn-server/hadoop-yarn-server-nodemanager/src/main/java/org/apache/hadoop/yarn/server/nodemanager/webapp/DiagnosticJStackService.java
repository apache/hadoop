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


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

        String nodeManagerPid = getNodeManagerPid();

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



        List<String> applicationPids = getApplicationPids(appId);

        return runJStack(applicationPids, numberOfJStack);
    }


    public static String getNodeManagerPid() {
        return String.valueOf(ProcessHandle.current().pid());
    }

    public List<String> getApplicationPids(String appId) throws IOException {
        // List<String> pids = new ArrayList<>();

        ApplicationId appIdObj = ApplicationId.fromString(appId);
        Application app = context.getApplications().get(appIdObj);
        if (app != null) {
            Map<ContainerId, Container> containers = app.getContainers();
            for (ContainerId containerId : containers.keySet()){
                LOG.info("Found container: {}", containerId);
                String pidForContainerId = context.getContainerExecutor().getProcessId(containerId);
                LOG.info("Parent PID for container: {}", pidForContainerId);

                Optional<ProcessHandle> parentProcess = ProcessHandle.of(Long.parseLong(pidForContainerId));
                parentProcess.ifPresent(processHandle -> processHandle.descendants().forEach(
                        childProcess -> {
                            Optional<String> cmd = childProcess.info().command();
                            if (cmd.isPresent() && cmd.get().contains("java")) {
                                LOG.info("Found actual java pid: {}", childProcess.pid());
                            }
                        }
                ));

                // pids.add(pidForContainerId);
            }
        }

        // return pids;

        String psCmd = "ps aux | grep jvm/java | grep " + appId + " | grep -v -e /bin/bash -e grep";

        Shell.ShellCommandExecutor cmd = new Shell.ShellCommandExecutor(
                new String[]{ "bash", "-c", psCmd},
                null,
                null,
                10_000
        );

        cmd.execute();
        return extractPids(cmd.getOutput());

    }

    public static List<String> extractPids(String psOutput) {

        LOG.info("Process output: " + psOutput);

        List<String> pids = new ArrayList<>();
        for(String line : psOutput.split("\n")) {
            // root       414  1.3  1.7 8124480 434520 ?      Sl   11:36
            String [] parts = line.trim().split("\\s+");
            if (parts.length > 1){
                pids.add(parts[1]);
            }
        }

        return pids;
    }


    public static String runJStack(List<String> pids, int numJStacks) throws IOException {
        StringBuilder result = new StringBuilder();

        for(String pid : pids){
            result.append(runJStack(pid, numJStacks));
        }

        return result.toString();
    }

    public static String runJStack(String pid, int numJStacks) throws IOException {
        Optional<ProcessHandle> processHandle = ProcessHandle.of(Long.parseLong(pid));

        if (processHandle.isEmpty()){
            throw new IOException("Process with PID " + pid + " is no longer exists");
        }

        String processOwner = processHandle.get().info().user().orElse("root");


        Shell.ShellCommandExecutor cmd =
                new Shell.ShellCommandExecutor(
                        new String[]{"sudo", "-u", processOwner, "jstack", pid},
                        null,
                        null,
                        60_000
                );

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < numJStacks; i++) {
            cmd.execute();

            result.append("--- JStack iteration -")
                    .append(i)
                    .append(" for PID: ")
                    .append(pid)
                    .append("---\n")
                    .append(cmd.getOutput())
                    .append("\n");
        }


        return result.toString();
    }


}
