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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DiagnosticJStackService {

    private static final Logger LOG = LoggerFactory
            .getLogger(DiagnosticJStackService.class);

    public static String collectNodeThreadDump(int numberOfJStack)
            throws Exception {
        if (Shell.WINDOWS) {
            throw new UnsupportedOperationException("Not implemented for Windows");
        }

        List<String> nodeManagerPids = getNodeManagerPids();

        return runJStack(nodeManagerPids, numberOfJStack);

    }



    public static String collectApplicationThreadDump(String appId, int numberOfJStack)
            throws Exception {
        if (Shell.WINDOWS) {
            throw new UnsupportedOperationException("Not implemented for Windows.");
        }
        List<String> applicationPids = getApplicationPids(appId);

        return runJStack(applicationPids, numberOfJStack);
    }


    public static List<String> getNodeManagerPids() throws IOException {
        Shell.ShellCommandExecutor cmd = new Shell.ShellCommandExecutor(
                new String[]{
                        "bash",
                        "-c",
                        "ps aux | grep nodemanager | grep -v grep"
                },
                null,
                null,
                10_000
        );
        cmd.execute();
        return extractPids(cmd.getOutput());
    }

    public static List<String> getApplicationPids(String appId) throws IOException {
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

            Shell.ShellCommandExecutor cmd =
                    new Shell.ShellCommandExecutor(
                            new String[]{"jstack", pid},
                            null,
                            null,
                            60_000
                    );

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
        }

        return result.toString();
    }

}
