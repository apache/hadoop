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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DiagnosticJStackService {

    private static final Logger LOG = LoggerFactory
            .getLogger(DiagnosticJStackService.class);
    private static final String PYTHON_COMMAND = "python3";
    private static String scriptLocation = null;

    static {
        try {
            // Extract script from JAR to a temp file
            InputStream in = DiagnosticJStackService.class.getClassLoader()
                    .getResourceAsStream("diagnostics/jstack_collector.py");
            File tempScript = File.createTempFile("jstack_collector", ".py");
            Files.copy(in, tempScript.toPath(), StandardCopyOption.REPLACE_EXISTING);
            tempScript.setExecutable(true); // Set execute permission
            scriptLocation = tempScript.getAbsolutePath();
        } catch (IOException e) {
            LOG.error("Failed to extract Python script from JAR", e);
        }
    }

    public static String collectNodeThreadDump(String numberOfJStack)
            throws Exception {
        if (Shell.WINDOWS) {
            throw new UnsupportedOperationException("Not implemented for Windows");
        }

        ProcessBuilder pb = createProcessBuilder(numberOfJStack);

        return executeCommand(pb);

    }



    public static String collectApplicationThreadDump(String appId, String numberOfJStack)
            throws Exception {
        if (Shell.WINDOWS) {
            throw new UnsupportedOperationException("Not implemented for Windows.");
        }
        ProcessBuilder pb = createProcessBuilder(appId, numberOfJStack);

        LOG.info("Diagnostic process environment: {}", pb.environment());

        return executeCommand(pb);
    }

    protected static ProcessBuilder createProcessBuilder(String numberOfJStack) {
        List<String> commandList =
                new ArrayList<>(Arrays.asList(PYTHON_COMMAND, scriptLocation, numberOfJStack));

        return new ProcessBuilder(commandList);
    }


    protected static ProcessBuilder createProcessBuilder(String appId, String numberOfJStack) {
        List<String> commandList =
                new ArrayList<>(Arrays.asList(PYTHON_COMMAND, scriptLocation, appId, numberOfJStack));

        return new ProcessBuilder(commandList);
    }

    private static String executeCommand(ProcessBuilder pb)
            throws Exception {
        Process process = pb.start();
        int exitCode;
        StringBuilder outputBuilder = new StringBuilder();
        StringBuilder errorBuilder = new StringBuilder();

        try (
                BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream(),
                        StandardCharsets.UTF_8));
                BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream(),
                        StandardCharsets.UTF_8));
        ) {

            String line;
            while ((line = stdoutReader.readLine()) != null) {
                outputBuilder.append(line).append("\n");
            }

            while ((line = stderrReader.readLine()) != null) {
                errorBuilder.append(line).append("\n");
            }
            if (!errorBuilder.toString().isEmpty()) {
                LOG.error("Python script stderr: {}", errorBuilder);
            }

            process.waitFor();
        } catch (Exception e) {
            LOG.error("Error getting JStack: {}", pb.command());
            throw e;
        }
        exitCode = process.exitValue();
        if (exitCode != 0) {
            throw new IOException("The JStack collector script exited with non-zero " +
                    "exit code: " + exitCode);
        }

        return outputBuilder.toString();
    }

}
