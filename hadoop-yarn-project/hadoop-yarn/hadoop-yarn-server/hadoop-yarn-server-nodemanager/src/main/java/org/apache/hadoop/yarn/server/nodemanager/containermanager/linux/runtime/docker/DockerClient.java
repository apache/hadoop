/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DockerClient {
  private static final Logger LOG =
       LoggerFactory.getLogger(DockerClient.class);
  private static final String TMP_FILE_PREFIX = "docker.";
  private static final String TMP_FILE_SUFFIX = ".cmd";
  private static final String TMP_ENV_FILE_SUFFIX = ".env";

  private String writeEnvFile(DockerRunCommand cmd, String filePrefix,
      File cmdDir) throws IOException {
    File dockerEnvFile = File.createTempFile(TMP_FILE_PREFIX + filePrefix,
        TMP_ENV_FILE_SUFFIX, cmdDir);
    try (
        Writer envWriter = new OutputStreamWriter(
            new FileOutputStream(dockerEnvFile), "UTF-8");
        PrintWriter envPrintWriter = new PrintWriter(envWriter);
    ) {
      for (Map.Entry<String, String> entry : cmd.getEnv()
          .entrySet()) {
        envPrintWriter.println(entry.getKey() + "=" + entry.getValue());
      }
      return dockerEnvFile.getAbsolutePath();
    }
  }

  public String writeCommandToTempFile(DockerCommand cmd,
      ContainerId containerId, Context nmContext)
      throws ContainerExecutionException {
    String filePrefix = containerId.toString();
    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    File dockerCommandFile;
    File cmdDir = null;

    if(nmContext == null || nmContext.getLocalDirsHandler() == null) {
      throw new ContainerExecutionException(
          "Unable to write temporary docker command");
    }

    try {
      String cmdDirPath = nmContext.getLocalDirsHandler().getLocalPathForWrite(
          ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR +
          appId + Path.SEPARATOR + filePrefix + Path.SEPARATOR).toString();
      cmdDir = new File(cmdDirPath);
      if (!cmdDir.mkdirs() && !cmdDir.exists()) {
        throw new IOException("Cannot create container private directory "
            + cmdDir);
      }
      dockerCommandFile = File.createTempFile(TMP_FILE_PREFIX + filePrefix,
          TMP_FILE_SUFFIX, cmdDir);
      try (
        Writer writer = new OutputStreamWriter(
            new FileOutputStream(dockerCommandFile.toString()), "UTF-8");
        PrintWriter printWriter = new PrintWriter(writer);
      ) {
        printWriter.println("[docker-command-execution]");
        for (Map.Entry<String, List<String>> entry :
            cmd.getDockerCommandWithArguments().entrySet()) {
          if (entry.getKey().contains("=")) {
            throw new ContainerExecutionException(
                "'=' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + entry.getValue());
          }
          String value = StringUtils.join(",", entry.getValue());
          if (value.contains("\n")) {
            throw new ContainerExecutionException(
                "'\\n' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + value);
          }
          printWriter.println("  " + entry.getKey() + "=" + value);
        }
        if (cmd instanceof DockerRunCommand) {
          DockerRunCommand runCommand = (DockerRunCommand) cmd;
          if (runCommand.containsEnv()) {
            String path = writeEnvFile(runCommand, filePrefix, cmdDir);
            printWriter.println("  environ=" + path);
          }
        }
        return dockerCommandFile.toString();
      }
    } catch (IOException e) {
      LOG.warn("Unable to write docker command to " + cmdDir);
      throw new ContainerExecutionException(e);
    }
  }
}
