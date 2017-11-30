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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
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
  private final String tmpDirPath;

  public DockerClient(Configuration conf) throws ContainerExecutionException {

    String tmpDirBase = conf.get("hadoop.tmp.dir");
    if (tmpDirBase == null) {
      throw new ContainerExecutionException("hadoop.tmp.dir not set!");
    }
    tmpDirPath = tmpDirBase + "/nm-docker-cmds";

    File tmpDir = new File(tmpDirPath);
    if (!(tmpDir.exists() || tmpDir.mkdirs())) {
      LOG.warn("Unable to create directory: " + tmpDirPath);
      throw new ContainerExecutionException("Unable to create directory: " +
          tmpDirPath);
    }
  }

  public String writeCommandToTempFile(DockerCommand cmd, String filePrefix)
      throws ContainerExecutionException {
    File dockerCommandFile = null;
    try {
      dockerCommandFile = File.createTempFile(TMP_FILE_PREFIX + filePrefix,
          TMP_FILE_SUFFIX, new
          File(tmpDirPath));

      Writer writer = new OutputStreamWriter(
          new FileOutputStream(dockerCommandFile), "UTF-8");
      PrintWriter printWriter = new PrintWriter(writer);
      printWriter.println("[docker-command-execution]");
      for (Map.Entry<String, List<String>> entry :
          cmd.getDockerCommandWithArguments().entrySet()) {
        if (entry.getKey().contains("=")) {
          throw new ContainerExecutionException(
              "'=' found in entry for docker command file, key = " + entry
                  .getKey() + "; value = " + entry.getValue());
        }
        if (entry.getValue().contains("\n")) {
          throw new ContainerExecutionException(
              "'\\n' found in entry for docker command file, key = " + entry
                  .getKey() + "; value = " + entry.getValue());
        }
        printWriter.println("  " + entry.getKey() + "=" + StringUtils
            .join(",", entry.getValue()));
      }
      printWriter.close();

      return dockerCommandFile.getAbsolutePath();
    } catch (IOException e) {
      LOG.warn("Unable to write docker command to temporary file!");
      throw new ContainerExecutionException(e);
    }
  }
}
