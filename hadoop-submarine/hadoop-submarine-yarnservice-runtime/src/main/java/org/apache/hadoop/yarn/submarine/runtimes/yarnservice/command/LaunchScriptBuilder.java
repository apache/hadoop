/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command;

import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class is a builder to conveniently create launch scripts.
 * All dependencies are provided with the constructor except
 * the launch command.
 */
public class LaunchScriptBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(
      LaunchScriptBuilder.class);

  private final File file;
  private final HadoopEnvironmentSetup hadoopEnvSetup;
  private final RunJobParameters parameters;
  private final Component component;
  private final OutputStreamWriter writer;
  private final StringBuilder scriptBuffer;
  private String launchCommand;

  LaunchScriptBuilder(String launchScriptPrefix,
      HadoopEnvironmentSetup hadoopEnvSetup, RunJobParameters parameters,
      Component component) throws IOException {
    this.file = File.createTempFile(launchScriptPrefix +
        "-launch-script", ".sh");
    this.hadoopEnvSetup = hadoopEnvSetup;
    this.parameters = parameters;
    this.component = component;
    this.writer = new OutputStreamWriter(new FileOutputStream(file), UTF_8);
    this.scriptBuffer = new StringBuilder();
  }

  public void append(String s) {
    scriptBuffer.append(s);
  }

  public LaunchScriptBuilder withLaunchCommand(String command) {
    this.launchCommand = command;
    return this;
  }

  public String build() throws IOException {
    if (launchCommand != null) {
      append(launchCommand);
    } else {
      LOG.warn("LaunchScript object was null!");
      if (LOG.isDebugEnabled()) {
        LOG.debug("LaunchScript's Builder object: {}", this);
      }
    }

    try (PrintWriter pw = new PrintWriter(writer)) {
      writeBashHeader(pw);
      hadoopEnvSetup.addHdfsClassPath(parameters, pw, component);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Appending command to launch script: {}", scriptBuffer);
      }
      pw.append(scriptBuffer);
    }
    return file.getAbsolutePath();
  }

  @Override
  public String toString() {
    return "LaunchScriptBuilder{" +
        "file=" + file +
        ", hadoopEnvSetup=" + hadoopEnvSetup +
        ", parameters=" + parameters +
        ", component=" + component +
        ", writer=" + writer +
        ", scriptBuffer=" + scriptBuffer +
        ", launchCommand='" + launchCommand + '\'' +
        '}';
  }

  private void writeBashHeader(PrintWriter pw) {
    pw.append("#!/bin/bash\n");
  }
}
