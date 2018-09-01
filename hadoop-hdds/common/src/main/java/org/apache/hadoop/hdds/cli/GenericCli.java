/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.RunLast;

/**
 * This is a generic parent class for all the ozone related cli tools.
 */
public class GenericCli implements Callable<Void> {

  @Option(names = {"--verbose"},
      description = "More verbose output. Show the stack trace of the errors.")
  private boolean verbose;

  @Option(names = {"-D", "--set"})
  private Map<String, String> configurationOverrides = new HashMap<>();

  private final CommandLine cmd;

  public GenericCli() {
    cmd = new CommandLine(this);
  }

  public void run(String[] argv) {
    try {
      cmd.parseWithHandler(new RunLast(), argv);
    } catch (ExecutionException ex) {
      printError(ex.getCause());
      System.exit(-1);
    }
  }

  private void printError(Throwable error) {
    if (verbose) {
      error.printStackTrace(System.err);
    } else {
      System.err.println(error.getMessage().split("\n")[0]);
    }
  }

  @Override
  public Void call() throws Exception {
    throw new ParameterException(cmd, "Please choose a subcommand");
  }

  public OzoneConfiguration createOzoneConfiguration() {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    if (configurationOverrides != null) {
      for (Entry<String, String> entry : configurationOverrides.entrySet()) {
        ozoneConf
            .set(entry.getKey(), configurationOverrides.get(entry.getValue()));
      }
    }
    return ozoneConf;
  }
}
