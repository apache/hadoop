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

import com.google.common.annotations.VisibleForTesting;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;

/**
 * This is a generic parent class for all the ozone related cli tools.
 */
public class GenericCli implements Callable<Void>, GenericParentCommand {

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
      execute(argv);
    } catch (ExecutionException ex) {
      printError(ex.getCause() == null ? ex : ex.getCause());
      System.exit(-1);
    }
  }

  @VisibleForTesting
  public void execute(String[] argv) {
    cmd.parseWithHandler(new RunLast(), argv);
  }

  protected void printError(Throwable error) {
    //message could be null in case of NPE. This is unexpected so we can
    //print out the stack trace.
    if (verbose || error.getMessage() == null
        || error.getMessage().length() == 0) {
      error.printStackTrace(System.err);
    } else {
      System.err.println(error.getMessage().split("\n")[0]);
    }
    if(error instanceof MissingSubcommandException){
      System.err.println(((MissingSubcommandException) error).getUsage());
    }
  }

  @Override
  public Void call() throws Exception {
    throw new MissingSubcommandException(cmd.getUsageMessage());
  }

  @Override
  public OzoneConfiguration createOzoneConfiguration() {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    if (configurationOverrides != null) {
      for (Entry<String, String> entry : configurationOverrides.entrySet()) {
        ozoneConf.set(entry.getKey(), entry.getValue());
      }
    }
    return ozoneConf;
  }

  @VisibleForTesting
  public picocli.CommandLine getCmd() {
    return cmd;
  }

  @Override
  public boolean isVerbose() {
    return verbose;
  }
}
