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
package org.apache.hadoop.yarn.client.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class GpgCLI extends Configured implements Tool {

  public GpgCLI() {
    super();
  }

  public GpgCLI(Configuration conf) {
    super(conf);
  }

  private static void printHelp() {
    StringBuilder summary = new StringBuilder();
    summary.append("gpgadmin command line for YARN Federation Policy management.\n");
    StringBuilder helpBuilder = new StringBuilder();
    System.out.println(summary);
    helpBuilder.append(" -help [cmd]: Displays help for the given command or all commands" +
        " if none is specified.");
    System.out.println(helpBuilder);
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
  }

  private static void printUsage() {
    StringBuilder usageBuilder = new StringBuilder();
    buildUsageMsg(usageBuilder);
    System.err.println(usageBuilder);
    ToolRunner.printGenericCommandUsage(System.err);
  }

  private static void buildUsageMsg(StringBuilder builder) {
    builder.append("gpgadmin is only used in Yarn Federation Mode.\n");
    builder.append("Usage: gpgadmin\n");
    builder.append("   -help" + " [cmd]\n");
  }

  @Override
  public int run(String[] args) throws Exception {
    YarnConfiguration yarnConf = getConf() == null ?
        new YarnConfiguration() : new YarnConfiguration(getConf());
    boolean isFederationEnabled = yarnConf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);

    if (args.length < 1 || !isFederationEnabled) {
      printUsage();
      return -1;
    }

    String cmd = args[0];
    if ("-help".equals(cmd)) {
      printHelp();
      return 0;
    }

    return 0;
  }
}
