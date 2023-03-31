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
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;

import java.io.IOException;

public class RouterCLI extends Configured implements Tool {

  public RouterCLI() {
    super();
  }

  public RouterCLI(Configuration conf) {
    super(conf);
  }

  private static void printHelp() {
    StringBuilder summary = new StringBuilder();
    summary.append("router-admin is the command to execute " +
        "YARN Federation administrative commands.\n");
    StringBuilder helpBuilder = new StringBuilder();
    System.out.println(summary);
    helpBuilder.append("   -help [cmd]: Displays help for the given command or all commands" +
        " if none is specified.");
    System.out.println(helpBuilder);
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
  }

  protected ResourceManagerAdministrationProtocol createAdminProtocol()
      throws IOException {
    // Get the current configuration
    final YarnConfiguration conf = new YarnConfiguration(getConf());
    return ClientRMProxy.createRMProxy(conf, ResourceManagerAdministrationProtocol.class);
  }

  private static void buildUsageMsg(StringBuilder builder) {
    builder.append("router-admin is only used in Yarn Federation Mode.\n");
    builder.append("Usage: router-admin\n");
    builder.append("   -help" + " [cmd]\n");
  }

  private static void printUsage() {
    StringBuilder usageBuilder = new StringBuilder();
    buildUsageMsg(usageBuilder);
    System.err.println(usageBuilder);
    ToolRunner.printGenericCommandUsage(System.err);
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
