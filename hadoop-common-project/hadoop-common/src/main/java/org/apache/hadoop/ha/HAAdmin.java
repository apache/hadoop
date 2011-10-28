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
package org.apache.hadoop.ha;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.ImmutableMap;

/**
 * A command-line tool for making calls in the HAServiceProtocol.
 * For example,. this can be used to force a daemon to standby or active
 * mode, or to trigger a health-check.
 */
@InterfaceAudience.Private
public class HAAdmin extends Configured implements Tool {
  
  private static Map<String, UsageInfo> USAGE =
    ImmutableMap.<String, UsageInfo>builder()
    .put("-transitionToActive",
        new UsageInfo("<host:port>", "Transitions the daemon into Active state"))
    .put("-transitionToStandby",
        new UsageInfo("<host:port>", "Transitions the daemon into Passive state"))
    .put("-checkHealth",
        new UsageInfo("<host:port>",
            "Requests that the daemon perform a health check.\n" + 
            "The HAAdmin tool will exit with a non-zero exit code\n" +
            "if the check fails."))
    .put("-help",
        new UsageInfo("<command>", "Displays help on the specified command"))
    .build();

  /** Output stream for errors, for use in tests */
  PrintStream errOut = System.err;
  PrintStream out = System.out;

  private static void printUsage(PrintStream errOut) {
    errOut.println("Usage: java HAAdmin");
    for (Map.Entry<String, UsageInfo> e : USAGE.entrySet()) {
      String cmd = e.getKey();
      UsageInfo usage = e.getValue();
      
      errOut.println("    [" + cmd + " " + usage.args + "]"); 
    }
    errOut.println();
    ToolRunner.printGenericCommandUsage(errOut);    
  }
  
  private static void printUsage(PrintStream errOut, String cmd) {
    UsageInfo usage = USAGE.get(cmd);
    if (usage == null) {
      throw new RuntimeException("No usage for cmd " + cmd);
    }
    errOut.println("Usage: java HAAdmin [" + cmd + " " + usage.args + "]");
  }

  private int transitionToActive(final String[] argv)
      throws IOException, ServiceFailedException {
    if (argv.length != 2) {
      errOut.println("transitionToActive: incorrect number of arguments");
      printUsage(errOut, "-transitionToActive");
      return -1;
    }
    
    HAServiceProtocol proto = getProtocol(argv[1]);
    proto.transitionToActive();
    return 0;
  }

  
  private int transitionToStandby(final String[] argv)
      throws IOException, ServiceFailedException {
    if (argv.length != 2) {
      errOut.println("transitionToStandby: incorrect number of arguments");
      printUsage(errOut, "-transitionToStandby");
      return -1;
    }
    
    HAServiceProtocol proto = getProtocol(argv[1]);
    proto.transitionToStandby();
    return 0;
  }
  
  private int checkHealth(final String[] argv)
      throws IOException, ServiceFailedException {
    if (argv.length != 2) {
      errOut.println("checkHealth: incorrect number of arguments");
      printUsage(errOut, "-checkHealth");
      return -1;
    }
    
    HAServiceProtocol proto = getProtocol(argv[1]);
    try {
      proto.monitorHealth();
    } catch (HealthCheckFailedException e) {
      errOut.println("Health check failed: " + e.getLocalizedMessage());
      return 1;
    }
    return 0;
  }

  /**
   * Return a proxy to the specified target host:port.
   */
  protected HAServiceProtocol getProtocol(String target)
      throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(target);
    return (HAServiceProtocol)RPC.getProxy(
          HAServiceProtocol.class, HAServiceProtocol.versionID,
          addr, getConf());
  }

      
  @Override
  public int run(String[] argv) throws Exception {
    if (argv.length < 1) {
      printUsage(errOut);
      return -1;
    }

    int i = 0;
    String cmd = argv[i++];

    if (!cmd.startsWith("-")) {
      errOut.println("Bad command '" + cmd + "': expected command starting with '-'");
      printUsage(errOut);
      return -1;
    }
    
    if ("-transitionToActive".equals(cmd)) {
      return transitionToActive(argv);
    } else if ("-transitionToStandby".equals(cmd)) {
      return transitionToStandby(argv);
    } else if ("-checkHealth".equals(cmd)) {
      return checkHealth(argv);
    } else if ("-help".equals(cmd)) {
      return help(argv);
    } else {
      errOut.println(cmd.substring(1) + ": Unknown command");
      printUsage(errOut);
      return -1;
    } 
  }
  
  private int help(String[] argv) {
    if (argv.length != 2) {
      printUsage(errOut, "-help");
      return -1;
    }
    String cmd = argv[1];
    if (!cmd.startsWith("-")) {
      cmd = "-" + cmd;
    }
    UsageInfo usageInfo = USAGE.get(cmd);
    if (usageInfo == null) {
      errOut.println(cmd + ": Unknown command");
      printUsage(errOut);
      return -1;
    }
    
    errOut .println(cmd + " [" + usageInfo.args + "]: " + usageInfo.help);
    return 1;
  }

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new HAAdmin(), argv);
    System.exit(res);
  }
  
  
  private static class UsageInfo {
    private final String args;
    private final String help;
    
    public UsageInfo(String args, String help) {
      this.args = args;
      this.help = help;
    }
  }
}
