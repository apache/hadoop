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
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.ImmutableMap;

/**
 * A command-line tool for making calls in the HAServiceProtocol.
 * For example,. this can be used to force a service to standby or active
 * mode, or to trigger a health-check.
 */
@InterfaceAudience.Private

public abstract class HAAdmin extends Configured implements Tool {
  
  private static final String FORCEFENCE  = "forcefence";
  private static final String FORCEACTIVE = "forceactive";
  private static final Log LOG = LogFactory.getLog(HAAdmin.class);

  private int rpcTimeoutForChecks = -1;
  
  private static Map<String, UsageInfo> USAGE =
    ImmutableMap.<String, UsageInfo>builder()
    .put("-transitionToActive",
        new UsageInfo("<serviceId>", "Transitions the service into Active state"))
    .put("-transitionToStandby",
        new UsageInfo("<serviceId>", "Transitions the service into Standby state"))
    .put("-failover",
        new UsageInfo("[--"+FORCEFENCE+"] [--"+FORCEACTIVE+"] <serviceId> <serviceId>",
            "Failover from the first service to the second.\n" +
            "Unconditionally fence services if the "+FORCEFENCE+" option is used.\n" +
            "Try to failover to the target service even if it is not ready if the " + 
            FORCEACTIVE + " option is used."))
    .put("-getServiceState",
        new UsageInfo("<serviceId>", "Returns the state of the service"))
    .put("-checkHealth",
        new UsageInfo("<serviceId>",
            "Requests that the service perform a health check.\n" + 
            "The HAAdmin tool will exit with a non-zero exit code\n" +
            "if the check fails."))
    .put("-help",
        new UsageInfo("<command>", "Displays help on the specified command"))
    .build();

  /** Output stream for errors, for use in tests */
  protected PrintStream errOut = System.err;
  PrintStream out = System.out;

  protected abstract HAServiceTarget resolveTarget(String string);

  protected String getUsageString() {
    return "Usage: HAAdmin";
  }

  protected void printUsage(PrintStream errOut) {
    errOut.println(getUsageString());
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
    errOut.println("Usage: HAAdmin [" + cmd + " " + usage.args + "]");
  }

  private int transitionToActive(final String[] argv)
      throws IOException, ServiceFailedException {
    if (argv.length != 2) {
      errOut.println("transitionToActive: incorrect number of arguments");
      printUsage(errOut, "-transitionToActive");
      return -1;
    }
    
    HAServiceProtocol proto = resolveTarget(argv[1]).getProxy(
        getConf(), 0);
    HAServiceProtocolHelper.transitionToActive(proto);
    return 0;
  }

  private int transitionToStandby(final String[] argv)
      throws IOException, ServiceFailedException {
    if (argv.length != 2) {
      errOut.println("transitionToStandby: incorrect number of arguments");
      printUsage(errOut, "-transitionToStandby");
      return -1;
    }
    
    HAServiceProtocol proto = resolveTarget(argv[1]).getProxy(
        getConf(), 0);
    HAServiceProtocolHelper.transitionToStandby(proto);
    return 0;
  }

  private int failover(final String[] argv)
      throws IOException, ServiceFailedException {
    boolean forceFence = false;
    boolean forceActive = false;

    Options failoverOpts = new Options();
    // "-failover" isn't really an option but we need to add
    // it to appease CommandLineParser
    failoverOpts.addOption("failover", false, "failover");
    failoverOpts.addOption(FORCEFENCE, false, "force fencing");
    failoverOpts.addOption(FORCEACTIVE, false, "force failover");

    CommandLineParser parser = new GnuParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(failoverOpts, argv);
      forceFence = cmd.hasOption(FORCEFENCE);
      forceActive = cmd.hasOption(FORCEACTIVE);
    } catch (ParseException pe) {
      errOut.println("failover: incorrect arguments");
      printUsage(errOut, "-failover");
      return -1;
    }
    
    int numOpts = cmd.getOptions() == null ? 0 : cmd.getOptions().length;
    final String[] args = cmd.getArgs();

    if (numOpts > 2 || args.length != 2) {
      errOut.println("failover: incorrect arguments");
      printUsage(errOut, "-failover");
      return -1;
    }

    HAServiceTarget fromNode = resolveTarget(args[0]);
    HAServiceTarget toNode = resolveTarget(args[1]);
    
    FailoverController fc = new FailoverController(getConf());
    
    try {
      fc.failover(fromNode, toNode, forceFence, forceActive); 
      out.println("Failover from "+args[0]+" to "+args[1]+" successful");
    } catch (FailoverFailedException ffe) {
      errOut.println("Failover failed: " + ffe.getLocalizedMessage());
      return -1;
    }
    return 0;
  }

  private int checkHealth(final String[] argv)
      throws IOException, ServiceFailedException {
    if (argv.length != 2) {
      errOut.println("checkHealth: incorrect number of arguments");
      printUsage(errOut, "-checkHealth");
      return -1;
    }
    
    HAServiceProtocol proto = resolveTarget(argv[1]).getProxy(
        getConf(), rpcTimeoutForChecks);
    try {
      HAServiceProtocolHelper.monitorHealth(proto);
    } catch (HealthCheckFailedException e) {
      errOut.println("Health check failed: " + e.getLocalizedMessage());
      return -1;
    }
    return 0;
  }

  private int getServiceState(final String[] argv)
      throws IOException, ServiceFailedException {
    if (argv.length != 2) {
      errOut.println("getServiceState: incorrect number of arguments");
      printUsage(errOut, "-getServiceState");
      return -1;
    }

    HAServiceProtocol proto = resolveTarget(argv[1]).getProxy(
        getConf(), rpcTimeoutForChecks);
    out.println(proto.getServiceStatus().getState());
    return 0;
  }

  /**
   * Return the serviceId as is, we are assuming it was
   * given as a service address of form <host:ipcport>.
   */
  protected String getServiceAddr(String serviceId) {
    return serviceId;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      rpcTimeoutForChecks = conf.getInt(
          CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
          CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);
    }
  }

  @Override
  public int run(String[] argv) throws Exception {
    try {
      return runCmd(argv);
    } catch (IllegalArgumentException iae) {
      errOut.println("Illegal argument: " + iae.getLocalizedMessage());
      return -1;
    } catch (IOException ioe) {
      errOut.println("Operation failed: " + ioe.getLocalizedMessage());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Operation failed", ioe);
      }
      return -1;
    }
  }
  
  protected int runCmd(String[] argv) throws Exception {
    if (argv.length < 1) {
      printUsage(errOut);
      return -1;
    }

    String cmd = argv[0];

    if (!cmd.startsWith("-")) {
      errOut.println("Bad command '" + cmd + "': expected command starting with '-'");
      printUsage(errOut);
      return -1;
    }

    if ("-transitionToActive".equals(cmd)) {
      return transitionToActive(argv);
    } else if ("-transitionToStandby".equals(cmd)) {
      return transitionToStandby(argv);
    } else if ("-failover".equals(cmd)) {
      return failover(argv);
    } else if ("-getServiceState".equals(cmd)) {
      return getServiceState(argv);
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
    
    errOut.println(cmd + " [" + usageInfo.args + "]: " + usageInfo.help);
    return 0;
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
