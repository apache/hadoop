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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A command-line tool for making calls in the HAServiceProtocol.
 * For example,. this can be used to force a service to standby or active
 * mode, or to trigger a health-check.
 */
@InterfaceAudience.Private

public abstract class HAAdmin extends Configured implements Tool {

  protected static final String FORCEACTIVE = "forceactive";

  /**
   * Undocumented flag which allows an administrator to use manual failover
   * state transitions even when auto-failover is enabled. This is an unsafe
   * operation, which is why it is not documented in the usage below.
   */
  protected static final String FORCEMANUAL = "forcemanual";
  private static final Logger LOG = LoggerFactory.getLogger(HAAdmin.class);

  private int rpcTimeoutForChecks = -1;
  
  protected final static Map<String, UsageInfo> USAGE =
    ImmutableMap.<String, UsageInfo>builder()
    .put("-transitionToActive",
        new UsageInfo("[--"+FORCEACTIVE+"] <serviceId>", "Transitions the service into Active state"))
    .put("-transitionToStandby",
        new UsageInfo("<serviceId>", "Transitions the service into Standby state"))
    .put("-getServiceState",
        new UsageInfo("<serviceId>", "Returns the state of the service"))
      .put("-getAllServiceState",
          new UsageInfo(null, "Returns the state of all the services"))
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
  protected PrintStream out = System.out;
  private RequestSource requestSource = RequestSource.REQUEST_BY_USER;

  protected RequestSource getRequestSource() {
    return requestSource;
  }

  protected void setRequestSource(RequestSource requestSource) {
    this.requestSource = requestSource;
  }

  protected HAAdmin() {
    super();
  }

  protected HAAdmin(Configuration conf) {
    super(conf);
  }

  protected abstract HAServiceTarget resolveTarget(String string);
  
  protected Collection<String> getTargetIds(String targetNodeToActivate) {
    return new ArrayList<String>(
        Arrays.asList(new String[]{targetNodeToActivate}));
  }

  protected String getUsageString() {
    return "Usage: HAAdmin";
  }

  protected void printUsage(PrintStream pStr,
      Map<String, UsageInfo> helpEntries) {
    pStr.println(getUsageString());
    for (Map.Entry<String, UsageInfo> e : helpEntries.entrySet()) {
      String cmd = e.getKey();
      UsageInfo usage = e.getValue();

      if (usage.args == null) {
        pStr.println("    [" + cmd + "]");
      } else {
        pStr.println("    [" + cmd + " " + usage.args + "]");
      }
    }
    pStr.println();
    ToolRunner.printGenericCommandUsage(pStr);
  }

  protected void printUsage(PrintStream pStr) {
    printUsage(pStr, USAGE);
  }

  protected void printUsage(PrintStream pStr, String cmd,
      Map<String, UsageInfo> helpEntries) {
    UsageInfo usage = helpEntries.get(cmd);
    if (usage == null) {
      throw new RuntimeException("No usage for cmd " + cmd);
    }
    if (usage.args == null) {
      pStr.println(getUsageString() + " [" + cmd + "]");
    } else {
      pStr.println(getUsageString() + " [" + cmd + " " + usage.args + "]");
    }
  }

  protected void printUsage(PrintStream pStr, String cmd) {
    printUsage(pStr, cmd, USAGE);
  }

  private int transitionToActive(final CommandLine cmd)
      throws IOException, ServiceFailedException {
    String[] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("transitionToActive: incorrect number of arguments");
      printUsage(errOut, "-transitionToActive");
      return -1;
    }
    /*  returns true if other target node is active or some exception occurred 
        and forceActive was not set  */
    if(!cmd.hasOption(FORCEACTIVE)) {
      if(isOtherTargetNodeActive(argv[0], cmd.hasOption(FORCEACTIVE))) {
        return -1;
      }
    }
    HAServiceTarget target = resolveTarget(argv[0]);
    if (!checkManualStateManagementOK(target)) {
      return -1;
    }
    HAServiceProtocol proto = target.getProxy(
        getConf(), 0);
    HAServiceProtocolHelper.transitionToActive(proto, createReqInfo());
    return 0;
  }
  
  /**
   * Checks whether other target node is active or not
   * @param targetNodeToActivate
   * @return true if other target node is active or some other exception 
   * occurred and forceActive was set otherwise false
   * @throws IOException
   */
  private boolean isOtherTargetNodeActive(String targetNodeToActivate, boolean forceActive)
      throws IOException  {
    Collection<String> targetIds = getTargetIds(targetNodeToActivate);
    targetIds.remove(targetNodeToActivate);
    for(String targetId : targetIds) {
      HAServiceTarget target = resolveTarget(targetId);
      if (!checkManualStateManagementOK(target)) {
        return true;
      }
      try {
        HAServiceProtocol proto = target.getProxy(getConf(), 5000);
        if(proto.getServiceStatus().getState() == HAServiceState.ACTIVE) {
          errOut.println("transitionToActive: Node " +  targetId +" is already active");
          printUsage(errOut, "-transitionToActive");
          return true;
        }
      } catch (Exception e) {
        //If forceActive switch is false then return true
        if(!forceActive) {
          errOut.println("Unexpected error occurred  " + e.getMessage());
          printUsage(errOut, "-transitionToActive");
          return true; 
        }
      }
    }
    return false;
  }
  
  private int transitionToStandby(final CommandLine cmd)
      throws IOException, ServiceFailedException {
    String[] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("transitionToStandby: incorrect number of arguments");
      printUsage(errOut, "-transitionToStandby");
      return -1;
    }
    
    HAServiceTarget target = resolveTarget(argv[0]);
    if (!checkManualStateManagementOK(target)) {
      return -1;
    }
    HAServiceProtocol proto = target.getProxy(
        getConf(), 0);
    HAServiceProtocolHelper.transitionToStandby(proto, createReqInfo());
    return 0;
  }

  /**
   * Ensure that we are allowed to manually manage the HA state of the target
   * service. If automatic failover is configured, then the automatic
   * failover controllers should be doing state management, and it is generally
   * an error to use the HAAdmin command line to do so.
   * 
   * @param target the target to check
   * @return true if manual state management is allowed
   */
  protected boolean checkManualStateManagementOK(HAServiceTarget target) {
    if (target.isAutoFailoverEnabled()) {
      if (requestSource != RequestSource.REQUEST_BY_USER_FORCED) {
        errOut.println(
            "Automatic failover is enabled for " + target + "\n" +
            "Refusing to manually manage HA state, since it may cause\n" +
            "a split-brain scenario or other incorrect state.\n" +
            "If you are very sure you know what you are doing, please \n" +
            "specify the --" + FORCEMANUAL + " flag.");
        return false;
      } else {
        LOG.warn("Proceeding with manual HA state management even though\n" +
            "automatic failover is enabled for " + target);
        return true;
      }
    }
    return true;
  }

  protected StateChangeRequestInfo createReqInfo() {
    return new StateChangeRequestInfo(requestSource);
  }

  /**
   * Initiate a graceful failover by talking to the target node's ZKFC.
   * This sends an RPC to the ZKFC, which coordinates the failover.
   *
   * @param toNode the node to fail to
   * @return status code (0 for success)
   * @throws IOException if failover does not succeed
   */
  protected int gracefulFailoverThroughZKFCs(HAServiceTarget toNode)
      throws IOException {

    int timeout = FailoverController.getRpcTimeoutToNewActive(getConf());
    ZKFCProtocol proxy = toNode.getZKFCProxy(getConf(), timeout);
    try {
      proxy.gracefulFailover();
      out.println("Failover to " + toNode + " successful");
    } catch (ServiceFailedException sfe) {
      errOut.println("Failover failed: " + sfe.getLocalizedMessage());
      return -1;
    }

    return 0;
  }

  private int checkHealth(final CommandLine cmd)
      throws IOException, ServiceFailedException {
    String[] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("checkHealth: incorrect number of arguments");
      printUsage(errOut, "-checkHealth");
      return -1;
    }
    HAServiceProtocol proto = resolveTarget(argv[0]).getProxy(
        getConf(), rpcTimeoutForChecks);
    try {
      HAServiceProtocolHelper.monitorHealth(proto, createReqInfo());
    } catch (HealthCheckFailedException e) {
      errOut.println("Health check failed: " + e.getLocalizedMessage());
      return -1;
    }
    return 0;
  }

  private int getServiceState(final CommandLine cmd)
      throws IOException, ServiceFailedException {
    String[] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("getServiceState: incorrect number of arguments");
      printUsage(errOut, "-getServiceState");
      return -1;
    }

    HAServiceProtocol proto = resolveTarget(argv[0]).getProxy(
        getConf(), rpcTimeoutForChecks);
    out.println(proto.getServiceStatus().getState());
    return 0;
  }

  /**
   * Return the serviceId as is, we are assuming it was
   * given as a service address of form {@literal <}host:ipcport{@literal >}.
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

  protected boolean checkParameterValidity(String[] argv,
      Map<String, UsageInfo> helpEntries){

    if (argv.length < 1) {
      printUsage(errOut, helpEntries);
      return false;
    }

    String cmd = argv[0];
    if (!cmd.startsWith("-")) {
      errOut.println("Bad command '" + cmd +
          "': expected command starting with '-'");
      printUsage(errOut, helpEntries);
      return false;
    }

    if (!helpEntries.containsKey(cmd)) {
      errOut.println(cmd.substring(1) + ": Unknown command");
      printUsage(errOut, helpEntries);
      return false;
    }
    return true;
  }

  protected boolean checkParameterValidity(String[] argv){
    return checkParameterValidity(argv, USAGE);
  }

  protected int runCmd(String[] argv) throws Exception {
    if (!checkParameterValidity(argv, USAGE)){
      return -1;
    }

    String cmd = argv[0];
    Options opts = new Options();
    // Add command-specific options
    if("-transitionToActive".equals(cmd)) {
      addTransitionToActiveCliOpts(opts);
    }
    // Mutative commands take FORCEMANUAL option
    if ("-transitionToActive".equals(cmd) ||
        "-transitionToStandby".equals(cmd)) {
      opts.addOption(FORCEMANUAL, false,
          "force manual control even if auto-failover is enabled");
    }
    CommandLine cmdLine = parseOpts(cmd, opts, argv);
    if (cmdLine == null) {
      // error already printed
      return -1;
    }
    
    if (cmdLine.hasOption(FORCEMANUAL)) {
      if (!confirmForceManual()) {
        LOG.error("Aborted");
        return -1;
      }
      // Instruct the NNs to honor this request even if they're
      // configured for manual failover.
      requestSource = RequestSource.REQUEST_BY_USER_FORCED;
    }

    if ("-transitionToActive".equals(cmd)) {
      return transitionToActive(cmdLine);
    } else if ("-transitionToStandby".equals(cmd)) {
      return transitionToStandby(cmdLine);
    } else if ("-getServiceState".equals(cmd)) {
      return getServiceState(cmdLine);
    } else if ("-getAllServiceState".equals(cmd)) {
      return getAllServiceState();
    } else if ("-checkHealth".equals(cmd)) {
      return checkHealth(cmdLine);
    } else if ("-help".equals(cmd)) {
      return help(argv);
    } else {
      // we already checked command validity above, so getting here
      // would be a coding error
      throw new AssertionError("Should not get here, command: " + cmd);
    } 
  }

  protected int getAllServiceState() {
    Collection<String> targetIds = getTargetIds(null);
    if (targetIds.isEmpty()) {
      errOut.println("Failed to get service IDs");
      return -1;
    }
    for (String targetId : targetIds) {
      HAServiceTarget target = resolveTarget(targetId);
      String address = target.getAddress().getHostName() + ":"
          + target.getAddress().getPort();
      try {
        HAServiceProtocol proto = target.getProxy(getConf(),
            rpcTimeoutForChecks);
        out.println(String.format("%-50s %-10s", address, proto
            .getServiceStatus().getState()));
      } catch (IOException e) {
        out.println(String.format("%-50s %-10s", address,
            "Failed to connect: " + e.getMessage()));
      }
    }
    return 0;
  }

  protected boolean confirmForceManual() throws IOException {
     return ToolRunner.confirmPrompt(
        "You have specified the --" + FORCEMANUAL + " flag. This flag is " +
        "dangerous, as it can induce a split-brain scenario that WILL " +
        "CORRUPT your HDFS namespace, possibly irrecoverably.\n" +
        "\n" +
        "It is recommended not to use this flag, but instead to shut down the " +
        "cluster and disable automatic failover if you prefer to manually " +
        "manage your HA state.\n" +
        "\n" +
        "You may abort safely by answering 'n' or hitting ^C now.\n" +
        "\n" +
        "Are you sure you want to continue?");
  }


  
  /**
   * Add CLI options which are specific to the transitionToActive command and
   * no others.
   */
  private void addTransitionToActiveCliOpts(Options transitionToActiveCliOpts) {
    transitionToActiveCliOpts.addOption(FORCEACTIVE, false, "force active");
  }

  protected CommandLine parseOpts(String cmdName, Options opts, String[] argv,
      Map<String, UsageInfo> helpEntries) {
    try {
      // Strip off the first arg, since that's just the command name
      argv = Arrays.copyOfRange(argv, 1, argv.length);
      return new GnuParser().parse(opts, argv);
    } catch (ParseException pe) {
      errOut.println(cmdName.substring(1) +
          ": incorrect arguments");
      printUsage(errOut, cmdName, helpEntries);
      return null;
    }
  }
  
  protected CommandLine parseOpts(String cmdName, Options opts, String[] argv) {
    return parseOpts(cmdName, opts, argv, USAGE);
  }
  protected int help(String[] argv) {
    return help(argv, USAGE);
  }

  protected int help(String[] argv, Map<String, UsageInfo> helpEntries) {
    if (argv.length == 1) { // only -help
      printUsage(out, helpEntries);
      return 0;
    } else if (argv.length != 2) {
      printUsage(errOut, "-help", helpEntries);
      return -1;
    }
    String cmd = argv[1];
    if (!cmd.startsWith("-")) {
      cmd = "-" + cmd;
    }
    UsageInfo usageInfo = helpEntries.get(cmd);
    if (usageInfo == null) {
      errOut.println(cmd + ": Unknown command");
      printUsage(errOut, helpEntries);
      return -1;
    }

    if (usageInfo.args == null) {
      out.println(cmd + ": " + usageInfo.help);
    } else {
      out.println(cmd + " [" + usageInfo.args + "]: " + usageInfo.help);
    }
    return 0;
  }

  /**
   * UsageInfo class holds args and help details.
   */
  public static class UsageInfo {
    public final String args;
    public final String help;
    
    public UsageInfo(String args, String help) {
      this.args = args;
      this.help = help;
    }
  }
}
