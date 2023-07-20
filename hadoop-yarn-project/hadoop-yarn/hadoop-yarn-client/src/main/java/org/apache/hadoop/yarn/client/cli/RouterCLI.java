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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ha.HAAdmin.UsageInfo;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.util.FormattingCLIUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusters;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight.checkHeadRoomAlphaValid;
import static org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight.checkSubClusterQueueWeightRatioValid;

public class RouterCLI extends Configured implements Tool {


  private static final Logger LOG = LoggerFactory.getLogger(RouterCLI.class);

  protected final static Map<String, UsageInfo> ADMIN_USAGE =
      ImmutableMap.<String, UsageInfo>builder()
         // Command1: deregisterSubCluster
        .put("-deregisterSubCluster", new UsageInfo(
        "[-sc|--subClusterId [subCluster Id]]",
        "Deregister SubCluster, If the interval between the heartbeat time of the subCluster " +
        "and the current time exceeds the timeout period, " +
        "set the state of the subCluster to SC_LOST."))
         // Command2: policy
        .put("-policy", new UsageInfo(
        "[-s|--save [queue;router weight;amrm weight;headroomalpha]]",
        "We provide a set of commands for Policy:" +
        " Include list policies, save policies, batch save policies. " +
        " (Note: The policy type will be directly read from the" +
        " yarn.federation.policy-manager in the local yarn-site.xml.)" +
        " eg. (routeradmin -policy [-s|--save] root.a;SC-1:0.7,SC-2:0.3;SC-1:0.7,SC-2:0.3;1.0)"))
        .build();

  // Common Constant
  private static final String SEMICOLON = ";";

  // Command Constant
  private static final String CMD_EMPTY = "";
  private static final int EXIT_SUCCESS = 0;
  private static final int EXIT_ERROR = -1;

  // Command1: deregisterSubCluster
  private static final String DEREGISTER_SUBCLUSTER_TITLE =
      "Yarn Federation Deregister SubCluster";
  // Columns information
  private static final List<String> DEREGISTER_SUBCLUSTER_HEADER = Arrays.asList(
      "SubCluster Id", "Deregister State", "Last HeartBeatTime", "Information", "SubCluster State");
  // Constant
  private static final String OPTION_SC = "sc";
  private static final String OPTION_SUBCLUSTERID = "subClusterId";
  private static final String CMD_DEREGISTERSUBCLUSTER = "-deregisterSubCluster";
  private static final String CMD_HELP = "-help";

  // Command2: policy
  // save policy
  private static final String OPTION_S = "s";
  private static final String OPTION_SAVE = "save";
  private static final String CMD_POLICY = "-policy";

  public RouterCLI() {
    super();
  }

  public RouterCLI(Configuration conf) {
    super(conf);
  }

  private static void buildHelpMsg(String cmd, StringBuilder builder) {
    UsageInfo usageInfo = ADMIN_USAGE.get(cmd);
    if (usageInfo == null) {
      return;
    }

    if (usageInfo.args != null) {
      String space = (usageInfo.args == "") ? "" : " ";
      builder.append("   ")
          .append(cmd)
          .append(space)
          .append(usageInfo.args)
          .append(": ")
          .append(usageInfo.help);
    } else {
      builder.append("   ")
          .append(cmd)
          .append(": ")
          .append(usageInfo.help);
    }
  }

  private static void buildIndividualUsageMsg(String cmd, StringBuilder builder) {
    UsageInfo usageInfo = ADMIN_USAGE.get(cmd);
    if (usageInfo == null) {
      return;
    }
    if (usageInfo.args == null) {
      builder.append("Usage: routeradmin [")
          .append(cmd)
          .append("]\n");
    } else {
      String space = (usageInfo.args == "") ? "" : " ";
      builder.append("Usage: routeradmin [")
          .append(cmd)
          .append(space)
          .append(usageInfo.args)
          .append("]\n");
    }
  }

  private static void printHelp() {
    StringBuilder summary = new StringBuilder();
    summary.append("routeradmin is the command to execute ")
        .append("YARN Federation administrative commands.\n")
        .append("The full syntax is: \n\n")
        .append("routeradmin\n")
        .append("   [-deregisterSubCluster [-sc|--subClusterId [subCluster Id]]\n")
        .append("   [-policy [-s|--save [queue;router weight;amrm weight;headroomalpha]]\n")
        .append("   [-help [cmd]]").append("\n");
    StringBuilder helpBuilder = new StringBuilder();
    System.out.println(summary);

    for (String cmdKey : ADMIN_USAGE.keySet()) {
      buildHelpMsg(cmdKey, helpBuilder);
      helpBuilder.append("\n");
    }

    helpBuilder.append("   -help [cmd]: Displays help for the given command or all commands")
        .append(" if none is specified.");
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
    builder.append("routeradmin is only used in Yarn Federation Mode.\n");
    builder.append("Usage: routeradmin\n");
    for (Map.Entry<String, UsageInfo> cmdEntry : ADMIN_USAGE.entrySet()) {
      UsageInfo usageInfo = cmdEntry.getValue();
      builder.append("   ")
          .append(cmdEntry.getKey())
          .append(" ")
          .append(usageInfo.args)
          .append("\n");
    }
    builder.append("   -help [cmd]\n");
  }

  private static void printUsage(String cmd) {
    StringBuilder usageBuilder = new StringBuilder();
    if (ADMIN_USAGE.containsKey(cmd)) {
      buildIndividualUsageMsg(cmd, usageBuilder);
    } else {
      buildUsageMsg(usageBuilder);
    }
    System.err.println(usageBuilder);
    ToolRunner.printGenericCommandUsage(System.err);
  }

  /**
   * According to the parameter Deregister SubCluster.
   *
   * @param args parameter array.
   * @return If the Deregister SubCluster operation is successful,
   * it will return 0. Otherwise, it will return -1.
   *
   * @throws IOException raised on errors performing I/O.
   * @throws YarnException exceptions from yarn servers.
   * @throws ParseException Exceptions thrown during parsing of a command-line.
   */
  private int handleDeregisterSubCluster(String[] args)
      throws IOException, YarnException, ParseException {

    // Prepare Options.
    Options opts = new Options();
    opts.addOption("deregisterSubCluster", false,
        "Deregister YARN subCluster, if subCluster Heartbeat Timeout.");
    Option subClusterOpt = new Option(OPTION_SC, OPTION_SUBCLUSTERID, true,
        "The subCluster can be specified using either the '-sc' or '--subCluster' option. " +
         " If the subCluster's Heartbeat Timeout, it will be marked as 'SC_LOST'.");
    subClusterOpt.setOptionalArg(true);
    opts.addOption(subClusterOpt);

    // Parse command line arguments.
    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.out.println("Missing argument for options");
      printUsage(args[0]);
      return EXIT_ERROR;
    }

    // Try to parse the subClusterId.
    String subClusterId = null;
    if (cliParser.hasOption(OPTION_SC) || cliParser.hasOption(OPTION_SUBCLUSTERID)) {
      subClusterId = cliParser.getOptionValue(OPTION_SC);
      if (subClusterId == null) {
        subClusterId = cliParser.getOptionValue(OPTION_SUBCLUSTERID);
      }
    }

    // If subClusterId is not empty, try deregisterSubCluster subCluster,
    // otherwise try deregisterSubCluster all subCluster.
    if (StringUtils.isNotBlank(subClusterId)) {
      return deregisterSubCluster(subClusterId);
    } else {
      return deregisterSubCluster();
    }
  }

  private int deregisterSubCluster(String subClusterId)
      throws IOException, YarnException {
    PrintWriter writer = new PrintWriter(new OutputStreamWriter(
        System.out, Charset.forName(StandardCharsets.UTF_8.name())));
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    DeregisterSubClusterRequest request =
        DeregisterSubClusterRequest.newInstance(subClusterId);
    DeregisterSubClusterResponse response = adminProtocol.deregisterSubCluster(request);
    FormattingCLIUtils formattingCLIUtils = new FormattingCLIUtils(DEREGISTER_SUBCLUSTER_TITLE)
        .addHeaders(DEREGISTER_SUBCLUSTER_HEADER);
    List<DeregisterSubClusters> deregisterSubClusters = response.getDeregisterSubClusters();
    deregisterSubClusters.forEach(deregisterSubCluster -> {
      String responseSubClusterId = deregisterSubCluster.getSubClusterId();
      String deregisterState = deregisterSubCluster.getDeregisterState();
      String lastHeartBeatTime = deregisterSubCluster.getLastHeartBeatTime();
      String info = deregisterSubCluster.getInformation();
      String subClusterState = deregisterSubCluster.getSubClusterState();
      formattingCLIUtils.addLine(responseSubClusterId, deregisterState,
          lastHeartBeatTime, info, subClusterState);
    });
    writer.print(formattingCLIUtils.render());
    writer.flush();
    return EXIT_SUCCESS;
  }

  private int deregisterSubCluster() throws IOException, YarnException {
    deregisterSubCluster(CMD_EMPTY);
    return EXIT_SUCCESS;
  }

  private int handlePolicy(String[] args)
      throws IOException, YarnException, ParseException {

    // Prepare Options.
    Options opts = new Options();
    opts.addOption("policy", false,
        "We provide a set of commands for Policy Include list policies, " +
        "save policies, batch save policies.");
    Option saveOpt = new Option(OPTION_S, OPTION_SAVE, true,
        "We will save the policy information of the queue, " +
        "including queue and weight information");
    saveOpt.setOptionalArg(true);
    opts.addOption(saveOpt);

    // Parse command line arguments.
    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.out.println("Missing argument for options");
      printUsage(args[0]);
      return EXIT_ERROR;
    }

    // Try to parse the cmd save.
    if (cliParser.hasOption(OPTION_S) || cliParser.hasOption(OPTION_SAVE)) {
      String policy = cliParser.getOptionValue(OPTION_S);
      if (StringUtils.isBlank(policy)) {
        policy = cliParser.getOptionValue(OPTION_SAVE);
      }
      return handleSavePolicy(policy);
    }

    return EXIT_ERROR;
  }

  private int handleSavePolicy(String policy) {
    LOG.info("Save Federation Policy = {}.", policy);
    try {
      SaveFederationQueuePolicyRequest request = parsePolicy(policy);
      ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
      SaveFederationQueuePolicyResponse response = adminProtocol.saveFederationQueuePolicy(request);
      System.out.println(response.getMessage());
      return EXIT_SUCCESS;
    } catch (YarnException | IOException e) {
      LOG.error("handleSavePolicy error.", e);
      return EXIT_ERROR;
    }
  }

  /**
   * We will parse the policy, and it has specific formatting requirements.
   *
   * 1. queue,router weight,amrm weight,headroomalpha {@link FederationQueueWeight}.
   * 2. the sum of weights for all sub-clusters in routerWeight/amrmWeight should be 1.
   *
   * @param policy queue weight.
   * @return If the conversion is correct, we will get the FederationQueueWeight,
   * otherwise an exception will be thrown.
   * @throws YarnException exceptions from yarn servers.
   */
  protected SaveFederationQueuePolicyRequest parsePolicy(String policy) throws YarnException {

    String[] policyItems = policy.split(SEMICOLON);
    if (policyItems == null || policyItems.length != 4) {
      throw new YarnException("The policy cannot be empty or the policy is incorrect. \n" +
          " Required information to provide: queue,router weight,amrm weight,headroomalpha \n" +
          " eg. root.a;SC-1:0.7,SC-2:0.3;SC-1:0.7,SC-2:0.3;1.0");
    }

    String queue = policyItems[0];
    String routerWeight = policyItems[1];
    String amrmWeight = policyItems[2];
    String headroomalpha = policyItems[3];

    LOG.info("Policy: [Queue = {}, RouterWeight = {}, AmRmWeight = {}, Headroomalpha = {}]",
        queue, routerWeight, amrmWeight, headroomalpha);

    checkSubClusterQueueWeightRatioValid(routerWeight);
    checkSubClusterQueueWeightRatioValid(amrmWeight);
    checkHeadRoomAlphaValid(headroomalpha);

    FederationQueueWeight federationQueueWeight =
        FederationQueueWeight.newInstance(routerWeight, amrmWeight, headroomalpha);
    String policyManager = getConf().get(YarnConfiguration.FEDERATION_POLICY_MANAGER,
        YarnConfiguration.DEFAULT_FEDERATION_POLICY_MANAGER);
    SaveFederationQueuePolicyRequest request = SaveFederationQueuePolicyRequest.newInstance(
        queue, federationQueueWeight, policyManager);

    return request;
  }

  @Override
  public int run(String[] args) throws Exception {
    YarnConfiguration yarnConf = getConf() == null ?
        new YarnConfiguration() : new YarnConfiguration(getConf());
    boolean isFederationEnabled = yarnConf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);

    if (args.length < 1 || !isFederationEnabled) {
      printUsage(CMD_EMPTY);
      return EXIT_ERROR;
    }

    String cmd = args[0];

    if (CMD_HELP.equals(cmd)) {
      if (args.length > 1) {
        printUsage(args[1]);
      } else {
        printHelp();
      }
      return EXIT_SUCCESS;
    }

    if (CMD_DEREGISTERSUBCLUSTER.equals(cmd)) {
      return handleDeregisterSubCluster(args);
    }

    if (CMD_POLICY.equals(cmd)) {
      return handlePolicy(args);
    }

    return EXIT_SUCCESS;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RouterCLI(), args);
    System.exit(result);
  }
}
