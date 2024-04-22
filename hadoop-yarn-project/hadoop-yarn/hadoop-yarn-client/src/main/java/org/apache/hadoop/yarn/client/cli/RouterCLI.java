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
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ha.HAAdmin.UsageInfo;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.util.FormattingCLIUtils;
import org.apache.hadoop.yarn.client.util.MemoryPageUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusters;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight;
import org.apache.hadoop.yarn.server.api.protocolrecords.QueryFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.QueryFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationSubCluster;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetSubClustersRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetSubClustersResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight.checkHeadRoomAlphaValid;
import static org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight.checkSubClusterQueueWeightRatioValid;

public class RouterCLI extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(RouterCLI.class);

  // Common Constant
  private static final String SEMICOLON = ";";

  // Command Constant
  private static final String CMD_EMPTY = "";
  private static final int EXIT_SUCCESS = 0;
  private static final int EXIT_ERROR = -1;
  private static final String CMD_HELP = "-help";

  // Command1: deregisterSubCluster
  private static final String DEREGISTER_SUBCLUSTER_TITLE =
      "Yarn Federation Deregister SubCluster";
  // Columns information
  private static final List<String> DEREGISTER_SUBCLUSTER_HEADER = Arrays.asList(
      "SubCluster Id", "Deregister State", "Last HeartBeatTime", "Information", "SubCluster State");
  // Constant
  private static final String OPTION_SC = "sc";
  private static final String OPTION_SUBCLUSTERID = "subClusterId";
  private static final String OPTION_GET_SUBCLUSTERS = "getSubClusters";
  private static final String OPTION_DEREGISTER_SUBCLUSTER = "deregisterSubCluster";
  private static final String CMD_SUBCLUSTER = "-subCluster";
  private static final String CMD_DEREGISTER_SUBCLUSTER = "-deregisterSubCluster";

  // DeregisterSubCluster Command Parameters
  protected final static UsageInfo DEREGISTER_SUBCLUSTER_USAGE = new UsageInfo(
      "-deregisterSubCluster <-sc|--subClusterId>",
      "This command is used to deregister subCluster, " +
      "If the interval between the heartbeat time of the subCluster and" +
      "the current time exceeds the timeout period, set the state of the subCluster to SC_LOST.");

  // DeregisterSubCluster Command Examples
  protected final static String DEREGISTER_SUBCLUSTER_EXAMPLE_1 =
      "yarn routeradmin -subCluster -deregisterSubCluster -sc SC-1";
  protected final static String DEREGISTER_SUBCLUSTER_EXAMPLE_2 =
      "yarn routeradmin -subCluster -deregisterSubCluster --subClusterId SC-1";

  // DeregisterSubCluster Command Help Information
  protected final static String DEREGISTER_SUBCLUSTER_HELP_INFO =
      "deregister subCluster, If the interval between the heartbeat time of the subCluster and" +
      "the current time exceeds the timeout period, set the state of the subCluster to SC_LOST.";

  protected final static UsageInfo GET_SUBCLUSTER_USAGE = new UsageInfo("-getSubClusters",
      "This command is used to get information about all subclusters.");

  private static final String GET_SUBCLUSTER_TITLE = "Yarn Federation SubCluster";

  // Columns information
  private static final List<String> GET_SUBCLUSTER_HEADER = Arrays.asList(
      "SubCluster Id", "SubCluster State", "Last HeartBeatTime");

  // GetSubCluster Command Examples
  protected final static String GET_SUBCLUSTER_EXAMPLE =
      "yarn routeradmin -subCluster -getSubClusters";

  protected final static RouterCmdUsageInfos SUBCLUSTER_USAGEINFOS =
      new RouterCmdUsageInfos()
      // deregisterSubCluster
      .addUsageInfo(DEREGISTER_SUBCLUSTER_USAGE)
      .addExampleDescs(DEREGISTER_SUBCLUSTER_USAGE.args, "If we want to deregisterSubCluster SC-1")
      .addExample(DEREGISTER_SUBCLUSTER_USAGE.args, DEREGISTER_SUBCLUSTER_EXAMPLE_1)
      .addExample(DEREGISTER_SUBCLUSTER_USAGE.args, DEREGISTER_SUBCLUSTER_EXAMPLE_2)
      // getSubCluster
      .addUsageInfo(GET_SUBCLUSTER_USAGE)
      .addExampleDescs(GET_SUBCLUSTER_USAGE.args,
      "If we want to get information about all subClusters in Federation")
      .addExample(GET_SUBCLUSTER_USAGE.args, GET_SUBCLUSTER_EXAMPLE);

  // Command2: policy

  private static final String CMD_POLICY = "-policy";

  // save policy
  private static final String OPTION_S = "s";
  private static final String OPTION_SAVE = "save";
  // batch save policy
  private static final String OPTION_BATCH_S = "bs";
  private static final String OPTION_BATCH_SAVE = "batch-save";
  private static final String OPTION_FORMAT = "format";
  private static final String FORMAT_XML = "xml";
  private static final String OPTION_FILE = "f";
  private static final String OPTION_INPUT_FILE = "input-file";
  // list policy
  private static final String OPTION_L = "l";
  private static final String OPTION_LIST = "list";
  private static final String OPTION_PAGE_SIZE = "pageSize";
  private static final String OPTION_CURRENT_PAGE = "currentPage";
  private static final String OPTION_QUEUE = "queue";
  private static final String OPTION_QUEUES = "queues";
  // delete policy
  private static final String OPTION_D = "d";
  private static final String OPTION_DELETE = "delete";

  private static final String XML_TAG_SUBCLUSTERIDINFO = "subClusterIdInfo";
  private static final String XML_TAG_AMRMPOLICYWEIGHTS = "amrmPolicyWeights";
  private static final String XML_TAG_ROUTERPOLICYWEIGHTS = "routerPolicyWeights";
  private static final String XML_TAG_HEADROOMALPHA = "headroomAlpha";
  private static final String XML_TAG_FEDERATION_WEIGHTS = "federationWeights";
  private static final String XML_TAG_QUEUE = "queue";
  private static final String XML_TAG_NAME = "name";

  private static final String LIST_POLICIES_TITLE =
      "Yarn Federation Queue Policies";

  // Columns information
  private static final List<String> LIST_POLICIES_HEADER = Arrays.asList(
      "Queue Name", "AMRM Weight", "Router Weight");

  // Policy Commands
  protected final static UsageInfo POLICY_SAVE_USAGE = new UsageInfo(
      "-s|--save (<queue;router weight;amrm weight;headroomalpha>)",
      "This command is used to save the policy information of the queue, " +
      "including queue and weight information.");

  protected final static String POLICY_SAVE_USAGE_EXAMPLE_DESC =
      "We have two sub-clusters, SC-1 and SC-2. \\" +
      "We want to configure a weight policy for the 'root.a' queue. \\" +
      "The Router Weight is set to SC-1 with a weight of 0.7 and SC-2 with a weight of 0.3. \\" +
      "The AMRM Weight is set SC-1 to 0.6 and SC-2 to 0.4. \\" +
      "We are using the default value of 0.1 for headroomalpha.";

  protected final static String POLICY_SAVE_USAGE_EXAMPLE_1 =
      "yarn routeradmin -policy -s root.a;SC-1:0.7,SC-2:0.3;SC-1:0.6,SC-2:0.4;1.0";
  protected final static String POLICY_SAVE_USAGE_EXAMPLE_2 =
      "yarn routeradmin -policy --save root.a;SC-1:0.7,SC-2:0.3;SC-1:0.6,SC-2:0.4;1.0";

  protected final static UsageInfo POLICY_BATCH_SAVE_USAGE = new UsageInfo(
      "-bs|--batch-save (--format <xml>) (-f|--input-file <fileName>)",
      "This command can batch load weight information for queues " +
       "based on the provided `federation-weights.xml` file.");

  protected final static String POLICY_BATCH_SAVE_USAGE_EXAMPLE_DESC =
      "We have two sub-clusters, SC-1 and SC-2. \\" +
      "We would like to configure weights for 'root.a' and 'root.b' queues. \\" +
      "We can set the weights for 'root.a' and 'root.b' in the 'federation-weights.xml' file. \\" +
      "and then use the batch-save command to save the configurations in bulk.";

  protected final static String POLICY_BATCH_SAVE_USAGE_EXAMPLE_1 =
      "yarn routeradmin -policy -bs --format xml -f federation-weights.xml";
  protected final static String POLICY_BATCH_SAVE_USAGE_EXAMPLE_2 =
      "yarn routeradmin -policy --batch-save --format xml -f federation-weights.xml";

  protected final static UsageInfo POLICY_LIST_USAGE = new UsageInfo(
      "-l|--list [--pageSize][--currentPage][--queue][--queues]",
      "This command is used to display the configured queue weight information.");

  protected final static String POLICY_LIST_USAGE_EXAMPLE_DESC =
      "We can display the list of already configured queue weight information. \\" +
      "We can use the --queue option to query the weight information for a specific queue \\" +
      " or use the --queues option to query the weight information for multiple queues. \\";

  protected final static String POLICY_LIST_USAGE_EXAMPLE_1 =
      "yarn routeradmin -policy -l --pageSize 20 --currentPage 1 --queue root.a";

  protected final static String POLICY_LIST_USAGE_EXAMPLE_2 =
      "yarn routeradmin -policy -list --pageSize 20 --currentPage 1 --queues root.a,root.b";

  protected final static UsageInfo POLICY_DELETE_USAGE = new UsageInfo(
      "-d|--delete [--queue]",
      "This command is used to delete the policy of the queue.");

  protected final static String POLICY_DELETE_USAGE_EXAMPLE_DESC =
      "We delete the weight information of root.a. \\" +
      "We can use --queue to specify the name of the queue.";

  protected final static String POLICY_DELETE_USAGE_EXAMPLE1 =
      "yarn routeradmin -policy -d --queue root.a";

  protected final static String POLICY_DELETE_USAGE_EXAMPLE2 =
      "yarn routeradmin -policy --delete --queue root.a";

  protected final static RouterCmdUsageInfos POLICY_USAGEINFOS = new RouterCmdUsageInfos()
       // Policy Save
      .addUsageInfo(POLICY_SAVE_USAGE)
      .addExampleDescs(POLICY_SAVE_USAGE.args, POLICY_SAVE_USAGE_EXAMPLE_DESC)
      .addExample(POLICY_SAVE_USAGE.args, POLICY_SAVE_USAGE_EXAMPLE_1)
      .addExample(POLICY_SAVE_USAGE.args, POLICY_SAVE_USAGE_EXAMPLE_2)
      // Policy Batch Save
      .addUsageInfo(POLICY_BATCH_SAVE_USAGE)
      .addExampleDescs(POLICY_BATCH_SAVE_USAGE.args, POLICY_BATCH_SAVE_USAGE_EXAMPLE_DESC)
      .addExample(POLICY_BATCH_SAVE_USAGE.args, POLICY_BATCH_SAVE_USAGE_EXAMPLE_1)
      .addExample(POLICY_BATCH_SAVE_USAGE.args, POLICY_BATCH_SAVE_USAGE_EXAMPLE_2)
       // Policy List Save
      .addUsageInfo(POLICY_LIST_USAGE)
      .addExampleDescs(POLICY_LIST_USAGE.args, POLICY_LIST_USAGE_EXAMPLE_DESC)
      .addExample(POLICY_LIST_USAGE.args, POLICY_LIST_USAGE_EXAMPLE_1)
      .addExample(POLICY_LIST_USAGE.args, POLICY_LIST_USAGE_EXAMPLE_2)
       // Policy Delete
      .addUsageInfo(POLICY_DELETE_USAGE)
      .addExampleDescs(POLICY_DELETE_USAGE.args, POLICY_DELETE_USAGE_EXAMPLE_DESC)
      .addExample(POLICY_DELETE_USAGE.args, POLICY_DELETE_USAGE_EXAMPLE1)
      .addExample(POLICY_DELETE_USAGE.args, POLICY_DELETE_USAGE_EXAMPLE2);

  // Command3: application
  private static final String CMD_APPLICATION = "-application";

  // Application Delete
  protected final static UsageInfo APPLICATION_DELETE_USAGE = new UsageInfo(
      "--delete <application_id>",
      "This command is used to delete the specified application.");

  protected final static String APPLICATION_DELETE_USAGE_EXAMPLE_DESC =
      "If we want to delete application_1440536969523_0001.";

  protected final static String APPLICATION_DELETE_USAGE_EXAMPLE_1 =
      "yarn routeradmin -application --delete application_1440536969523_0001";

  protected final static RouterCmdUsageInfos APPLICATION_USAGEINFOS = new RouterCmdUsageInfos()
      // application delete
      .addUsageInfo(APPLICATION_DELETE_USAGE)
      .addExampleDescs(APPLICATION_DELETE_USAGE.args, APPLICATION_DELETE_USAGE_EXAMPLE_DESC)
      .addExample(APPLICATION_DELETE_USAGE.args, APPLICATION_DELETE_USAGE_EXAMPLE_1);

  // delete application
  private static final String OPTION_DELETE_APP = "delete";

  protected final static Map<String, RouterCmdUsageInfos> ADMIN_USAGE =
      ImmutableMap.<String, RouterCmdUsageInfos>builder()
      // Command1: subCluster
      .put(CMD_SUBCLUSTER, SUBCLUSTER_USAGEINFOS)
      // Command2: policy
      .put(CMD_POLICY, POLICY_USAGEINFOS)
      // Command3: application
      .put(CMD_APPLICATION, APPLICATION_USAGEINFOS)
      .build();

  public RouterCLI() {
    super();
  }

  public RouterCLI(Configuration conf) {
    super(conf);
  }

  private static void buildHelpMsg(String cmd, StringBuilder builder) {
    RouterCmdUsageInfos routerUsageInfo = ADMIN_USAGE.get(cmd);

    if (routerUsageInfo == null) {
      return;
    }
    builder.append("[").append(cmd).append("]\n");

    if (!routerUsageInfo.helpInfos.isEmpty()) {
      builder.append("\t Description: \n");
      for (String helpInfo : routerUsageInfo.helpInfos) {
        builder.append("\t\t").append(helpInfo).append("\n\n");
      }
    }

    if (!routerUsageInfo.usageInfos.isEmpty()) {
      builder.append("\t UsageInfos: \n");
      for (UsageInfo usageInfo : routerUsageInfo.usageInfos) {
        builder.append("\t\t").append(usageInfo.args)
            .append(": ")
            .append("\n\t\t")
            .append(usageInfo.help).append("\n\n");
      }
    }

    if (MapUtils.isNotEmpty(routerUsageInfo.examples)) {
      builder.append("\t Examples: \n");
      int count = 1;
      for (Map.Entry<String, List<String>> example : routerUsageInfo.examples.entrySet()) {

        String keyCmd = example.getKey();
        builder.append("\t\t")
            .append("Cmd:").append(count)
            .append(". ").append(keyCmd)
            .append(": \n\n");

        // Print Command Description
        List<String> exampleDescs = routerUsageInfo.exampleDescs.get(keyCmd);
        if (CollectionUtils.isNotEmpty(exampleDescs)) {
          builder.append("\t\t").append("Cmd Requirement Description:\n");
          for (String value : exampleDescs) {
            String[] valueDescs = StringUtils.split(value, "\\");
            for (String valueDesc : valueDescs) {
              builder.append("\t\t").append(valueDesc).append("\n");
            }
          }
        }

        builder.append("\n");

        // Print Command example
        List<String> valueExamples = example.getValue();
        if (CollectionUtils.isNotEmpty(valueExamples)) {
          builder.append("\t\t").append("Cmd Examples:\n");
          for (String valueExample : valueExamples) {
            builder.append("\t\t").append(valueExample).append("\n");
          }
        }
        builder.append("\n");
        count++;
      }
    }
  }

  private static void printHelp() {
    StringBuilder summary = new StringBuilder();
    summary.append("routeradmin is the command to execute ")
        .append("YARN Federation administrative commands.\n")
        .append("The full syntax is: \n\n")
        .append("routeradmin\n");
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
    for (String cmdKey : ADMIN_USAGE.keySet()) {
      buildHelpMsg(cmdKey, builder);
      builder.append("\n");
    }
    builder.append("   -help [cmd]\n");
  }

  private static void printUsage(String cmd) {
    StringBuilder usageBuilder = new StringBuilder();
    if (ADMIN_USAGE.containsKey(cmd)) {
      buildHelpMsg(cmd, usageBuilder);
    } else {
      buildUsageMsg(usageBuilder);
    }
    System.err.println(usageBuilder);
    ToolRunner.printGenericCommandUsage(System.err);
  }

  private int handleSubCluster(String[] args) throws ParseException, IOException, YarnException {
    // Prepare Options.
    Options opts = new Options();
    opts.addOption("subCluster", false,
         "We provide a set of commands for SubCluster Include deregisterSubCluster, " +
         "get SubClusters.");
    opts.addOption("deregisterSubCluster", false,
        "Deregister YARN subCluster, if subCluster Heartbeat Timeout.");
    opts.addOption("getSubClusters", false,
        "Get information about all subClusters of Federation.");
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

    // deregister subCluster
    if (cliParser.hasOption(OPTION_DEREGISTER_SUBCLUSTER)) {
      String subClusterId = null;
      if (cliParser.hasOption(OPTION_SC) || cliParser.hasOption(OPTION_SUBCLUSTERID)) {
        subClusterId = cliParser.getOptionValue(OPTION_SC);
        if (subClusterId == null) {
          subClusterId = cliParser.getOptionValue(OPTION_SUBCLUSTERID);
        }
      }
      return handleDeregisterSubCluster(subClusterId);
    } else if (cliParser.hasOption(OPTION_GET_SUBCLUSTERS)) {
      // get subClusters
      return handleGetSubClusters();
    } else {
      // printUsage
      printUsage(args[0]);
    }

    return EXIT_ERROR;
  }

  private int handleGetSubClusters() throws IOException, YarnException {
    PrintWriter writer = new PrintWriter(new OutputStreamWriter(
        System.out, StandardCharsets.UTF_8));
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    GetSubClustersRequest request = GetSubClustersRequest.newInstance();
    GetSubClustersResponse response = adminProtocol.getFederationSubClusters(request);
    FormattingCLIUtils formattingCLIUtils = new FormattingCLIUtils(GET_SUBCLUSTER_TITLE)
        .addHeaders(GET_SUBCLUSTER_HEADER);
    List<FederationSubCluster> federationSubClusters = response.getFederationSubClusters();
    federationSubClusters.forEach(federationSubCluster -> {
      String responseSubClusterId = federationSubCluster.getSubClusterId();
      String state = federationSubCluster.getSubClusterState();
      String lastHeartBeatTime = federationSubCluster.getLastHeartBeatTime();
      formattingCLIUtils.addLine(responseSubClusterId, state, lastHeartBeatTime);
    });
    writer.print(formattingCLIUtils.render());
    writer.flush();
    return EXIT_SUCCESS;
  }

  /**
   * According to the parameter Deregister SubCluster.
   *
   * @param subClusterId subClusterId.
   * @return If the Deregister SubCluster operation is successful,
   * it will return 0. Otherwise, it will return -1.
   *
   * @throws IOException raised on errors performing I/O.
   * @throws YarnException exceptions from yarn servers.
   * @throws ParseException Exceptions thrown during parsing of a command-line.
   */
  private int handleDeregisterSubCluster(String subClusterId)
      throws IOException, YarnException, ParseException {

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
        System.out, StandardCharsets.UTF_8));
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
    Option batchSaveOpt = new Option(OPTION_BATCH_S, OPTION_BATCH_SAVE, false,
        "We will save queue policies in bulk, " +
         "where users can provide XML files containing the policies. " +
         "This command will parse the file contents and store the results " +
         "in the FederationStateStore.");
    Option formatOpt = new Option(null, "format", true,
        "Users can specify the file format using this option. " +
        "Currently, there are one supported file formats: XML." +
        "These files contain the policy information for storing queue policies.");
    Option fileOpt = new Option("f", "input-file", true,
        "The location of the input configuration file. ");
    formatOpt.setOptionalArg(true);
    Option listOpt = new Option(OPTION_L, OPTION_LIST, false,
        "We can display the configured queue strategy according to the parameters.");
    Option pageSizeOpt = new Option(null, "pageSize", true,
        "The number of policies displayed per page.");
    Option currentPageOpt = new Option(null, "currentPage", true,
        "Since users may configure numerous policies, we will choose to display them in pages. " +
        "This parameter represents the page number to be displayed.");
    Option queueOpt = new Option(null, "queue", true,
        "the queue we need to filter. example: root.a");
    Option queuesOpt = new Option(null, "queues", true,
        "list of queues to filter. example: root.a,root.b,root.c");
    Option deleteOpt = new Option(OPTION_D, OPTION_DELETE, false, "");

    opts.addOption(saveOpt);
    opts.addOption(batchSaveOpt);
    opts.addOption(formatOpt);
    opts.addOption(fileOpt);
    opts.addOption(listOpt);
    opts.addOption(pageSizeOpt);
    opts.addOption(currentPageOpt);
    opts.addOption(queueOpt);
    opts.addOption(queuesOpt);
    opts.addOption(deleteOpt);

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
    // Save a single queue policy
    if (cliParser.hasOption(OPTION_S) || cliParser.hasOption(OPTION_SAVE)) {
      String policy = cliParser.getOptionValue(OPTION_S);
      if (StringUtils.isBlank(policy)) {
        policy = cliParser.getOptionValue(OPTION_SAVE);
      }
      return handleSavePolicy(policy);
    } else if (cliParser.hasOption(OPTION_BATCH_S) || cliParser.hasOption(OPTION_BATCH_SAVE)) {
      // Save Queue Policies in Batches
      // Determine whether the file format is accurate, XML or JSON format.
      // If it is not XML or JSON, we will directly prompt the user with an error message.
      String format = null;
      if (cliParser.hasOption(OPTION_FORMAT)) {
        format = cliParser.getOptionValue(OPTION_FORMAT);
        if (StringUtils.isBlank(format) ||
            !StringUtils.equalsAnyIgnoreCase(format, FORMAT_XML)) {
          System.out.println("We currently only support policy configuration files " +
              "in XML formats.");
          return EXIT_ERROR;
        }
      }

      // Parse configuration file path.
      String filePath = null;
      if (cliParser.hasOption(OPTION_FILE) || cliParser.hasOption(OPTION_INPUT_FILE)) {
        filePath = cliParser.getOptionValue(OPTION_FILE);
        if (StringUtils.isBlank(filePath)) {
          filePath = cliParser.getOptionValue(OPTION_INPUT_FILE);
        }
      }

      // Batch SavePolicies.
      return handBatchSavePolicies(format, filePath);
    } else if(cliParser.hasOption(OPTION_L) || cliParser.hasOption(OPTION_LIST)) {

      int pageSize = 10;
      if (cliParser.hasOption(OPTION_PAGE_SIZE)) {
        pageSize = Integer.parseInt(cliParser.getOptionValue(OPTION_PAGE_SIZE));
      }

      int currentPage = 1;
      if (cliParser.hasOption(OPTION_CURRENT_PAGE)) {
        currentPage = Integer.parseInt(cliParser.getOptionValue(OPTION_CURRENT_PAGE));
      }

      String queue = null;
      if (cliParser.hasOption(OPTION_QUEUE)) {
        queue = cliParser.getOptionValue(OPTION_QUEUE);
      }

      List<String> queues = null;
      if (cliParser.hasOption(OPTION_QUEUES)) {
        String tmpQueues = cliParser.getOptionValue(OPTION_QUEUES);
        queues = Arrays.stream(tmpQueues.split(",")).collect(Collectors.toList());
      }

      // List Policies.
      return handListPolicies(pageSize, currentPage, queue, queues);
    } else if (cliParser.hasOption(OPTION_D) || cliParser.hasOption(OPTION_DELETE)) {
      String queue = cliParser.getOptionValue(OPTION_QUEUE);
      // Delete Policy.
      return handDeletePolicy(queue);
    } else {
      // printUsage
      printUsage(args[0]);
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

  private int handBatchSavePolicies(String format, String policyFile) {

    if(StringUtils.isBlank(format)) {
      LOG.error("Batch Save Federation Policies. Format is Empty.");
      return EXIT_ERROR;
    }

    if(StringUtils.isBlank(policyFile)) {
      LOG.error("Batch Save Federation Policies. policyFile is Empty.");
      return EXIT_ERROR;
    }

    LOG.info("Batch Save Federation Policies. Format = {}, PolicyFile = {}.",
        format, policyFile);

    switch (format) {
    case FORMAT_XML:
      return parseXml2PoliciesAndBatchSavePolicies(policyFile);
    default:
      System.out.println("We currently only support XML formats.");
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

  /**
   * Parse Policies from XML and save them in batches to FederationStateStore.
   *
   * We save 20 policies in one batch.
   * If the user needs to save 1000 policies, it will cycle 50 times.
   *
   * Every time a page is saved, we will print whether a page
   * has been saved successfully or failed.
   *
   * @param policiesXml Policies Xml Path.
   * @return 0, success; 1, failed.
   */
  protected int parseXml2PoliciesAndBatchSavePolicies(String policiesXml) {
    try {
      List<FederationQueueWeight> federationQueueWeightsList = parsePoliciesByXml(policiesXml);
      MemoryPageUtils<FederationQueueWeight> memoryPageUtils = new MemoryPageUtils<>(20);
      federationQueueWeightsList.forEach(federationQueueWeight ->
          memoryPageUtils.addToMemory(federationQueueWeight));
      int pages = memoryPageUtils.getPages();
      for (int i = 0; i < pages; i++) {
        List<FederationQueueWeight> federationQueueWeights =
            memoryPageUtils.readFromMemory(i);
        BatchSaveFederationQueuePoliciesRequest request =
            BatchSaveFederationQueuePoliciesRequest.newInstance(federationQueueWeights);
        ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
        BatchSaveFederationQueuePoliciesResponse response =
            adminProtocol.batchSaveFederationQueuePolicies(request);
        System.out.println("page <" + (i + 1) + "> : " + response.getMessage());
      }
    } catch (Exception e) {
      LOG.error("BatchSaveFederationQueuePolicies error", e);
    }
    return EXIT_ERROR;
  }

  /**
   * Parse FederationQueueWeight from the xml configuration file.
   * <p>
   * We allow users to provide an xml configuration file,
   * which stores the weight information of the queue.
   *
   * @param policiesXml Policies Xml Path.
   * @return FederationQueueWeight List.
   * @throws IOException an I/O exception of some sort has occurred.
   * @throws SAXException Encapsulate a general SAX error or warning.
   * @throws ParserConfigurationException a serious configuration error..
   */
  protected List<FederationQueueWeight> parsePoliciesByXml(String policiesXml)
      throws IOException, SAXException, ParserConfigurationException {

    List<FederationQueueWeight> weights = new ArrayList<>();

    File xmlFile = new File(policiesXml);
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document document = builder.parse(xmlFile);

    NodeList federationsList = document.getElementsByTagName(XML_TAG_FEDERATION_WEIGHTS);

    for (int i = 0; i < federationsList.getLength(); i++) {

      Node federationNode = federationsList.item(i);

      if (federationNode.getNodeType() == Node.ELEMENT_NODE) {
        Element federationElement = (Element) federationNode;
        NodeList queueList = federationElement.getElementsByTagName(XML_TAG_QUEUE);

        for (int j = 0; j < queueList.getLength(); j++) {

          Node queueNode = queueList.item(j);
          if (queueNode.getNodeType() == Node.ELEMENT_NODE) {
            Element queueElement = (Element) queueNode;
            // parse queueName.
            String queueName = queueElement.getElementsByTagName(XML_TAG_NAME)
                .item(0).getTextContent();

            // parse amrmPolicyWeights / routerPolicyWeights.
            String amrmWeight = parsePolicyWeightsNode(queueElement, XML_TAG_AMRMPOLICYWEIGHTS);
            String routerWeight = parsePolicyWeightsNode(queueElement, XML_TAG_ROUTERPOLICYWEIGHTS);

            // parse headroomAlpha.
            String headroomAlpha = queueElement.getElementsByTagName(XML_TAG_HEADROOMALPHA)
                .item(0).getTextContent();

            String policyManager = getConf().get(YarnConfiguration.FEDERATION_POLICY_MANAGER,
                YarnConfiguration.DEFAULT_FEDERATION_POLICY_MANAGER);

            LOG.debug("Queue: {}, AmrmPolicyWeights: {}, RouterWeight: {}, HeadroomAlpha: {}.",
                queueName, amrmWeight, routerWeight, headroomAlpha);

            FederationQueueWeight weight = FederationQueueWeight.newInstance(routerWeight,
                amrmWeight, headroomAlpha, queueName, policyManager);

            weights.add(weight);
          }
        }
      }
    }

    return weights;
  }

  /**
   * We will parse the policyWeight information.
   *
   * @param queueElement xml Element.
   * @param weightType weightType, including 2 types, AmrmPolicyWeight and RouterPolicyWeight.
   * @return concatenated string of sub-cluster weights.
   */
  private String parsePolicyWeightsNode(Element queueElement, String weightType) {
    NodeList amrmPolicyWeightsList = queueElement.getElementsByTagName(weightType);
    Node amrmPolicyWeightsNode = amrmPolicyWeightsList.item(0);
    List<String> amRmPolicyWeights = new ArrayList<>();
    if (amrmPolicyWeightsNode.getNodeType() == Node.ELEMENT_NODE) {
      Element amrmPolicyWeightsElement = (Element) amrmPolicyWeightsNode;
      NodeList subClusterIdInfoList =
          amrmPolicyWeightsElement.getElementsByTagName(XML_TAG_SUBCLUSTERIDINFO);
      for (int i = 0; i < subClusterIdInfoList.getLength(); i++) {
        Node subClusterIdInfoNode = subClusterIdInfoList.item(i);
        if (subClusterIdInfoNode.getNodeType() == Node.ELEMENT_NODE) {
          Element subClusterIdInfoElement = (Element) subClusterIdInfoNode;
          String subClusterId =
              subClusterIdInfoElement.getElementsByTagName("id").item(0).getTextContent();
          String weight =
              subClusterIdInfoElement.getElementsByTagName("weight").item(0).getTextContent();
          LOG.debug("WeightType[{}] - SubCluster ID: {}, Weight: {}.",
              weightType, subClusterId, weight);
          amRmPolicyWeights.add(subClusterId + ":" + weight);
        }
      }
    }
    return StringUtils.join(amRmPolicyWeights, ",");
  }

  /**
   * Handles the list federation policies based on the specified parameters.
   *
   * @param pageSize Records displayed per page.
   * @param currentPage The current page number.
   * @param queue The name of the queue to be filtered.
   * @param queues list of queues to filter.
   * @return 0, success; 1, failed.
   */
  protected int handListPolicies(int pageSize, int currentPage, String queue, List<String> queues) {
    LOG.info("List Federation Policies,  pageSize = {}, currentPage = {}, queue = {}, queues = {}",
        pageSize, currentPage, queue, queues);
    try {
      PrintWriter writer = new PrintWriter(new OutputStreamWriter(
          System.out, StandardCharsets.UTF_8));
      QueryFederationQueuePoliciesRequest request =
          QueryFederationQueuePoliciesRequest.newInstance(pageSize, currentPage, queue, queues);
      ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
      QueryFederationQueuePoliciesResponse response =
          adminProtocol.listFederationQueuePolicies(request);
      System.out.println("TotalPage = " + response.getTotalPage());

      FormattingCLIUtils formattingCLIUtils = new FormattingCLIUtils(LIST_POLICIES_TITLE)
          .addHeaders(LIST_POLICIES_HEADER);
      List<FederationQueueWeight> federationQueueWeights = response.getFederationQueueWeights();
      federationQueueWeights.forEach(federationQueueWeight -> {
        String queueName = federationQueueWeight.getQueue();
        String amrmWeight = federationQueueWeight.getAmrmWeight();
        String routerWeight = federationQueueWeight.getRouterWeight();
        formattingCLIUtils.addLine(queueName, amrmWeight, routerWeight);
      });
      writer.print(formattingCLIUtils.render());
      writer.flush();
      return EXIT_SUCCESS;
    } catch (YarnException | IOException e) {
      LOG.error("handleSavePolicy error.", e);
      return EXIT_ERROR;
    }
  }

  private int handleDeleteApplication(String application) {
    LOG.info("Delete Application = {}.", application);
    try {
      DeleteFederationApplicationRequest request =
          DeleteFederationApplicationRequest.newInstance(application);
      ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
      DeleteFederationApplicationResponse response =
          adminProtocol.deleteFederationApplication(request);
      System.out.println(response.getMessage());
      return EXIT_SUCCESS;
    } catch (Exception e) {
      LOG.error("handleSavePolicy error.", e);
      return EXIT_ERROR;
    }
  }

  private int handleApplication(String[] args)
      throws IOException, YarnException, ParseException {
    // Prepare Options.
    Options opts = new Options();
    opts.addOption("application", false,
        "We provide a set of commands to query and clean applications.");
    Option deleteOpt = new Option(null, OPTION_DELETE_APP, true,
        "We will clean up the provided application.");
    opts.addOption(deleteOpt);

    // Parse command line arguments.
    CommandLine cliParser;
    try {
      cliParser = new DefaultParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.out.println("Missing argument for options");
      printUsage(args[0]);
      return EXIT_ERROR;
    }

    if (cliParser.hasOption(OPTION_DELETE_APP)) {
      String application = cliParser.getOptionValue(OPTION_DELETE_APP);
      return handleDeleteApplication(application);
    }

    return 0;
  }

  /**
   * Delete queue weight information.
   *
   * @param queue Queue whose policy needs to be deleted.
   * @return 0, success; 1, failed.
   */
  protected int handDeletePolicy(String queue) {
    LOG.info("Delete {} Policy.", queue);
    try {
      if (StringUtils.isBlank(queue)) {
        System.err.println("Queue cannot be empty.");
      }
      List<String> queues = new ArrayList<>();
      queues.add(queue);
      DeleteFederationQueuePoliciesRequest request =
          DeleteFederationQueuePoliciesRequest.newInstance(queues);
      ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
      DeleteFederationQueuePoliciesResponse response =
          adminProtocol.deleteFederationPoliciesByQueues(request);
      System.out.println(response.getMessage());
      return EXIT_SUCCESS;
    } catch (Exception e) {
      LOG.error("handDeletePolicy queue = {} error.", queue, e);
      return EXIT_ERROR;
    }
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
    } else if (CMD_SUBCLUSTER.equals(cmd)) {
      return handleSubCluster(args);
    } else if (CMD_POLICY.equals(cmd)) {
      return handlePolicy(args);
    } else if (CMD_APPLICATION.equals(cmd)) {
      return handleApplication(args);
    } else {
      System.out.println("No related commands found.");
      printHelp();
    }

    return EXIT_SUCCESS;
  }

  public static UsageInfo getPolicyBatchSaveUsage() {
    return POLICY_BATCH_SAVE_USAGE;
  }

  static class RouterCmdUsageInfos {
    private List<UsageInfo> usageInfos;
    private List<String> helpInfos;
    private Map<String, List<String>> examples;
    protected Map<String, List<String>> exampleDescs;

    RouterCmdUsageInfos() {
      this.usageInfos = new ArrayList<>();
      this.helpInfos = new ArrayList<>();
      this.examples = new LinkedHashMap<>();
      this.exampleDescs = new LinkedHashMap<>();
    }

    public RouterCmdUsageInfos addUsageInfo(UsageInfo usageInfo) {
      this.usageInfos.add(usageInfo);
      return this;
    }

    public RouterCmdUsageInfos addHelpInfo(String helpInfo) {
      this.helpInfos.add(helpInfo);
      return this;
    }

    private RouterCmdUsageInfos addExample(String cmd, String example) {
      List<String> exampleList = this.examples.getOrDefault(cmd, new ArrayList<>());
      exampleList.add(example);
      this.examples.put(cmd, exampleList);
      return this;
    }

    private RouterCmdUsageInfos addExampleDescs(String cmd, String exampleDesc) {
      List<String> exampleDescList = this.exampleDescs.getOrDefault(cmd, new ArrayList<>());
      exampleDescList.add(exampleDesc);
      this.exampleDescs.put(cmd, exampleDescList);
      return this;
    }

    public Map<String, List<String>> getExamples() {
      return examples;
    }
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RouterCLI(), args);
    System.exit(result);
  }

  @VisibleForTesting
  public Map<String, RouterCmdUsageInfos> getAdminUsage(){
    return ADMIN_USAGE;
  }
}
