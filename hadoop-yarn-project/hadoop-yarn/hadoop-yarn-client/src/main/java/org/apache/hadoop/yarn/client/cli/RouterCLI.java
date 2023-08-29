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
import org.apache.hadoop.yarn.client.util.MemoryPageUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusters;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
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
        "[-s|--save [queue;router weight;amrm weight;headroomalpha]] " +
         "[-bs|--batch-save [--format xml] [-f|--input-file fileName]]",
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
  private static final String OPTION_BATCH_S = "bs";
  private static final String OPTION_SAVE = "save";
  private static final String OPTION_BATCH_SAVE = "batch-save";
  private static final String OPTION_FORMAT = "format";
  private static final String OPTION_FILE = "f";
  private static final String OPTION_INPUT_FILE = "input-file";

  private static final String CMD_POLICY = "-policy";
  private static final String FORMAT_XML = "xml";
  private static final String FORMAT_JSON = "json";
  private static final String XML_TAG_SUBCLUSTERIDINFO = "subClusterIdInfo";
  private static final String XML_TAG_AMRMPOLICYWEIGHTS = "amrmPolicyWeights";
  private static final String XML_TAG_ROUTERPOLICYWEIGHTS = "routerPolicyWeights";
  private static final String XML_TAG_HEADROOMALPHA = "headroomAlpha";
  private static final String XML_TAG_FEDERATION_WEIGHTS = "federationWeights";
  private static final String XML_TAG_QUEUE = "queue";
  private static final String XML_TAG_NAME = "name";

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
        .append("   [-policy [-s|--save [queue;router weight;amrm weight;headroomalpha] " +
                "[-bs|--batch-save [--format xml,json] [-f|--input-file fileName]]]\n")
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
    Option batchSaveOpt = new Option(OPTION_BATCH_S, OPTION_BATCH_SAVE, false,
        "We will save queue policies in bulk, " +
         "where users can provide XML or JSON files containing the policies. " +
         "This command will parse the file contents and store the results " +
         "in the FederationStateStore.");
    Option formatOpt = new Option(null, "format", true,
        "Users can specify the file format using this option. " +
        "Currently, there are one supported file formats: XML." +
        "These files contain the policy information for storing queue policies.");
    Option fileOpt = new Option("f", "input-file", true,
        "The location of the input configuration file. ");
    formatOpt.setOptionalArg(true);

    opts.addOption(saveOpt);
    opts.addOption(batchSaveOpt);
    opts.addOption(formatOpt);
    opts.addOption(fileOpt);

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
    } else if (CMD_DEREGISTERSUBCLUSTER.equals(cmd)) {
      return handleDeregisterSubCluster(args);
    } else if (CMD_POLICY.equals(cmd)) {
      return handlePolicy(args);
    } else {
      System.out.println("No related commands found.");
      printHelp();
    }

    return EXIT_SUCCESS;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RouterCLI(), args);
    System.exit(result);
  }
}
