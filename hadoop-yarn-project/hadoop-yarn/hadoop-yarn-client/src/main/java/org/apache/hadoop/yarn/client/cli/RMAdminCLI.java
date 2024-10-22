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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocolHelper;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
import org.apache.hadoop.yarn.client.util.YarnClientUtils;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import static org.apache.hadoop.yarn.client.util.YarnClientUtils.NO_LABEL_ERR_MSG;
import static org.apache.hadoop.yarn.client.util.YarnClientUtils.isYarnFederationEnabled;

@Private
@Unstable
public class RMAdminCLI extends HAAdmin {

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);
  static CommonNodeLabelsManager localNodeLabelsManager = null;
  private static final String NO_MAPPING_ERR_MSG =
      "No node-to-labels mappings are specified";
  private static final String INVALID_TIMEOUT_ERR_MSG =
      "Invalid timeout specified : ";
  private static final Pattern RESOURCE_TYPES_ARGS_PATTERN =
      Pattern.compile("^[0-9]*$");
  private static final String SUBCLUSTERID = "subClusterId";
  private static final Option OPTION_SUBCLUSTERID = new Option(SUBCLUSTERID, true,
       "We support setting subClusterId in YARN Federation mode to specify specific subClusters.");
  protected final static Map<String, UsageInfo> ADMIN_USAGE =
      ImmutableMap.<String, UsageInfo>builder()
          .put("-refreshQueues", new UsageInfo("",
              "Reload the queues' acls, states and scheduler specific " +
                  "properties. \n\t\tResourceManager will reload the " +
                  "mapred-queues configuration file."))
          .put("-refreshNodes",
              new UsageInfo("[-g|graceful [timeout in seconds] -client|server]",
              "Refresh the hosts information at the ResourceManager. Here "
              + "[-g|graceful [timeout in seconds] -client|server] is optional,"
              + " if we specify the timeout then ResourceManager will wait for "
              + "timeout before marking the NodeManager as decommissioned."
              + " The -client|server indicates if the timeout tracking should"
              + " be handled by the client or the ResourceManager. The client"
              + "-side tracking is blocking, while the server-side tracking"
              + " is not. Omitting the timeout, or a timeout of -1, indicates"
              + " an infinite timeout. Known Issue: the server-side tracking"
              + " will immediately decommission if an RM HA failover occurs."))
          .put("-refreshNodesResources", new UsageInfo("",
              "Refresh resources of NodeManagers at the ResourceManager."))
          .put("-refreshSuperUserGroupsConfiguration", new UsageInfo("",
              "Refresh superuser proxy groups mappings"))
          .put("-refreshUserToGroupsMappings", new UsageInfo("",
              "Refresh user-to-groups mappings"))
          .put("-refreshAdminAcls", new UsageInfo("",
              "Refresh acls for administration of ResourceManager"))
          .put("-refreshServiceAcl", new UsageInfo("",
              "Reload the service-level authorization policy file. \n\t\t" +
                  "ResourceManager will reload the authorization policy file."))
          .put("-getGroups", new UsageInfo("[username]",
              "Get the groups which given user belongs to."))
          .put("-addToClusterNodeLabels",
              new UsageInfo("<\"label1(exclusive=true),"
                  + "label2(exclusive=false),label3\">",
                  "add to cluster node labels. Default exclusivity is true"))
          .put("-removeFromClusterNodeLabels",
              new UsageInfo("<label1,label2,label3> (label splitted by \",\")",
                  "remove from cluster node labels"))
          .put("-replaceLabelsOnNode",
              new UsageInfo(
                  "<\"node1[:port]=label1 node2[:port]=label2\"> "
                  + "[-failOnUnknownNodes] ",
              "replace labels on nodes"
                  + " (please note that we do not support specifying multiple"
                  + " labels on a single host for now.)\n\t\t"
                  + "[-failOnUnknownNodes] is optional, when we set this"
                  + " option, it will fail if specified nodes are unknown."))
          .put("-directlyAccessNodeLabelStore",
              new UsageInfo("", "This is DEPRECATED, will be removed in future releases. Directly access node label store, "
                  + "with this option, all node label related operations"
                  + " will not connect RM. Instead, they will"
                  + " access/modify stored node labels directly."
                  + " By default, it is false (access via RM)."
                  + " AND PLEASE NOTE: if you configured"
                  + " yarn.node-labels.fs-store.root-dir to a local directory"
                  + " (instead of NFS or HDFS), this option will only work"
                  +
                  " when the command run on the machine where RM is running."))
          .put("-refreshClusterMaxPriority",
              new UsageInfo("",
                  "Refresh cluster max priority"))
          .put("-updateNodeResource",
              new UsageInfo("[NodeID] [MemSize] [vCores] ([OvercommitTimeout])"
                  + " \n\t\tor\n\t\t[NodeID] [resourcetypes] "
                  + "([OvercommitTimeout]). ",
                  "Update resource on specific node."))
          .build();

  private final static Map<String, UsageInfo> YARN_HA_USAGE =
      ImmutableMap.<String, UsageInfo> builder()
          .put("-failover", new UsageInfo(
                  "<serviceId> <serviceId>",
                  "Failover from the first service to the second.\n"))
          .putAll(USAGE)
          .build();

  public RMAdminCLI() {
    super();
  }

  public RMAdminCLI(Configuration conf) {
    super(conf);
  }

  protected void setErrOut(PrintStream errOut) {
    this.errOut = errOut;
  }

  protected void setOut(PrintStream out) {
    this.out = out;
  }

  private static void appendHAUsage(final StringBuilder usageBuilder) {
    for (Map.Entry<String, UsageInfo> cmdEntry : YARN_HA_USAGE.entrySet()) {
      if (cmdEntry.getKey().equals("-help")) {
        continue;
      }
      UsageInfo usageInfo = cmdEntry.getValue();
      if (usageInfo.args == null) {
        usageBuilder.append(" [" + cmdEntry.getKey() + "]");
      } else {
        usageBuilder.append(" [" + cmdEntry.getKey() + " " + usageInfo.args
            + "]");
      }
    }
  }

  private static void buildHelpMsg(String cmd, StringBuilder builder) {
    UsageInfo usageInfo = ADMIN_USAGE.get(cmd);
    if (usageInfo == null) {
      usageInfo = YARN_HA_USAGE.get(cmd);
      if (usageInfo == null) {
        return;
      }
    }
    if (usageInfo.args == null) {
      builder.append("   " + cmd + ": " + usageInfo.help);
    } else {
      String space = (usageInfo.args == "") ? "" : " ";
      builder.append("   " + cmd + space + usageInfo.args + ": "
          + usageInfo.help);
    }
  }

  private static void buildIndividualUsageMsg(String cmd,
                                              StringBuilder builder ) {
    boolean isHACommand = false;
    UsageInfo usageInfo = ADMIN_USAGE.get(cmd);
    if (usageInfo == null) {
      usageInfo = YARN_HA_USAGE.get(cmd);
      if (usageInfo == null) {
        return;
      }
      isHACommand = true;
    }
    if (usageInfo.args == null) {
      builder.append("Usage: yarn rmadmin [" + cmd + "]\n");
    } else {
      String space = (usageInfo.args == "") ? "" : " ";
      builder.append("Usage: yarn rmadmin [" + cmd + space + usageInfo.args
          + "]\n");
    }
    if (isHACommand) {
      builder.append(cmd + " can only be used when RM HA is enabled");
    }
  }

  private static void buildUsageMsg(StringBuilder builder,
      boolean isHAEnabled) {
    builder.append("Usage: yarn rmadmin\n");
    for (Map.Entry<String, UsageInfo> cmdEntry : ADMIN_USAGE.entrySet()) {
      UsageInfo usageInfo = cmdEntry.getValue();
      builder.append("   " + cmdEntry.getKey() + " " + usageInfo.args + "\n");
    }
    if (isHAEnabled) {
      for (Map.Entry<String, UsageInfo> cmdEntry : YARN_HA_USAGE.entrySet()) {
        String cmdKey = cmdEntry.getKey();
        if (!cmdKey.equals("-help")) {
          UsageInfo usageInfo = cmdEntry.getValue();
          if (usageInfo.args == null) {
            builder.append("   " + cmdKey + "\n");
          } else {
            builder.append("   " + cmdKey + " " + usageInfo.args + "\n");
          }
        }
      }
    }
    builder.append("   -help" + " [cmd]\n");
  }

  private static void printHelp(String cmd, boolean isHAEnabled) {
    StringBuilder summary = new StringBuilder();
    summary.append("rmadmin is the command to execute YARN administrative " +
        "commands.\n");
    summary.append("The full syntax is: \n\n"
        + "yarn rmadmin"
        + " [-refreshQueues]"
        + " [-refreshNodes [-g|graceful [timeout in seconds] -client|server]]"
        + " [-refreshNodesResources]"
        + " [-refreshSuperUserGroupsConfiguration]"
        + " [-refreshUserToGroupsMappings]"
        + " [-refreshAdminAcls]"
        + " [-refreshServiceAcl]"
        + " [-getGroup [username]]"
        + " [-addToClusterNodeLabels <\"label1(exclusive=true),"
        + "label2(exclusive=false),label3\">]"
        + " [-removeFromClusterNodeLabels <label1,label2,label3>]"
        + " [-replaceLabelsOnNode "
        + "<\"node1[:port]=label1 node2[:port]=label2\"> "
        + "[-failOnUnknownNodes]]"
        + " [-directlyAccessNodeLabelStore]"
        + " [-refreshClusterMaxPriority]"
        + " [-updateNodeResource [NodeID] [MemSize] [vCores]"
        + " ([OvercommitTimeout]) or -updateNodeResource [NodeID] "
        + "[ResourceTypes] ([OvercommitTimeout])]");
    if (isHAEnabled) {
      appendHAUsage(summary);
    }
    summary.append(" [-help [cmd]]")
        .append("\n");

    StringBuilder helpBuilder = new StringBuilder();
    System.out.println(summary);
    for (String cmdKey : ADMIN_USAGE.keySet()) {
      buildHelpMsg(cmdKey, helpBuilder);
      helpBuilder.append("\n");
    }
    if (isHAEnabled) {
      for (String cmdKey : YARN_HA_USAGE.keySet()) {
        if (!cmdKey.equals("-help")) {
          buildHelpMsg(cmdKey, helpBuilder);
          helpBuilder.append("\n");
        }
      }
    }
    helpBuilder.append("   -help [cmd]: Displays help for the given command or all commands" +
        " if none is specified.");
    System.out.println(helpBuilder);
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd, boolean isHAEnabled) {
    StringBuilder usageBuilder = new StringBuilder();
    if (ADMIN_USAGE.containsKey(cmd) || YARN_HA_USAGE.containsKey(cmd)) {
      buildIndividualUsageMsg(cmd, usageBuilder);
    } else {
      buildUsageMsg(usageBuilder, isHAEnabled);
    }
    System.err.println(usageBuilder);
    ToolRunner.printGenericCommandUsage(System.err);

  }
  
  protected ResourceManagerAdministrationProtocol createAdminProtocol()
      throws IOException {
    // Get the current configuration
    final YarnConfiguration conf = new YarnConfiguration(getConf());
    return ClientRMProxy.createRMProxy(conf,
        ResourceManagerAdministrationProtocol.class);
  }

  private int refreshQueues(String subClusterId) throws IOException, YarnException {
    // Refresh the queue properties
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshQueuesRequest request = 
      recordFactory.newRecordInstance(RefreshQueuesRequest.class);
    if (StringUtils.isNotBlank(subClusterId)) {
      request.setSubClusterId(subClusterId);
    }
    adminProtocol.refreshQueues(request);
    return 0;
  }

  private int refreshNodes(boolean graceful, String subClusterId)
      throws IOException, YarnException {
    // Refresh the nodes
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshNodesRequest request = RefreshNodesRequest.newInstance(
        graceful? DecommissionType.GRACEFUL : DecommissionType.NORMAL);
    if (isYarnFederationEnabled(getConf()) && StringUtils.isNotBlank(subClusterId)) {
      request.setSubClusterId(subClusterId);
    }
    adminProtocol.refreshNodes(request);
    return 0;
  }

  private int refreshNodes(int timeout, String trackingMode, String subClusterId)
      throws IOException, YarnException {
    boolean serverTracking = !"client".equals(trackingMode);
    // Graceful decommissioning with timeout
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshNodesRequest gracefulRequest = RefreshNodesRequest
        .newInstance(DecommissionType.GRACEFUL, timeout);
    if (isYarnFederationEnabled(getConf()) && StringUtils.isNotBlank(subClusterId)) {
      gracefulRequest.setSubClusterId(subClusterId);
    }
    adminProtocol.refreshNodes(gracefulRequest);
    if (serverTracking) {
      return 0;
    }
    CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest = recordFactory
        .newRecordInstance(CheckForDecommissioningNodesRequest.class);
    if (isYarnFederationEnabled(getConf()) && StringUtils.isNotBlank(subClusterId)) {
      checkForDecommissioningNodesRequest.setSubClusterId(subClusterId);
    }
    long waitingTime;
    boolean nodesDecommissioning = true;
    // As RM enforces timeout automatically, client usually don't need
    // to forcefully decommission nodes upon timeout.
    // Here we let the client waits a small additional seconds so to avoid
    // unnecessary double decommission.
    final int gracePeriod = 5;
    // timeout=-1 means wait for all the nodes to be gracefully
    // decommissioned
    for (waitingTime = 0;
        timeout == -1 || (timeout >= 0 && waitingTime < timeout + gracePeriod);
        waitingTime++) {
      // wait for one second to check nodes decommissioning status
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // Ignore the InterruptedException
      }
      CheckForDecommissioningNodesResponse checkForDecommissioningNodes = adminProtocol
          .checkForDecommissioningNodes(checkForDecommissioningNodesRequest);
      Set<NodeId> decommissioningNodes = checkForDecommissioningNodes
          .getDecommissioningNodes();
      if (decommissioningNodes.isEmpty()) {
        nodesDecommissioning = false;
        break;
      } else {
        StringBuilder nodes = new StringBuilder();
        for (NodeId nodeId : decommissioningNodes) {
          nodes.append(nodeId).append(",");
        }
        nodes.deleteCharAt(nodes.length() - 1);
        System.out.println("Nodes '" + nodes + "' are still decommissioning.");
      }
    }
    if (nodesDecommissioning) {
      System.out.println("Graceful decommissioning not completed in " + timeout
          + " seconds, issuing forceful decommissioning command.");
      RefreshNodesRequest forcefulRequest = RefreshNodesRequest
          .newInstance(DecommissionType.FORCEFUL);
      if (isYarnFederationEnabled(getConf()) && StringUtils.isNotBlank(subClusterId)) {
        forcefulRequest.setSubClusterId(subClusterId);
      }
      adminProtocol.refreshNodes(forcefulRequest);
    } else {
      System.out.println("Graceful decommissioning completed in " + waitingTime
          + " seconds.");
    }
    return 0;
  }

  private int refreshNodesResources(String subClusterId)
      throws IOException, YarnException {
    // Refresh the resources at the Nodemanager
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshNodesResourcesRequest request =
        recordFactory.newRecordInstance(RefreshNodesResourcesRequest.class);
    if (StringUtils.isNotBlank(subClusterId)) {
      request.setSubClusterId(subClusterId);
    }
    adminProtocol.refreshNodesResources(request);
    return 0;
  }

  private int refreshNodes(String subClusterId) throws IOException, YarnException {
    return refreshNodes(false, subClusterId);
  }

  private int refreshUserToGroupsMappings(String subClusterId) throws IOException,
      YarnException {
    // Refresh the user-to-groups mappings
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshUserToGroupsMappingsRequest request = 
      recordFactory.newRecordInstance(RefreshUserToGroupsMappingsRequest.class);
    if (StringUtils.isNotBlank(subClusterId)) {
      request.setSubClusterId(subClusterId);
    }
    adminProtocol.refreshUserToGroupsMappings(request);
    return 0;
  }
  
  private int refreshSuperUserGroupsConfiguration(String subClusterId) throws IOException,
      YarnException {
    // Refresh the super-user groups
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshSuperUserGroupsConfigurationRequest request = 
      recordFactory.newRecordInstance(RefreshSuperUserGroupsConfigurationRequest.class);
    if (StringUtils.isNotBlank(subClusterId)) {
      request.setSubClusterId(subClusterId);
    }
    adminProtocol.refreshSuperUserGroupsConfiguration(request);
    return 0;
  }
  
  private int refreshAdminAcls(String subClusterId) throws IOException, YarnException {
    // Refresh the admin acls
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshAdminAclsRequest request = 
      recordFactory.newRecordInstance(RefreshAdminAclsRequest.class);
    if (StringUtils.isNotBlank(subClusterId)) {
      request.setSubClusterId(subClusterId);
    }
    adminProtocol.refreshAdminAcls(request);
    return 0;
  }
  
  private int refreshServiceAcls(String subClusterId) throws IOException, YarnException {
    // Refresh the service acls
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshServiceAclsRequest request = 
      recordFactory.newRecordInstance(RefreshServiceAclsRequest.class);
    if (StringUtils.isNotBlank(subClusterId)) {
      request.setSubClusterId(subClusterId);
    }
    adminProtocol.refreshServiceAcls(request);
    return 0;
  }

  private int refreshClusterMaxPriority(String subClusterId) throws IOException, YarnException {
    // Refresh cluster max priority
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshClusterMaxPriorityRequest request =
        recordFactory.newRecordInstance(RefreshClusterMaxPriorityRequest.class);
    if (StringUtils.isNotBlank(subClusterId)) {
      request.setSubClusterId(subClusterId);
    }
    adminProtocol.refreshClusterMaxPriority(request);
    return 0;
  }

  private int updateNodeResource(String nodeIdStr, Resource resource,
      int overCommitTimeout, String subClusterId) throws YarnException, IOException {

    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    UpdateNodeResourceRequest request =
      recordFactory.newRecordInstance(UpdateNodeResourceRequest.class);
    NodeId nodeId = NodeId.fromString(nodeIdStr);

    Map<NodeId, ResourceOption> resourceMap = new HashMap<>();
    resourceMap.put(
        nodeId, ResourceOption.newInstance(resource, overCommitTimeout));
    request.setNodeResourceMap(resourceMap);
    if (StringUtils.isNotBlank(subClusterId)) {
      request.setSubClusterId(subClusterId);
    }
    adminProtocol.updateNodeResource(request);
    return 0;
  }

  // complain negative value for cpu or memory.
  private boolean invalidResourceValue(int memValue, int coreValue) {
    return (memValue < 0) || (coreValue < 0);
  }

  private int getGroups(String[] usernames) throws IOException {
    // Get groups users belongs to
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();

    if (usernames.length == 0) {
      usernames = new String[] { UserGroupInformation.getCurrentUser().getUserName() };
    }
    
    for (String username : usernames) {
      StringBuilder sb = new StringBuilder();
      sb.append(username + " :");
      for (String group : adminProtocol.getGroupsForUser(username)) {
        sb.append(" ")
            .append(group);
      }
      System.out.println(sb);
    }
    
    return 0;
  }
  
  // Make it protected to make unit test can change it.
  protected static synchronized CommonNodeLabelsManager
      getNodeLabelManagerInstance(Configuration conf) {
    if (localNodeLabelsManager == null) {
      localNodeLabelsManager = new CommonNodeLabelsManager();
      localNodeLabelsManager.init(conf);
      localNodeLabelsManager.start();
    }
    return localNodeLabelsManager;
  }

  private Set<String> buildNodeLabelNamesFromStr(String args) {
    Set<String> labels = new HashSet<String>();
    for (String p : args.split(",")) {
      if (!p.trim().isEmpty()) {
        labels.add(p.trim());
      }
    }

    if (labels.isEmpty()) {
      throw new IllegalArgumentException(NO_LABEL_ERR_MSG);
    }
    return labels;
  }

  private int handleAddToClusterNodeLabels(String[] args, String cmd,
      boolean isHAEnabled) throws IOException, YarnException, ParseException {
    Options opts = new Options();
    opts.addOption("addToClusterNodeLabels", true,
        "Add to cluster node labels.");
    opts.addOption("directlyAccessNodeLabelStore", false,
        "Directly access node label store.");
    opts.addOption(OPTION_SUBCLUSTERID);

    int exitCode = -1;
    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.err.println(NO_LABEL_ERR_MSG);
      printUsage(args[0], isHAEnabled);
      return exitCode;
    }

    String subClusterId = parseSubClusterId(cliParser);
    List<NodeLabel> labels = YarnClientUtils.buildNodeLabelsFromStr(
        cliParser.getOptionValue("addToClusterNodeLabels"));
    if (cliParser.hasOption("directlyAccessNodeLabelStore")) {
      getNodeLabelManagerInstance(getConf()).addToCluserNodeLabels(labels);
    } else {
      ResourceManagerAdministrationProtocol adminProtocol =
          createAdminProtocol();
      AddToClusterNodeLabelsRequest request =
          AddToClusterNodeLabelsRequest.newInstance(labels);
      if (StringUtils.isNotBlank(subClusterId)) {
        request.setSubClusterId(subClusterId);
      }
      adminProtocol.addToClusterNodeLabels(request);
    }
    return 0;
  }

  private int handleRemoveFromClusterNodeLabels(String[] args, String cmd,
      boolean isHAEnabled) throws IOException, YarnException, ParseException {
    Options opts = new Options();
    opts.addOption("removeFromClusterNodeLabels", true,
        "Remove From cluster node labels.");
    opts.addOption("directlyAccessNodeLabelStore", false,
        "Directly access node label store.");
    opts.addOption(OPTION_SUBCLUSTERID);
    int exitCode = -1;
    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.err.println(NO_LABEL_ERR_MSG);
      printUsage(args[0], isHAEnabled);
      return exitCode;
    }

    String subClusterId = parseSubClusterId(cliParser);
    Set<String> labels = buildNodeLabelNamesFromStr(
        cliParser.getOptionValue("removeFromClusterNodeLabels"));
    if (cliParser.hasOption("directlyAccessNodeLabelStore")) {
      getNodeLabelManagerInstance(getConf()).removeFromClusterNodeLabels(
          labels);
    } else {
      ResourceManagerAdministrationProtocol adminProtocol =
          createAdminProtocol();
      RemoveFromClusterNodeLabelsRequest request =
          RemoveFromClusterNodeLabelsRequest.newInstance(labels);
      if(StringUtils.isNotBlank(subClusterId)) {
        request.setSubClusterId(subClusterId);
      }
      adminProtocol.removeFromClusterNodeLabels(request);
    }

    return 0;
  }
  
  private Map<NodeId, Set<String>> buildNodeLabelsMapFromStr(String args) {
    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();

    for (String nodeToLabels : args.split("[ \n]")) {
      nodeToLabels = nodeToLabels.trim();
      if (nodeToLabels.isEmpty() || nodeToLabels.startsWith("#")) {
        continue;
      }

      String[] splits = nodeToLabels.split("=");
      int labelsStartIndex = 0;
      String nodeIdStr = splits[0];

      if (splits.length == 2) {
        splits = splits[1].split(",");
      } else if (nodeToLabels.endsWith("=")) {
        //case where no labels are mapped to a node
        splits = new String[0];
      } else {
        // "," also supported for compatibility
        splits = nodeToLabels.split(",");
        nodeIdStr = splits[0];
        labelsStartIndex = 1;
      }

      Preconditions.checkArgument(!nodeIdStr.trim().isEmpty(),
          "node name cannot be empty");

      NodeId nodeId = ConverterUtils.toNodeIdWithDefaultPort(nodeIdStr);
      map.put(nodeId, new HashSet<String>());

      for (int i = labelsStartIndex; i < splits.length; i++) {
        if (!splits[i].trim().isEmpty()) {
          map.get(nodeId).add(splits[i].trim());
        }
      }
      
      int nLabels = map.get(nodeId).size();
      Preconditions.checkArgument(nLabels <= 1, "%s labels specified on host=%s"
          + ", please note that we do not support specifying multiple"
          + " labels on a single host for now.", nLabels, nodeIdStr);
    }

    if (map.isEmpty()) {
      throw new IllegalArgumentException(NO_MAPPING_ERR_MSG);
    }
    return map;
  }

  private int handleReplaceLabelsOnNodes(String[] args, String cmd,
      boolean isHAEnabled) throws IOException, YarnException, ParseException {
    Options opts = new Options();
    opts.addOption("replaceLabelsOnNode", true,
        "Replace label on node.");
    opts.addOption("failOnUnknownNodes", false,
        "Fail on unknown nodes.");
    opts.addOption("directlyAccessNodeLabelStore", false,
        "Directly access node label store.");
    opts.addOption(OPTION_SUBCLUSTERID);
    int exitCode = -1;
    CommandLine cliParser = null;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.err.println(NO_MAPPING_ERR_MSG);
      printUsage(args[0], isHAEnabled);
      return exitCode;
    }

    Map<NodeId, Set<String>> map = buildNodeLabelsMapFromStr(
        cliParser.getOptionValue("replaceLabelsOnNode"));
    String subClusterId = parseSubClusterId(cliParser);
    return replaceLabelsOnNodes(map,
        cliParser.hasOption("failOnUnknownNodes"),
        cliParser.hasOption("directlyAccessNodeLabelStore"), subClusterId);
  }

  private int replaceLabelsOnNodes(Map<NodeId, Set<String>> map,
      boolean failOnUnknownNodes, boolean directlyAccessNodeLabelStore,
      String subClusterId)
      throws IOException, YarnException {
    if (directlyAccessNodeLabelStore) {
      getNodeLabelManagerInstance(getConf()).replaceLabelsOnNode(map);
    } else {
      ResourceManagerAdministrationProtocol adminProtocol =
          createAdminProtocol();
      ReplaceLabelsOnNodeRequest request =
          ReplaceLabelsOnNodeRequest.newInstance(map);
      request.setFailOnUnknownNodes(failOnUnknownNodes);
      if (StringUtils.isNotBlank(subClusterId) && isYarnFederationEnabled(getConf())) {
        request.setSubClusterId(subClusterId);
      }
      adminProtocol.replaceLabelsOnNode(request);
    }
    return 0;
  }

  private int failover(CommandLine cmd)
      throws IOException, ServiceFailedException {
    int numOpts = cmd.getOptions() == null ? 0 : cmd.getOptions().length;
    final String[] args = cmd.getArgs();
    if (numOpts > 1 || args.length != 2) {
      errOut.println("failover: incorrect arguments");
      printUsage(errOut, "-failover", YARN_HA_USAGE);
      return -1;
    }
    HAServiceTarget fromNode = resolveTarget(args[0]);
    HAServiceTarget toNode = resolveTarget(args[1]);
    setRequestSource(HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED);
    HAServiceProtocol proto = fromNode.getProxy(
            getConf(), 0);
    HAServiceProtocolHelper.transitionToStandby(proto, createReqInfo());
    HAServiceProtocol proto1 = toNode.getProxy(
            getConf(), 0);
    HAServiceProtocolHelper.transitionToActive(proto1, createReqInfo());
    out.println("Failover from "+args[0]+" to "+args[1]+" successful");
    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    YarnConfiguration yarnConf =
        getConf() == null ? new YarnConfiguration() : new YarnConfiguration(
            getConf());
    boolean isHAEnabled =
        yarnConf.getBoolean(YarnConfiguration.RM_HA_ENABLED,
            YarnConfiguration.DEFAULT_RM_HA_ENABLED);

    if (args.length < 1) {
      printUsage("", isHAEnabled);
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = args[i++];

    exitCode = 0;
    if ("-help".equals(cmd)) {
      if (i < args.length) {
        printUsage(args[i], isHAEnabled);
      } else {
        printHelp("", isHAEnabled);
      }
      return exitCode;
    }

    if (USAGE.containsKey(cmd)) {
      if (isHAEnabled) {
        return super.run(args);
      }
      System.out.println("Cannot run " + cmd
          + " when ResourceManager HA is not enabled");
      return -1;
    }else if("-failover".equals(cmd)) {
      if (isHAEnabled) {
        Options opts = new Options();
        CommandLine cmdLine = parseOpts(cmd, opts, args, YARN_HA_USAGE);
        if (cmdLine == null) {
          return -1;
        }
        return failover(cmdLine);
      }
      System.out.println("Cannot run " + cmd
          + " when ResourceManager HA is not enabled");
      return -1;
    }

    //
    // verify that we have enough command line parameters
    //
    String subClusterId = StringUtils.EMPTY;
    if ("-refreshAdminAcls".equals(cmd) || "-refreshQueues".equals(cmd) ||
        "-refreshNodesResources".equals(cmd) ||
        "-refreshServiceAcl".equals(cmd) ||
        "-refreshUserToGroupsMappings".equals(cmd) ||
        "-refreshSuperUserGroupsConfiguration".equals(cmd) ||
        "-refreshClusterMaxPriority".equals(cmd)) {
      subClusterId = parseSubClusterId(args, isHAEnabled);
      // If we enable Federation mode, the number of args may be either one or three.
      // Example: -refreshQueues or -refreshQueues -subClusterId SC-1
      if (isYarnFederationEnabled(getConf()) && args.length != 1 && args.length != 3) {
        printUsage(cmd, isHAEnabled);
        return exitCode;
      } else if (!isYarnFederationEnabled(getConf()) && args.length != 1) {
        // If Federation mode is not enabled, then the number of args can only be one.
        // Example: -refreshQueues
        printUsage(cmd, isHAEnabled);
        return exitCode;
      }
    }

    // If it is federation mode, we will print federation mode information
    if (isYarnFederationEnabled(getConf())) {
      System.out.println("Using YARN Federation mode.");
    }

    try {
      if ("-refreshQueues".equals(cmd)) {
        exitCode = refreshQueues(subClusterId);
      } else if ("-refreshNodes".equals(cmd)) {
        exitCode = handleRefreshNodes(args, cmd, isHAEnabled);
      } else if ("-refreshNodesResources".equals(cmd)) {
        exitCode = refreshNodesResources(subClusterId);
      } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
        exitCode = refreshUserToGroupsMappings(subClusterId);
      } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
        exitCode = refreshSuperUserGroupsConfiguration(subClusterId);
      } else if ("-refreshAdminAcls".equals(cmd)) {
        exitCode = refreshAdminAcls(subClusterId);
      } else if ("-refreshServiceAcl".equals(cmd)) {
        exitCode = refreshServiceAcls(subClusterId);
      } else if ("-refreshClusterMaxPriority".equals(cmd)) {
        exitCode = refreshClusterMaxPriority(subClusterId);
      } else if ("-getGroups".equals(cmd)) {
        String[] usernames = Arrays.copyOfRange(args, i, args.length);
        exitCode = getGroups(usernames);
      } else if ("-updateNodeResource".equals(cmd)) {
        exitCode = handleUpdateNodeResource(args, cmd, isHAEnabled, subClusterId);
      } else if ("-addToClusterNodeLabels".equals(cmd)) {
        exitCode = handleAddToClusterNodeLabels(args, cmd, isHAEnabled);
      } else if ("-removeFromClusterNodeLabels".equals(cmd)) {
        exitCode = handleRemoveFromClusterNodeLabels(args, cmd, isHAEnabled);
      } else if ("-replaceLabelsOnNode".equals(cmd)) {
        exitCode = handleReplaceLabelsOnNodes(args, cmd, isHAEnabled);
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("", isHAEnabled);
      }

    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd, isHAEnabled);
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error message, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": "
                           + content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": "
                           + ex.getLocalizedMessage());
      }
    } catch (Exception e) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": "
                         + e.getLocalizedMessage());
    }
    if (null != localNodeLabelsManager) {
      localNodeLabelsManager.stop();
    }
    return exitCode;
  }

  // A helper method to reduce the number of lines of run()
  private int handleRefreshNodes(String[] args, String cmd, boolean isHAEnabled)
      throws IOException, YarnException, ParseException {
    Options opts = new Options();
    opts.addOption("refreshNodes", false,
        "Refresh the hosts information at the ResourceManager.");
    Option gracefulOpt = new Option("g", "graceful", true,
        "Wait for timeout before marking the NodeManager as decommissioned.");
    gracefulOpt.setOptionalArg(true);
    opts.addOption(gracefulOpt);
    opts.addOption("client", false,
        "Indicates the timeout tracking should be handled by the client.");
    opts.addOption("server", false,
        "Indicates the timeout tracking should be handled by the RM.");
    opts.addOption(OPTION_SUBCLUSTERID);

    int exitCode = -1;
    CommandLine cliParser = null;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.out.println("Missing argument for options");
      printUsage(args[0], isHAEnabled);
      return exitCode;
    }

    int timeout = -1;
    String subClusterId = parseSubClusterId(cliParser);
    if (cliParser.hasOption("g")) {
      String strTimeout = cliParser.getOptionValue("g");
      if (strTimeout != null) {
        timeout = validateTimeout(strTimeout);
      }
      String trackingMode = null;
      if (cliParser.hasOption("client")) {
        trackingMode = "client";
      } else if (cliParser.hasOption("server")) {
        trackingMode = "server";
      } else {
        printUsage(cmd, isHAEnabled);
        return -1;
      }
      return refreshNodes(timeout, trackingMode, subClusterId);
    } else {
      return refreshNodes(subClusterId);
    }
  }

  /**
   * Handle resources of two different formats:
   *
   * 1. -updateNodeResource [NodeID] [MemSize] [vCores] ([overCommitTimeout])
   * 2. -updateNodeResource [NodeID] [ResourceTypes] ([overCommitTimeout])
   *
   * Incase of No. of args is 4 or 5, 2nd arg should contain only numbers to
   * satisfy the 1st format. Otherwise, 2nd format flow continues.
   * @param args arguments of the command
   * @param cmd whole command to be parsed
   * @param isHAEnabled Is HA enabled or not?
   * @return 1 on success, -1 on errors
   * @throws IOException if any issues thrown from RPC layer
   * @throws YarnException if any issues thrown from server
   */
  private int handleUpdateNodeResource(
      String[] args, String cmd, boolean isHAEnabled, String subClusterId)
          throws YarnException, IOException {
    int i = 1;
    int overCommitTimeout = ResourceOption.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT;
    String nodeID = args[i++];
    Resource resource = Resource.newInstance(0, 0);
    if (args.length < 3 || args.length > 5) {
      System.err.println("Number of parameters specified for " +
          "updateNodeResource is wrong.");
      printUsage(cmd, isHAEnabled);
      return -1;
    } else if ((args.length == 4 || args.length == 5) &&
        RESOURCE_TYPES_ARGS_PATTERN.matcher(args[2]).matches()) {
      int memSize = Integer.parseInt(args[i++]);
      int cores = Integer.parseInt(args[i++]);

      // check resource value first
      if (invalidResourceValue(memSize, cores)) {
        throw new IllegalArgumentException("Invalid resource value: " + "(" +
            memSize + "," + cores + ") for updateNodeResource.");
      }
      resource = Resources.createResource(memSize, cores);
    } else {
      String resourceTypes = args[i++];
      if (!resourceTypes.contains("=")) {
        System.err.println("Resource Types parameter specified for "
            + "updateNodeResource is wrong. It should be comma-delimited "
            + "key value pairs. For example, memory-mb=1024Mi,"
            + "vcores=1,resource1=3Gi,resource2=2");
        printUsage(cmd, isHAEnabled);
        return -1;
      }
      resource = parseCommandAndCreateResource(resourceTypes);
      ResourceUtils.areMandatoryResourcesAvailable(resource);
    }
    if (i == args.length - 1) {
      overCommitTimeout = Integer.parseInt(args[i]);
    }
    return updateNodeResource(nodeID, resource, overCommitTimeout, subClusterId);
  }

  private Resource parseCommandAndCreateResource(String resourceTypes) {
    Resource resource = Resource.newInstance(0, 0);
    Map<String, ResourceInformation> resourceTypesFromRM =
        ResourceUtils.getResourceTypes();
    String[] resourceTypesArr = resourceTypes.split(",");
    for (int k = 0; k < resourceTypesArr.length; k++) {
      String resourceType = resourceTypesArr[k];
      String[] resourceTypeArray = resourceType.split("=");
      if (resourceTypeArray.length == 2) {
        String resName = StringUtils.trim(resourceTypeArray[0]);
        String resValue = StringUtils.trim(resourceTypeArray[1]);
        if (resourceTypesFromRM.containsKey(resName)) {
          String[] resourceValue = ResourceUtils.parseResourceValue(resValue);
          if (resourceValue.length == 2) {
            long value = Long.parseLong(resourceValue[1]);
            if (!resourceTypesFromRM.get(resName).getUnits()
                .equals(resourceValue[0])) {
              value = UnitsConversionUtil.convert(resourceValue[0],
                  resourceTypesFromRM.get(resName).getUnits(), value);
            }
            ResourceInformation ri = ResourceInformation.newInstance(resName,
                resourceValue[0], value);
            resource.setResourceInformation(resName, ri);
          } else {
            throw new IllegalArgumentException("Invalid resource value: " +
                resValue + ". Unable to extract unit and actual value.");
          }
        } else {
          throw new IllegalArgumentException("Invalid resource type: " +
              resName + ". Not allowed.");
        }
      } else {
        throw new IllegalArgumentException("Invalid resource type value: " +
            "("+ resourceType + ") for updateNodeResource. "
                + "It should be key value pairs separated using '=' symbol.");
      }
    }
    return resource;
  }

  private int validateTimeout(String strTimeout) {
    int timeout;
    try {
      timeout = Integer.parseInt(strTimeout);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(INVALID_TIMEOUT_ERR_MSG + strTimeout);
    }
    if (timeout < -1) {
      throw new IllegalArgumentException(INVALID_TIMEOUT_ERR_MSG + timeout);
    }
    return timeout;
  }

  private String validateTrackingMode(String mode) {
    if ("-client".equals(mode)) {
      return "client";
    }
    if ("-server".equals(mode)) {
      return "server";
    }
    throw new IllegalArgumentException("Invalid mode specified: " + mode);
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      conf = addSecurityConfiguration(conf);
    }
    super.setConf(conf);
  }

  /**
   * Add the requisite security principal settings to the given Configuration,
   * returning a copy.
   * @param conf the original config
   * @return a copy with the security settings added
   */
  private static Configuration addSecurityConfiguration(Configuration conf) {
    // Make a copy so we don't mutate it. Also use an YarnConfiguration to
    // force loading of yarn-site.xml.
    conf = new YarnConfiguration(conf);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        conf.get(YarnConfiguration.RM_PRINCIPAL, ""));
    return conf;
  }

  @Override
  protected HAServiceTarget resolveTarget(String rmId) {
    Collection<String> rmIds = HAUtil.getRMHAIds(getConf());
    if (!rmIds.contains(rmId)) {
      StringBuilder msg = new StringBuilder();
      msg.append(rmId + " is not a valid serviceId. It should be one of ");
      for (String id : rmIds) {
        msg.append(id + " ");
      }
      throw new IllegalArgumentException(msg.toString());
    }
    try {
      YarnConfiguration conf = new YarnConfiguration(getConf());
      conf.set(YarnConfiguration.RM_HA_ID, rmId);
      return new RMHAServiceTarget(conf);
    } catch (IllegalArgumentException iae) {
      throw new YarnRuntimeException("Could not connect to " + rmId +
          "; the configuration for it might be missing");
    } catch (IOException ioe) {
      throw new YarnRuntimeException(
          "Could not connect to RM HA Admin for node " + rmId);
    }
  }

  /**
   * returns the list of all resourcemanager ids for the given configuration.
   */
  @Override
  protected Collection<String> getTargetIds(String targetNodeToActivate) {
    return HAUtil.getRMHAIds(getConf());
  }

  @Override
  protected String getUsageString() {
    return "Usage: rmadmin";
  }

  /**
   * Parse subClusterId.
   * This method will only parse subClusterId when Yarn Federation mode is enabled.
   *
   * @param cliParser CommandLine.
   * @return subClusterId.
   */
  protected String parseSubClusterId(CommandLine cliParser) {
    // If YARN Federation mode is not enabled, return empty.
    if (!isYarnFederationEnabled(getConf())) {
      return StringUtils.EMPTY;
    }

    String subClusterId = cliParser.getOptionValue(OPTION_SUBCLUSTERID);
    if (StringUtils.isBlank(subClusterId)) {
      return StringUtils.EMPTY;
    }

    System.out.println("SubClusterId : " + subClusterId);
    return subClusterId;
  }

  protected String parseSubClusterId(String[] args, boolean isHAEnabled) {
    // If YARN Federation mode is not enabled, return empty.
    if (!isYarnFederationEnabled(getConf())) {
      return StringUtils.EMPTY;
    }

    Options opts = new Options();
    opts.addOption("refreshQueues", false,
        "Refresh the hosts information at the ResourceManager.");
    opts.addOption("refreshNodesResources", false, "");
    opts.addOption("refreshUserToGroupsMappings", false, "");
    opts.addOption("refreshUserToGroupsMappings", false, "");
    opts.addOption("updateNodeResource", false, "");
    opts.addOption(OPTION_SUBCLUSTERID);

    CommandLine cliParser = null;
    try {
      cliParser = new DefaultParser().parse(opts, args);
    } catch (ParseException ex) {
      System.err.println("parseSubClusterId error, " + ex.getMessage());
      printUsage(args[0], isHAEnabled);
    }

    return parseSubClusterId(cliParser);
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RMAdminCLI(), args);
    System.exit(result);
  }
}
