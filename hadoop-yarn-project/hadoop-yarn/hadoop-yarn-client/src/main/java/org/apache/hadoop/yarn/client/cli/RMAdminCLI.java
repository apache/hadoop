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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
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
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Private
@Unstable
public class RMAdminCLI extends HAAdmin {

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);
  private boolean directlyAccessNodeLabelStore = false;
  static CommonNodeLabelsManager localNodeLabelsManager = null;
  private static final String NO_LABEL_ERR_MSG =
      "No cluster node-labels are specified";
  private static final String NO_MAPPING_ERR_MSG =
      "No node-to-labels mappings are specified";
  private static final String INVALID_TIMEOUT_ERR_MSG =
      "Invalid timeout specified : ";
  private static final String ADD_LABEL_FORMAT_ERR_MSG =
      "Input format for adding node-labels is not correct, it should be "
          + "labelName1[(exclusive=true/false)],LabelName2[] ..";

  protected final static Map<String, UsageInfo> ADMIN_USAGE =
      ImmutableMap.<String, UsageInfo>builder()
          .put("-refreshQueues", new UsageInfo("",
              "Reload the queues' acls, states and scheduler specific " +
                  "properties. \n\t\tResourceManager will reload the " +
                  "mapred-queues configuration file."))
          .put("-refreshNodes", new UsageInfo("[-g [timeout in seconds]]",
              "Refresh the hosts information at the ResourceManager. Here "
              + "[-g [timeout in seconds] is optional, if we specify the "
              + "timeout then ResourceManager will wait for timeout before "
              + "marking the NodeManager as decommissioned."))
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
                  "ResoureceManager will reload the authorization policy file."))
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
                  "<\"node1[:port]=label1,label2 node2[:port]=label1,label2\">",
                  "replace labels on nodes"
                      + " (please note that we do not support specifying multiple"
                      + " labels on a single host for now.)"))
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
              new UsageInfo("[NodeID] [MemSize] [vCores] ([OvercommitTimeout])",
                  "Update resource on specific node."))
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

  private static void appendHAUsage(final StringBuilder usageBuilder) {
    for (Map.Entry<String,UsageInfo> cmdEntry : USAGE.entrySet()) {
      if (cmdEntry.getKey().equals("-help")
          || cmdEntry.getKey().equals("-failover")) {
        continue;
      }
      UsageInfo usageInfo = cmdEntry.getValue();
      usageBuilder.append(" [" + cmdEntry.getKey() + " " + usageInfo.args + "]");
    }
  }

  private static void buildHelpMsg(String cmd, StringBuilder builder) {
    UsageInfo usageInfo = ADMIN_USAGE.get(cmd);
    if (usageInfo == null) {
      usageInfo = USAGE.get(cmd);
      if (usageInfo == null) {
        return;
      }
    }
    String space = (usageInfo.args == "") ? "" : " ";
    builder.append("   " + cmd + space + usageInfo.args + ": " +
        usageInfo.help);
  }

  private static void buildIndividualUsageMsg(String cmd,
                                              StringBuilder builder ) {
    boolean isHACommand = false;
    UsageInfo usageInfo = ADMIN_USAGE.get(cmd);
    if (usageInfo == null) {
      usageInfo = USAGE.get(cmd);
      if (usageInfo == null) {
        return;
      }
      isHACommand = true;
    }
    String space = (usageInfo.args == "") ? "" : " ";
    builder.append("Usage: yarn rmadmin ["
        + cmd + space + usageInfo.args
        + "]\n");
    if (isHACommand) {
      builder.append(cmd + " can only be used when RM HA is enabled");
    }
  }

  private static void buildUsageMsg(StringBuilder builder,
      boolean isHAEnabled) {
    builder.append("Usage: yarn rmadmin\n");
    for (Map.Entry<String,UsageInfo> cmdEntry : ADMIN_USAGE.entrySet()) {
      UsageInfo usageInfo = cmdEntry.getValue();
      builder.append("   " + cmdEntry.getKey() + " " + usageInfo.args + "\n");
    }
    if (isHAEnabled) {
      for (Map.Entry<String,UsageInfo> cmdEntry : USAGE.entrySet()) {
        String cmdKey = cmdEntry.getKey();
        if (!cmdKey.equals("-help")) {
          UsageInfo usageInfo = cmdEntry.getValue();
          builder.append("   " + cmdKey + " " + usageInfo.args + "\n");
        }
      }
    }
    builder.append("   -help" + " [cmd]\n");
  }

  private static void printHelp(String cmd, boolean isHAEnabled) {
    StringBuilder summary = new StringBuilder();
    summary.append("rmadmin is the command to execute YARN administrative " +
        "commands.\n");
    summary.append("The full syntax is: \n\n" +
    "yarn rmadmin" +
      " [-refreshQueues]" +
      " [-refreshNodes [-g [timeout in seconds]]]" +
      " [-refreshNodesResources]" +
      " [-refreshSuperUserGroupsConfiguration]" +
      " [-refreshUserToGroupsMappings]" +
      " [-refreshAdminAcls]" +
      " [-refreshServiceAcl]" +
      " [-getGroup [username]]" +
      " [-addToClusterNodeLabels <\"label1(exclusive=true),"
                  + "label2(exclusive=false),label3\">]" +
      " [-removeFromClusterNodeLabels <label1,label2,label3>]" +
      " [-replaceLabelsOnNode <\"node1[:port]=label1,label2 node2[:port]=label1\">]" +
      " [-directlyAccessNodeLabelStore]" +
      " [-updateNodeResource [NodeID] [MemSize] [vCores] ([OvercommitTimeout])");
    if (isHAEnabled) {
      appendHAUsage(summary);
    }
    summary.append(" [-help [cmd]]");
    summary.append("\n");

    StringBuilder helpBuilder = new StringBuilder();
    System.out.println(summary);
    for (String cmdKey : ADMIN_USAGE.keySet()) {
      buildHelpMsg(cmdKey, helpBuilder);
      helpBuilder.append("\n");
    }
    if (isHAEnabled) {
      for (String cmdKey : USAGE.keySet()) {
        if (!cmdKey.equals("-help") && !cmdKey.equals("-failover")) {
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
    if (ADMIN_USAGE.containsKey(cmd) || USAGE.containsKey(cmd)) {
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
  
  private int refreshQueues() throws IOException, YarnException {
    // Refresh the queue properties
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshQueuesRequest request = 
      recordFactory.newRecordInstance(RefreshQueuesRequest.class);
    adminProtocol.refreshQueues(request);
    return 0;
  }

  private int refreshNodes() throws IOException, YarnException {
    // Refresh the nodes
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshNodesRequest request = RefreshNodesRequest
        .newInstance(DecommissionType.NORMAL);
    adminProtocol.refreshNodes(request);
    return 0;
  }

  private int refreshNodes(long timeout) throws IOException, YarnException {
    // Graceful decommissioning with timeout
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshNodesRequest gracefulRequest = RefreshNodesRequest
        .newInstance(DecommissionType.GRACEFUL);
    adminProtocol.refreshNodes(gracefulRequest);
    CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest = recordFactory
        .newRecordInstance(CheckForDecommissioningNodesRequest.class);
    long waitingTime;
    boolean nodesDecommissioning = true;
    // timeout=-1 means wait for all the nodes to be gracefully
    // decommissioned
    for (waitingTime = 0; waitingTime < timeout || timeout == -1; waitingTime++) {
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
          + " seconds, issueing forceful decommissioning command.");
      RefreshNodesRequest forcefulRequest = RefreshNodesRequest
          .newInstance(DecommissionType.FORCEFUL);
      adminProtocol.refreshNodes(forcefulRequest);
    } else {
      System.out.println("Graceful decommissioning completed in " + waitingTime
          + " seconds.");
    }
    return 0;
  }

  private int refreshNodesResources() throws IOException, YarnException {
    // Refresh the resources at the Nodemanager
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshNodesResourcesRequest request =
    recordFactory.newRecordInstance(RefreshNodesResourcesRequest.class);
    adminProtocol.refreshNodesResources(request);
    return 0;
  }

  private int refreshUserToGroupsMappings() throws IOException,
      YarnException {
    // Refresh the user-to-groups mappings
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshUserToGroupsMappingsRequest request = 
      recordFactory.newRecordInstance(RefreshUserToGroupsMappingsRequest.class);
    adminProtocol.refreshUserToGroupsMappings(request);
    return 0;
  }
  
  private int refreshSuperUserGroupsConfiguration() throws IOException,
      YarnException {
    // Refresh the super-user groups
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshSuperUserGroupsConfigurationRequest request = 
      recordFactory.newRecordInstance(RefreshSuperUserGroupsConfigurationRequest.class);
    adminProtocol.refreshSuperUserGroupsConfiguration(request);
    return 0;
  }
  
  private int refreshAdminAcls() throws IOException, YarnException {
    // Refresh the admin acls
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshAdminAclsRequest request = 
      recordFactory.newRecordInstance(RefreshAdminAclsRequest.class);
    adminProtocol.refreshAdminAcls(request);
    return 0;
  }
  
  private int refreshServiceAcls() throws IOException, YarnException {
    // Refresh the service acls
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshServiceAclsRequest request = 
      recordFactory.newRecordInstance(RefreshServiceAclsRequest.class);
    adminProtocol.refreshServiceAcls(request);
    return 0;
  }

  private int refreshClusterMaxPriority() throws IOException, YarnException {
    // Refresh cluster max priority
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshClusterMaxPriorityRequest request =
        recordFactory.newRecordInstance(RefreshClusterMaxPriorityRequest.class);
    adminProtocol.refreshClusterMaxPriority(request);
    return 0;
  }

  private int updateNodeResource(String nodeIdStr, int memSize,
      int cores, int overCommitTimeout) throws IOException, YarnException {
    // check resource value first
    if (invalidResourceValue(memSize, cores)) {
      throw new IllegalArgumentException("Invalid resource value: " + "(" +
          memSize + "," + cores + ") for updateNodeResource.");
    }
    // Refresh the nodes
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    UpdateNodeResourceRequest request =
      recordFactory.newRecordInstance(UpdateNodeResourceRequest.class);
    NodeId nodeId = NodeId.fromString(nodeIdStr);
    
    Resource resource = Resources.createResource(memSize, cores);
    Map<NodeId, ResourceOption> resourceMap =
        new HashMap<NodeId, ResourceOption>();
    resourceMap.put(
        nodeId, ResourceOption.newInstance(resource, overCommitTimeout));
    request.setNodeResourceMap(resourceMap);
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
        sb.append(" ");
        sb.append(group);
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
  
  private List<NodeLabel> buildNodeLabelsFromStr(String args) {
    List<NodeLabel> nodeLabels = new ArrayList<>();
    for (String p : args.split(",")) {
      if (!p.trim().isEmpty()) {
        String labelName = p;

        // Try to parse exclusive
        boolean exclusive = NodeLabel.DEFAULT_NODE_LABEL_EXCLUSIVITY;
        int leftParenthesisIdx = p.indexOf("(");
        int rightParenthesisIdx = p.indexOf(")");

        if ((leftParenthesisIdx == -1 && rightParenthesisIdx != -1)
            || (leftParenthesisIdx != -1 && rightParenthesisIdx == -1)) {
          // Parenthese not match
          throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
        }

        if (leftParenthesisIdx > 0 && rightParenthesisIdx > 0) {
          if (leftParenthesisIdx > rightParenthesisIdx) {
            // Parentese not match
            throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
          }

          String property = p.substring(p.indexOf("(") + 1, p.indexOf(")"));
          if (property.contains("=")) {
            String key = property.substring(0, property.indexOf("=")).trim();
            String value =
                property
                    .substring(property.indexOf("=") + 1, property.length())
                    .trim();

            // Now we only support one property, which is exclusive, so check if
            // key = exclusive and value = {true/false}
            if (key.equals("exclusive")
                && ImmutableSet.of("true", "false").contains(value)) {
              exclusive = Boolean.parseBoolean(value);
            } else {
              throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
            }
          } else if (!property.trim().isEmpty()) {
            throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
          }
        }

        // Try to get labelName if there's "(..)"
        if (labelName.contains("(")) {
          labelName = labelName.substring(0, labelName.indexOf("(")).trim();
        }

        nodeLabels.add(NodeLabel.newInstance(labelName, exclusive));
      }
    }

    if (nodeLabels.isEmpty()) {
      throw new IllegalArgumentException(NO_LABEL_ERR_MSG);
    }
    return nodeLabels;
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

  private int addToClusterNodeLabels(String args) throws IOException,
      YarnException {
    List<NodeLabel> labels = buildNodeLabelsFromStr(args);

    if (directlyAccessNodeLabelStore) {
      getNodeLabelManagerInstance(getConf()).addToCluserNodeLabels(labels);
    } else {
      ResourceManagerAdministrationProtocol adminProtocol =
          createAdminProtocol();
      AddToClusterNodeLabelsRequest request =
          AddToClusterNodeLabelsRequest.newInstance(labels);
      adminProtocol.addToClusterNodeLabels(request);
    }
    return 0;
  }

  private int removeFromClusterNodeLabels(String args) throws IOException,
      YarnException {
    Set<String> labels = buildNodeLabelNamesFromStr(args);

    if (directlyAccessNodeLabelStore) {
      getNodeLabelManagerInstance(getConf()).removeFromClusterNodeLabels(
          labels);
    } else {
      ResourceManagerAdministrationProtocol adminProtocol =
          createAdminProtocol();
      RemoveFromClusterNodeLabelsRequest request =
          RemoveFromClusterNodeLabelsRequest.newInstance(labels);
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
      Preconditions.checkArgument(nLabels <= 1, "%d labels specified on host=%s"
          + ", please note that we do not support specifying multiple"
          + " labels on a single host for now.", nLabels, nodeIdStr);
    }

    if (map.isEmpty()) {
      throw new IllegalArgumentException(NO_MAPPING_ERR_MSG);
    }
    return map;
  }

  private int replaceLabelsOnNodes(String args) throws IOException,
      YarnException {
    Map<NodeId, Set<String>> map = buildNodeLabelsMapFromStr(args);
    return replaceLabelsOnNodes(map);
  }

  private int replaceLabelsOnNodes(Map<NodeId, Set<String>> map)
      throws IOException, YarnException {
    if (directlyAccessNodeLabelStore) {
      getNodeLabelManagerInstance(getConf()).replaceLabelsOnNode(map);
    } else {
      ResourceManagerAdministrationProtocol adminProtocol =
          createAdminProtocol();
      ReplaceLabelsOnNodeRequest request =
          ReplaceLabelsOnNodeRequest.newInstance(map);
      adminProtocol.replaceLabelsOnNode(request);
    }
    return 0;
  }
  
  @Override
  public int run(String[] args) throws Exception {
    // -directlyAccessNodeLabelStore is a additional option for node label
    // access, so just search if we have specified this option, and remove it
    List<String> argsList = new ArrayList<String>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-directlyAccessNodeLabelStore")) {
        directlyAccessNodeLabelStore = true;
      } else {
        argsList.add(args[i]);
      }
    }
    args = argsList.toArray(new String[0]);
    
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
    }

    //
    // verify that we have enough command line parameters
    //
    if ("-refreshAdminAcls".equals(cmd) || "-refreshQueues".equals(cmd) ||
        "-refreshNodesResources".equals(cmd) ||
        "-refreshServiceAcl".equals(cmd) ||
        "-refreshUserToGroupsMappings".equals(cmd) ||
        "-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      if (args.length != 1) {
        printUsage(cmd, isHAEnabled);
        return exitCode;
      }
    }
    
    try {
      if ("-refreshQueues".equals(cmd)) {
        exitCode = refreshQueues();
      } else if ("-refreshNodes".equals(cmd)) {
        if (args.length == 1) {
          exitCode = refreshNodes();
        } else if (args.length == 3) {
          // if the graceful timeout specified
          if ("-g".equals(args[1])) {
            long timeout = validateTimeout(args[2]);
            exitCode = refreshNodes(timeout);
          } else {
            printUsage(cmd, isHAEnabled);
            return -1;
          }
        } else {
          printUsage(cmd, isHAEnabled);
          return -1;
        }
      } else if ("-refreshNodesResources".equals(cmd)) {
        exitCode = refreshNodesResources();
      } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
        exitCode = refreshUserToGroupsMappings();
      } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
        exitCode = refreshSuperUserGroupsConfiguration();
      } else if ("-refreshAdminAcls".equals(cmd)) {
        exitCode = refreshAdminAcls();
      } else if ("-refreshServiceAcl".equals(cmd)) {
        exitCode = refreshServiceAcls();
      } else if ("-refreshClusterMaxPriority".equals(cmd)) {
        exitCode = refreshClusterMaxPriority();
      } else if ("-getGroups".equals(cmd)) {
        String[] usernames = Arrays.copyOfRange(args, i, args.length);
        exitCode = getGroups(usernames);
      } else if ("-updateNodeResource".equals(cmd)) {
        if (args.length < 4 || args.length > 5) {
          System.err.println("Number of parameters specified for " +
              "updateNodeResource is wrong.");
          printUsage(cmd, isHAEnabled);
          exitCode = -1;
        } else {
          String nodeID = args[i++];
          String memSize = args[i++];
          String cores = args[i++];
          int overCommitTimeout = ResourceOption.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT;
          if (i == args.length - 1) {
            overCommitTimeout = Integer.parseInt(args[i]);
          }
          exitCode = updateNodeResource(nodeID, Integer.parseInt(memSize),
              Integer.parseInt(cores), overCommitTimeout);
        }
      } else if ("-addToClusterNodeLabels".equals(cmd)) {
        if (i >= args.length) {
          System.err.println(NO_LABEL_ERR_MSG);
          printUsage("", isHAEnabled);
          exitCode = -1;
        } else {
          exitCode = addToClusterNodeLabels(args[i]);
        }
      } else if ("-removeFromClusterNodeLabels".equals(cmd)) {
        if (i >= args.length) {
          System.err.println(NO_LABEL_ERR_MSG);
          printUsage("", isHAEnabled);
          exitCode = -1;
        } else {
          exitCode = removeFromClusterNodeLabels(args[i]);
        }
      } else if ("-replaceLabelsOnNode".equals(cmd)) {
        if (i >= args.length) {
          System.err.println(NO_MAPPING_ERR_MSG);
          printUsage("", isHAEnabled);
          exitCode = -1;
        } else {
          exitCode = replaceLabelsOnNodes(args[i]);
        }
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

  private long validateTimeout(String strTimeout) {
    long timeout;
    try {
      timeout = Long.parseLong(strTimeout);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(INVALID_TIMEOUT_ERR_MSG + strTimeout);
    }
    if (timeout < -1) {
      throw new IllegalArgumentException(INVALID_TIMEOUT_ERR_MSG + timeout);
    }
    return timeout;
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

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RMAdminCLI(), args);
    System.exit(result);
  }
}
