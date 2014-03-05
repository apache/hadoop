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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;

@Private
@Unstable
public class RMAdminCLI extends HAAdmin {

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  protected final static Map<String, UsageInfo> ADMIN_USAGE =
      ImmutableMap.<String, UsageInfo>builder()
          .put("-refreshQueues", new UsageInfo("",
              "Reload the queues' acls, states and scheduler specific " +
                  "properties. \n\t\tResourceManager will reload the " +
                  "mapred-queues configuration file."))
          .put("-refreshNodes", new UsageInfo("",
              "Refresh the hosts information at the ResourceManager."))
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
          .put("-help", new UsageInfo("[cmd]",
              "Displays help for the given command or all commands if none " +
                  "is specified."))
          .build();

  public RMAdminCLI() {
    super();
  }

  public RMAdminCLI(Configuration conf) {
    super(conf);
  }

  private static void appendHAUsage(final StringBuilder usageBuilder) {
    for (String cmdKey : USAGE.keySet()) {
      if (cmdKey.equals("-help")) {
        continue;
      }
      UsageInfo usageInfo = USAGE.get(cmdKey);
      usageBuilder.append(" [" + cmdKey + " " + usageInfo.args + "]");
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
    for (String cmdKey : ADMIN_USAGE.keySet()) {
      UsageInfo usageInfo = ADMIN_USAGE.get(cmdKey);
      builder.append("   " + cmdKey + " " + usageInfo.args + "\n");
    }
    if (isHAEnabled) {
      for (String cmdKey : USAGE.keySet()) {
        if (!cmdKey.equals("-help")) {
          UsageInfo usageInfo = USAGE.get(cmdKey);
          builder.append("   " + cmdKey + " " + usageInfo.args + "\n");
        }
      }
    }
  }

  private static void printHelp(String cmd, boolean isHAEnabled) {
    StringBuilder summary = new StringBuilder();
    summary.append("rmadmin is the command to execute YARN administrative " +
        "commands.\n");
    summary.append("The full syntax is: \n\n" +
    "yarn rmadmin" +
      " [-refreshQueues]" +
      " [-refreshNodes]" +
      " [-refreshSuperUserGroupsConfiguration]" +
      " [-refreshUserToGroupsMappings]" +
      " [-refreshAdminAcls]" +
      " [-refreshServiceAcl]" +
      " [-getGroup [username]]" +
      " [-help [cmd]]");
    if (isHAEnabled) {
      appendHAUsage(summary);
    }
    summary.append("\n");

    StringBuilder helpBuilder = new StringBuilder();
    System.out.println(summary);
    for (String cmdKey : ADMIN_USAGE.keySet()) {
      buildHelpMsg(cmdKey, helpBuilder);
      helpBuilder.append("\n");
    }
    if (isHAEnabled) {
      for (String cmdKey : USAGE.keySet()) {
        if (!cmdKey.equals("-help")) {
          buildHelpMsg(cmdKey, helpBuilder);
          helpBuilder.append("\n");
        }
      }
    }
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

  protected ResourceManagerAdministrationProtocol createAdminProtocol() throws IOException {
    // Get the current configuration
    final YarnConfiguration conf = new YarnConfiguration(getConf());
    return ClientRMProxy.createRMProxy(conf, ResourceManagerAdministrationProtocol.class);
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
    RefreshNodesRequest request = 
      recordFactory.newRecordInstance(RefreshNodesRequest.class);
    adminProtocol.refreshNodes(request);
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
    }

    //
    // verify that we have enough command line parameters
    //
    if ("-refreshAdminAcls".equals(cmd) || "-refreshQueues".equals(cmd) ||
        "-refreshNodes".equals(cmd) || "-refreshServiceAcl".equals(cmd) ||
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
        exitCode = refreshNodes();
      } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
        exitCode = refreshUserToGroupsMappings();
      } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
        exitCode = refreshSuperUserGroupsConfiguration();
      } else if ("-refreshAdminAcls".equals(cmd)) {
        exitCode = refreshAdminAcls();
      } else if ("-refreshServiceAcl".equals(cmd)) {
        exitCode = refreshServiceAcls();
      } else if ("-getGroups".equals(cmd)) {
        String[] usernames = Arrays.copyOfRange(args, i, args.length);
        exitCode = getGroups(usernames);
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
      // out the first line of the error mesage, ignore the stack trace.
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
    return exitCode;
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

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RMAdminCLI(), args);
    System.exit(result);
  }
}
