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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
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
public class RMAdminCLI extends Configured implements Tool {

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public RMAdminCLI() {
    super();
  }

  public RMAdminCLI(Configuration conf) {
    super(conf);
  }

  private static void printHelp(String cmd) {
    String summary = "rmadmin is the command to execute Map-Reduce administrative commands.\n" +
    "The full syntax is: \n\n" +
    "hadoop rmadmin" +
      " [-refreshQueues]" +
      " [-refreshNodes]" +
      " [-refreshSuperUserGroupsConfiguration]" +
      " [-refreshUserToGroupsMappings]" +
      " [-refreshAdminAcls]" +
      " [-refreshServiceAcl]" +
      " [-getGroup [username]]" +
      " [-help [cmd]]\n";

    String refreshQueues =
      "-refreshQueues: Reload the queues' acls, states and "
      + "scheduler specific properties.\n"
      + "\t\tResourceManager will reload the mapred-queues configuration file.\n";

    String refreshNodes = 
      "-refreshNodes: Refresh the hosts information at the ResourceManager.\n";
    
    String refreshUserToGroupsMappings = 
      "-refreshUserToGroupsMappings: Refresh user-to-groups mappings\n";
    
    String refreshSuperUserGroupsConfiguration = 
      "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings\n";

    String refreshAdminAcls =
      "-refreshAdminAcls: Refresh acls for administration of ResourceManager\n";

    String refreshServiceAcl = 
      "-refreshServiceAcl: Reload the service-level authorization policy file\n" +
      "\t\tResoureceManager will reload the authorization policy file.\n";
    
    String getGroups = 
      "-getGroups [username]: Get the groups which given user belongs to\n";

    String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n" +
      "\t\tis specified.\n";

    if ("refreshQueues".equals(cmd)) {
      System.out.println(refreshQueues);
    }  else if ("refreshNodes".equals(cmd)) {
      System.out.println(refreshNodes);
    } else if ("refreshUserToGroupsMappings".equals(cmd)) {
      System.out.println(refreshUserToGroupsMappings);
    } else if ("refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.out.println(refreshSuperUserGroupsConfiguration);
    } else if ("refreshAdminAcls".equals(cmd)) {
      System.out.println(refreshAdminAcls);
    } else if ("refreshServiceAcl".equals(cmd)) {
      System.out.println(refreshServiceAcl);
    } else if ("getGroups".equals(cmd)) {
      System.out.println(getGroups);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(refreshQueues);
      System.out.println(refreshNodes);
      System.out.println(refreshUserToGroupsMappings);
      System.out.println(refreshSuperUserGroupsConfiguration);
      System.out.println(refreshAdminAcls);
      System.out.println(refreshServiceAcl);
      System.out.println(getGroups);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-refreshQueues".equals(cmd)) {
      System.err.println("Usage: java RMAdmin" + " [-refreshQueues]");
    } else if ("-refreshNodes".equals(cmd)){
      System.err.println("Usage: java RMAdmin" + " [-refreshNodes]");
    } else if ("-refreshUserToGroupsMappings".equals(cmd)){
      System.err.println("Usage: java RMAdmin" + " [-refreshUserToGroupsMappings]");
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)){
      System.err.println("Usage: java RMAdmin" + " [-refreshSuperUserGroupsConfiguration]");
    } else if ("-refreshAdminAcls".equals(cmd)){
      System.err.println("Usage: java RMAdmin" + " [-refreshAdminAcls]");
    } else if ("-refreshService".equals(cmd)){
      System.err.println("Usage: java RMAdmin" + " [-refreshServiceAcl]");
    } else if ("-getGroups".equals(cmd)){
      System.err.println("Usage: java RMAdmin" + " [-getGroups [username]]");
    } else {
      System.err.println("Usage: java RMAdmin");
      System.err.println("           [-refreshQueues]");
      System.err.println("           [-refreshNodes]");
      System.err.println("           [-refreshUserToGroupsMappings]");
      System.err.println("           [-refreshSuperUserGroupsConfiguration]");
      System.err.println("           [-refreshAdminAcls]");
      System.err.println("           [-refreshServiceAcl]");
      System.err.println("           [-getGroups [username]]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
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
    if (args.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = args[i++];
    //
    // verify that we have enough command line parameters
    //
    if ("-refreshAdminAcls".equals(cmd) || "-refreshQueues".equals(cmd) ||
        "-refreshNodes".equals(cmd) || "-refreshServiceAcl".equals(cmd) ||
        "-refreshUserToGroupsMappings".equals(cmd) ||
        "-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      if (args.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    }
    
    exitCode = 0;
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
      } else if ("-help".equals(cmd)) {
        if (i < args.length) {
          printUsage(args[i]);
        } else {
          printHelp("");
        }
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
        printUsage("");
      }

    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
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

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RMAdminCLI(), args);
    System.exit(result);
  }
}
