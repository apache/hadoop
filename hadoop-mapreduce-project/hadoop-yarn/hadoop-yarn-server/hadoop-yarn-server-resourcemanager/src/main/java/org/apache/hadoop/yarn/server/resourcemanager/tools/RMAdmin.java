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

package org.apache.hadoop.yarn.server.resourcemanager.tools;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.admin.AdminSecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.api.RMAdminProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshUserToGroupsMappingsRequest;

public class RMAdmin extends Configured implements Tool {

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public RMAdmin() {
    super();
  }

  public RMAdmin(Configuration conf) {
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
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(refreshQueues);
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
    } else {
      System.err.println("Usage: java RMAdmin");
      System.err.println("           [-refreshQueues]");
      System.err.println("           [-refreshNodes]");
      System.err.println("           [-refreshUserToGroupsMappings]");
      System.err.println("           [-refreshSuperUserGroupsConfiguration]");
      System.err.println("           [-refreshAdminAcls]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  private static UserGroupInformation getUGI(Configuration conf
  ) throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  private RMAdminProtocol createAdminProtocol() throws IOException {
    // Get the current configuration
    final YarnConfiguration conf = new YarnConfiguration(getConf());

    // Create the client
    final String adminAddress =
      conf.get(RMConfig.ADMIN_ADDRESS,
          RMConfig.DEFAULT_ADMIN_BIND_ADDRESS);
    final YarnRPC rpc = YarnRPC.create(conf);
    
    if (UserGroupInformation.isSecurityEnabled()) {
      conf.setClass(YarnConfiguration.YARN_SECURITY_INFO,
          AdminSecurityInfo.class, SecurityInfo.class);
    }
    
    RMAdminProtocol adminProtocol =
      getUGI(conf).doAs(new PrivilegedAction<RMAdminProtocol>() {
        @Override
        public RMAdminProtocol run() {
          return (RMAdminProtocol) rpc.getProxy(RMAdminProtocol.class,
              NetUtils.createSocketAddr(adminAddress), conf);
        }
      });

    return adminProtocol;
  }
  
  private int refreshQueues() throws IOException {
    // Refresh the queue properties
    RMAdminProtocol adminProtocol = createAdminProtocol();
    RefreshQueuesRequest request = 
      recordFactory.newRecordInstance(RefreshQueuesRequest.class);
    adminProtocol.refreshQueues(request);
    return 0;
  }

  private int refreshNodes() throws IOException {
    // Refresh the nodes
    RMAdminProtocol adminProtocol = createAdminProtocol();
    RefreshNodesRequest request = 
      recordFactory.newRecordInstance(RefreshNodesRequest.class);
    adminProtocol.refreshNodes(request);
    return 0;
  }
  
  private int refreshUserToGroupsMappings() throws IOException {
    // Refresh the user-to-groups mappings
    RMAdminProtocol adminProtocol = createAdminProtocol();
    RefreshUserToGroupsMappingsRequest request = 
      recordFactory.newRecordInstance(RefreshUserToGroupsMappingsRequest.class);
    adminProtocol.refreshUserToGroupsMappings(request);
    return 0;
  }
  
  private int refreshSuperUserGroupsConfiguration() throws IOException {
    // Refresh the super-user groups
    RMAdminProtocol adminProtocol = createAdminProtocol();
    RefreshSuperUserGroupsConfigurationRequest request = 
      recordFactory.newRecordInstance(RefreshSuperUserGroupsConfigurationRequest.class);
    adminProtocol.refreshSuperUserGroupsConfiguration(request);
    return 0;
  }
  
  private int refreshAdminAcls() throws IOException {
    // Refresh the admin acls
    RMAdminProtocol adminProtocol = createAdminProtocol();
    RefreshAdminAclsRequest request = 
      recordFactory.newRecordInstance(RefreshAdminAclsRequest.class);
    adminProtocol.refreshAdminAcls(request);
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
        "-refreshNodes".equals(cmd) ||
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
    int result = ToolRunner.run(new RMAdmin(), args);
    System.exit(result);
  }
}
