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

package org.apache.hadoop.mapreduce.v2.hs.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.hs.HSProxies;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Private
public class HSAdmin extends Configured implements Tool {

  public HSAdmin() {
    super();
  }

  public HSAdmin(JobConf conf) {
    super(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      conf = addSecurityConfiguration(conf);
    }
    super.setConf(conf);
  }

  private Configuration addSecurityConfiguration(Configuration conf) {
    conf = new JobConf(conf);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        conf.get(JHAdminConfig.MR_HISTORY_PRINCIPAL, ""));
    return conf;
  }

  /**
   * Displays format of commands.
   * 
   * @param cmd
   *          The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-refreshUserToGroupsMappings".equals(cmd)) {
      System.err
          .println("Usage: mapred hsadmin [-refreshUserToGroupsMappings]");
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.err
          .println("Usage: mapred hsadmin [-refreshSuperUserGroupsConfiguration]");
    } else if ("-refreshAdminAcls".equals(cmd)) {
      System.err.println("Usage: mapred hsadmin [-refreshAdminAcls]");
    } else if ("-refreshLoadedJobCache".equals(cmd)) {
      System.err.println("Usage: mapred hsadmin [-refreshLoadedJobCache]");
    } else if ("-refreshJobRetentionSettings".equals(cmd)) {
      System.err
          .println("Usage: mapred hsadmin [-refreshJobRetentionSettings]");
    } else if ("-refreshLogRetentionSettings".equals(cmd)) {
      System.err
          .println("Usage: mapred hsadmin [-refreshLogRetentionSettings]");
    } else if ("-getGroups".equals(cmd)) {
      System.err.println("Usage: mapred hsadmin" + " [-getGroups [username]]");
    } else {
      System.err.println("Usage: mapred hsadmin");
      System.err.println("           [-refreshUserToGroupsMappings]");
      System.err.println("           [-refreshSuperUserGroupsConfiguration]");
      System.err.println("           [-refreshAdminAcls]");
      System.err.println("           [-refreshLoadedJobCache]");
      System.err.println("           [-refreshJobRetentionSettings]");
      System.err.println("           [-refreshLogRetentionSettings]");
      System.err.println("           [-getGroups [username]]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  private static void printHelp(String cmd) {
    String summary = "hsadmin is the command to execute Job History server administrative commands.\n"
        + "The full syntax is: \n\n"
        + "mapred hsadmin"
        + " [-refreshUserToGroupsMappings]"
        + " [-refreshSuperUserGroupsConfiguration]"
        + " [-refreshAdminAcls]"
        + " [-refreshLoadedJobCache]"
        + " [-refreshLogRetentionSettings]"
        + " [-refreshJobRetentionSettings]"
        + " [-getGroups [username]]" + " [-help [cmd]]\n";

    String refreshUserToGroupsMappings = "-refreshUserToGroupsMappings: Refresh user-to-groups mappings\n";

    String refreshSuperUserGroupsConfiguration = "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings\n";

    String refreshAdminAcls = "-refreshAdminAcls: Refresh acls for administration of Job history server\n";

    String refreshLoadedJobCache = "-refreshLoadedJobCache: Refresh loaded job cache of Job history server\n";

    String refreshJobRetentionSettings = "-refreshJobRetentionSettings:" + 
        "Refresh job history period,job cleaner settings\n";

    String refreshLogRetentionSettings = "-refreshLogRetentionSettings:" + 
        "Refresh log retention period and log retention check interval\n";
    
    String getGroups = "-getGroups [username]: Get the groups which given user belongs to\n";

    String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n"
        + "\t\tis specified.\n";

    if ("refreshUserToGroupsMappings".equals(cmd)) {
      System.out.println(refreshUserToGroupsMappings);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else if ("refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.out.println(refreshSuperUserGroupsConfiguration);
    } else if ("refreshAdminAcls".equals(cmd)) {
      System.out.println(refreshAdminAcls);
    } else if ("refreshLoadedJobCache".equals(cmd)) {
      System.out.println(refreshLoadedJobCache);
    } else if ("refreshJobRetentionSettings".equals(cmd)) {
      System.out.println(refreshJobRetentionSettings);
    } else if ("refreshLogRetentionSettings".equals(cmd)) {
      System.out.println(refreshLogRetentionSettings);
    } else if ("getGroups".equals(cmd)) {
      System.out.println(getGroups);
    } else {
      System.out.println(summary);
      System.out.println(refreshUserToGroupsMappings);
      System.out.println(refreshSuperUserGroupsConfiguration);
      System.out.println(refreshAdminAcls);
      System.out.println(refreshLoadedJobCache);
      System.out.println(refreshJobRetentionSettings);
      System.out.println(refreshLogRetentionSettings);
      System.out.println(getGroups);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  private int getGroups(String[] usernames) throws IOException {
    // Get groups users belongs to
    if (usernames.length == 0) {
      usernames = new String[] { UserGroupInformation.getCurrentUser()
          .getUserName() };
    }

    // Get the current configuration
    Configuration conf = getConf();

    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    GetUserMappingsProtocol getUserMappingProtocol = HSProxies.createProxy(
        conf, address, GetUserMappingsProtocol.class,
        UserGroupInformation.getCurrentUser());
    for (String username : usernames) {
      StringBuilder sb = new StringBuilder();
      sb.append(username + " :");
      for (String group : getUserMappingProtocol.getGroupsForUser(username)) {
        sb.append(" ");
        sb.append(group);
      }
      System.out.println(sb);
    }

    return 0;
  }

  private int refreshUserToGroupsMappings() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();

    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    RefreshUserMappingsProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, RefreshUserMappingsProtocol.class,
        UserGroupInformation.getCurrentUser());
    // Refresh the user-to-groups mappings
    refreshProtocol.refreshUserToGroupsMappings();

    return 0;
  }

  private int refreshSuperUserGroupsConfiguration() throws IOException {
    // Refresh the super-user groups
    Configuration conf = getConf();
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    RefreshUserMappingsProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, RefreshUserMappingsProtocol.class,
        UserGroupInformation.getCurrentUser());
    // Refresh the super-user group mappings
    refreshProtocol.refreshSuperUserGroupsConfiguration();

    return 0;
  }

  private int refreshAdminAcls() throws IOException {
    // Refresh the admin acls
    Configuration conf = getConf();
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    HSAdminRefreshProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, HSAdminRefreshProtocol.class,
        UserGroupInformation.getCurrentUser());

    refreshProtocol.refreshAdminAcls();
    return 0;
  }

  private int refreshLoadedJobCache() throws IOException {
    // Refresh the loaded job cache
    Configuration conf = getConf();
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    HSAdminRefreshProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, HSAdminRefreshProtocol.class,
        UserGroupInformation.getCurrentUser());

    refreshProtocol.refreshLoadedJobCache();
    return 0;
  }
    
  private int refreshJobRetentionSettings() throws IOException {
    // Refresh job retention settings
    Configuration conf = getConf();
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    HSAdminRefreshProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, HSAdminRefreshProtocol.class,
        UserGroupInformation.getCurrentUser());

    refreshProtocol.refreshJobRetentionSettings();
    return 0;
  }

  private int refreshLogRetentionSettings() throws IOException {
    // Refresh log retention settings
    Configuration conf = getConf();
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    HSAdminRefreshProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, HSAdminRefreshProtocol.class,
        UserGroupInformation.getCurrentUser());

    refreshProtocol.refreshLogRetentionSettings();
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

    if ("-refreshUserToGroupsMappings".equals(cmd)
        || "-refreshSuperUserGroupsConfiguration".equals(cmd)
        || "-refreshAdminAcls".equals(cmd)
        || "-refreshLoadedJobCache".equals(cmd)
        || "-refreshJobRetentionSettings".equals(cmd)
        || "-refreshLogRetentionSettings".equals(cmd)) {
      if (args.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    }

    exitCode = 0;
    if ("-refreshUserToGroupsMappings".equals(cmd)) {
      exitCode = refreshUserToGroupsMappings();
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      exitCode = refreshSuperUserGroupsConfiguration();
    } else if ("-refreshAdminAcls".equals(cmd)) {
      exitCode = refreshAdminAcls();
    } else if ("-refreshLoadedJobCache".equals(cmd)) {
      exitCode = refreshLoadedJobCache();
    } else if ("-refreshJobRetentionSettings".equals(cmd)) {
      exitCode = refreshJobRetentionSettings();
    } else if ("-refreshLogRetentionSettings".equals(cmd)) {
      exitCode = refreshLogRetentionSettings();
    } else if ("-getGroups".equals(cmd)) {
      String[] usernames = Arrays.copyOfRange(args, i, args.length);
      exitCode = getGroups(usernames);
    } else if ("-help".equals(cmd)) {
      if (i < args.length) {
        printHelp(args[i]);
      } else {
        printHelp("");
      }
    } else {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": Unknown command");
      printUsage("");
    }
    return exitCode;
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    int result = ToolRunner.run(new HSAdmin(conf), args);
    System.exit(result);
  }
}
