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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

@Public
@Evolving
public class LogsCLI extends Configured implements Tool {

  private static final String CONTAINER_ID_OPTION = "containerId";
  private static final String APPLICATION_ID_OPTION = "applicationId";
  private static final String NODE_ADDRESS_OPTION = "nodeAddress";
  private static final String APP_OWNER_OPTION = "appOwner";
  public static final String HELP_CMD = "help";

  @Override
  public int run(String[] args) throws Exception {

    Options opts = new Options();
    opts.addOption(HELP_CMD, false, "Displays help for all commands.");
    Option appIdOpt =
        new Option(APPLICATION_ID_OPTION, true, "ApplicationId (required)");
    appIdOpt.setRequired(true);
    opts.addOption(appIdOpt);
    opts.addOption(CONTAINER_ID_OPTION, true,
      "ContainerId (must be specified if node address is specified)");
    opts.addOption(NODE_ADDRESS_OPTION, true, "NodeAddress in the format "
      + "nodename:port (must be specified if container id is specified)");
    opts.addOption(APP_OWNER_OPTION, true,
      "AppOwner (assumed to be current user if not specified)");
    opts.getOption(APPLICATION_ID_OPTION).setArgName("Application ID");
    opts.getOption(CONTAINER_ID_OPTION).setArgName("Container ID");
    opts.getOption(NODE_ADDRESS_OPTION).setArgName("Node Address");
    opts.getOption(APP_OWNER_OPTION).setArgName("Application Owner");

    Options printOpts = new Options();
    printOpts.addOption(opts.getOption(HELP_CMD));
    printOpts.addOption(opts.getOption(CONTAINER_ID_OPTION));
    printOpts.addOption(opts.getOption(NODE_ADDRESS_OPTION));
    printOpts.addOption(opts.getOption(APP_OWNER_OPTION));

    if (args.length < 1) {
      printHelpMessage(printOpts);
      return -1;
    }
    if (args[0].equals("-help")) {
      printHelpMessage(printOpts);
      return 0;
    }
    CommandLineParser parser = new GnuParser();
    String appIdStr = null;
    String containerIdStr = null;
    String nodeAddress = null;
    String appOwner = null;
    try {
      CommandLine commandLine = parser.parse(opts, args, true);
      appIdStr = commandLine.getOptionValue(APPLICATION_ID_OPTION);
      containerIdStr = commandLine.getOptionValue(CONTAINER_ID_OPTION);
      nodeAddress = commandLine.getOptionValue(NODE_ADDRESS_OPTION);
      appOwner = commandLine.getOptionValue(APP_OWNER_OPTION);
    } catch (ParseException e) {
      System.err.println("options parsing failed: " + e.getMessage());
      printHelpMessage(printOpts);
      return -1;
    }

    if (appIdStr == null) {
      System.err.println("ApplicationId cannot be null!");
      printHelpMessage(printOpts);
      return -1;
    }

    ApplicationId appId = null;
    try {
      appId = ConverterUtils.toApplicationId(appIdStr);
    } catch (Exception e) {
      System.err.println("Invalid ApplicationId specified");
      return -1;
    }

    try {
      int resultCode = verifyApplicationState(appId);
      if (resultCode != 0) {
        System.out.println("Logs are not avaiable right now.");
        return resultCode;
      }
    } catch (Exception e) {
      System.err.println("Unable to get ApplicationState."
          + " Attempting to fetch logs directly from the filesystem.");
    }

    LogCLIHelpers logCliHelper = new LogCLIHelpers();
    logCliHelper.setConf(getConf());
    
    if (appOwner == null || appOwner.isEmpty()) {
      appOwner = UserGroupInformation.getCurrentUser().getShortUserName();
    }
    int resultCode = 0;
    if (containerIdStr == null && nodeAddress == null) {
      resultCode = logCliHelper.dumpAllContainersLogs(appId, appOwner, System.out);
    } else if ((containerIdStr == null && nodeAddress != null)
        || (containerIdStr != null && nodeAddress == null)) {
      System.out.println("ContainerId or NodeAddress cannot be null!");
      printHelpMessage(printOpts);
      resultCode = -1;
    } else {
      resultCode =
          logCliHelper.dumpAContainersLogs(appIdStr, containerIdStr,
            nodeAddress, appOwner);
    }

    return resultCode;
  }

  private int verifyApplicationState(ApplicationId appId) throws IOException,
      YarnException {
    YarnClient yarnClient = createYarnClient();

    try {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      switch (appReport.getYarnApplicationState()) {
      case NEW:
      case NEW_SAVING:
      case SUBMITTED:
        return -1;
      case ACCEPTED:
      case RUNNING:
      case FAILED:
      case FINISHED:
      case KILLED:
      default:
        break;

      }
    } finally {
      yarnClient.close();
    }
    return 0;
  }
  
  @VisibleForTesting
  protected YarnClient createYarnClient() {
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(getConf());
    yarnClient.start();
    return yarnClient;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new YarnConfiguration();
    LogsCLI logDumper = new LogsCLI();
    logDumper.setConf(conf);
    int exitCode = logDumper.run(args);
    System.exit(exitCode);
  }

  private void printHelpMessage(Options options) {
    System.out.println("Retrieve logs for completed YARN applications.");
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("yarn logs -applicationId <application ID> [OPTIONS]", new Options());
    formatter.setSyntaxPrefix("");
    formatter.printHelp("general options are:", options);
  }
}
