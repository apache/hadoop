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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class ApplicationCLI extends YarnCLI {
  private static final String APPLICATIONS_PATTERN =
    "%30s\t%20s\t%20s\t%10s\t%10s\t%18s\t%18s\t%15s\t%35s" +
    System.getProperty("line.separator");

  private static final String APP_TYPE_CMD = "appTypes";

  public static void main(String[] args) throws Exception {
    ApplicationCLI cli = new ApplicationCLI();
    cli.setSysOutPrintStream(System.out);
    cli.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(cli, args);
    cli.stop();
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {

    Options opts = new Options();
    opts.addOption(STATUS_CMD, true, "Prints the status of the application.");
    opts.addOption(LIST_CMD, false, "List applications from the RM. " +
        "Supports optional use of --appTypes to filter applications " +
        "based on application type.");
    opts.addOption(KILL_CMD, true, "Kills the application.");
    opts.addOption(HELP_CMD, false, "Displays help for all commands.");
    Option appTypeOpt = new Option(APP_TYPE_CMD, true,
        "Works with --list to filter applications based on their type.");
    appTypeOpt.setValueSeparator(',');
    appTypeOpt.setArgs(Option.UNLIMITED_VALUES);
    appTypeOpt.setArgName("Comma-separated list of application types");
    opts.addOption(appTypeOpt);
    opts.getOption(KILL_CMD).setArgName("Application ID");
    opts.getOption(STATUS_CMD).setArgName("Application ID");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    int exitCode = -1;
    if (cliParser.hasOption(STATUS_CMD)) {
      if (args.length != 2) {
        printUsage(opts);
        return exitCode;
      }
      printApplicationReport(cliParser.getOptionValue(STATUS_CMD));
    } else if (cliParser.hasOption(LIST_CMD)) {
      Set<String> appTypes = new HashSet<String>();
      if(cliParser.hasOption(APP_TYPE_CMD)) {
        String[] types = cliParser.getOptionValues(APP_TYPE_CMD);
        if (types != null) {
          for (String type : types) {
            if (!type.trim().isEmpty()) {
              appTypes.add(type.trim());
            }
          }
        }
      }
      listApplications(appTypes);
    } else if (cliParser.hasOption(KILL_CMD)) {
      if (args.length != 2) {
        printUsage(opts);
        return exitCode;
      }
      killApplication(cliParser.getOptionValue(KILL_CMD));
    } else if (cliParser.hasOption(HELP_CMD)) {
      printUsage(opts);
      return 0;
    } else {
      syserr.println("Invalid Command Usage : ");
      printUsage(opts);
    }
    return 0;
  }

  /**
   * It prints the usage of the command
   * 
   * @param opts
   */
  @VisibleForTesting
  void printUsage(Options opts) {
    new HelpFormatter().printHelp("application", opts);
  }

  /**
   * Lists the applications matching the given application Types
   * present in the Resource Manager
   * 
   * @param appTypes
   * @throws YarnException
   * @throws IOException
   */
  private void listApplications(Set<String> appTypes)
      throws YarnException, IOException {
    PrintWriter writer = new PrintWriter(sysout);
    List<ApplicationReport> appsReport =
        client.getApplications(appTypes);

    writer.println("Total Applications:" + appsReport.size());
    writer.printf(APPLICATIONS_PATTERN, "Application-Id",
        "Application-Name","Application-Type", "User", "Queue", 
        "State", "Final-State","Progress", "Tracking-URL");
    for (ApplicationReport appReport : appsReport) {
      DecimalFormat formatter = new DecimalFormat("###.##%");
      String progress = formatter.format(appReport.getProgress());
      writer.printf(APPLICATIONS_PATTERN, appReport.getApplicationId(),
          appReport.getName(),appReport.getApplicationType(), appReport.getUser(),
          appReport.getQueue(),appReport.getYarnApplicationState(),
          appReport.getFinalApplicationStatus(),progress,
          appReport.getOriginalTrackingUrl());
    }
    writer.flush();
  }

  /**
   * Kills the application with the application id as appId
   * 
   * @param applicationId
   * @throws YarnException
   * @throws IOException
   */
  private void killApplication(String applicationId)
      throws YarnException, IOException {
    ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
    ApplicationReport appReport = client.getApplicationReport(appId);
    if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED
        || appReport.getYarnApplicationState() == YarnApplicationState.KILLED
        || appReport.getYarnApplicationState() == YarnApplicationState.FAILED) {
      sysout.println("Application " + applicationId + " has already finished ");
    } else {
      sysout.println("Killing application " + applicationId);
      client.killApplication(appId);
    }
  }

  /**
   * Prints the application report for an application id.
   * 
   * @param applicationId
   * @throws YarnException
   */
  private void printApplicationReport(String applicationId)
      throws YarnException, IOException {
    ApplicationReport appReport = client.getApplicationReport(ConverterUtils
        .toApplicationId(applicationId));
    // Use PrintWriter.println, which uses correct platform line ending.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter appReportStr = new PrintWriter(baos);
    if (appReport != null) {
      appReportStr.println("Application Report : ");
      appReportStr.print("\tApplication-Id : ");
      appReportStr.println(appReport.getApplicationId());
      appReportStr.print("\tApplication-Name : ");
      appReportStr.println(appReport.getName());
      appReportStr.print("\tApplication-Type : ");
      appReportStr.println(appReport.getApplicationType());
      appReportStr.print("\tUser : ");
      appReportStr.println(appReport.getUser());
      appReportStr.print("\tQueue : ");
      appReportStr.println(appReport.getQueue());
      appReportStr.print("\tStart-Time : ");
      appReportStr.println(appReport.getStartTime());
      appReportStr.print("\tFinish-Time : ");
      appReportStr.println(appReport.getFinishTime());
      appReportStr.print("\tProgress : ");
      DecimalFormat formatter = new DecimalFormat("###.##%");
      String progress = formatter.format(appReport.getProgress());
      appReportStr.println(progress);
      appReportStr.print("\tState : ");
      appReportStr.println(appReport.getYarnApplicationState());
      appReportStr.print("\tFinal-State : ");
      appReportStr.println(appReport.getFinalApplicationStatus());
      appReportStr.print("\tTracking-URL : ");
      appReportStr.println(appReport.getOriginalTrackingUrl());
      appReportStr.print("\tRPC Port : ");
      appReportStr.println(appReport.getRpcPort());
      appReportStr.print("\tAM Host : ");
      appReportStr.println(appReport.getHost());
      appReportStr.print("\tDiagnostics : ");
      appReportStr.print(appReport.getDiagnostics());
    } else {
      appReportStr.print("Application with id '" + applicationId
          + "' doesn't exist in RM.");
    }
    appReportStr.close();
    sysout.println(baos.toString("UTF-8"));
  }

}
