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

import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ApplicationCLI extends YarnCLI {
  private static final String APPLICATIONS_PATTERN = "%30s\t%20s\t%10s\t%10s\t%18s\t%18s\t%35s\n";

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
    opts.addOption(LIST_CMD, false, "Lists all the Applications from RM.");
    opts.addOption(KILL_CMD, true, "Kills the application.");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    int exitCode = -1;
    if (cliParser.hasOption(STATUS_CMD)) {
      if (args.length != 2) {
        printUsage(opts);
        return exitCode;
      }
      printApplicationReport(cliParser.getOptionValue(STATUS_CMD));
    } else if (cliParser.hasOption(LIST_CMD)) {
      listAllApplications();
    } else if (cliParser.hasOption(KILL_CMD)) {
      if (args.length != 2) {
        printUsage(opts);
        return exitCode;
      }
      killApplication(cliParser.getOptionValue(KILL_CMD));
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
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("application", opts);
  }

  /**
   * Lists all the applications present in the Resource Manager
   * 
   * @throws YarnRemoteException
   */
  private void listAllApplications() throws YarnRemoteException {
    PrintWriter writer = new PrintWriter(sysout);
    List<ApplicationReport> appsReport = client.getApplicationList();

    writer.println("Total Applications:" + appsReport.size());
    writer.printf(APPLICATIONS_PATTERN, "Application-Id",
        "Application-Name", "User", "Queue", "State", "Final-State",
        "Tracking-URL");
    for (ApplicationReport appReport : appsReport) {
      writer.printf(APPLICATIONS_PATTERN, appReport.getApplicationId(),
          appReport.getName(), appReport.getUser(), appReport.getQueue(),
          appReport.getYarnApplicationState(), appReport
              .getFinalApplicationStatus(), appReport.getOriginalTrackingUrl());
    }
    writer.flush();
  }

  /**
   * Kills the application with the application id as appId
   * 
   * @param applicationId
   * @throws YarnRemoteException
   */
  private void killApplication(String applicationId) throws YarnRemoteException {
    ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
    sysout.println("Killing application " + applicationId);
    client.killApplication(appId);
  }

  /**
   * Prints the application report for an application id.
   * 
   * @param applicationId
   * @throws YarnRemoteException
   */
  private void printApplicationReport(String applicationId)
      throws YarnRemoteException {
    ApplicationReport appReport = client.getApplicationReport(ConverterUtils
        .toApplicationId(applicationId));
    StringBuffer appReportStr = new StringBuffer();
    if (appReport != null) {
      appReportStr.append("Application Report : ");
      appReportStr.append("\n\tApplication-Id : ");
      appReportStr.append(appReport.getApplicationId());
      appReportStr.append("\n\tApplication-Name : ");
      appReportStr.append(appReport.getName());
      appReportStr.append("\n\tUser : ");
      appReportStr.append(appReport.getUser());
      appReportStr.append("\n\tQueue : ");
      appReportStr.append(appReport.getQueue());
      appReportStr.append("\n\tStart-Time : ");
      appReportStr.append(appReport.getStartTime());
      appReportStr.append("\n\tFinish-Time : ");
      appReportStr.append(appReport.getFinishTime());
      appReportStr.append("\n\tState : ");
      appReportStr.append(appReport.getYarnApplicationState());
      appReportStr.append("\n\tFinal-State : ");
      appReportStr.append(appReport.getFinalApplicationStatus());
      appReportStr.append("\n\tTracking-URL : ");
      appReportStr.append(appReport.getOriginalTrackingUrl());
      appReportStr.append("\n\tDiagnostics : ");
      appReportStr.append(appReport.getDiagnostics());
    } else {
      appReportStr.append("Application with id '" + applicationId
          + "' doesn't exist in RM.");
    }
    sysout.println(appReportStr.toString());
  }

}