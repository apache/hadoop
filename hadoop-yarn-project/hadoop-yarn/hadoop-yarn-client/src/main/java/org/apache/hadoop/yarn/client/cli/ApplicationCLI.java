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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class ApplicationCLI extends YarnCLI {
  private static final String APPLICATIONS_PATTERN = 
    "%30s\t%20s\t%20s\t%10s\t%10s\t%18s\t%18s\t%15s\t%35s"
      + System.getProperty("line.separator");
  private static final String APPLICATION_ATTEMPTS_PATTERN =
    "%30s\t%20s\t%35s\t%35s"
      + System.getProperty("line.separator");

  private static final String APP_TYPE_CMD = "appTypes";
  private static final String APP_STATE_CMD = "appStates";
  private static final String ALLSTATES_OPTION = "ALL";
  private static final String QUEUE_CMD = "queue";

  @VisibleForTesting
  protected static final String CONTAINER_PATTERN =
    "%30s\t%20s\t%20s\t%20s\t%20s\t%20s\t%35s"
      + System.getProperty("line.separator");

  public static final String APPLICATION = "application";
  public static final String APPLICATION_ATTEMPT = "applicationattempt";
  public static final String CONTAINER = "container";

  private boolean allAppStates;

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
    String title = null;
    if (args.length > 0 && args[0].equalsIgnoreCase(APPLICATION)) {
      title = APPLICATION;
      opts.addOption(STATUS_CMD, true,
          "Prints the status of the application.");
      opts.addOption(LIST_CMD, false, "List applications. "
          + "Supports optional use of -appTypes to filter applications "
          + "based on application type, "
          + "and -appStates to filter applications based on application state.");
      opts.addOption(KILL_CMD, true, "Kills the application.");
      opts.addOption(MOVE_TO_QUEUE_CMD, true, "Moves the application to a "
          + "different queue.");
      opts.addOption(QUEUE_CMD, true, "Works with the movetoqueue command to"
          + " specify which queue to move an application to.");
      opts.addOption(HELP_CMD, false, "Displays help for all commands.");
      Option appTypeOpt = new Option(APP_TYPE_CMD, true, "Works with -list to "
          + "filter applications based on "
          + "input comma-separated list of application types.");
      appTypeOpt.setValueSeparator(',');
      appTypeOpt.setArgs(Option.UNLIMITED_VALUES);
      appTypeOpt.setArgName("Types");
      opts.addOption(appTypeOpt);
      Option appStateOpt = new Option(APP_STATE_CMD, true, "Works with -list "
          + "to filter applications based on input comma-separated list of "
          + "application states. " + getAllValidApplicationStates());
      appStateOpt.setValueSeparator(',');
      appStateOpt.setArgs(Option.UNLIMITED_VALUES);
      appStateOpt.setArgName("States");
      opts.addOption(appStateOpt);
      opts.getOption(KILL_CMD).setArgName("Application ID");
      opts.getOption(MOVE_TO_QUEUE_CMD).setArgName("Application ID");
      opts.getOption(QUEUE_CMD).setArgName("Queue Name");
      opts.getOption(STATUS_CMD).setArgName("Application ID");
    } else if (args.length > 0 && args[0].equalsIgnoreCase(APPLICATION_ATTEMPT)) {
      title = APPLICATION_ATTEMPT;
      opts.addOption(STATUS_CMD, true,
          "Prints the status of the application attempt.");
      opts.addOption(LIST_CMD, true,
          "List application attempts for aplication.");
      opts.addOption(HELP_CMD, false, "Displays help for all commands.");
      opts.getOption(STATUS_CMD).setArgName("Application Attempt ID");
      opts.getOption(LIST_CMD).setArgName("Application ID");
    } else if (args.length > 0 && args[0].equalsIgnoreCase(CONTAINER)) {
      title = CONTAINER;
      opts.addOption(STATUS_CMD, true,
          "Prints the status of the container.");
      opts.addOption(LIST_CMD, true,
          "List containers for application attempt.");
      opts.addOption(HELP_CMD, false, "Displays help for all commands.");
      opts.getOption(STATUS_CMD).setArgName("Container ID");
      opts.getOption(LIST_CMD).setArgName("Application Attempt ID");
    }

    int exitCode = -1;
    CommandLine cliParser = null;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      sysout.println("Missing argument for options");
      printUsage(title, opts);
      return exitCode;
    }

    if (cliParser.hasOption(STATUS_CMD)) {
      if (args.length != 3) {
        printUsage(title, opts);
        return exitCode;
      }
      if (args[0].equalsIgnoreCase(APPLICATION)) {
        exitCode = printApplicationReport(cliParser.getOptionValue(STATUS_CMD));
      } else if (args[0].equalsIgnoreCase(APPLICATION_ATTEMPT)) {
        exitCode = printApplicationAttemptReport(cliParser
            .getOptionValue(STATUS_CMD));
      } else if (args[0].equalsIgnoreCase(CONTAINER)) {
        exitCode = printContainerReport(cliParser.getOptionValue(STATUS_CMD));
      }
      return exitCode;
    } else if (cliParser.hasOption(LIST_CMD)) {
      if (args[0].equalsIgnoreCase(APPLICATION)) {
        allAppStates = false;
        Set<String> appTypes = new HashSet<String>();
        if (cliParser.hasOption(APP_TYPE_CMD)) {
          String[] types = cliParser.getOptionValues(APP_TYPE_CMD);
          if (types != null) {
            for (String type : types) {
              if (!type.trim().isEmpty()) {
                appTypes.add(StringUtils.toUpperCase(type).trim());
              }
            }
          }
        }

        EnumSet<YarnApplicationState> appStates = EnumSet
            .noneOf(YarnApplicationState.class);
        if (cliParser.hasOption(APP_STATE_CMD)) {
          String[] states = cliParser.getOptionValues(APP_STATE_CMD);
          if (states != null) {
            for (String state : states) {
              if (!state.trim().isEmpty()) {
                if (state.trim().equalsIgnoreCase(ALLSTATES_OPTION)) {
                  allAppStates = true;
                  break;
                }
                try {
                  appStates.add(YarnApplicationState.valueOf(
                      StringUtils.toUpperCase(state).trim()));
                } catch (IllegalArgumentException ex) {
                  sysout.println("The application state " + state
                      + " is invalid.");
                  sysout.println(getAllValidApplicationStates());
                  return exitCode;
                }
              }
            }
          }
        }
        listApplications(appTypes, appStates);
      } else if (args[0].equalsIgnoreCase(APPLICATION_ATTEMPT)) {
        if (args.length != 3) {
          printUsage(title, opts);
          return exitCode;
        }
        listApplicationAttempts(cliParser.getOptionValue(LIST_CMD));
      } else if (args[0].equalsIgnoreCase(CONTAINER)) {
        if (args.length != 3) {
          printUsage(title, opts);
          return exitCode;
        }
        listContainers(cliParser.getOptionValue(LIST_CMD));
      }
    } else if (cliParser.hasOption(KILL_CMD)) {
      if (args.length != 3) {
        printUsage(title, opts);
        return exitCode;
      }
      try{
        killApplication(cliParser.getOptionValue(KILL_CMD));
      } catch (ApplicationNotFoundException e) {
        return exitCode;
      }
    } else if (cliParser.hasOption(MOVE_TO_QUEUE_CMD)) {
      if (!cliParser.hasOption(QUEUE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      moveApplicationAcrossQueues(cliParser.getOptionValue(MOVE_TO_QUEUE_CMD),
          cliParser.getOptionValue(QUEUE_CMD));
    } else if (cliParser.hasOption(HELP_CMD)) {
      printUsage(title, opts);
      return 0;
    } else {
      syserr.println("Invalid Command Usage : ");
      printUsage(title, opts);
    }
    return 0;
  }

  /**
   * It prints the usage of the command
   * 
   * @param opts
   */
  @VisibleForTesting
  void printUsage(String title, Options opts) {
    new HelpFormatter().printHelp(title, opts);
  }

  /**
   * Prints the application attempt report for an application attempt id.
   * 
   * @param applicationAttemptId
   * @return exitCode
   * @throws YarnException
   */
  private int printApplicationAttemptReport(String applicationAttemptId)
      throws YarnException, IOException {
    ApplicationAttemptReport appAttemptReport = null;
    try {
      appAttemptReport = client.getApplicationAttemptReport(ConverterUtils
          .toApplicationAttemptId(applicationAttemptId));
    } catch (ApplicationNotFoundException e) {
      sysout.println("Application for AppAttempt with id '"
          + applicationAttemptId + "' doesn't exist in RM or Timeline Server.");
      return -1;
    } catch (ApplicationAttemptNotFoundException e) {
      sysout.println("Application Attempt with id '" + applicationAttemptId
          + "' doesn't exist in RM or Timeline Server.");
      return -1;
    }
    // Use PrintWriter.println, which uses correct platform line ending.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter appAttemptReportStr = new PrintWriter(
        new OutputStreamWriter(baos, Charset.forName("UTF-8")));
    if (appAttemptReport != null) {
      appAttemptReportStr.println("Application Attempt Report : ");
      appAttemptReportStr.print("\tApplicationAttempt-Id : ");
      appAttemptReportStr.println(appAttemptReport.getApplicationAttemptId());
      appAttemptReportStr.print("\tState : ");
      appAttemptReportStr.println(appAttemptReport
          .getYarnApplicationAttemptState());
      appAttemptReportStr.print("\tAMContainer : ");
      appAttemptReportStr.println(appAttemptReport.getAMContainerId()
          .toString());
      appAttemptReportStr.print("\tTracking-URL : ");
      appAttemptReportStr.println(appAttemptReport.getTrackingUrl());
      appAttemptReportStr.print("\tRPC Port : ");
      appAttemptReportStr.println(appAttemptReport.getRpcPort());
      appAttemptReportStr.print("\tAM Host : ");
      appAttemptReportStr.println(appAttemptReport.getHost());
      appAttemptReportStr.print("\tDiagnostics : ");
      appAttemptReportStr.print(appAttemptReport.getDiagnostics());
    } else {
      appAttemptReportStr.print("Application Attempt with id '"
          + applicationAttemptId + "' doesn't exist in Timeline Server.");
      appAttemptReportStr.close();
      sysout.println(baos.toString("UTF-8"));
      return -1;
    }
    appAttemptReportStr.close();
    sysout.println(baos.toString("UTF-8"));
    return 0;
  }

  /**
   * Prints the container report for an container id.
   * 
   * @param containerId
   * @return exitCode
   * @throws YarnException
   */
  private int printContainerReport(String containerId) throws YarnException,
      IOException {
    ContainerReport containerReport = null;
    try {
      containerReport = client.getContainerReport((ConverterUtils
          .toContainerId(containerId)));
    } catch (ApplicationNotFoundException e) {
      sysout.println("Application for Container with id '" + containerId
          + "' doesn't exist in RM or Timeline Server.");
      return -1;
    } catch (ApplicationAttemptNotFoundException e) {
      sysout.println("Application Attempt for Container with id '"
          + containerId + "' doesn't exist in RM or Timeline Server.");
      return -1;
    } catch (ContainerNotFoundException e) {
      sysout.println("Container with id '" + containerId
          + "' doesn't exist in RM or Timeline Server.");
      return -1;
    }
    // Use PrintWriter.println, which uses correct platform line ending.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter containerReportStr = new PrintWriter(
        new OutputStreamWriter(baos, Charset.forName("UTF-8")));
    if (containerReport != null) {
      containerReportStr.println("Container Report : ");
      containerReportStr.print("\tContainer-Id : ");
      containerReportStr.println(containerReport.getContainerId());
      containerReportStr.print("\tStart-Time : ");
      containerReportStr.println(containerReport.getCreationTime());
      containerReportStr.print("\tFinish-Time : ");
      containerReportStr.println(containerReport.getFinishTime());
      containerReportStr.print("\tState : ");
      containerReportStr.println(containerReport.getContainerState());
      containerReportStr.print("\tLOG-URL : ");
      containerReportStr.println(containerReport.getLogUrl());
      containerReportStr.print("\tHost : ");
      containerReportStr.println(containerReport.getAssignedNode());
      containerReportStr.print("\tNodeHttpAddress : ");
      containerReportStr.println(containerReport.getNodeHttpAddress() == null
          ? "N/A" : containerReport.getNodeHttpAddress());
      containerReportStr.print("\tDiagnostics : ");
      containerReportStr.print(containerReport.getDiagnosticsInfo());
    } else {
      containerReportStr.print("Container with id '" + containerId
          + "' doesn't exist in Timeline Server.");
      containerReportStr.close();
      sysout.println(baos.toString("UTF-8"));
      return -1;
    }
    containerReportStr.close();
    sysout.println(baos.toString("UTF-8"));
    return 0;
  }

  /**
   * Lists the applications matching the given application Types And application
   * States present in the Resource Manager
   * 
   * @param appTypes
   * @param appStates
   * @throws YarnException
   * @throws IOException
   */
  private void listApplications(Set<String> appTypes,
      EnumSet<YarnApplicationState> appStates) throws YarnException,
      IOException {
    PrintWriter writer = new PrintWriter(
        new OutputStreamWriter(sysout, Charset.forName("UTF-8")));
    if (allAppStates) {
      for (YarnApplicationState appState : YarnApplicationState.values()) {
        appStates.add(appState);
      }
    } else {
      if (appStates.isEmpty()) {
        appStates.add(YarnApplicationState.RUNNING);
        appStates.add(YarnApplicationState.ACCEPTED);
        appStates.add(YarnApplicationState.SUBMITTED);
      }
    }

    List<ApplicationReport> appsReport = client.getApplications(appTypes,
        appStates);

    writer.println("Total number of applications (application-types: "
        + appTypes + " and states: " + appStates + ")" + ":"
        + appsReport.size());
    writer.printf(APPLICATIONS_PATTERN, "Application-Id", "Application-Name",
        "Application-Type", "User", "Queue", "State", "Final-State",
        "Progress", "Tracking-URL");
    for (ApplicationReport appReport : appsReport) {
      DecimalFormat formatter = new DecimalFormat("###.##%");
      String progress = formatter.format(appReport.getProgress());
      writer.printf(APPLICATIONS_PATTERN, appReport.getApplicationId(),
          appReport.getName(), appReport.getApplicationType(), appReport
              .getUser(), appReport.getQueue(), appReport
              .getYarnApplicationState(),
          appReport.getFinalApplicationStatus(), progress, appReport
              .getOriginalTrackingUrl());
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
  private void killApplication(String applicationId) throws YarnException,
      IOException {
    ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
    ApplicationReport  appReport = null;
    try {
      appReport = client.getApplicationReport(appId);
    } catch (ApplicationNotFoundException e) {
      sysout.println("Application with id '" + applicationId +
          "' doesn't exist in RM.");
      throw e;
    }

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
   * Moves the application with the given ID to the given queue.
   */
  private void moveApplicationAcrossQueues(String applicationId, String queue)
      throws YarnException, IOException {
    ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
    ApplicationReport appReport = client.getApplicationReport(appId);
    if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED
        || appReport.getYarnApplicationState() == YarnApplicationState.KILLED
        || appReport.getYarnApplicationState() == YarnApplicationState.FAILED) {
      sysout.println("Application " + applicationId + " has already finished ");
    } else {
      sysout.println("Moving application " + applicationId + " to queue " + queue);
      client.moveApplicationAcrossQueues(appId, queue);
      sysout.println("Successfully completed move.");
    }
  }

  /**
   * Prints the application report for an application id.
   * 
   * @param applicationId
   * @return exitCode
   * @throws YarnException
   */
  private int printApplicationReport(String applicationId)
      throws YarnException, IOException {
    ApplicationReport appReport = null;
    try {
      appReport = client.getApplicationReport(ConverterUtils
          .toApplicationId(applicationId));
    } catch (ApplicationNotFoundException e) {
      sysout.println("Application with id '" + applicationId
          + "' doesn't exist in RM or Timeline Server.");
      return -1;
    }
    // Use PrintWriter.println, which uses correct platform line ending.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter appReportStr = new PrintWriter(
        new OutputStreamWriter(baos, Charset.forName("UTF-8")));
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
      appReportStr.print("\tAggregate Resource Allocation : ");

      ApplicationResourceUsageReport usageReport =
          appReport.getApplicationResourceUsageReport();
      if (usageReport != null) {
        //completed app report in the timeline server doesn't have usage report
        appReportStr.print(usageReport.getMemorySeconds() + " MB-seconds, ");
        appReportStr.println(usageReport.getVcoreSeconds() + " vcore-seconds");
      } else {
        appReportStr.println("N/A");
      }
      appReportStr.print("\tDiagnostics : ");
      appReportStr.print(appReport.getDiagnostics());
    } else {
      appReportStr.print("Application with id '" + applicationId
          + "' doesn't exist in RM.");
      appReportStr.close();
      sysout.println(baos.toString("UTF-8"));
      return -1;
    }
    appReportStr.close();
    sysout.println(baos.toString("UTF-8"));
    return 0;
  }

  private String getAllValidApplicationStates() {
    StringBuilder sb = new StringBuilder();
    sb.append("The valid application state can be" + " one of the following: ");
    sb.append(ALLSTATES_OPTION + ",");
    for (YarnApplicationState appState : YarnApplicationState.values()) {
      sb.append(appState + ",");
    }
    String output = sb.toString();
    return output.substring(0, output.length() - 1);
  }

  /**
   * Lists the application attempts matching the given applicationid
   * 
   * @param applicationId
   * @throws YarnException
   * @throws IOException
   */
  private void listApplicationAttempts(String applicationId) throws YarnException,
      IOException {
    PrintWriter writer = new PrintWriter(
        new OutputStreamWriter(sysout, Charset.forName("UTF-8")));

    List<ApplicationAttemptReport> appAttemptsReport = client
        .getApplicationAttempts(ConverterUtils.toApplicationId(applicationId));
    writer.println("Total number of application attempts " + ":"
        + appAttemptsReport.size());
    writer.printf(APPLICATION_ATTEMPTS_PATTERN, "ApplicationAttempt-Id",
        "State", "AM-Container-Id", "Tracking-URL");
    for (ApplicationAttemptReport appAttemptReport : appAttemptsReport) {
      writer.printf(APPLICATION_ATTEMPTS_PATTERN, appAttemptReport
          .getApplicationAttemptId(), appAttemptReport
          .getYarnApplicationAttemptState(), appAttemptReport
          .getAMContainerId().toString(), appAttemptReport.getTrackingUrl());
    }
    writer.flush();
  }

  /**
   * Lists the containers matching the given application attempts
   * 
   * @param appAttemptId
   * @throws YarnException
   * @throws IOException
   */
  private void listContainers(String appAttemptId) throws YarnException,
      IOException {
    PrintWriter writer = new PrintWriter(
        new OutputStreamWriter(sysout, Charset.forName("UTF-8")));

    List<ContainerReport> appsReport = client
        .getContainers(ConverterUtils.toApplicationAttemptId(appAttemptId));
    writer.println("Total number of containers " + ":" + appsReport.size());
    writer.printf(CONTAINER_PATTERN, "Container-Id", "Start Time",
        "Finish Time", "State", "Host", "Node Http Address", "LOG-URL");
    for (ContainerReport containerReport : appsReport) {
      writer.printf(
          CONTAINER_PATTERN,
          containerReport.getContainerId(),
          Times.format(containerReport.getCreationTime()),
          Times.format(containerReport.getFinishTime()),      
          containerReport.getContainerState(), containerReport
              .getAssignedNode(), containerReport.getNodeHttpAddress() == null
                  ? "N/A" : containerReport.getNodeHttpAddress(),
          containerReport.getLogUrl());
    }
    writer.flush();
  }
}
