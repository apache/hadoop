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
import java.util.Collection;
import java.util.Collections;
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
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Times;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.yarn.util.StringHelper.getResourceSecondsString;

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
  private static final String APP_TAG_CMD = "appTags";
  private static final String ALLSTATES_OPTION = "ALL";
  private static final String QUEUE_CMD = "queue";

  @VisibleForTesting
  protected static final String CONTAINER_PATTERN =
    "%30s\t%20s\t%20s\t%20s\t%20s\t%20s\t%35s"
      + System.getProperty("line.separator");

  public static final String APPLICATION = "application";
  public static final String APPLICATION_ATTEMPT = "applicationattempt";
  public static final String CONTAINER = "container";
  public static final String APP_ID = "appId";
  public static final String UPDATE_PRIORITY = "updatePriority";
  public static final String UPDATE_LIFETIME = "updateLifetime";
  public static final String CHANGE_APPLICATION_QUEUE = "changeQueue";

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
          + "based on application type, -appStates to filter applications "
          + "based on application state and -appTags to filter applications "
          + "based on application tag.");
      opts.addOption(MOVE_TO_QUEUE_CMD, true, "Moves the application to a "
          + "different queue. Deprecated command. Use 'changeQueue' instead.");
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
      Option appTagOpt = new Option(APP_TAG_CMD, true, "Works with -list to "
          + "filter applications based on input comma-separated list of "
          + "application tags.");
      appTagOpt.setValueSeparator(',');
      appTagOpt.setArgs(Option.UNLIMITED_VALUES);
      appTagOpt.setArgName("Tags");
      opts.addOption(appTagOpt);
      opts.addOption(APP_ID, true, "Specify Application Id to be operated");
      opts.addOption(UPDATE_PRIORITY, true,
          "update priority of an application. ApplicationId can be"
              + " passed using 'appId' option.");
      opts.addOption(UPDATE_LIFETIME, true,
          "update timeout of an application from NOW. ApplicationId can be"
              + " passed using 'appId' option. Timeout value is in seconds.");
      opts.addOption(CHANGE_APPLICATION_QUEUE, true,
          "Moves application to a new queue. ApplicationId can be"
              + " passed using 'appId' option. 'movetoqueue' command is"
              + " deprecated, this new command 'changeQueue' performs same"
              + " functionality.");
      Option killOpt = new Option(KILL_CMD, true, "Kills the application. "
          + "Set of applications can be provided separated with space");
      killOpt.setValueSeparator(' ');
      killOpt.setArgs(Option.UNLIMITED_VALUES);
      killOpt.setArgName("Application ID");
      opts.addOption(killOpt);
      opts.getOption(MOVE_TO_QUEUE_CMD).setArgName("Application ID");
      opts.getOption(QUEUE_CMD).setArgName("Queue Name");
      opts.getOption(STATUS_CMD).setArgName("Application ID");
      opts.getOption(APP_ID).setArgName("Application ID");
      opts.getOption(UPDATE_PRIORITY).setArgName("Priority");
      opts.getOption(UPDATE_LIFETIME).setArgName("Timeout");
      opts.getOption(CHANGE_APPLICATION_QUEUE).setArgName("Queue Name");
    } else if (args.length > 0 && args[0].equalsIgnoreCase(APPLICATION_ATTEMPT)) {
      title = APPLICATION_ATTEMPT;
      opts.addOption(STATUS_CMD, true,
          "Prints the status of the application attempt.");
      opts.addOption(LIST_CMD, true,
          "List application attempts for application.");
      opts.addOption(FAIL_CMD, true, "Fails application attempt.");
      opts.addOption(HELP_CMD, false, "Displays help for all commands.");
      opts.getOption(STATUS_CMD).setArgName("Application Attempt ID");
      opts.getOption(LIST_CMD).setArgName("Application ID");
      opts.getOption(FAIL_CMD).setArgName("Application Attempt ID");
    } else if (args.length > 0 && args[0].equalsIgnoreCase(CONTAINER)) {
      title = CONTAINER;
      opts.addOption(STATUS_CMD, true,
          "Prints the status of the container.");
      opts.addOption(LIST_CMD, true,
          "List containers for application attempt.");
      opts.addOption(HELP_CMD, false, "Displays help for all commands.");
      opts.getOption(STATUS_CMD).setArgName("Container ID");
      opts.getOption(LIST_CMD).setArgName("Application Attempt ID");
      opts.addOption(SIGNAL_CMD, true,
          "Signal the container. The available signal commands are " +
          java.util.Arrays.asList(SignalContainerCommand.values()) +
          " Default command is OUTPUT_THREAD_DUMP.");
      opts.getOption(SIGNAL_CMD).setArgName("container ID [signal command]");
      opts.getOption(SIGNAL_CMD).setArgs(3);
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

        Set<String> appTags = new HashSet<String>();
        if (cliParser.hasOption(APP_TAG_CMD)) {
          String[] tags = cliParser.getOptionValues(APP_TAG_CMD);
          if (tags != null) {
            for (String tag : tags) {
              if (!tag.trim().isEmpty()) {
                appTags.add(tag.trim());
              }
            }
          }
        }
        listApplications(appTypes, appStates, appTags);
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
      if (args.length < 3 || hasAnyOtherCLIOptions(cliParser, opts, KILL_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      return killApplication(cliParser.getOptionValues(KILL_CMD));
    } else if (cliParser.hasOption(MOVE_TO_QUEUE_CMD)) {
      if (!cliParser.hasOption(QUEUE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      moveApplicationAcrossQueues(cliParser.getOptionValue(MOVE_TO_QUEUE_CMD),
          cliParser.getOptionValue(QUEUE_CMD));
    } else if (cliParser.hasOption(FAIL_CMD)) {
      if (!args[0].equalsIgnoreCase(APPLICATION_ATTEMPT)) {
        printUsage(title, opts);
        return exitCode;
      }
      failApplicationAttempt(cliParser.getOptionValue(FAIL_CMD));
    } else if (cliParser.hasOption(HELP_CMD)) {
      printUsage(title, opts);
      return 0;
    } else if (cliParser.hasOption(UPDATE_PRIORITY)) {
      if (!cliParser.hasOption(APP_ID)) {
        printUsage(title, opts);
        return exitCode;
      }
      updateApplicationPriority(cliParser.getOptionValue(APP_ID),
          cliParser.getOptionValue(UPDATE_PRIORITY));
    } else if (cliParser.hasOption(UPDATE_LIFETIME)) {
      if (!cliParser.hasOption(APP_ID)) {
        printUsage(title, opts);
        return exitCode;
      }

      long timeoutInSec =
          Long.parseLong(cliParser.getOptionValue(UPDATE_LIFETIME));

      updateApplicationTimeout(cliParser.getOptionValue(APP_ID),
          ApplicationTimeoutType.LIFETIME, timeoutInSec);
    } else if (cliParser.hasOption(CHANGE_APPLICATION_QUEUE)) {
      if (!cliParser.hasOption(APP_ID)) {
        printUsage(title, opts);
        return exitCode;
      }
      moveApplicationAcrossQueues(cliParser.getOptionValue(APP_ID),
          cliParser.getOptionValue(CHANGE_APPLICATION_QUEUE));
    } else if (cliParser.hasOption(SIGNAL_CMD)) {
      if (args.length < 3 || args.length > 4) {
        printUsage(title, opts);
        return exitCode;
      }
      final String[] signalArgs = cliParser.getOptionValues(SIGNAL_CMD);
      final String containerId = signalArgs[0];
      SignalContainerCommand command =
          SignalContainerCommand.OUTPUT_THREAD_DUMP;
      if (signalArgs.length == 2) {
        command = SignalContainerCommand.valueOf(signalArgs[1]);
      }
      signalToContainer(containerId, command);
    } else {
      syserr.println("Invalid Command Usage : ");
      printUsage(title, opts);
    }
    return 0;
  }

  private void updateApplicationTimeout(String applicationId,
      ApplicationTimeoutType timeoutType, long timeoutInSec)
      throws YarnException, IOException {
    ApplicationId appId = ApplicationId.fromString(applicationId);
    String newTimeout =
        Times.formatISO8601(System.currentTimeMillis() + timeoutInSec * 1000);
    sysout.println("Updating timeout for given timeoutType: "
        + timeoutType.toString() + " of an application " + applicationId);
    UpdateApplicationTimeoutsRequest request = UpdateApplicationTimeoutsRequest
        .newInstance(appId, Collections.singletonMap(timeoutType, newTimeout));
    UpdateApplicationTimeoutsResponse updateApplicationTimeouts =
        client.updateApplicationTimeouts(request);
    String updatedTimeout =
        updateApplicationTimeouts.getApplicationTimeouts().get(timeoutType);

    if (timeoutType.equals(ApplicationTimeoutType.LIFETIME)
        && !newTimeout.equals(updatedTimeout)) {
      sysout.println("Updated lifetime of an application  " + applicationId
          + " to queue max/default lifetime." + " New expiry time is "
          + updatedTimeout);
      return;
    }
    sysout.println(
        "Successfully updated " + timeoutType.toString() + " of an application "
            + applicationId + ". New expiry time is " + updatedTimeout);
  }

  /**
   * Signals the containerId
   *
   * @param containerIdStr the container id
   * @param command the signal command
   * @throws YarnException
   */
  private void signalToContainer(String containerIdStr,
      SignalContainerCommand command) throws YarnException, IOException {
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    sysout.println("Signalling container " + containerIdStr);
    client.signalToContainer(containerId, command);
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
      appAttemptReport = client.getApplicationAttemptReport(
          ApplicationAttemptId.fromString(applicationAttemptId));
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
      appAttemptReportStr
          .println(appAttemptReport.getAMContainerId() == null ? "N/A"
              : appAttemptReport.getAMContainerId().toString());
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
      containerReport = client.getContainerReport(ContainerId.fromString(containerId));
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
   * Lists the applications matching the given application Types, application
   * States and application Tags present in the Resource Manager.
   * 
   * @param appTypes
   * @param appStates
   * @param appTags
   * @throws YarnException
   * @throws IOException
   */
  private void listApplications(Set<String> appTypes,
      EnumSet<YarnApplicationState> appStates, Set<String> appTags)
      throws YarnException, IOException {
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
        appStates, appTags);

    writer.println("Total number of applications (application-types: "
        + appTypes + ", states: " + appStates + " and tags: " + appTags + ")"
        + ":" + appsReport.size());
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
   * Kills applications with the application id as appId
   *
   * @param Array of applicationIds
   * @return errorCode
   * @throws YarnException
   * @throws IOException
   */
  private int killApplication(String[] applicationIds) throws YarnException,
      IOException {
    int returnCode = -1;
    for (String applicationId : applicationIds) {
      try {
        killApplication(applicationId);
        returnCode = 0;
      } catch (ApplicationNotFoundException e) {
        // Suppress all ApplicationNotFoundException for now.
        continue;
      }
    }

    return returnCode;
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
    ApplicationId appId = ApplicationId.fromString(applicationId);
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
    ApplicationId appId = ApplicationId.fromString(applicationId);
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
   * Fails an application attempt.
   *
   * @param attemptId ID of the attempt to fail. If provided, applicationId
   *        parameter is not used.
   * @throws YarnException
   * @throws IOException
   */
  private void failApplicationAttempt(String attemptId) throws YarnException,
      IOException {
    ApplicationId appId;
    ApplicationAttemptId attId;
    attId = ApplicationAttemptId.fromString(attemptId);
    appId = attId.getApplicationId();

    sysout.println("Failing attempt " + attId + " of application " + appId);
    client.failApplicationAttempt(attId);
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
      appReport = client.getApplicationReport(
          ApplicationId.fromString(applicationId));
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
      appReportStr.print("\tApplication Priority : ");
      appReportStr.println(appReport.getPriority());
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
      ApplicationResourceUsageReport usageReport =
          appReport.getApplicationResourceUsageReport();
      printResourceUsage(appReportStr, usageReport);
      appReportStr.print("\tLog Aggregation Status : ");
      appReportStr.println(appReport.getLogAggregationStatus() == null ? "N/A"
          : appReport.getLogAggregationStatus());
      appReportStr.print("\tDiagnostics : ");
      appReportStr.println(appReport.getDiagnostics());
      appReportStr.print("\tUnmanaged Application : ");
      appReportStr.println(appReport.isUnmanagedApp());
      appReportStr.print("\tApplication Node Label Expression : ");
      appReportStr.println(appReport.getAppNodeLabelExpression());
      appReportStr.print("\tAM container Node Label Expression : ");
      appReportStr.println(appReport.getAmNodeLabelExpression());
      for (ApplicationTimeout timeout : appReport.getApplicationTimeouts()
          .values()) {
        appReportStr.print("\tTimeoutType : " + timeout.getTimeoutType());
        appReportStr.print("\tExpiryTime : " + timeout.getExpiryTime());
        appReportStr.println(
            "\tRemainingTime : " + timeout.getRemainingTime() + "seconds");
      }
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

  private void printResourceUsage(PrintWriter appReportStr,
      ApplicationResourceUsageReport usageReport) {
    appReportStr.print("\tAggregate Resource Allocation : ");
    if (usageReport != null) {
      appReportStr.println(
          getResourceSecondsString(usageReport.getResourceSecondsMap()));
      appReportStr.print("\tAggregate Resource Preempted : ");
      appReportStr.println(getResourceSecondsString(
          usageReport.getPreemptedResourceSecondsMap()));
    } else {
      appReportStr.println("N/A");
      appReportStr.print("\tAggregate Resource Preempted : ");
      appReportStr.println("N/A");
    }
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
        .getApplicationAttempts(ApplicationId.fromString(applicationId));
    writer.println("Total number of application attempts " + ":"
        + appAttemptsReport.size());
    writer.printf(APPLICATION_ATTEMPTS_PATTERN, "ApplicationAttempt-Id",
        "State", "AM-Container-Id", "Tracking-URL");
    for (ApplicationAttemptReport appAttemptReport : appAttemptsReport) {
      writer.printf(APPLICATION_ATTEMPTS_PATTERN, appAttemptReport
          .getApplicationAttemptId(), appAttemptReport
          .getYarnApplicationAttemptState(), appAttemptReport
          .getAMContainerId() == null ? "N/A" : appAttemptReport
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

    List<ContainerReport> appsReport = client.getContainers(
        ApplicationAttemptId.fromString(appAttemptId));
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

  /**
   * Updates priority of an application with the given ID.
   */
  private void updateApplicationPriority(String applicationId, String priority)
      throws YarnException, IOException {
    ApplicationId appId = ApplicationId.fromString(applicationId);
    Priority newAppPriority = Priority.newInstance(Integer.parseInt(priority));
    sysout.println("Updating priority of an application " + applicationId);
    Priority updateApplicationPriority =
        client.updateApplicationPriority(appId, newAppPriority);
    if (newAppPriority.equals(updateApplicationPriority)) {
      sysout.println("Successfully updated the application "
          + applicationId + " with priority '" + priority + "'");
    } else {
      sysout
          .println("Updated priority of an application  "
              + applicationId
          + " to cluster max priority OR keeping old priority"
          + " as application is in final states");
    }
  }

  @SuppressWarnings("unchecked")
  private boolean hasAnyOtherCLIOptions(CommandLine cliParser, Options opts,
      String excludeOption) {
    Collection<Option> ops = opts.getOptions();
    for (Option op : ops) {
      // Skip exclude option from the option list
      if (op.getOpt().equals(excludeOption)) {
        continue;
      }
      if (cliParser.hasOption(op.getOpt())) {
        return true;
      }
    }
    return false;
  }
}
