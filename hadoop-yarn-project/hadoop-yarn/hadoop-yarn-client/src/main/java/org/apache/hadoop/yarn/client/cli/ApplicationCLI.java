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
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.*;

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
import org.apache.hadoop.yarn.client.api.AppAdminClient;
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

  public static final String APP = "app";
  public static final String APPLICATION = "application";
  public static final String APPLICATION_ATTEMPT = "applicationattempt";
  public static final String CONTAINER = "container";
  public static final String APP_ID = "appId";
  public static final String UPDATE_PRIORITY = "updatePriority";
  public static final String UPDATE_LIFETIME = "updateLifetime";
  public static final String CHANGE_APPLICATION_QUEUE = "changeQueue";

  // app admin options
  public static final String LAUNCH_CMD = "launch";
  public static final String STOP_CMD = "stop";
  public static final String START_CMD = "start";
  public static final String SAVE_CMD = "save";
  public static final String DESTROY_CMD = "destroy";
  public static final String FLEX_CMD = "flex";
  public static final String COMPONENT = "component";
  public static final String DECOMMISSION = "decommission";
  public static final String ENABLE_FAST_LAUNCH = "enableFastLaunch";
  public static final String UPGRADE_CMD = "upgrade";
  public static final String UPGRADE_EXPRESS = "express";
  public static final String UPGRADE_CANCEL = "cancel";
  public static final String UPGRADE_INITIATE = "initiate";
  public static final String UPGRADE_AUTO_FINALIZE = "autoFinalize";
  public static final String UPGRADE_FINALIZE = "finalize";
  public static final String COMPONENT_INSTS = "instances";
  public static final String COMPONENTS = "components";
  public static final String VERSION = "version";
  public static final String STATES = "states";

  private static String firstArg = null;

  private boolean allAppStates;

  public static void main(String[] args) throws Exception {
    ApplicationCLI cli = new ApplicationCLI();
    cli.setSysOutPrintStream(System.out);
    cli.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(cli, preProcessArgs(args));
    cli.stop();
    System.exit(res);
  }

  @VisibleForTesting
  public static String[] preProcessArgs(String[] args) {
    if (args.length > 0) {
      // first argument (app|application|applicationattempt|container) must
      // be stripped off for GenericOptionsParser to work
      firstArg = args[0];
      return Arrays.copyOfRange(args, 1, args.length);
    } else {
      return args;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Options opts = new Options();
    String title = null;
    if (firstArg != null) {
      title = firstArg;
    } else if (args.length > 0) {
      title = args[0];
    }
    if (title != null && (title.equalsIgnoreCase(APPLICATION) || title
        .equalsIgnoreCase(APP))) {
      title = APPLICATION;
      opts.addOption(STATUS_CMD, true,
          "Prints the status of the application. If app ID is"
              + " provided, it prints the generic YARN application status."
              + " If name is provided, it prints the application specific"
              + " status based on app's own implementation, and -appTypes"
              + " option must be specified unless it is the default"
              + " yarn-service type.");
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
      opts.getOption(STATUS_CMD).setArgName("Application Name or ID");
      opts.getOption(APP_ID).setArgName("Application ID");
      opts.getOption(UPDATE_PRIORITY).setArgName("Priority");
      opts.getOption(UPDATE_LIFETIME).setArgName("Timeout");
      opts.getOption(CHANGE_APPLICATION_QUEUE).setArgName("Queue Name");
      opts.addOption(LAUNCH_CMD, true, "Launches application from " +
          "specification file (saves specification and starts application). " +
          "Options -updateLifetime and -changeQueue can be specified to alter" +
          " the values provided in the file. Supports -appTypes option to " +
          "specify which client implementation to use.");
      opts.addOption(STOP_CMD, true, "Stops application gracefully (may be " +
          "started again later). If name is provided, appType must be " +
          "provided unless it is the default yarn-service. If ID is provided," +
          " the appType will be looked up. Supports -appTypes option to " +
          "specify which client implementation to use.");
      opts.addOption(START_CMD, true, "Starts a previously saved " +
          "application. Supports -appTypes option to specify which client " +
          "implementation to use.");
      opts.addOption(SAVE_CMD, true, "Saves specification file for " +
          "an application. Options -updateLifetime and -changeQueue can be " +
          "specified to alter the values provided in the file. Supports " +
          "-appTypes option to specify which client implementation to use.");
      opts.addOption(DESTROY_CMD, true, "Destroys a saved application " +
          "specification and removes all application data permanently. " +
          "Supports -appTypes option to specify which client implementation " +
          "to use.");
      opts.addOption(FLEX_CMD, true, "Changes number of " +
          "running containers for a component of an application / " +
          "long-running service. Requires -component option. If name is " +
          "provided, appType must be provided unless it is the default " +
          "yarn-service. If ID is provided, the appType will be looked up. " +
          "Supports -appTypes option to specify which client implementation " +
          "to use.");
      opts.addOption(DECOMMISSION, true, "Decommissions component " +
          "instances for an application / long-running service. Requires " +
          "-instances option. Supports -appTypes option to specify which " +
          "client implementation to use.");
      opts.addOption(COMPONENT, true, "Works with -flex option to change " +
          "the number of components/containers running for an application / " +
          "long-running service. Supports absolute or relative changes, such " +
          "as +1, 2, or -3.");
      opts.addOption(ENABLE_FAST_LAUNCH, true, "Uploads AM dependencies " +
          "to HDFS to make future launches faster. Supports -appTypes option " +
          "to specify which client implementation to use. Optionally a " +
          "destination folder for the tarball can be specified.");
      opts.addOption(UPGRADE_CMD, true, "Upgrades an application/long-" +
          "running service. It requires either -initiate, -instances, or " +
          "-finalize options.");
      opts.addOption(UPGRADE_EXPRESS, true, "Works with -upgrade option to " +
          "perform express upgrade.  It requires the upgraded application " +
          "specification file.");
      opts.addOption(UPGRADE_INITIATE, true, "Works with -upgrade option to " +
          "initiate the application upgrade. It requires the upgraded " +
          "application specification file.");
      opts.addOption(COMPONENT_INSTS, true, "Works with -upgrade option to " +
          "trigger the upgrade of specified component instances of the " +
          "application. Also works with -decommission option to decommission " +
          "specified component instances. Multiple instances should be " +
          "separated by commas.");
      opts.addOption(COMPONENTS, true, "Works with -upgrade option to " +
          "trigger the upgrade of specified components of the application. " +
          "Multiple components should be separated by commas.");
      opts.addOption(UPGRADE_FINALIZE, false, "Works with -upgrade option to " +
          "finalize the upgrade.");
      opts.addOption(UPGRADE_AUTO_FINALIZE, false, "Works with -upgrade and " +
          "-initiate options to initiate the upgrade of the application with " +
          "the ability to finalize the upgrade automatically.");
      opts.addOption(UPGRADE_CANCEL, false, "Works with -upgrade option to " +
          "cancel current upgrade.");
      opts.getOption(LAUNCH_CMD).setArgName("Application Name> <File Name");
      opts.getOption(LAUNCH_CMD).setArgs(2);
      opts.getOption(START_CMD).setArgName("Application Name");
      opts.getOption(STOP_CMD).setArgName("Application Name or ID");
      opts.getOption(SAVE_CMD).setArgName("Application Name> <File Name");
      opts.getOption(SAVE_CMD).setArgs(2);
      opts.getOption(DESTROY_CMD).setArgName("Application Name");
      opts.getOption(FLEX_CMD).setArgName("Application Name or ID");
      opts.getOption(COMPONENT).setArgName("Component Name> <Count");
      opts.getOption(COMPONENT).setArgs(2);
      opts.getOption(ENABLE_FAST_LAUNCH).setOptionalArg(true);
      opts.getOption(ENABLE_FAST_LAUNCH).setArgName("Destination Folder");
      opts.getOption(UPGRADE_CMD).setArgName("Application Name");
      opts.getOption(UPGRADE_CMD).setArgs(1);
      opts.getOption(UPGRADE_INITIATE).setArgName("File Name");
      opts.getOption(UPGRADE_INITIATE).setArgs(1);
      opts.getOption(COMPONENT_INSTS).setArgName("Component Instances");
      opts.getOption(COMPONENT_INSTS).setValueSeparator(',');
      opts.getOption(COMPONENT_INSTS).setArgs(Option.UNLIMITED_VALUES);
      opts.getOption(COMPONENTS).setArgName("Components");
      opts.getOption(COMPONENTS).setValueSeparator(',');
      opts.getOption(COMPONENTS).setArgs(Option.UNLIMITED_VALUES);
      opts.getOption(DECOMMISSION).setArgName("Application Name");
      opts.getOption(DECOMMISSION).setArgs(1);
    } else if (title != null && title.equalsIgnoreCase(APPLICATION_ATTEMPT)) {
      opts.addOption(STATUS_CMD, true,
          "Prints the status of the application attempt.");
      opts.addOption(LIST_CMD, true,
          "List application attempts for application.");
      opts.addOption(FAIL_CMD, true, "Fails application attempt.");
      opts.addOption(HELP_CMD, false, "Displays help for all commands.");
      opts.getOption(STATUS_CMD).setArgName("Application Attempt ID");
      opts.getOption(LIST_CMD).setArgName("Application ID");
      opts.getOption(FAIL_CMD).setArgName("Application Attempt ID");
    } else if (title != null && title.equalsIgnoreCase(CONTAINER)) {
      opts.addOption(STATUS_CMD, true,
          "Prints the status of the container.");
      opts.addOption(LIST_CMD, true,
          "List containers for application attempt when application " +
          "attempt ID is provided. When application name is provided, " +
          "then it finds the instances of the application based on app's " +
          "own implementation, and -appTypes option must be specified " +
          "unless it is the default yarn-service type. With app name, it " +
          "supports optional use of -version to filter instances based on " +
          "app version, -components to filter instances based on component " +
          "names, -states to filter instances based on instance state.");
      opts.addOption(HELP_CMD, false, "Displays help for all commands.");
      opts.getOption(STATUS_CMD).setArgName("Container ID");
      opts.getOption(LIST_CMD).setArgName("Application Name or Attempt ID");
      opts.addOption(APP_TYPE_CMD, true, "Works with -list to " +
          "specify the app type when application name is provided.");
      opts.getOption(APP_TYPE_CMD).setValueSeparator(',');
      opts.getOption(APP_TYPE_CMD).setArgs(Option.UNLIMITED_VALUES);
      opts.getOption(APP_TYPE_CMD).setArgName("Types");

      opts.addOption(VERSION, true, "Works with -list "
          + "to filter instances based on input application version.");
      opts.getOption(VERSION).setArgs(1);

      opts.addOption(COMPONENTS, true, "Works with -list to " +
          "filter instances based on input comma-separated list of " +
          "component names.");
      opts.getOption(COMPONENTS).setValueSeparator(',');
      opts.getOption(COMPONENTS).setArgs(Option.UNLIMITED_VALUES);

      opts.addOption(STATES, true, "Works with -list to " +
          "filter instances based on input comma-separated list of " +
          "instance states.");
      opts.getOption(STATES).setValueSeparator(',');
      opts.getOption(STATES).setArgs(Option.UNLIMITED_VALUES);

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
    String[] unparsedArgs = cliParser.getArgs();
    if (firstArg == null) {
      if (unparsedArgs.length != 1) {
        printUsage(title, opts);
        return exitCode;
      }
    } else {
      if (unparsedArgs.length != 0) {
        printUsage(title, opts);
        return exitCode;
      }
    }

    if (cliParser.hasOption(STATUS_CMD)) {
      if (hasAnyOtherCLIOptions(cliParser, opts, STATUS_CMD, APP_TYPE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      if (title.equalsIgnoreCase(APPLICATION) ||
          title.equalsIgnoreCase(APP)) {
        String appIdOrName = cliParser.getOptionValue(STATUS_CMD);
        try {
          // try parsing appIdOrName, if it succeeds, it means it's appId
          ApplicationId.fromString(appIdOrName);
          exitCode = printApplicationReport(appIdOrName);
        } catch (IllegalArgumentException e) {
          // not appId format, it could be appName.
          // Print app specific report, if app-type is not provided,
          // assume it is yarn-service type.
          AppAdminClient client = AppAdminClient
              .createAppAdminClient(getSingleAppTypeFromCLI(cliParser),
                  getConf());
          try {
            sysout.println(client.getStatusString(appIdOrName));
            exitCode = 0;
          } catch (ApplicationNotFoundException exception) {
            System.err.println("Application with name '" + appIdOrName
                + "' doesn't exist in RM or Timeline Server.");
            return -1;
          } catch (Exception ie) {
            System.err.println(ie.getMessage());
            return -1;
          }
        }
      } else if (title.equalsIgnoreCase(APPLICATION_ATTEMPT)) {
        exitCode = printApplicationAttemptReport(cliParser
            .getOptionValue(STATUS_CMD));
      } else if (title.equalsIgnoreCase(CONTAINER)) {
        exitCode = printContainerReport(cliParser.getOptionValue(STATUS_CMD));
      }
      return exitCode;
    } else if (cliParser.hasOption(LIST_CMD)) {
      if (title.equalsIgnoreCase(APPLICATION) ||
          title.equalsIgnoreCase(APP)) {
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
      } else if (title.equalsIgnoreCase(APPLICATION_ATTEMPT)) {
        if (hasAnyOtherCLIOptions(cliParser, opts, LIST_CMD)) {
          printUsage(title, opts);
          return exitCode;
        }
        listApplicationAttempts(cliParser.getOptionValue(LIST_CMD));
      } else if (title.equalsIgnoreCase(CONTAINER)) {
        if (hasAnyOtherCLIOptions(cliParser, opts, LIST_CMD, APP_TYPE_CMD,
            VERSION, COMPONENTS, STATES)) {
          printUsage(title, opts);
          return exitCode;
        }
        String appAttemptIdOrName = cliParser.getOptionValue(LIST_CMD);
        try {
          // try parsing attempt id, if it succeeds, it means it's appId
          ApplicationAttemptId.fromString(appAttemptIdOrName);
          listContainers(appAttemptIdOrName);
        } catch (IllegalArgumentException e) {
          // not appAttemptId format, it could be appName. If app-type is not
          // provided, assume it is yarn-service type.
          AppAdminClient client = AppAdminClient
              .createAppAdminClient(getSingleAppTypeFromCLI(cliParser),
                  getConf());
          String version = cliParser.getOptionValue(VERSION);
          String[] components = cliParser.getOptionValues(COMPONENTS);
          String[] instanceStates = cliParser.getOptionValues(STATES);
          try {
            sysout.println(client.getInstances(appAttemptIdOrName,
                components == null ? null : Arrays.asList(components),
                version, instanceStates == null ? null :
                    Arrays.asList(instanceStates)));
            return 0;
          } catch (ApplicationNotFoundException exception) {
            System.err.println("Application with name '" + appAttemptIdOrName
                + "' doesn't exist in RM or Timeline Server.");
            return -1;
          } catch (Exception ex) {
            System.err.println(ex.getMessage());
            return -1;
          }
        }
      }
    } else if (cliParser.hasOption(KILL_CMD)) {
      if (hasAnyOtherCLIOptions(cliParser, opts, KILL_CMD)) {
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
      if (!title.equalsIgnoreCase(APPLICATION_ATTEMPT)) {
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
    } else if (cliParser.hasOption(SIGNAL_CMD)) {
      if (hasAnyOtherCLIOptions(cliParser, opts, SIGNAL_CMD)) {
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
    } else if (cliParser.hasOption(LAUNCH_CMD)) {
      if (hasAnyOtherCLIOptions(cliParser, opts, LAUNCH_CMD, APP_TYPE_CMD,
          UPDATE_LIFETIME, CHANGE_APPLICATION_QUEUE)) {
        printUsage(title, opts);
        return exitCode;
      }
      String appType = getSingleAppTypeFromCLI(cliParser);
      Long lifetime = null;
      if (cliParser.hasOption(UPDATE_LIFETIME)) {
        lifetime = Long.parseLong(cliParser.getOptionValue(UPDATE_LIFETIME));
      }
      String queue = null;
      if (cliParser.hasOption(CHANGE_APPLICATION_QUEUE)) {
        queue = cliParser.getOptionValue(CHANGE_APPLICATION_QUEUE);
      }
      String[] nameAndFile = cliParser.getOptionValues(LAUNCH_CMD);
      return AppAdminClient.createAppAdminClient(appType, getConf())
          .actionLaunch(nameAndFile[1], nameAndFile[0], lifetime, queue);
    } else if (cliParser.hasOption(STOP_CMD)) {
      if (hasAnyOtherCLIOptions(cliParser, opts, STOP_CMD, APP_TYPE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      String[] appNameAndType = getAppNameAndType(cliParser, STOP_CMD);
      return AppAdminClient.createAppAdminClient(appNameAndType[1], getConf())
          .actionStop(appNameAndType[0]);
    } else if (cliParser.hasOption(START_CMD)) {
      if (hasAnyOtherCLIOptions(cliParser, opts, START_CMD, APP_TYPE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      String appType = getSingleAppTypeFromCLI(cliParser);
      return AppAdminClient.createAppAdminClient(appType, getConf())
          .actionStart(cliParser.getOptionValue(START_CMD));
    } else if (cliParser.hasOption(SAVE_CMD)) {
      if (hasAnyOtherCLIOptions(cliParser, opts, SAVE_CMD, APP_TYPE_CMD,
          UPDATE_LIFETIME, CHANGE_APPLICATION_QUEUE)) {
        printUsage(title, opts);
        return exitCode;
      }
      String appType = getSingleAppTypeFromCLI(cliParser);
      Long lifetime = null;
      if (cliParser.hasOption(UPDATE_LIFETIME)) {
        lifetime = Long.parseLong(cliParser.getOptionValue(UPDATE_LIFETIME));
      }
      String queue = null;
      if (cliParser.hasOption(CHANGE_APPLICATION_QUEUE)) {
        queue = cliParser.getOptionValue(CHANGE_APPLICATION_QUEUE);
      }
      String[] nameAndFile = cliParser.getOptionValues(SAVE_CMD);
      return AppAdminClient.createAppAdminClient(appType, getConf())
          .actionSave(nameAndFile[1], nameAndFile[0], lifetime, queue);
    } else if (cliParser.hasOption(DESTROY_CMD)) {
      if (hasAnyOtherCLIOptions(cliParser, opts, DESTROY_CMD, APP_TYPE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      String appType = getSingleAppTypeFromCLI(cliParser);
      return AppAdminClient.createAppAdminClient(appType, getConf())
          .actionDestroy(cliParser.getOptionValue(DESTROY_CMD));
    } else if (cliParser.hasOption(FLEX_CMD)) {
      if (!cliParser.hasOption(COMPONENT) ||
          hasAnyOtherCLIOptions(cliParser, opts, FLEX_CMD, COMPONENT,
              APP_TYPE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      String[] rawCounts = cliParser.getOptionValues(COMPONENT);
      Map<String, String> counts = new HashMap<>(rawCounts.length/2);
      for (int i = 0; i < rawCounts.length - 1; i+=2) {
        counts.put(rawCounts[i], rawCounts[i+1]);
      }
      String[] appNameAndType = getAppNameAndType(cliParser, FLEX_CMD);
      return AppAdminClient.createAppAdminClient(appNameAndType[1], getConf())
          .actionFlex(appNameAndType[0], counts);
    } else if (cliParser.hasOption(ENABLE_FAST_LAUNCH)) {
      String appType = getSingleAppTypeFromCLI(cliParser);
      String uploadDestinationFolder = cliParser
          .getOptionValue(ENABLE_FAST_LAUNCH);
      if (hasAnyOtherCLIOptions(cliParser, opts, ENABLE_FAST_LAUNCH,
          APP_TYPE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      return AppAdminClient.createAppAdminClient(appType, getConf())
          .enableFastLaunch(uploadDestinationFolder);
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
    } else if (cliParser.hasOption(UPGRADE_CMD)) {
      if (hasAnyOtherCLIOptions(cliParser, opts, UPGRADE_CMD, UPGRADE_EXPRESS,
          UPGRADE_INITIATE, UPGRADE_AUTO_FINALIZE, UPGRADE_FINALIZE,
          UPGRADE_CANCEL, COMPONENT_INSTS, COMPONENTS, APP_TYPE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      String appType = getSingleAppTypeFromCLI(cliParser);
      AppAdminClient client =  AppAdminClient.createAppAdminClient(appType,
          getConf());
      String appName = cliParser.getOptionValue(UPGRADE_CMD);
      if (cliParser.hasOption(UPGRADE_EXPRESS)) {
        File file = new File(cliParser.getOptionValue(UPGRADE_EXPRESS));
        if (!file.exists()) {
          System.err.println(file.getAbsolutePath() + " does not exist.");
          return exitCode;
        }
        return client.actionUpgradeExpress(appName, file);
      } else if (cliParser.hasOption(UPGRADE_INITIATE)) {
        if (hasAnyOtherCLIOptions(cliParser, opts, UPGRADE_CMD,
            UPGRADE_INITIATE, UPGRADE_AUTO_FINALIZE, APP_TYPE_CMD)) {
          printUsage(title, opts);
          return exitCode;
        }
        String fileName = cliParser.getOptionValue(UPGRADE_INITIATE);
        if (cliParser.hasOption(UPGRADE_AUTO_FINALIZE)) {
          return client.initiateUpgrade(appName, fileName, true);
        } else {
          return client.initiateUpgrade(appName, fileName, false);
        }
      } else if (cliParser.hasOption(COMPONENT_INSTS)) {
        if (hasAnyOtherCLIOptions(cliParser, opts, UPGRADE_CMD,
            COMPONENT_INSTS, APP_TYPE_CMD)) {
          printUsage(title, opts);
          return exitCode;
        }
        String[] instances = cliParser.getOptionValues(COMPONENT_INSTS);
        return client.actionUpgradeInstances(appName, Arrays.asList(instances));
      } else if (cliParser.hasOption(COMPONENTS)) {
        if (hasAnyOtherCLIOptions(cliParser, opts, UPGRADE_CMD,
            COMPONENTS, APP_TYPE_CMD)) {
          printUsage(title, opts);
          return exitCode;
        }
        String[] components = cliParser.getOptionValues(COMPONENTS);
        return client.actionUpgradeComponents(appName,
            Arrays.asList(components));
      } else if (cliParser.hasOption(UPGRADE_FINALIZE)) {
        if (hasAnyOtherCLIOptions(cliParser, opts, UPGRADE_CMD,
            UPGRADE_FINALIZE, APP_TYPE_CMD)) {
          printUsage(title, opts);
          return exitCode;
        }
        return client.actionStart(appName);
      } else if (cliParser.hasOption(UPGRADE_CANCEL)) {
        if (hasAnyOtherCLIOptions(cliParser, opts, UPGRADE_CMD,
            UPGRADE_CANCEL, APP_TYPE_CMD)) {
          printUsage(title, opts);
          return exitCode;
        }
        return client.actionCancelUpgrade(appName);
      }
    } else if (cliParser.hasOption(DECOMMISSION)) {
      if (!cliParser.hasOption(COMPONENT_INSTS) ||
          hasAnyOtherCLIOptions(cliParser, opts, DECOMMISSION, COMPONENT_INSTS,
              APP_TYPE_CMD)) {
        printUsage(title, opts);
        return exitCode;
      }
      String[] instances = cliParser.getOptionValues(COMPONENT_INSTS);
      String[] appNameAndType = getAppNameAndType(cliParser, DECOMMISSION);
      return AppAdminClient.createAppAdminClient(appNameAndType[1], getConf())
          .actionDecommissionInstances(appNameAndType[0],
              Arrays.asList(instances));
    } else {
      syserr.println("Invalid Command Usage : ");
      printUsage(title, opts);
    }
    return 0;
  }

  private ApplicationReport getApplicationReport(ApplicationId applicationId)
      throws IOException, YarnException {
    ApplicationReport appReport = null;
    try {
      appReport = client.getApplicationReport(applicationId);
    } catch (ApplicationNotFoundException e) {
      throw new YarnException("Application with id '" + applicationId
          + "' doesn't exist in RM or Timeline Server.");
    }
    return appReport;
  }

  private String[] getAppNameAndType(CommandLine cliParser, String option)
      throws IOException, YarnException {
    String applicationIdOrName = cliParser.getOptionValue(option);
    try {
      ApplicationId id = ApplicationId.fromString(applicationIdOrName);
      ApplicationReport report = getApplicationReport(id);
      return new String[]{report.getName(), report.getApplicationType()};
    } catch (IllegalArgumentException e) {
      // assume CLI option provided the app name
      // and read appType from command line since id wasn't provided
      String appType = getSingleAppTypeFromCLI(cliParser);
      return new String[]{applicationIdOrName, appType};
    }
  }

  private static String getSingleAppTypeFromCLI(CommandLine cliParser) {
    if (cliParser.hasOption(APP_TYPE_CMD)) {
      String[] types = cliParser.getOptionValues(APP_TYPE_CMD);
      if (types != null) {
        for (String type : types) {
          if (!type.trim().isEmpty()) {
            return StringUtils.toLowerCase(type).trim();
          }
        }
      }
    }
    return AppAdminClient.DEFAULT_TYPE;
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
      containerReportStr.print("\tExecution-Type : ");
      containerReportStr.println(containerReport.getExecutionType());
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
   * @param applicationIds Array of applicationIds
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
   * @return ApplicationReport
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
      String... excludeOptions) {
    Collection<Option> ops = opts.getOptions();
    Set<String> excludeSet = new HashSet<>(Arrays.asList(excludeOptions));
    for (Option op : ops) {
      // Skip exclude options from the option list
      if (excludeSet.contains(op.getOpt())) {
        continue;
      }
      if (cliParser.hasOption(op.getOpt())) {
        return true;
      }
    }
    return false;
  }
}
