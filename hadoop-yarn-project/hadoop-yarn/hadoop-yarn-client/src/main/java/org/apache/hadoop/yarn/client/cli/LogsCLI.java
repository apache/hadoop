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
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

@Public
@Evolving
public class LogsCLI extends Configured implements Tool {

  private static final String CONTAINER_ID_OPTION = "containerId";
  private static final String APPLICATION_ID_OPTION = "applicationId";
  private static final String NODE_ADDRESS_OPTION = "nodeAddress";
  private static final String APP_OWNER_OPTION = "appOwner";
  private static final String AM_CONTAINER_OPTION = "am";
  private static final String CONTAINER_LOG_FILES = "logFiles";
  private static final String SHOW_META_INFO = "show_meta_info";
  private static final String LIST_NODES_OPTION = "list_nodes";
  private static final String OUT_OPTION = "out";
  private static final String SIZE_OPTION = "size";
  public static final String HELP_CMD = "help";

  @Override
  public int run(String[] args) throws Exception {
    Options opts = createCommandOpts();
    Options printOpts = createPrintOpts(opts);
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
    boolean getAMContainerLogs = false;
    boolean showMetaInfo = false;
    boolean nodesList = false;
    String[] logFiles = null;
    List<String> amContainersList = new ArrayList<String>();
    String localDir = null;
    long bytes = Long.MAX_VALUE;
    try {
      CommandLine commandLine = parser.parse(opts, args, true);
      appIdStr = commandLine.getOptionValue(APPLICATION_ID_OPTION);
      containerIdStr = commandLine.getOptionValue(CONTAINER_ID_OPTION);
      nodeAddress = commandLine.getOptionValue(NODE_ADDRESS_OPTION);
      appOwner = commandLine.getOptionValue(APP_OWNER_OPTION);
      getAMContainerLogs = commandLine.hasOption(AM_CONTAINER_OPTION);
      showMetaInfo = commandLine.hasOption(SHOW_META_INFO);
      nodesList = commandLine.hasOption(LIST_NODES_OPTION);
      localDir = commandLine.getOptionValue(OUT_OPTION);
      if (getAMContainerLogs) {
        try {
          amContainersList = parseAMContainer(commandLine, printOpts);
        } catch (NumberFormatException ex) {
          System.err.println(ex.getMessage());
          return -1;
        }
      }
      if (commandLine.hasOption(CONTAINER_LOG_FILES)) {
        logFiles = commandLine.getOptionValues(CONTAINER_LOG_FILES);
      }
      if (commandLine.hasOption(SIZE_OPTION)) {
        bytes = Long.parseLong(commandLine.getOptionValue(SIZE_OPTION));
      }
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
      appId = ApplicationId.fromString(appIdStr);
    } catch (Exception e) {
      System.err.println("Invalid ApplicationId specified");
      return -1;
    }

    LogCLIHelpers logCliHelper = new LogCLIHelpers();
    logCliHelper.setConf(getConf());

    YarnApplicationState appState = YarnApplicationState.NEW;
    ApplicationReport appReport = null;
    try {
      appReport = getApplicationReport(appId);
      appState = appReport.getYarnApplicationState();
      if (appState == YarnApplicationState.NEW
          || appState == YarnApplicationState.NEW_SAVING
          || appState == YarnApplicationState.SUBMITTED) {
        System.err.println("Logs are not avaiable right now.");
        return -1;
      }
    } catch (IOException | YarnException e) {
      // If we can not get appReport from either RM or ATS
      // We will assume that this app has already finished.
      appState = YarnApplicationState.FINISHED;
      System.err.println("Unable to get ApplicationState."
          + " Attempting to fetch logs directly from the filesystem.");
    }

    if (appOwner == null || appOwner.isEmpty()) {
      appOwner = guessAppOwner(appReport, appId);
      if (appOwner == null) {
        System.err.println("Can not find the appOwner. "
            + "Please specify the correct appOwner");
        System.err.println("Could not locate application logs for " + appId);
        return -1;
      }
    }

    List<String> logs = new ArrayList<String>();
    if (fetchAllLogFiles(logFiles)) {
      logs.add(".*");
    } else if (logFiles != null && logFiles.length > 0) {
      logs = Arrays.asList(logFiles);
    }

    ContainerLogsRequest request = new ContainerLogsRequest(appId,
        isApplicationFinished(appState), appOwner, nodeAddress, null,
        containerIdStr, localDir, logs, bytes);

    if (showMetaInfo) {
      return showMetaInfo(request, logCliHelper);
    }

    if (nodesList) {
      return showNodeLists(request, logCliHelper);
    }

    // To get am logs
    if (getAMContainerLogs) {
      return fetchAMContainerLogs(request, amContainersList,
          logCliHelper);
    }

    int resultCode = 0;
    if (containerIdStr != null) {
      ContainerId containerId = ContainerId.fromString(containerIdStr);
      if (!containerId.getApplicationAttemptId().getApplicationId()
          .equals(appId)) {
        System.err.println("The Application:" + appId
            + " does not have the container:" + containerId);
        return -1;
      }
      return fetchContainerLogs(request, logCliHelper);
    } else {
      if (nodeAddress == null) {
        resultCode = fetchApplicationLogs(request, logCliHelper);
      } else {
        System.err.println("Should at least provide ContainerId!");
        printHelpMessage(printOpts);
        resultCode = -1;
      }
    }
    return resultCode;
  }

  private ApplicationReport getApplicationReport(ApplicationId appId)
      throws IOException, YarnException {
    YarnClient yarnClient = createYarnClient();

    try {
      return yarnClient.getApplicationReport(appId);
    } finally {
      yarnClient.close();
    }
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
    formatter.printHelp("yarn logs -applicationId <application ID> [OPTIONS]",
        new Options());
    formatter.setSyntaxPrefix("");
    formatter.printHelp("general options are:", options);
  }

  protected List<JSONObject> getAMContainerInfoForRMWebService(
      Configuration conf, String appId) throws ClientHandlerException,
      UniformInterfaceException, JSONException {
    Client webServiceClient = Client.create();
    String webAppAddress = WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getWebAppBindURL(conf, YarnConfiguration.RM_BIND_HOST,
        WebAppUtils.getRMWebAppURLWithoutScheme(conf));
    WebResource webResource = webServiceClient.resource(webAppAddress);

    ClientResponse response =
        webResource.path("ws").path("v1").path("cluster").path("apps")
          .path(appId).path("appattempts").accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    JSONObject json =
        response.getEntity(JSONObject.class).getJSONObject("appAttempts");
    JSONArray requests = json.getJSONArray("appAttempt");
    List<JSONObject> amContainersList = new ArrayList<JSONObject>();
    for (int i = 0; i < requests.length(); i++) {
      amContainersList.add(requests.getJSONObject(i));
    }
    return amContainersList;
  }

  private List<JSONObject> getAMContainerInfoForAHSWebService(
      Configuration conf, String appId) throws ClientHandlerException,
      UniformInterfaceException, JSONException {
    Client webServiceClient = Client.create();
    String webAppAddress =
        WebAppUtils.getHttpSchemePrefix(conf)
            + WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
    WebResource webResource = webServiceClient.resource(webAppAddress);

    ClientResponse response =
        webResource.path("ws").path("v1").path("applicationhistory")
          .path("apps").path(appId).path("appattempts")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    JSONObject json = response.getEntity(JSONObject.class);
    JSONArray requests = json.getJSONArray("appAttempt");
    List<JSONObject> amContainersList = new ArrayList<JSONObject>();
    for (int i = 0; i < requests.length(); i++) {
      amContainersList.add(requests.getJSONObject(i));
    }
    Collections.reverse(amContainersList);
    return amContainersList;
  }

  private boolean fetchAllLogFiles(String[] logFiles) {
    if(logFiles != null) {
      List<String> logs = Arrays.asList(logFiles);
      if(logs.contains("ALL") || logs.contains(".*")) {
        return true;
      }
    }
    return false;
  }

  private List<String> getContainerLogFiles(Configuration conf,
      String containerIdStr, String nodeHttpAddress) throws IOException {
    List<String> logFiles = new ArrayList<>();
    Client webServiceClient = Client.create();
    try {
      WebResource webResource = webServiceClient
          .resource(WebAppUtils.getHttpSchemePrefix(conf) + nodeHttpAddress);
      ClientResponse response =
          webResource.path("ws").path("v1").path("node").path("containers")
              .path(containerIdStr).accept(MediaType.APPLICATION_XML)
              .get(ClientResponse.class);
      if (response.getStatusInfo().getStatusCode() ==
          ClientResponse.Status.OK.getStatusCode()) {
        try {
          String xml = response.getEntity(String.class);
          DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
          DocumentBuilder db = dbf.newDocumentBuilder();
          InputSource is = new InputSource();
          is.setCharacterStream(new StringReader(xml));
          Document dom = db.parse(is);
          NodeList elements = dom.getElementsByTagName("containerLogFiles");
          for (int i = 0; i < elements.getLength(); i++) {
            logFiles.add(elements.item(i).getTextContent());
          }
        } catch (Exception e) {
          System.err.println("Unable to parse xml from webservice. Error:");
          System.err.println(e.getMessage());
          throw new IOException(e);
        }
      }

    } catch (ClientHandlerException | UniformInterfaceException ex) {
      System.err.println("Unable to fetch log files list");
      throw new IOException(ex);
    }
    return logFiles;
  }

  @Private
  @VisibleForTesting
  public void printContainerLogsFromRunningApplication(Configuration conf,
      ContainerLogsRequest request, LogCLIHelpers logCliHelper)
      throws IOException {
    String containerIdStr = request.getContainerId().toString();
    String localDir = request.getOutputLocalDir();
    String nodeHttpAddress = request.getNodeHttpAddress();
    String nodeId = request.getNodeId();
    PrintStream out = logCliHelper.createPrintStream(localDir, nodeId,
        containerIdStr);
    try {
      // fetch all the log files for the container
      // filter the log files based on the given --logFiles pattern
      List<String> allLogs=
          getContainerLogFiles(getConf(), containerIdStr, nodeHttpAddress);
      List<String> matchedFiles = getMatchedLogFiles(
          request, allLogs, true);
      if (matchedFiles.isEmpty()) {
        return;
      }
      ContainerLogsRequest newOptions = new ContainerLogsRequest(request);
      newOptions.setLogTypes(matchedFiles);

      Client webServiceClient = Client.create();
      String containerString = "\n\nContainer: " + containerIdStr + " on "
          + nodeId;
      out.println(containerString);
      out.println(StringUtils.repeat("=", containerString.length()));

      for (String logFile : newOptions.getLogTypes()) {
        out.println("LogType:" + logFile);
        out.println("Log Upload Time:"
            + Times.format(System.currentTimeMillis()));
        out.println("Log Contents:");
        try {
          WebResource webResource =
              webServiceClient.resource(WebAppUtils.getHttpSchemePrefix(conf)
                  + nodeHttpAddress);
          ClientResponse response =
              webResource.path("ws").path("v1").path("node")
                .path("containerlogs").path(containerIdStr).path(logFile)
                .queryParam("size", Long.toString(request.getBytes()))
                .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
          out.println(response.getEntity(String.class));
          out.println("End of LogType:" + logFile + ". This log file belongs"
              + " to a running container (" + containerIdStr + ") and so may"
              + " not be complete.");
          out.flush();
        } catch (ClientHandlerException | UniformInterfaceException ex) {
          System.err.println("Can not find the log file:" + logFile
              + " for the container:" + containerIdStr + " in NodeManager:"
              + nodeId);
        }
      }
      // for the case, we have already uploaded partial logs in HDFS
      logCliHelper.dumpAContainersLogsForALogType(newOptions, false);
    } finally {
      logCliHelper.closePrintStream(out);
    }
  }

  private int printContainerLogsForFinishedApplication(
      ContainerLogsRequest request, LogCLIHelpers logCliHelper)
      throws IOException {
    ContainerLogsRequest newOptions = getMatchedLogOptions(
        request, logCliHelper);
    if (newOptions == null) {
      return -1;
    }
    return logCliHelper.dumpAContainersLogsForALogType(newOptions);
  }

  private int printContainerLogsForFinishedApplicationWithoutNodeId(
      ContainerLogsRequest request, LogCLIHelpers logCliHelper)
      throws IOException {
    ContainerLogsRequest newOptions = getMatchedLogOptions(
        request, logCliHelper);
    if (newOptions == null) {
      return -1;
    }
    return logCliHelper.dumpAContainersLogsForALogTypeWithoutNodeId(
        newOptions);
  }

  @Private
  @VisibleForTesting
  public ContainerReport getContainerReport(String containerIdStr)
      throws YarnException, IOException {
    YarnClient yarnClient = createYarnClient();
    try {
      return yarnClient.getContainerReport(
          ContainerId.fromString(containerIdStr));
    } finally {
      yarnClient.close();
    }
  }

  private boolean isApplicationFinished(YarnApplicationState appState) {
    return appState == YarnApplicationState.FINISHED
        || appState == YarnApplicationState.FAILED
        || appState == YarnApplicationState.KILLED; 
  }

  private int printAMContainerLogs(Configuration conf,
      ContainerLogsRequest request, List<String> amContainers,
      LogCLIHelpers logCliHelper) throws Exception {
    List<JSONObject> amContainersList = null;
    List<ContainerLogsRequest> requests =
        new ArrayList<ContainerLogsRequest>();
    boolean getAMContainerLists = false;
    String appId = request.getAppId().toString();
    String errorMessage = "";
    try {
      amContainersList = getAMContainerInfoForRMWebService(conf, appId);
      if (amContainersList != null && !amContainersList.isEmpty()) {
        getAMContainerLists = true;
        for (JSONObject amContainer : amContainersList) {
          ContainerLogsRequest amRequest = new ContainerLogsRequest(request);
          amRequest.setContainerId(amContainer.getString("containerId"));
          amRequest.setNodeHttpAddress(
              amContainer.getString("nodeHttpAddress"));
          amRequest.setNodeId(amContainer.getString("nodeId"));
          requests.add(amRequest);
        }
      }
    } catch (Exception ex) {
      errorMessage = ex.getMessage();
      if (request.isAppFinished()) {
        try {
          amContainersList = getAMContainerInfoForAHSWebService(conf, appId);
          if (amContainersList != null && !amContainersList.isEmpty()) {
            getAMContainerLists = true;
            for (JSONObject amContainer : amContainersList) {
              ContainerLogsRequest amRequest = new ContainerLogsRequest(
                  request);
              amRequest.setContainerId(amContainer.getString("amContainerId"));
              requests.add(amRequest);
            }
          }
        } catch (Exception e) {
          errorMessage = e.getMessage();
        }
      }
    }

    if (!getAMContainerLists) {
      System.err.println("Unable to get AM container informations "
          + "for the application:" + appId);
      System.err.println(errorMessage);
      return -1;
    }

    if (amContainers.contains("ALL")) {
      for (ContainerLogsRequest amRequest : requests) {
        outputAMContainerLogs(amRequest, conf, logCliHelper);
      }
      System.out.println();      
      System.out.println("Specified ALL for -am option. "
          + "Printed logs for all am containers.");
    } else {
      for (String amContainer : amContainers) {
        int amContainerId = Integer.parseInt(amContainer.trim());
        if (amContainerId == -1) {
          outputAMContainerLogs(requests.get(requests.size() - 1), conf,
              logCliHelper);
        } else {
          if (amContainerId <= requests.size()) {
            outputAMContainerLogs(requests.get(amContainerId - 1), conf,
                logCliHelper);
          } else {
            System.err.println(String.format("ERROR: Specified AM containerId"
                + " (%s) exceeds the number of AM containers (%s).",
                amContainerId, requests.size()));
            return -1;
          }
        }
      }
    }
    return 0;
  }

  private void outputAMContainerLogs(ContainerLogsRequest request,
      Configuration conf, LogCLIHelpers logCliHelper) throws Exception {
    String nodeHttpAddress = request.getNodeHttpAddress();
    String containerId = request.getContainerId();
    String nodeId = request.getNodeId();

    if (request.isAppFinished()) {
      if (containerId != null && !containerId.isEmpty()) {
        if (nodeId == null || nodeId.isEmpty()) {
          try {
            nodeId =
                getContainerReport(containerId).getAssignedNode().toString();
            request.setNodeId(nodeId);
          } catch (Exception ex) {
            System.err.println(ex);
            nodeId = null;
          }
        }
        if (nodeId != null && !nodeId.isEmpty()) {
          printContainerLogsForFinishedApplication(request,
              logCliHelper);
        }
      }
    } else {
      if (nodeHttpAddress != null && containerId != null
          && !nodeHttpAddress.isEmpty() && !containerId.isEmpty()) {
        printContainerLogsFromRunningApplication(conf,
            request, logCliHelper);
      }
    }
  }

  private int showMetaInfo(ContainerLogsRequest request,
      LogCLIHelpers logCliHelper) throws IOException {
    if (!request.isAppFinished()) {
      System.err.println("The -show_meta_info command can be only used "
          + "with finished applications");
      return -1;
    } else {
      logCliHelper.printLogMetadata(request, System.out, System.err);
      return 0;
    }
  }

  private int showNodeLists(ContainerLogsRequest request,
      LogCLIHelpers logCliHelper) throws IOException {
    if (!request.isAppFinished()) {
      System.err.println("The -list_nodes command can be only used with "
          + "finished applications");
      return -1;
    } else {
      logCliHelper.printNodesList(request, System.out, System.err);
      return 0;
    }
  }

  private Options createCommandOpts() {
    Options opts = new Options();
    opts.addOption(HELP_CMD, false, "Displays help for all commands.");
    Option appIdOpt =
        new Option(APPLICATION_ID_OPTION, true, "ApplicationId (required)");
    appIdOpt.setRequired(true);
    opts.addOption(appIdOpt);
    opts.addOption(CONTAINER_ID_OPTION, true, "ContainerId. "
        + "By default, it will only print syslog if the application is runing."
        + " Work with -logFiles to get other logs.");
    opts.addOption(NODE_ADDRESS_OPTION, true, "NodeAddress in the format "
        + "nodename:port");
    opts.addOption(APP_OWNER_OPTION, true,
        "AppOwner (assumed to be current user if not specified)");
    Option amOption = new Option(AM_CONTAINER_OPTION, true,
        "Prints the AM Container logs for this application. "
        + "Specify comma-separated value to get logs for related AM "
        + "Container. For example, If we specify -am 1,2, we will get "
        + "the logs for the first AM Container as well as the second "
        + "AM Container. To get logs for all AM Containers, use -am ALL. "
        + "To get logs for the latest AM Container, use -am -1. "
        + "By default, it will only print out syslog. Work with -logFiles "
        + "to get other logs");
    amOption.setValueSeparator(',');
    amOption.setArgs(Option.UNLIMITED_VALUES);
    amOption.setArgName("AM Containers");
    opts.addOption(amOption);
    Option logFileOpt = new Option(CONTAINER_LOG_FILES, true,
        "Work with -am/-containerId and specify comma-separated value "
        + "to get specified container log files. Use \"ALL\" to fetch all the "
        + "log files for the container. It also supports Java Regex.");
    logFileOpt.setValueSeparator(',');
    logFileOpt.setArgs(Option.UNLIMITED_VALUES);
    logFileOpt.setArgName("Log File Name");
    opts.addOption(logFileOpt);
    opts.addOption(SHOW_META_INFO, false, "Show the log metadata, "
        + "including log-file names, the size of the log files. "
        + "You can combine this with --containerId to get log metadata for "
        + "the specific container, or with --nodeAddress to get log metadata "
        + "for all the containers on the specific NodeManager. "
        + "Currently, this option can only be used for finished "
        + "applications.");
    opts.addOption(LIST_NODES_OPTION, false,
        "Show the list of nodes that successfully aggregated logs. "
        + "This option can only be used with finished applications.");
    opts.addOption(OUT_OPTION, true, "Local directory for storing individual "
        + "container logs. The container logs will be stored based on the "
        + "node the container ran on.");
    opts.addOption(SIZE_OPTION, true, "Prints the log file's first 'n' bytes "
        + "or the last 'n' bytes. Use negative values as bytes to read from "
        + "the end and positive values as bytes to read from the beginning.");
    opts.getOption(APPLICATION_ID_OPTION).setArgName("Application ID");
    opts.getOption(CONTAINER_ID_OPTION).setArgName("Container ID");
    opts.getOption(NODE_ADDRESS_OPTION).setArgName("Node Address");
    opts.getOption(APP_OWNER_OPTION).setArgName("Application Owner");
    opts.getOption(AM_CONTAINER_OPTION).setArgName("AM Containers");
    opts.getOption(OUT_OPTION).setArgName("Local Directory");
    opts.getOption(SIZE_OPTION).setArgName("size");
    return opts;
  }

  private Options createPrintOpts(Options commandOpts) {
    Options printOpts = new Options();
    printOpts.addOption(commandOpts.getOption(HELP_CMD));
    printOpts.addOption(commandOpts.getOption(CONTAINER_ID_OPTION));
    printOpts.addOption(commandOpts.getOption(NODE_ADDRESS_OPTION));
    printOpts.addOption(commandOpts.getOption(APP_OWNER_OPTION));
    printOpts.addOption(commandOpts.getOption(AM_CONTAINER_OPTION));
    printOpts.addOption(commandOpts.getOption(CONTAINER_LOG_FILES));
    printOpts.addOption(commandOpts.getOption(SHOW_META_INFO));
    printOpts.addOption(commandOpts.getOption(LIST_NODES_OPTION));
    printOpts.addOption(commandOpts.getOption(OUT_OPTION));
    printOpts.addOption(commandOpts.getOption(SIZE_OPTION));
    return printOpts;
  }

  private List<String> parseAMContainer(CommandLine commandLine,
      Options printOpts) throws NumberFormatException {
    List<String> amContainersList = new ArrayList<String>();
    String[] amContainers = commandLine.getOptionValues(AM_CONTAINER_OPTION);
    for (String am : amContainers) {
      boolean errorInput = false;
      if (!am.trim().equalsIgnoreCase("ALL")) {
        try {
          int id = Integer.parseInt(am.trim());
          if (id != -1 && id <= 0) {
            errorInput = true;
          }
        } catch (NumberFormatException ex) {
          errorInput = true;
        }
        if (errorInput) {
          String errMessage =
              "Invalid input for option -am. Valid inputs are 'ALL', -1 "
              + "and any other integer which is larger than 0.";
          printHelpMessage(printOpts);
          throw new NumberFormatException(errMessage);
        }
        amContainersList.add(am.trim());
      } else {
        amContainersList.add("ALL");
        break;
      }
    }
    return amContainersList;
  }

  private int fetchAMContainerLogs(ContainerLogsRequest request,
      List<String> amContainersList, LogCLIHelpers logCliHelper)
      throws Exception {
    List<String> logFiles = request.getLogTypes();
    // if we do not specify the value for CONTAINER_LOG_FILES option,
    // we will only output syslog
    if (logFiles == null || logFiles.isEmpty()) {
      logFiles = Arrays.asList("syslog");
    }
    request.setLogTypes(logFiles);
    // If the application is running, we will call the RM WebService
    // to get the AppAttempts which includes the nodeHttpAddress
    // and containerId for all the AM Containers.
    // After that, we will call NodeManager webService to get the
    // related logs
    if (!request.isAppFinished()) {
      return printAMContainerLogs(getConf(), request, amContainersList,
          logCliHelper);
    } else {
      // If the application is in the final state, we will call RM webservice
      // to get all AppAttempts information first. If we get nothing,
      // we will try to call AHS webservice to get related AppAttempts
      // which includes nodeAddress for the AM Containers.
      // After that, we will use nodeAddress and containerId
      // to get logs from HDFS directly.
      if (getConf().getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
          YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
        return printAMContainerLogs(getConf(), request, amContainersList,
            logCliHelper);
      } else {
        ApplicationId appId = request.getAppId();
        String appOwner = request.getAppOwner();
        System.err.println("Can not get AMContainers logs for "
            + "the application:" + appId + " with the appOwner:" + appOwner);
        System.err.println("This application:" + appId + " has finished."
            + " Please enable the application-history service or explicitly"
            + " use 'yarn logs -applicationId <appId> "
            + "-containerId <containerId> --nodeAddress <nodeHttpAddress>' "
            + "to get the container logs.");
        return -1;
      }
    }
  }

  private int fetchContainerLogs(ContainerLogsRequest request,
      LogCLIHelpers logCliHelper) throws IOException {
    int resultCode = 0;
    String appIdStr = request.getAppId().toString();
    String containerIdStr = request.getContainerId();
    String nodeAddress = request.getNodeId();
    String appOwner = request.getAppOwner();
    boolean isAppFinished = request.isAppFinished();
    List<String> logFiles = request.getLogTypes();
    // if we provide the node address and the application is in the final
    // state, we could directly get logs from HDFS.
    if (nodeAddress != null && isAppFinished) {
      // if user specified "ALL" as the logFiles param, pass empty list
      // to logCliHelper so that it fetches all the logs
      return printContainerLogsForFinishedApplication(
          request, logCliHelper);
    }
    String nodeHttpAddress = null;
    String nodeId = null;
    try {
      // If the nodeAddress is not provided, we will try to get
      // the ContainerReport. In the containerReport, we could get
      // nodeAddress and nodeHttpAddress
      ContainerReport report = getContainerReport(containerIdStr);
      nodeHttpAddress =
          report.getNodeHttpAddress().replaceFirst(
            WebAppUtils.getHttpSchemePrefix(getConf()), "");
      nodeId = report.getAssignedNode().toString();
      request.setNodeId(nodeId);
      request.setNodeHttpAddress(nodeHttpAddress);
    } catch (IOException | YarnException ex) {
      if (isAppFinished) {
        return printContainerLogsForFinishedApplicationWithoutNodeId(
            request, logCliHelper);
      } else {
        System.err.println("Unable to get logs for this container:"
            + containerIdStr + "for the application:" + appIdStr
            + " with the appOwner: " + appOwner);
        System.err.println("The application: " + appIdStr
            + " is still running, and we can not get Container report "
            + "for the container: " + containerIdStr +". Please try later "
            + "or after the application finishes.");
        return -1;
      }
    }
    // If the application is not in the final state,
    // we will provide the NodeHttpAddress and get the container logs
    // by calling NodeManager webservice.
    if (!isAppFinished) {
      // if we do not specify the value for CONTAINER_LOG_FILES option,
      // we will only output syslog
      if (logFiles == null || logFiles.isEmpty()) {
        logFiles = Arrays.asList("syslog");
      }
      request.setLogTypes(logFiles);
      printContainerLogsFromRunningApplication(getConf(), request,
          logCliHelper);
    } else {
      // If the application is in the final state, we will directly
      // get the container logs from HDFS.
      resultCode = printContainerLogsForFinishedApplication(
          request, logCliHelper);
    }
    return resultCode;
  }

  private int fetchApplicationLogs(ContainerLogsRequest options,
      LogCLIHelpers logCliHelper) throws IOException, YarnException {
    // If the application has finished, we would fetch the logs
    // from HDFS.
    // If the application is still running, we would get the full
    // list of the containers first, then fetch the logs for each
    // container from NM.
    int resultCode = 0;
    if (options.isAppFinished()) {
      ContainerLogsRequest newOptions = getMatchedLogOptions(
          options, logCliHelper);
      if (newOptions == null) {
        resultCode = -1;
      } else {
        resultCode =
            logCliHelper.dumpAllContainersLogs(newOptions);
      }
    } else {
      List<ContainerLogsRequest> containerLogRequests =
          getContainersLogRequestForRunningApplication(options);
      for (ContainerLogsRequest container : containerLogRequests) {
        printContainerLogsFromRunningApplication(getConf(), container,
            logCliHelper);
      }
    }
    if (resultCode == -1) {
      System.err.println("Can not find the logs for the application: "
          + options.getAppId() + " with the appOwner: "
          + options.getAppOwner());
    }
    return resultCode;
  }

  private String guessAppOwner(ApplicationReport appReport,
      ApplicationId appId) throws IOException {
    String appOwner = null;
    if (appReport != null) {
      //always use the app owner from the app report if possible
      appOwner = appReport.getUser();
    } else {
      appOwner = UserGroupInformation.getCurrentUser().getShortUserName();
      appOwner = LogCLIHelpers.getOwnerForAppIdOrNull(
          appId, appOwner, getConf());
    }
    return appOwner;
  }

  private ContainerLogsRequest getMatchedLogOptions(
      ContainerLogsRequest request, LogCLIHelpers logCliHelper)
      throws IOException {
    ContainerLogsRequest newOptions = new ContainerLogsRequest(request);
    if (request.getLogTypes() != null && !request.getLogTypes().isEmpty()) {
      List<String> matchedFiles = new ArrayList<String>();
      if (!request.getLogTypes().contains(".*")) {
        Set<String> files = logCliHelper.listContainerLogs(request);
        matchedFiles = getMatchedLogFiles(
            request, files, true);
        if (matchedFiles.isEmpty()) {
          return null;
        }
      }
      newOptions.setLogTypes(matchedFiles);
    }
    return newOptions;
  }

  private List<String> getMatchedLogFiles(ContainerLogsRequest options,
      Collection<String> candidate, boolean printError) throws IOException {
    List<String> matchedFiles = new ArrayList<String>();
    List<String> filePattern = options.getLogTypes();
    for (String file : candidate) {
      if (isFileMatching(file, filePattern)) {
        matchedFiles.add(file);
      }
    }
    if (matchedFiles.isEmpty()) {
      if (printError) {
        System.err.println("Can not find any log file matching the pattern: "
            + options.getLogTypes() + " for the application: "
            + options.getAppId());
      }
    }
    return matchedFiles;
  }

  private boolean isFileMatching(String fileType,
      List<String> logTypes) {
    for (String logType : logTypes) {
      Pattern filterPattern = Pattern.compile(logType);
      boolean match = filterPattern.matcher(fileType).find();
      if (match) {
        return true;
      }
    }
    return false;
  }

  private List<ContainerLogsRequest>
      getContainersLogRequestForRunningApplication(
          ContainerLogsRequest options) throws YarnException, IOException {
    List<ContainerLogsRequest> newOptionsList =
        new ArrayList<ContainerLogsRequest>();
    YarnClient yarnClient = createYarnClient();
    try {
      List<ApplicationAttemptReport> attempts =
          yarnClient.getApplicationAttempts(options.getAppId());
      for (ApplicationAttemptReport attempt : attempts) {
        List<ContainerReport> containers = yarnClient.getContainers(
            attempt.getApplicationAttemptId());
        for (ContainerReport container : containers) {
          ContainerLogsRequest newOptions = new ContainerLogsRequest(options);
          newOptions.setContainerId(container.getContainerId().toString());
          newOptions.setNodeId(container.getAssignedNode().toString());
          newOptions.setNodeHttpAddress(container.getNodeHttpAddress()
              .replaceFirst(WebAppUtils.getHttpSchemePrefix(getConf()), ""));
          // if we do not specify the value for CONTAINER_LOG_FILES option,
          // we will only output syslog
          List<String> logFiles = newOptions.getLogTypes();
          if (logFiles == null || logFiles.isEmpty()) {
            logFiles = Arrays.asList("syslog");
            newOptions.setLogTypes(logFiles);
          }
          newOptionsList.add(newOptions);
        }
      }
      return newOptionsList;
    } finally {
      yarnClient.close();
    }
  }
}
