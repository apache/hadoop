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

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.ws.rs.core.MediaType;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.logaggregation.LogToolUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.YarnWebServiceUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

@Public
@Evolving
public class LogsCLI extends Configured implements Tool {

  private static final String CONTAINER_ID_OPTION = "containerId";
  private static final String APPLICATION_ID_OPTION = "applicationId";
  private static final String NODE_ADDRESS_OPTION = "nodeAddress";
  private static final String APP_OWNER_OPTION = "appOwner";
  private static final String AM_CONTAINER_OPTION = "am";
  private static final String PER_CONTAINER_LOG_FILES_OPTION = "log_files";
  private static final String PER_CONTAINER_LOG_FILES_REGEX_OPTION
      = "log_files_pattern";
  private static final String LIST_NODES_OPTION = "list_nodes";
  private static final String SHOW_APPLICATION_LOG_INFO
      = "show_application_log_info";
  private static final String SHOW_CONTAINER_LOG_INFO
      = "show_container_log_info";
  private static final String OUT_OPTION = "out";
  private static final String SIZE_OPTION = "size";
  private static final String CLIENT_MAX_RETRY_OPTION = "client_max_retries";
  private static final String CLIENT_RETRY_INTERVAL_OPTION
      = "client_retry_interval_ms";
  public static final String HELP_CMD = "help";
  private static final String SIZE_LIMIT_OPTION = "size_limit_mb";

  private PrintStream outStream = System.out;
  private YarnClient yarnClient = null;
  private Client webServiceClient = null;

  private static final int DEFAULT_MAX_RETRIES = 30;
  private static final long DEFAULT_RETRY_INTERVAL = 1000;

  private static final long LOG_SIZE_LIMIT_DEFAULT = 10240L;

  private long logSizeLeft = LOG_SIZE_LIMIT_DEFAULT * 1024 * 1024;
  private long specifedLogLimits = LOG_SIZE_LIMIT_DEFAULT;

  @Private
  @VisibleForTesting
  ClientConnectionRetry connectionRetry;

  @Override
  public int run(String[] args) throws Exception {
    try {
      yarnClient = createYarnClient();
      webServiceClient = new Client(new URLConnectionClientHandler(
          new HttpURLConnectionFactory() {
          @Override
          public HttpURLConnection getHttpURLConnection(URL url)
              throws IOException {
            AuthenticatedURL.Token token = new AuthenticatedURL.Token();
            HttpURLConnection conn = null;
            try {
              conn = new AuthenticatedURL().openConnection(url, token);
            } catch (AuthenticationException e) {
              throw new IOException(e);
            }
            return conn;
          }
        }));
      return runCommand(args);
    } finally {
      if (yarnClient != null) {
        yarnClient.close();
      }
    }
  }

  private int runCommand(String[] args) throws Exception {
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
    boolean nodesList = false;
    boolean showApplicationLogInfo = false;
    boolean showContainerLogInfo = false;
    boolean useRegex = false;
    String[] logFiles = null;
    String[] logFilesRegex = null;
    List<String> amContainersList = new ArrayList<String>();
    String localDir = null;
    long bytes = Long.MAX_VALUE;
    boolean ignoreSizeLimit = false;
    int maxRetries = DEFAULT_MAX_RETRIES;
    long retryInterval = DEFAULT_RETRY_INTERVAL;
    try {
      CommandLine commandLine = parser.parse(opts, args, false);
      appIdStr = commandLine.getOptionValue(APPLICATION_ID_OPTION);
      containerIdStr = commandLine.getOptionValue(CONTAINER_ID_OPTION);
      nodeAddress = commandLine.getOptionValue(NODE_ADDRESS_OPTION);
      appOwner = commandLine.getOptionValue(APP_OWNER_OPTION);
      getAMContainerLogs = commandLine.hasOption(AM_CONTAINER_OPTION);
      nodesList = commandLine.hasOption(LIST_NODES_OPTION);
      localDir = commandLine.getOptionValue(OUT_OPTION);
      showApplicationLogInfo = commandLine.hasOption(
          SHOW_APPLICATION_LOG_INFO);
      showContainerLogInfo = commandLine.hasOption(SHOW_CONTAINER_LOG_INFO);
      if (getAMContainerLogs) {
        try {
          amContainersList = parseAMContainer(commandLine, printOpts);
        } catch (NumberFormatException ex) {
          System.err.println(ex.getMessage());
          return -1;
        }
      }
      if (commandLine.hasOption(PER_CONTAINER_LOG_FILES_OPTION)) {
        logFiles = commandLine.getOptionValues(PER_CONTAINER_LOG_FILES_OPTION);
      }
      if (commandLine.hasOption(PER_CONTAINER_LOG_FILES_REGEX_OPTION)) {
        logFilesRegex = commandLine.getOptionValues(
            PER_CONTAINER_LOG_FILES_REGEX_OPTION);
        useRegex = true;
      }
      if (commandLine.hasOption(SIZE_OPTION)) {
        bytes = Long.parseLong(commandLine.getOptionValue(SIZE_OPTION));
      }
      if (commandLine.hasOption(CLIENT_MAX_RETRY_OPTION)) {
        maxRetries = Integer.parseInt(commandLine.getOptionValue(
            CLIENT_MAX_RETRY_OPTION));
      }
      if (commandLine.hasOption(CLIENT_RETRY_INTERVAL_OPTION)) {
        retryInterval = Long.parseLong(commandLine.getOptionValue(
            CLIENT_RETRY_INTERVAL_OPTION));
      }
      if (commandLine.hasOption(SIZE_LIMIT_OPTION)) {
        specifedLogLimits = Long.parseLong(commandLine.getOptionValue(
            SIZE_LIMIT_OPTION));
        logSizeLeft = specifedLogLimits * 1024 * 1024;
      }
      if (logSizeLeft < 0L) {
        ignoreSizeLimit = true;
      }
    } catch (ParseException e) {
      System.err.println("options parsing failed: " + e.getMessage());
      printHelpMessage(printOpts);
      return -1;
    }

    if (appIdStr == null && containerIdStr == null) {
      System.err.println("Both applicationId and containerId are missing, "
          + " one of them must be specified.");
      printHelpMessage(printOpts);
      return -1;
    }

    ApplicationId appId = null;
    if (appIdStr != null) {
      try {
        appId = ApplicationId.fromString(appIdStr);
      } catch (Exception e) {
        System.err.println("Invalid ApplicationId specified");
        return -1;
      }
    }

    if (containerIdStr != null) {
      try {
        ContainerId containerId = ContainerId.fromString(containerIdStr);
        if (appId == null) {
          appId = containerId.getApplicationAttemptId().getApplicationId();
        } else if (!containerId.getApplicationAttemptId().getApplicationId()
            .equals(appId)) {
          System.err.println("The Application:" + appId
              + " does not have the container:" + containerId);
          return -1;
        }
      } catch (Exception e) {
        System.err.println("Invalid ContainerId specified");
        return -1;
      }
    }

    if (showApplicationLogInfo && showContainerLogInfo) {
      System.err.println("Invalid options. Can only accept one of "
          + "show_application_log_info/show_container_log_info.");
      return -1;
    }

    if (logFiles != null && logFiles.length > 0 && logFilesRegex != null
        && logFilesRegex.length > 0) {
      System.err.println("Invalid options. Can only accept one of "
          + "log_files/log_files_pattern.");
      return -1;
    }
    if (localDir != null) {
      File file = new File(localDir);
      if (file.exists() && file.isFile()) {
        System.err.println("Invalid value for -out option. "
            + "Please provide a directory.");
        return -1;
      }
    }

    // Set up Retry WebService Client
    connectionRetry = new ClientConnectionRetry(maxRetries, retryInterval);
    ClientJerseyRetryFilter retryFilter = new ClientJerseyRetryFilter();
    webServiceClient.addFilter(retryFilter);

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
        System.err.println("Logs are not available right now.");
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

    Set<String> logs = new HashSet<String>();
    if (fetchAllLogFiles(logFiles, logFilesRegex)) {
      logs.add("ALL");
    } else if (logFiles != null && logFiles.length > 0) {
      logs.addAll(Arrays.asList(logFiles));
    } else if (logFilesRegex != null && logFilesRegex.length > 0) {
      logs.addAll(Arrays.asList(logFilesRegex));
    }


    ContainerLogsRequest request = new ContainerLogsRequest(appId,
        isApplicationFinished(appState), appOwner, nodeAddress, null,
        containerIdStr, localDir, logs, bytes, null);

    if (showContainerLogInfo) {
      return showContainerLogInfo(request, logCliHelper);
    }

    if (nodesList) {
      return showNodeLists(request, logCliHelper);
    }

    if (showApplicationLogInfo) {
      return showApplicationLogInfo(request, logCliHelper);
    }
    // To get am logs
    if (getAMContainerLogs) {
      return fetchAMContainerLogs(request, amContainersList,
          logCliHelper, useRegex, ignoreSizeLimit);
    }

    int resultCode = 0;
    if (containerIdStr != null) {
      return fetchContainerLogs(request, logCliHelper, useRegex,
          ignoreSizeLimit);
    } else {
      if (nodeAddress == null) {
        resultCode = fetchApplicationLogs(request, logCliHelper, useRegex,
            ignoreSizeLimit);
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
    return yarnClient.getApplicationReport(appId);
  }
  
  @VisibleForTesting
  protected YarnClient createYarnClient() {
    YarnClient client = YarnClient.createYarnClient();
    client.init(getConf());
    client.start();
    return client;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new YarnConfiguration();
    LogsCLI logDumper = new LogsCLI();
    logDumper.setConf(conf);
    int exitCode = logDumper.run(args);
    System.exit(exitCode);
  }

  private void printHelpMessage(Options options) {
    outStream.println("Retrieve logs for YARN applications.");
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("yarn logs -applicationId <application ID> [OPTIONS]",
        new Options());
    formatter.setSyntaxPrefix("");
    formatter.printHelp("general options are:", options);
  }

  protected List<JSONObject> getAMContainerInfoForRMWebService(
      Configuration conf, String appId) throws ClientHandlerException,
      UniformInterfaceException, JSONException {
    String webAppAddress = WebAppUtils.getRMWebAppURLWithScheme(conf);

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

  private boolean fetchAllLogFiles(String[] logFiles, String[] logFilesRegex) {

    // If no value is specified for the PER_CONTAINER_LOG_FILES_OPTION option
    // and PER_CONTAINER_LOG_FILES_REGEX_OPTION
    // we will assume all logs.
    if ((logFiles == null || logFiles.length == 0) && (
        logFilesRegex == null || logFilesRegex.length == 0)) {
      return true;
    }

    if (logFiles != null && logFiles.length > 0) {
      List<String> logs = Arrays.asList(logFiles);
      if (logs.contains("ALL") || logs.contains("*")) {
        return true;
      }
    }

    if (logFilesRegex != null && logFilesRegex.length > 0) {
      List<String> logsRegex = Arrays.asList(logFilesRegex);
      if (logsRegex.contains(".*")) {
        return true;
      }
    }

    return false;
  }

  private List<Pair<ContainerLogFileInfo, String>> getContainerLogFiles(
      Configuration conf, String containerIdStr, String nodeHttpAddress)
      throws IOException {
    List<Pair<ContainerLogFileInfo, String>> logFileInfos
        = new ArrayList<>();
    try {
      WebResource webResource = webServiceClient
          .resource(WebAppUtils.getHttpSchemePrefix(conf) + nodeHttpAddress);
      ClientResponse response =
          webResource.path("ws").path("v1").path("node").path("containers")
              .path(containerIdStr).path("logs")
              .accept(MediaType.APPLICATION_JSON)
              .get(ClientResponse.class);
      if (response.getStatusInfo().getStatusCode() ==
          ClientResponse.Status.OK.getStatusCode()) {
        try {
          JSONArray array = new JSONArray();
          JSONObject json = response.getEntity(JSONObject.class);
          if (!json.has("containerLogsInfo")) {
            return logFileInfos;
          }
          Object logsInfoObj = json.get("containerLogsInfo");
          if (logsInfoObj instanceof JSONObject) {
            array.put((JSONObject)logsInfoObj);
          } else if (logsInfoObj instanceof JSONArray) {
            JSONArray logsArray = (JSONArray)logsInfoObj;
            for (int i=0; i < logsArray.length(); i++) {
              array.put(logsArray.getJSONObject(i));
            }
          }
          for (int i = 0; i < array.length(); i++) {
            JSONObject log = array.getJSONObject(i);
            String aggregateType = log.has("logAggregationType") ?
                log.getString("logAggregationType") : "N/A";
            if (!log.has("containerLogInfo")) {
              continue;
            }
            Object ob = log.get("containerLogInfo");
            if (ob instanceof JSONArray) {
              JSONArray obArray = (JSONArray)ob;
              for (int j = 0; j < obArray.length(); j++) {
                logFileInfos.add(new Pair<ContainerLogFileInfo, String>(
                    generatePerContainerLogFileInfoFromJSON(
                        obArray.getJSONObject(j)), aggregateType));
              }
            } else if (ob instanceof JSONObject) {
              logFileInfos.add(new Pair<ContainerLogFileInfo, String>(
                  generatePerContainerLogFileInfoFromJSON(
                      (JSONObject)ob), aggregateType));
            }
          }
        } catch (Exception e) {
          System.err.println("Unable to parse json from webservice. Error:");
          System.err.println(e.getMessage());
          throw new IOException(e);
        }
      }

    } catch (ClientHandlerException | UniformInterfaceException ex) {
      System.err.println("Unable to fetch log files list");
      throw new IOException(ex);
    }
    return logFileInfos;
  }

  private ContainerLogFileInfo generatePerContainerLogFileInfoFromJSON(
      JSONObject meta) throws JSONException {
    String fileName = meta.has("fileName") ?
        meta.getString("fileName") : "N/A";
    String fileSize = meta.has("fileSize") ?
        meta.getString("fileSize") : "N/A";
    String lastModificationTime = meta.has("lastModifiedTime") ?
        meta.getString("lastModifiedTime") : "N/A";
    return new ContainerLogFileInfo(fileName, fileSize,
        lastModificationTime);
  }

  @Private
  @VisibleForTesting
  public int printContainerLogsFromRunningApplication(Configuration conf,
      ContainerLogsRequest request, LogCLIHelpers logCliHelper,
      boolean useRegex, boolean ignoreSizeLimit) throws IOException {
    String containerIdStr = request.getContainerId().toString();
    String localDir = request.getOutputLocalDir();
    String nodeId = request.getNodeId();
    PrintStream out = LogToolUtils.createPrintStream(localDir, nodeId,
        containerIdStr);
    try {
      boolean foundAnyLogs = false;
      byte[] buffer = new byte[65536];
      for (String logFile : request.getLogTypes()) {
        InputStream is = null;
        try {
          ClientResponse response = getResponeFromNMWebService(conf,
              webServiceClient, request, logFile);
          if (response != null && response.getStatusInfo().getStatusCode() ==
              ClientResponse.Status.OK.getStatusCode()) {
            is = response.getEntityInputStream();
            int len = 0;
            while((len = is.read(buffer)) != -1) {
              out.write(buffer, 0, len);
            }
            out.println();
          } else {
            out.println("Can not get any logs for the log file: " + logFile);
            String msg = "Response from the NodeManager:" + nodeId +
                " WebService is " + ((response == null) ? "null":
                "not successful," + " HTTP error code: " +
                response.getStatus() + ", Server response:\n" +
                response.getEntity(String.class));
            out.println(msg);
          }
          out.flush();
          foundAnyLogs = true;
        } catch (ClientHandlerException | UniformInterfaceException ex) {
          System.err.println("Can not find the log file:" + logFile
              + " for the container:" + containerIdStr + " in NodeManager:"
              + nodeId);
        } finally {
          IOUtils.closeQuietly(is);
        }
      }

      if (foundAnyLogs) {
        return 0;
      } else {
        return -1;
      }
    } finally {
      logCliHelper.closePrintStream(out);
    }
  }

  @Private
  @VisibleForTesting
  public ContainerReport getContainerReport(String containerIdStr)
      throws YarnException, IOException {
    return yarnClient.getContainerReport(
        ContainerId.fromString(containerIdStr));
  }

  private boolean isApplicationFinished(YarnApplicationState appState) {
    return appState == YarnApplicationState.FINISHED
        || appState == YarnApplicationState.FAILED
        || appState == YarnApplicationState.KILLED; 
  }

  private int printAMContainerLogs(Configuration conf,
      ContainerLogsRequest request, List<String> amContainers,
      LogCLIHelpers logCliHelper, boolean useRegex, boolean ignoreSizeLimit)
      throws Exception {
    List<JSONObject> amContainersList = null;
    List<ContainerLogsRequest> requests =
        new ArrayList<ContainerLogsRequest>();
    boolean getAMContainerLists = false;
    String appId = request.getAppId().toString();
    StringBuilder errorMessage = new StringBuilder();
    // We will call RM webservice to get all AppAttempts information.
    // If we get nothing, we will try to call AHS webservice to get AppAttempts
    // which includes nodeAddress for the AM Containers.
    try {
      amContainersList = getAMContainerInfoForRMWebService(conf, appId);
      if (amContainersList != null && !amContainersList.isEmpty()) {
        getAMContainerLists = true;
        for (JSONObject amContainer : amContainersList) {
          ContainerLogsRequest amRequest = new ContainerLogsRequest(request);
          amRequest.setContainerId(amContainer.getString("containerId"));
          String httpAddress = amContainer.getString("nodeHttpAddress");
          if (httpAddress != null && !httpAddress.isEmpty()) {
            amRequest.setNodeHttpAddress(httpAddress);
          }
          amRequest.setNodeId(amContainer.getString("nodeId"));
          requests.add(amRequest);
        }
      }
    } catch (Exception ex) {
      errorMessage.append(ex.getMessage() + "\n");
      if (request.isAppFinished()) {
        if (!conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
          errorMessage.append("Please enable the timeline service "
              + "and make sure the timeline server is running.");
        } else {
          try {
            amContainersList = getAMContainerInfoForAHSWebService(conf, appId);
            if (amContainersList != null && !amContainersList.isEmpty()) {
              getAMContainerLists = true;
              for (JSONObject amContainer : amContainersList) {
                ContainerLogsRequest amRequest = new ContainerLogsRequest(
                    request);
                amRequest.setContainerId(
                    amContainer.getString("amContainerId"));
                requests.add(amRequest);
              }
            }
          } catch (Exception e) {
            errorMessage.append(e.getMessage());
          }
        }
      }
    }

    if (!getAMContainerLists) {
      System.err.println("Unable to get AM container informations "
          + "for the application:" + appId);
      System.err.println(errorMessage);
      System.err.println("Can not get AMContainers logs for "
          + "the application:" + appId + " with the appOwner:"
          + request.getAppOwner());
      return -1;
    }

    List<ContainerLogsRequest> candidates = new ArrayList<>();
    if (amContainers.contains("ALL")) {
      candidates.addAll(requests);
      outStream.println();
      outStream.println("Specified ALL for -am option. "
          + "Printed logs for all am containers.");
    } else {
      for (String amContainer : amContainers) {
        int amContainerId = Integer.parseInt(amContainer.trim());
        if (amContainerId == -1) {
          candidates.add(requests.get(requests.size() - 1));
        } else {
          if (amContainerId <= requests.size()) {
            candidates.add(requests.get(amContainerId - 1));
          } else {
            System.err.println(String.format("ERROR: Specified AM containerId"
                + " (%s) exceeds the number of AM containers (%s).",
                amContainerId, requests.size()));
            return -1;
          }
        }
      }
    }
    Map<String, ContainerLogsRequest> newOptions = new HashMap<>();
    if (request.isAppFinished()) {
      newOptions = getMatchedLogTypesForFinishedApp(candidates,
          logCliHelper, useRegex, ignoreSizeLimit);
    } else {
      newOptions = getMatchedLogTypesForRunningApp(candidates, useRegex,
          ignoreSizeLimit);
    }
    for (Entry<String, ContainerLogsRequest> amRequest
        : newOptions.entrySet()) {
      outputAMContainerLogs(amRequest.getValue(), conf, logCliHelper,
          useRegex, ignoreSizeLimit);
    }
    return 0;
  }

  private void outputAMContainerLogs(ContainerLogsRequest request,
      Configuration conf, LogCLIHelpers logCliHelper, boolean useRegex,
      boolean ignoreSizeLimit) throws Exception {
    String nodeHttpAddress = request.getNodeHttpAddress();
    String containerId = request.getContainerId();
    String nodeId = request.getNodeId();

    if (request.isAppFinished()) {
      if (containerId != null && !containerId.isEmpty()) {
        if (nodeId != null && !nodeId.isEmpty()) {
          logCliHelper.dumpAContainerLogsForLogType(request);
        } else {
          logCliHelper.dumpAContainerLogsForLogTypeWithoutNodeId(
              request);
        }
      }
    } else {
      if (nodeHttpAddress != null && containerId != null
          && !nodeHttpAddress.isEmpty() && !containerId.isEmpty()) {
        ContainerState containerState = getContainerReport(containerId)
            .getContainerState();
        request.setContainerState(containerState);
        printContainerLogsFromRunningApplication(conf,
            request, logCliHelper, useRegex, ignoreSizeLimit);
      }
    }
  }

  private int showContainerLogInfo(ContainerLogsRequest request,
      LogCLIHelpers logCliHelper) throws IOException, YarnException,
      ClientHandlerException, UniformInterfaceException, JSONException {
    if (!request.isAppFinished()) {
      return printContainerInfoFromRunningApplication(request, logCliHelper);
    } else {
      return logCliHelper.printAContainerLogMetadata(
          request, System.out, System.err);
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

  private int showApplicationLogInfo(ContainerLogsRequest request,
      LogCLIHelpers logCliHelper) throws IOException, YarnException {
    String appState = "Application State: "
        + (request.isAppFinished() ? "Completed." : "Running.");
    if (!request.isAppFinished()) {
      List<ContainerReport> reports =
          getContainerReportsFromRunningApplication(request);
      List<ContainerReport> filterReports = filterContainersInfo(
          request, reports);
      if (filterReports.isEmpty()) {
        System.err.println("Can not find any containers for the application:"
            + request.getAppId() + ".");
        return -1;
      }
      outStream.println(appState);
      for (ContainerReport report : filterReports) {
        outStream.println(String.format(LogCLIHelpers.CONTAINER_ON_NODE_PATTERN,
            report.getContainerId(), report.getAssignedNode()));
      }
      return 0;
    } else {
      outStream.println(appState);
      logCliHelper.printContainersList(request, System.out, System.err);
      return 0;
    }
  }

  /**
   * Create Command Options.
   * @return the command options
   */
  private Options createCommandOpts() {
    Options opts = new Options();
    opts.addOption(HELP_CMD, false, "Displays help for all commands.");
    Option appIdOpt =
        new Option(APPLICATION_ID_OPTION, true, "ApplicationId (required)");
    opts.addOption(appIdOpt);
    opts.addOption(CONTAINER_ID_OPTION, true, "ContainerId. "
        + "By default, it will print all available logs."
        + " Work with -log_files to get only specific logs. If specified, the"
        + " applicationId can be omitted");
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
        + "By default, it will print all available logs. Work with -log_files "
        + "to get only specific logs.");
    amOption.setValueSeparator(',');
    amOption.setArgs(Option.UNLIMITED_VALUES);
    amOption.setArgName("AM Containers");
    opts.addOption(amOption);
    Option logFileOpt = new Option(PER_CONTAINER_LOG_FILES_OPTION, true,
        "Specify comma-separated value "
        + "to get exact matched log files. Use \"ALL\" or \"*\" to "
        + "fetch all the log files for the container.");
    logFileOpt.setValueSeparator(',');
    logFileOpt.setArgs(Option.UNLIMITED_VALUES);
    logFileOpt.setArgName("Log File Name");
    opts.addOption(logFileOpt);
    Option logFileRegexOpt = new Option(PER_CONTAINER_LOG_FILES_REGEX_OPTION,
        true, "Specify comma-separated value "
        + "to get matched log files by using java regex. Use \".*\" to "
        + "fetch all the log files for the container.");
    logFileRegexOpt.setValueSeparator(',');
    logFileRegexOpt.setArgs(Option.UNLIMITED_VALUES);
    logFileRegexOpt.setArgName("Log File Pattern");
    opts.addOption(logFileRegexOpt);
    opts.addOption(SHOW_CONTAINER_LOG_INFO, false,
        "Show the container log metadata, "
        + "including log-file names, the size of the log files. "
        + "You can combine this with --containerId to get log metadata for "
        + "the specific container, or with --nodeAddress to get log metadata "
        + "for all the containers on the specific NodeManager.");
    opts.addOption(SHOW_APPLICATION_LOG_INFO, false, "Show the "
        + "containerIds which belong to the specific Application. "
        + "You can combine this with --nodeAddress to get containerIds "
        + "for all the containers on the specific NodeManager.");
    opts.addOption(LIST_NODES_OPTION, false,
        "Show the list of nodes that successfully aggregated logs. "
        + "This option can only be used with finished applications.");
    opts.addOption(OUT_OPTION, true, "Local directory for storing individual "
        + "container logs. The container logs will be stored based on the "
        + "node the container ran on.");
    opts.addOption(SIZE_OPTION, true, "Prints the log file's first 'n' bytes "
        + "or the last 'n' bytes. Use negative values as bytes to read from "
        + "the end and positive values as bytes to read from the beginning.");
    opts.addOption(CLIENT_MAX_RETRY_OPTION, true, "Set max retry number for a"
        + " retry client to get the container logs for the running "
        + "applications. Use a negative value to make retry forever. "
        + "The default value is 30.");
    opts.addOption(CLIENT_RETRY_INTERVAL_OPTION, true,
        "Work with --client_max_retries to create a retry client. "
        + "The default value is 1000.");
    opts.addOption(SIZE_LIMIT_OPTION, true, "Use this option to limit "
        + "the size of the total logs which could be fetched. "
        + "By default, we only allow to fetch at most "
        + LOG_SIZE_LIMIT_DEFAULT + " MB logs. If the total log size is "
        + "larger than the specified number, the CLI would fail. "
        + "The user could specify -1 to ignore the size limit "
        + "and fetch all logs.");
    opts.getOption(APPLICATION_ID_OPTION).setArgName("Application ID");
    opts.getOption(CONTAINER_ID_OPTION).setArgName("Container ID");
    opts.getOption(NODE_ADDRESS_OPTION).setArgName("Node Address");
    opts.getOption(APP_OWNER_OPTION).setArgName("Application Owner");
    opts.getOption(AM_CONTAINER_OPTION).setArgName("AM Containers");
    opts.getOption(OUT_OPTION).setArgName("Local Directory");
    opts.getOption(SIZE_OPTION).setArgName("size");
    opts.getOption(CLIENT_MAX_RETRY_OPTION).setArgName("Max Retries");
    opts.getOption(CLIENT_RETRY_INTERVAL_OPTION)
        .setArgName("Retry Interval");
    opts.getOption(SIZE_LIMIT_OPTION).setArgName("Size Limit");
    return opts;
  }

  /**
   * Create Print options for helper message.
   * @param commandOpts the options
   * @return the print options
   */
  private Options createPrintOpts(Options commandOpts) {
    Options printOpts = new Options();
    printOpts.addOption(commandOpts.getOption(HELP_CMD));
    printOpts.addOption(commandOpts.getOption(CONTAINER_ID_OPTION));
    printOpts.addOption(commandOpts.getOption(NODE_ADDRESS_OPTION));
    printOpts.addOption(commandOpts.getOption(APP_OWNER_OPTION));
    printOpts.addOption(commandOpts.getOption(AM_CONTAINER_OPTION));
    printOpts.addOption(commandOpts.getOption(PER_CONTAINER_LOG_FILES_OPTION));
    printOpts.addOption(commandOpts.getOption(LIST_NODES_OPTION));
    printOpts.addOption(commandOpts.getOption(SHOW_APPLICATION_LOG_INFO));
    printOpts.addOption(commandOpts.getOption(SHOW_CONTAINER_LOG_INFO));
    printOpts.addOption(commandOpts.getOption(OUT_OPTION));
    printOpts.addOption(commandOpts.getOption(SIZE_OPTION));
    printOpts.addOption(commandOpts.getOption(
        PER_CONTAINER_LOG_FILES_REGEX_OPTION));
    printOpts.addOption(commandOpts.getOption(CLIENT_MAX_RETRY_OPTION));
    printOpts.addOption(commandOpts.getOption(CLIENT_RETRY_INTERVAL_OPTION));
    printOpts.addOption(commandOpts.getOption(SIZE_LIMIT_OPTION));
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
      List<String> amContainersList, LogCLIHelpers logCliHelper,
      boolean useRegex, boolean ignoreSizeLimit) throws Exception {
    return printAMContainerLogs(getConf(), request, amContainersList,
        logCliHelper, useRegex, ignoreSizeLimit);
  }

  private int fetchContainerLogs(ContainerLogsRequest request,
      LogCLIHelpers logCliHelper, boolean useRegex, boolean ignoreSizeLimit)
      throws IOException, ClientHandlerException, UniformInterfaceException,
      JSONException {
    String appIdStr = request.getAppId().toString();
    String containerIdStr = request.getContainerId();
    String nodeAddress = request.getNodeId();
    String appOwner = request.getAppOwner();
    boolean isAppFinished = request.isAppFinished();
    // if the application is in the final state,
    // we could directly get logs from HDFS.
    if (isAppFinished) {
      // if user specified "ALL" as the logFiles param, pass empty list
      // to logCliHelper so that it fetches all the logs
      ContainerLogsRequest newOptions = getMatchedLogOptions(
          request, logCliHelper, useRegex, ignoreSizeLimit);
      if (newOptions == null) {
        System.err.println("Can not find any log file matching the pattern: "
            + request.getLogTypes() + " for the container: "
            + request.getContainerId() + " within the application: "
            + request.getAppId());
        return -1;
      }
      if (nodeAddress != null && !nodeAddress.isEmpty()) {
        return logCliHelper.dumpAContainerLogsForLogType(newOptions);
      } else {
        return logCliHelper.dumpAContainerLogsForLogTypeWithoutNodeId(
            newOptions);
      }
    }
    String nodeHttpAddress = null;
    String nodeId = null;
    try {
      // If the nodeAddress is not provided, we will try to get
      // the ContainerReport. In the containerReport, we could get
      // nodeAddress and nodeHttpAddress
      ContainerReport report = getContainerReport(containerIdStr);
      nodeHttpAddress = report.getNodeHttpAddress();
      if (nodeHttpAddress != null && !nodeHttpAddress.isEmpty()) {
        nodeHttpAddress = nodeHttpAddress.replaceFirst(
                WebAppUtils.getHttpSchemePrefix(getConf()), "");
        request.setNodeHttpAddress(nodeHttpAddress);
      }
      nodeId = report.getAssignedNode().toString();
      request.setNodeId(nodeId);
      request.setContainerState(report.getContainerState());
    } catch (IOException | YarnException ex) {
      nodeHttpAddress = getNodeHttpAddressFromRMWebString(request);
      if (nodeHttpAddress != null && !nodeHttpAddress.isEmpty()) {
        request.setNodeHttpAddress(nodeHttpAddress);
      } else {
        // for the case, we have already uploaded partial logs in HDFS
        int result = -1;
        ContainerLogsRequest newOptions = getMatchedLogOptions(
                request, logCliHelper, useRegex, ignoreSizeLimit);
        if (newOptions == null) {
          System.err.println("Can not find any log file matching the pattern: "
              + request.getLogTypes() + " for the container: "
              + request.getContainerId() + " within the application: "
              + request.getAppId());
        } else {
          if (nodeAddress != null && !nodeAddress.isEmpty()) {
            result = logCliHelper.dumpAContainerLogsForLogType(newOptions);
          } else {
            result = logCliHelper.dumpAContainerLogsForLogTypeWithoutNodeId(
                newOptions);
          }
        }
        if (result == -1) {
          System.err.println(
              "Unable to get logs for this container:"
                  + containerIdStr + " for the application:"
                  + appIdStr + " with the appOwner: " + appOwner);
          System.err.println("The application: " + appIdStr
              + " is still running, and we can not get Container report "
              + "for the container: " + containerIdStr + ". Please try later "
              + "or after the application finishes.");
        }
        return result;
      }
    }
    // If the application is not in the final state,
    // we will provide the NodeHttpAddress and get the container logs
    // by calling NodeManager webservice.
    ContainerLogsRequest newRequest = getMatchedOptionForRunningApp(
        request, useRegex, ignoreSizeLimit);
    if (newRequest == null) {
      return -1;
    }
    return printContainerLogsFromRunningApplication(getConf(), request,
          logCliHelper, useRegex, ignoreSizeLimit);
  }

  private int fetchApplicationLogs(ContainerLogsRequest options,
      LogCLIHelpers logCliHelper, boolean useRegex, boolean ignoreSizeLimit)
      throws IOException, YarnException {
    // If the application has finished, we would fetch the logs
    // from HDFS.
    // If the application is still running, we would get the full
    // list of the containers first, then fetch the logs for each
    // container from NM.
    int resultCode = -1;
    if (options.isAppFinished()) {
      ContainerLogsRequest newOptions = getMatchedLogOptions(
          options, logCliHelper, useRegex, ignoreSizeLimit);
      if (newOptions == null) {
        System.err.println("Can not find any log file matching the pattern: "
            + options.getLogTypes() + " for the application: "
            + options.getAppId());
      } else {
        resultCode =
            logCliHelper.dumpAllContainersLogs(newOptions);
      }
    } else {
      List<ContainerLogsRequest> containerLogRequests =
          getContainersLogRequestForRunningApplication(options);

      // get all matched container log types and check the total log size.
      Map<String, ContainerLogsRequest> matchedLogTypes =
          getMatchedLogTypesForRunningApp(containerLogRequests,
              useRegex, ignoreSizeLimit);

      for (Entry<String, ContainerLogsRequest> container
          : matchedLogTypes.entrySet()) {
        int result = printContainerLogsFromRunningApplication(getConf(),
            container.getValue(), logCliHelper,
            useRegex, ignoreSizeLimit);
        if (result == 0) {
          resultCode = 0;
        }
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
      ContainerLogsRequest request, LogCLIHelpers logCliHelper,
      boolean useRegex, boolean ignoreSizeLimit) throws IOException {
    ContainerLogsRequest newOptions = new ContainerLogsRequest(request);
    Set<ContainerLogFileInfo> files = logCliHelper.listContainerLogs(
        request);
    Set<String> matchedFiles = getMatchedLogFiles(request, files,
        useRegex, ignoreSizeLimit);
    if (matchedFiles.isEmpty()) {
      return null;
    } else {
      newOptions.setLogTypes(matchedFiles);
      return newOptions;
    }
  }

  private Set<String> getMatchedLogFiles(ContainerLogsRequest options,
      Collection<ContainerLogFileInfo> candidate, boolean useRegex,
      boolean ignoreSizeLimit) throws IOException {
    Set<String> matchedFiles = new HashSet<String>();
    Set<String> filePattern = options.getLogTypes();
    long size = options.getBytes();
    boolean getAll = options.getLogTypes().contains("ALL");
    Iterator<ContainerLogFileInfo> iterator = candidate.iterator();
    while(iterator.hasNext()) {
      boolean matchedFile = false;
      ContainerLogFileInfo logInfo = iterator.next();
      if (getAll) {
        matchedFile = true;
      } else if (useRegex) {
        if (isFileMatching(logInfo.getFileName(), filePattern)) {
          matchedFile = true;
        }
      } else {
        if (filePattern.contains(logInfo.getFileName())) {
          matchedFile = true;
        }
      }
      if (matchedFile) {
        matchedFiles.add(logInfo.getFileName());
        if (!ignoreSizeLimit) {
          decrLogSizeLimit(Math.min(
              Long.parseLong(logInfo.getFileSize()), size));
          if (getLogSizeLimitLeft() < 0) {
            throw new RuntimeException("The total log size is too large."
                + "The log size limit is " + specifedLogLimits + "MB. "
                + "Please specify a proper value --size option or if you "
                + "really want to fetch all, please "
                + "specify -1 for --size_limit_mb option.");
          }
        }
      }
    }
    return matchedFiles;
  }

  private boolean isFileMatching(String fileType,
      Set<String> logTypes) {
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
    List<ContainerReport> reports =
        getContainerReportsFromRunningApplication(options);
    for (ContainerReport container : reports) {
      ContainerLogsRequest newOptions = new ContainerLogsRequest(options);
      newOptions.setContainerId(container.getContainerId().toString());
      newOptions.setNodeId(container.getAssignedNode().toString());
      String httpAddress = container.getNodeHttpAddress();
      if (httpAddress != null && !httpAddress.isEmpty()) {
        newOptions.setNodeHttpAddress(httpAddress
            .replaceFirst(WebAppUtils.getHttpSchemePrefix(getConf()), ""));
      }
      newOptions.setContainerState(container.getContainerState());
      newOptionsList.add(newOptions);
    }
    return newOptionsList;
  }

  private List<ContainerReport> getContainerReportsFromRunningApplication(
      ContainerLogsRequest options) throws YarnException, IOException {
    List<ContainerReport> reports = new ArrayList<ContainerReport>();
    List<ApplicationAttemptReport> attempts =
        yarnClient.getApplicationAttempts(options.getAppId());
    Map<ContainerId, ContainerReport> containerMap = new TreeMap<
        ContainerId, ContainerReport>();
    for (ApplicationAttemptReport attempt : attempts) {
      List<ContainerReport> containers = yarnClient.getContainers(
          attempt.getApplicationAttemptId());
      for (ContainerReport container : containers) {
        if (!containerMap.containsKey(container.getContainerId())) {
          containerMap.put(container.getContainerId(), container);
        }
      }
    }
    reports.addAll(containerMap.values());
    return reports;
  }

  // filter the containerReports based on the nodeId and ContainerId
  private List<ContainerReport> filterContainersInfo(
      ContainerLogsRequest options, List<ContainerReport> containers) {
    List<ContainerReport> filterReports = new ArrayList<ContainerReport>(
        containers);
    String nodeId = options.getNodeId();
    boolean filterBasedOnNodeId = (nodeId != null && !nodeId.isEmpty());
    String containerId = options.getContainerId();
    boolean filterBasedOnContainerId = (containerId != null
        && !containerId.isEmpty());

    if (filterBasedOnNodeId || filterBasedOnContainerId) {
    // filter the reports based on the containerId and.or nodeId
      for(ContainerReport report : containers) {
        if (filterBasedOnContainerId) {
          if (!report.getContainerId().toString()
              .equalsIgnoreCase(containerId)) {
            filterReports.remove(report);
          }
        }

        if (filterBasedOnNodeId) {
          if (!report.getAssignedNode().toString().equalsIgnoreCase(nodeId)) {
            filterReports.remove(report);
          }
        }
      }
    }
    return filterReports;
  }

  private int printContainerInfoFromRunningApplication(
      ContainerLogsRequest options, LogCLIHelpers logCliHelper)
      throws YarnException, IOException, ClientHandlerException,
      UniformInterfaceException, JSONException {
    String containerIdStr = options.getContainerId();
    String nodeIdStr = options.getNodeId();
    List<ContainerReport> reports =
        getContainerReportsFromRunningApplication(options);
    List<ContainerReport> filteredReports = filterContainersInfo(
        options, reports);
    if (filteredReports.isEmpty()) {
      // if we specify the containerId as well as NodeAddress
      String nodeHttpAddress = null;
      if (options.getContainerId() != null
          && !options.getContainerId().isEmpty()) {
        nodeHttpAddress = getNodeHttpAddressFromRMWebString(options);
      }
      if (nodeHttpAddress != null) {
        outputContainerLogMeta(options.getContainerId(), options.getNodeId(),
            nodeHttpAddress);
        return 0;
      } else {
        int result = logCliHelper.printAContainerLogMetadata(
            options, System.out, System.err);
        if (result == -1) {
          StringBuilder sb = new StringBuilder();
          if (containerIdStr != null && !containerIdStr.isEmpty()) {
            sb.append("Trying to get container with ContainerId: "
                + containerIdStr + "\n");
          }
          if (nodeIdStr != null && !nodeIdStr.isEmpty()) {
            sb.append("Trying to get container from NodeManager: "
                + nodeIdStr + "\n");
          }
          sb.append("Can not find any matched containers for the application: "
              + options.getAppId());
          System.err.println(sb.toString());
        }
        return result;
      }
    }
    for (ContainerReport report : filteredReports) {
      String nodeId = report.getAssignedNode().toString();
      String nodeHttpAddress = report.getNodeHttpAddress().replaceFirst(
          WebAppUtils.getHttpSchemePrefix(getConf()), "");
      String containerId = report.getContainerId().toString();
      outputContainerLogMeta(containerId, nodeId, nodeHttpAddress);
    }
    return 0;
  }

  private void outputContainerLogMeta(String containerId, String nodeId,
      String nodeHttpAddress) throws IOException {
    String containerString = String.format(
        LogCLIHelpers.CONTAINER_ON_NODE_PATTERN, containerId, nodeId);
    outStream.println(containerString);
    outStream.println(StringUtils.repeat("=", containerString.length()));
    outStream.printf(LogCLIHelpers.PER_LOG_FILE_INFO_PATTERN,
        "LogFile", "LogLength", "LastModificationTime", "LogAggregationType");
    outStream.println(StringUtils.repeat("=", containerString.length() * 2));
    List<Pair<ContainerLogFileInfo, String>> infos = getContainerLogFiles(
        getConf(), containerId, nodeHttpAddress);
    for (Pair<ContainerLogFileInfo, String> info : infos) {
      outStream.printf(LogCLIHelpers.PER_LOG_FILE_INFO_PATTERN,
          info.getKey().getFileName(), info.getKey().getFileSize(),
          info.getKey().getLastModifiedTime(), info.getValue());
    }
  }

  @VisibleForTesting
  public Set<String> getMatchedContainerLogFiles(ContainerLogsRequest request,
      boolean useRegex, boolean ignoreSizeLimit) throws IOException {
    // fetch all the log files for the container
    // filter the log files based on the given -log_files pattern
    List<Pair<ContainerLogFileInfo, String>> allLogFileInfos=
        getContainerLogFiles(getConf(), request.getContainerId(),
            request.getNodeHttpAddress());
    List<ContainerLogFileInfo> fileNames = new ArrayList<
        ContainerLogFileInfo>();
    for (Pair<ContainerLogFileInfo, String> fileInfo : allLogFileInfos) {
      fileNames.add(fileInfo.getKey());
    }
    return getMatchedLogFiles(request, fileNames,
        useRegex, ignoreSizeLimit);
  }

  @VisibleForTesting
  public ClientResponse getResponeFromNMWebService(Configuration conf,
      Client webServiceClient, ContainerLogsRequest request, String logFile) {
    WebResource webResource =
        webServiceClient.resource(WebAppUtils.getHttpSchemePrefix(conf)
        + request.getNodeHttpAddress());
    return webResource.path("ws").path("v1").path("node")
        .path("containers").path(request.getContainerId()).path("logs")
        .path(logFile)
        .queryParam("size", Long.toString(request.getBytes()))
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
  }

  @VisibleForTesting
  public String getNodeHttpAddressFromRMWebString(ContainerLogsRequest request)
      throws ClientHandlerException, UniformInterfaceException, JSONException {
    if (request.getNodeId() == null || request.getNodeId().isEmpty()) {
      return null;
    }
    JSONObject nodeInfo = YarnWebServiceUtils
        .getNodeInfoFromRMWebService(getConf(), request.getNodeId())
        .getJSONObject("node");
    return nodeInfo.has("nodeHTTPAddress") ?
        nodeInfo.getString("nodeHTTPAddress") : null;
  }

  // Class to handle retry
  static class ClientConnectionRetry {

    // maxRetries < 0 means keep trying
    @Private
    @VisibleForTesting
    public int maxRetries;

    @Private
    @VisibleForTesting
    public long retryInterval;

    // Indicates if retries happened last time. Only tests should read it.
    // In unit tests, retryOn() calls should _not_ be concurrent.
    private boolean retried = false;

    @Private
    @VisibleForTesting
    boolean getRetired() {
      return retried;
    }

    // Constructor with default retry settings
    public ClientConnectionRetry(int inputMaxRetries,
        long inputRetryInterval) {
      this.maxRetries = inputMaxRetries;
      this.retryInterval = inputRetryInterval;
    }

    public Object retryOn(ClientRetryOp op)
        throws RuntimeException, IOException {
      int leftRetries = maxRetries;
      retried = false;

      // keep trying
      while (true) {
        try {
          // try perform the op, if fail, keep retrying
          return op.run();
        } catch (IOException | RuntimeException e) {
          // break if there's no retries left
          if (leftRetries == 0) {
            break;
          }
          if (op.shouldRetryOn(e)) {
            logException(e, leftRetries);
          } else {
            throw e;
          }
        }
        if (leftRetries > 0) {
          leftRetries--;
        }
        retried = true;
        try {
          // sleep for the given time interval
          Thread.sleep(retryInterval);
        } catch (InterruptedException ie) {
          System.out.println("Client retry sleep interrupted! ");
        }
      }
      throw new RuntimeException("Connection retries limit exceeded.");
    };

    private void logException(Exception e, int leftRetries) {
      if (leftRetries > 0) {
        System.out.println("Exception caught by ClientConnectionRetry,"
              + " will try " + leftRetries + " more time(s).\nMessage: "
              + e.getMessage());
      } else {
        // note that maxRetries may be -1 at the very beginning
        System.out.println("ConnectionException caught by ClientConnectionRetry,"
            + " will keep retrying.\nMessage: "
            + e.getMessage());
      }
    }
  }

  private class ClientJerseyRetryFilter extends ClientFilter {
    @Override
    public ClientResponse handle(final ClientRequest cr)
        throws ClientHandlerException {
      // Set up the retry operation
      ClientRetryOp jerseyRetryOp = new ClientRetryOp() {
        @Override
        public Object run() {
          // Try pass the request, if fail, keep retrying
          return getNext().handle(cr);
        }

        @Override
        public boolean shouldRetryOn(Exception e) {
          // Only retry on connection exceptions
          return (e instanceof ClientHandlerException)
              && (e.getCause() instanceof ConnectException ||
                  e.getCause() instanceof SocketTimeoutException ||
                  e.getCause() instanceof SocketException);
        }
      };
      try {
        return (ClientResponse) connectionRetry.retryOn(jerseyRetryOp);
      } catch (IOException e) {
        throw new ClientHandlerException("Jersey retry failed!\nMessage: "
              + e.getMessage());
      }
    }
  }

  // Abstract class for an operation that should be retried by client
  private static abstract class ClientRetryOp {
    // The operation that should be retried
    public abstract Object run() throws IOException;
    // The method to indicate if we should retry given the incoming exception
    public abstract boolean shouldRetryOn(Exception e);
  }

  private long getLogSizeLimitLeft() {
    return this.logSizeLeft;
  }

  private void decrLogSizeLimit(long used) {
    this.logSizeLeft -= used;
  }

  @Private
  @VisibleForTesting
  public ContainerLogsRequest getMatchedOptionForRunningApp(
      ContainerLogsRequest container, boolean useRegex,
      boolean ignoreSizeLimit) throws IOException {
    String containerIdStr = container.getContainerId().toString();
    String nodeHttpAddress = container.getNodeHttpAddress();
    if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()) {
      System.err.println("Can not get the logs for the container: "
          + containerIdStr);
      System.err.println("The node http address is required to get container "
          + "logs for the Running application.");
      return null;
    }

    Set<String> matchedFiles = getMatchedContainerLogFiles(container,
        useRegex, ignoreSizeLimit);
    if (matchedFiles.isEmpty()) {
      System.err.println("Can not find any log file matching the pattern: "
          + container.getLogTypes() + " for the container: " + containerIdStr
          + " within the application: " + container.getAppId());
      return null;
    }
    container.setLogTypes(matchedFiles);
    return container;
  }

  @Private
  @VisibleForTesting
  public Map<String, ContainerLogsRequest> getMatchedLogTypesForRunningApp(
      List<ContainerLogsRequest> containerLogRequests, boolean useRegex,
      boolean ignoreSizeLimit) {
    Map<String, ContainerLogsRequest> containerMatchedLog = new HashMap<>();
    for (ContainerLogsRequest container : containerLogRequests) {
      try {
        ContainerLogsRequest request = getMatchedOptionForRunningApp(
            container, useRegex, ignoreSizeLimit);
        if (request == null) {
          continue;
        }
        containerMatchedLog.put(container.getContainerId(), request);
      } catch(IOException ex) {
        System.err.println(ex);
        continue;
      }
    }
    return containerMatchedLog;
  }

  private Map<String, ContainerLogsRequest> getMatchedLogTypesForFinishedApp(
      List<ContainerLogsRequest> containerLogRequests,
      LogCLIHelpers logCliHelper, boolean useRegex,
      boolean ignoreSizeLimit) {
    Map<String, ContainerLogsRequest> containerMatchedLog = new HashMap<>();
    for (ContainerLogsRequest container : containerLogRequests) {
      try {
        ContainerLogsRequest request = getMatchedLogOptions(container,
            logCliHelper, useRegex, ignoreSizeLimit);
        if (request == null) {
          System.err.println("Can not find any log file matching the pattern: "
              + container.getLogTypes() + " for the container: "
              + container.getContainerId() + " within the application: "
              + container.getAppId());
          continue;
        }
        containerMatchedLog.put(container.getContainerId(), request);
      } catch (IOException ex) {
        System.err.println(ex);
        continue;
      }
    }
    return containerMatchedLog;
  }
}
