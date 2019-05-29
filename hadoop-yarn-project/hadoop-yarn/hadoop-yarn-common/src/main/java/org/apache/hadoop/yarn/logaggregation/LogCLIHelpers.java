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

package org.apache.hadoop.yarn.logaggregation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import com.google.common.annotations.VisibleForTesting;

public class LogCLIHelpers implements Configurable {

  public static final String PER_LOG_FILE_INFO_PATTERN =
      "%30s\t%30s\t%30s\t%30s" + System.getProperty("line.separator");
  public static final String CONTAINER_ON_NODE_PATTERN =
      "Container: %s on %s";

  private Configuration conf;
  private LogAggregationFileControllerFactory factory;

  @Private
  @VisibleForTesting
  public int dumpAContainersLogs(String appId, String containerId,
      String nodeId, String jobOwner) throws IOException {
    ContainerLogsRequest options = new ContainerLogsRequest();
    options.setAppId(ApplicationId.fromString(appId));
    options.setContainerId(containerId);
    options.setNodeId(nodeId);
    options.setAppOwner(jobOwner);
    Set<String> logs = new HashSet<String>();
    options.setLogTypes(logs);
    options.setBytes(Long.MAX_VALUE);
    return dumpAContainerLogsForLogType(options, false);
  }

  @Private
  @VisibleForTesting
  /**
   * Return the owner for a given AppId
   * @param remoteRootLogDir
   * @param appId
   * @param bestGuess
   * @param conf
   * @return the owner or null
   * @throws IOException
   */
  public static String getOwnerForAppIdOrNull(
      ApplicationId appId, String bestGuess,
      Configuration conf) throws IOException {
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(conf);
    List<LogAggregationFileController> fileControllers = factory
        .getConfiguredLogAggregationFileControllerList();

    if (fileControllers != null && !fileControllers.isEmpty()) {
      String owner = null;
      for (LogAggregationFileController fileFormat : fileControllers) {
        try {
          owner = guessOwnerWithFileFormat(fileFormat, appId, bestGuess, conf);
          if (owner != null) {
            return owner;
          }
        } catch (AccessControlException | AccessDeniedException ex) {
          return null;
        } catch (IOException io) {
          // Ignore IOException thrown from wrong file format
        }
      }
    } else {
      System.err.println("Can not find any valid fileControllers. " +
          " The configurated fileControllers: " +
          YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS);
    }
    return null;
  }

  public static String guessOwnerWithFileFormat(
      LogAggregationFileController fileFormat, ApplicationId appId,
      String bestGuess, Configuration conf) throws IOException {
    Path remoteRootLogDir = fileFormat.getRemoteRootLogDir();
    String suffix = fileFormat.getRemoteRootLogDirSuffix();
    Path fullPath = fileFormat.getRemoteAppLogDir(appId, bestGuess);
    FileContext fc =
        FileContext.getFileContext(remoteRootLogDir.toUri(), conf);
    String pathAccess = fullPath.toString();
    try {
      if (fc.util().exists(fullPath)) {
        return bestGuess;
      }
      Path toMatch = LogAggregationUtils.
          getRemoteAppLogDir(remoteRootLogDir, appId, "*", suffix);
      pathAccess = toMatch.toString();
      FileStatus[] matching  = fc.util().globStatus(toMatch);
      if (matching == null || matching.length != 1) {
        return null;
      }
      //fetch the user from the full path /app-logs/user[/suffix]/app_id
      Path parent = matching[0].getPath().getParent();
      //skip the suffix too
      if (suffix != null && !StringUtils.isEmpty(suffix)) {
        parent = parent.getParent();
      }
      return parent.getName();
    } catch (AccessControlException | AccessDeniedException ex) {
      logDirNoAccessPermission(pathAccess, bestGuess, ex.getMessage());
      throw ex;
    }
  }

  @Private
  @VisibleForTesting
  public int dumpAContainerLogsForLogType(ContainerLogsRequest options)
      throws IOException {
    return dumpAContainerLogsForLogType(options, true);
  }

  @Private
  @VisibleForTesting
  public int dumpAContainerLogsForLogType(ContainerLogsRequest options,
      boolean outputFailure) throws IOException {
    LogAggregationFileController fc = null;
    try {
      fc = this.getFileController(
          options.getAppId(), options.getAppOwner());
    } catch (IOException ex) {
      System.err.println(ex);
    }
    boolean foundAnyLogs = false;
    if (fc != null) {
      foundAnyLogs = fc.readAggregatedLogs(options, null);
    }
    if (!foundAnyLogs) {
      if (outputFailure) {
        containerLogNotFound(options.getContainerId());
      }
      return -1;
    }
    return 0;
  }

  @Private
  public int dumpAContainerLogsForLogTypeWithoutNodeId(
      ContainerLogsRequest options) throws IOException {
    LogAggregationFileController fc = null;
    try {
      fc = this.getFileController(
          options.getAppId(), options.getAppOwner());
    } catch (IOException ex) {
      System.err.println(ex);
    }
    boolean foundAnyLogs = false;
    if (fc != null) {
      foundAnyLogs = fc.readAggregatedLogs(options, null);
    }
    if (!foundAnyLogs) {
      containerLogNotFound(options.getContainerId());
      return -1;
    }
    return 0;
  }

  @Private
  public int dumpAllContainersLogs(ContainerLogsRequest options)
      throws IOException {
    LogAggregationFileController fc = null;
    try {
      fc = this.getFileController(
          options.getAppId(), options.getAppOwner());
    } catch (IOException ex) {
      System.err.println(ex);
    }
    boolean foundAnyLogs = false;
    if (fc != null) {
      foundAnyLogs = fc.readAggregatedLogs(options, null);
    }
    if (!foundAnyLogs) {
      emptyLogDir(LogAggregationUtils.getRemoteAppLogDir(
          conf, options.getAppId(), options.getAppOwner(),
          fc.getRemoteRootLogDir(), fc.getRemoteRootLogDirSuffix())
          .toString());
      return -1;
    }
    return 0;
  }

  @Private
  public int printAContainerLogMetadata(ContainerLogsRequest options,
      PrintStream out, PrintStream err)
      throws IOException {
    String nodeId = options.getNodeId();
    String containerIdStr = options.getContainerId();
    List<ContainerLogMeta> containersLogMeta;
    try {
      containersLogMeta = getFileController(options.getAppId(),
          options.getAppOwner()).readAggregatedLogsMeta(
          options);
    } catch (Exception ex) {
      err.println(ex.getMessage());
      return -1;
    }
    if (containersLogMeta.isEmpty()) {
      if (containerIdStr != null && nodeId != null) {
        err.println("The container " + containerIdStr + " couldn't be found "
            + "on the node specified: " + nodeId);
      } else if (nodeId != null) {
        err.println("Can not find log metadata for any containers on "
            + nodeId);
      } else if (containerIdStr != null) {
        err.println("Can not find log metadata for container: "
            + containerIdStr);
      }
      return -1;
    }

    for (ContainerLogMeta containerLogMeta : containersLogMeta) {
      String containerString = String.format(CONTAINER_ON_NODE_PATTERN,
          containerLogMeta.getContainerId(), containerLogMeta.getNodeId());
      out.println(containerString);
      out.println(StringUtils.repeat("=", containerString.length()));
      out.printf(PER_LOG_FILE_INFO_PATTERN, "LogFile", "LogLength",
          "LastModificationTime", "LogAggregationType");
      out.println(StringUtils.repeat("=", containerString.length() * 2));
      for (ContainerLogFileInfo logMeta : containerLogMeta
          .getContainerLogMeta()) {
        out.printf(PER_LOG_FILE_INFO_PATTERN, logMeta.getFileName(),
            logMeta.getFileSize(), logMeta.getLastModifiedTime(), "AGGREGATED");
      }
    }
    return 0;
  }

  @Private
  public void printNodesList(ContainerLogsRequest options,
      PrintStream out, PrintStream err) throws IOException {
    ApplicationId appId = options.getAppId();
    String appOwner = options.getAppOwner();
    LogAggregationFileController fileFormat = null;
    try {
      fileFormat = getFileController(appId, appOwner);
    } catch (Exception ex) {
      err.println(ex.getMessage());
      return;
    }
    RemoteIterator<FileStatus> nodeFiles = null;
    try {
      nodeFiles = LogAggregationUtils.getRemoteNodeFileDir(conf, appId,
          appOwner, fileFormat.getRemoteRootLogDir(),
          fileFormat.getRemoteRootLogDirSuffix());
    } catch (FileNotFoundException fnf) {
      logDirNotExist(fileFormat.getRemoteAppLogDir(appId,
          appOwner).toString());
    } catch (AccessControlException | AccessDeniedException ace) {
      logDirNoAccessPermission(fileFormat.getRemoteAppLogDir(
          appId, appOwner).toString(), appOwner,
          ace.getMessage());
    }
    if (nodeFiles == null) {
      return;
    }
    boolean foundNode = false;
    StringBuilder sb = new StringBuilder();
    while (nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      sb.append(thisNodeFile.getPath().getName() + "\n");
      foundNode = true;
    }
    if (!foundNode) {
      err.println("No nodes found that aggregated logs for "
          + "the application: " + appId);
    } else {
      out.println(sb.toString());
    }
  }

  @Private
  public void printContainersList(ContainerLogsRequest options,
      PrintStream out, PrintStream err) throws IOException {
    ApplicationId appId = options.getAppId();
    String nodeId = options.getNodeId();
    boolean foundAnyLogs = false;
    List<ContainerLogMeta> containersLogMeta = new ArrayList<>();
    try {
      containersLogMeta = getFileController(options.getAppId(),
          options.getAppOwner()).readAggregatedLogsMeta(
          options);
    } catch (Exception ex) {
      err.println(ex.getMessage());
    }
    for(ContainerLogMeta logMeta : containersLogMeta) {
      out.println(String.format(CONTAINER_ON_NODE_PATTERN,
          logMeta.getContainerId(),
          logMeta.getNodeId()));
      foundAnyLogs = true;
    }
    if (!foundAnyLogs) {
      if (nodeId != null) {
        err.println("Can not find information for any containers on "
            + nodeId);
      } else {
        err.println("Can not find any container information for "
            + "the application: " + appId);
      }
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  private static void containerLogNotFound(String containerId) {
    System.err.println("Logs for container " + containerId
        + " are not present in this log-file.");
  }

  private static void logDirNotExist(String remoteAppLogDir) {
    System.err.println(remoteAppLogDir + " does not exist.");
    System.err.println("Log aggregation has not completed or is not enabled.");
  }

  private static void emptyLogDir(String remoteAppLogDir) {
    System.err.println(remoteAppLogDir + " does not have any log files.");
  }

  private static void logDirNoAccessPermission(String remoteAppLogDir,
      String appOwner, String errorMessage) throws IOException {
    System.err.println("Guessed logs' owner is " + appOwner
        + " and current user "
        + UserGroupInformation.getCurrentUser().getUserName() + " does not "
        + "have permission to access " + remoteAppLogDir
        + ". Error message found: " + errorMessage);
  }

  public void closePrintStream(PrintStream out) {
    if (out != System.out) {
      IOUtils.closeQuietly(out);
    }
  }

  @Private
  public Set<ContainerLogFileInfo> listContainerLogs(
      ContainerLogsRequest options) throws IOException {
    List<ContainerLogMeta> containersLogMeta;
    Set<ContainerLogFileInfo> logTypes = new HashSet<ContainerLogFileInfo>();
    try {
      containersLogMeta = getFileController(options.getAppId(),
          options.getAppOwner()).readAggregatedLogsMeta(
          options);
    } catch (Exception ex) {
      System.err.println(ex.getMessage());
      return logTypes;
    }
    for (ContainerLogMeta logMeta: containersLogMeta) {
      for (ContainerLogFileInfo fileInfo : logMeta.getContainerLogMeta()) {
        logTypes.add(fileInfo);
      }
    }
    return logTypes;
  }

  private LogAggregationFileController getFileController(ApplicationId appId,
      String appOwner) throws IOException {
    if (factory == null) {
      factory = new LogAggregationFileControllerFactory(conf);
    }
    return factory.getFileControllerForRead(appId, appOwner);
  }
}
