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
package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;

import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains utilities for fetching a user's log file in a secure fashion.
 */
public class ContainerLogsUtils {
  public static final Logger LOG = LoggerFactory.getLogger(ContainerLogsUtils.class);
  
  /**
   * Finds the local directories that logs for the given container are stored
   * on.
   */
  public static List<File> getContainerLogDirs(ContainerId containerId,
      String remoteUser, Context context) throws YarnException {
    Container container = context.getContainers().get(containerId);

    Application application = getApplicationForContainer(containerId, context);
    checkAccess(remoteUser, application, context);
    // It is not required to have null check for container ( container == null )
    // and throw back exception.Because when container is completed, NodeManager
    // remove container information from its NMContext.Configuring log
    // aggregation to false, container log view request is forwarded to NM. NM
    // does not have completed container information,but still NM serve request for
    // reading container logs. 
    if (container != null) {
      checkState(container.getContainerState());
    }
    
    return getContainerLogDirs(containerId, context.getLocalDirsHandler());
  }
  
  static List<File> getContainerLogDirs(ContainerId containerId,
      LocalDirsHandlerService dirsHandler) throws YarnException {
    List<String> logDirs = dirsHandler.getLogDirsForRead();
    List<File> containerLogDirs = new ArrayList<File>(logDirs.size());
    for (String logDir : logDirs) {
      logDir = new File(logDir).toURI().getPath();
      String appIdStr = containerId
          .getApplicationAttemptId().getApplicationId().toString();
      File appLogDir = new File(logDir, appIdStr);
      containerLogDirs.add(new File(appLogDir, containerId.toString()));
    }
    return containerLogDirs;
  }
  
  /**
   * Finds the log file with the given filename for the given container.
   */
  public static File getContainerLogFile(ContainerId containerId,
      String fileName, String remoteUser, Context context) throws YarnException {
    Container container = context.getContainers().get(containerId);
    
    Application application = getApplicationForContainer(containerId, context);
    checkAccess(remoteUser, application, context);
    if (container != null) {
      checkState(container.getContainerState());
    }
    
    try {
      LocalDirsHandlerService dirsHandler = context.getLocalDirsHandler();
      String relativeContainerLogDir = ContainerLaunch.getRelativeContainerLogDir(
          application.getAppId().toString(), containerId.toString());
      Path logPath = dirsHandler.getLogPathToRead(
          relativeContainerLogDir + Path.SEPARATOR + fileName);
      URI logPathURI = new File(logPath.toString()).toURI();
      File logFile = new File(logPathURI.getPath());
      return logFile;
    } catch (IOException e) {
      LOG.warn("Failed to find log file", e);
      throw new NotFoundException("Cannot find this log on the local disk.");
    }
  }
  
  private static Application getApplicationForContainer(ContainerId containerId,
      Context context) {
    ApplicationId applicationId = containerId.getApplicationAttemptId()
        .getApplicationId();
    Application application = context.getApplications().get(
        applicationId);
    
    if (application == null) {
      throw new NotFoundException(
          "Unknown container. Container either has not started or "
              + "has already completed or "
              + "doesn't belong to this node at all.");
    }
    return application;
  }
  
  private static void checkAccess(String remoteUser, Application application,
      Context context) throws YarnException {
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !context.getApplicationACLsManager().checkAccess(callerUGI,
            ApplicationAccessType.VIEW_APP, application.getUser(),
            application.getAppId())) {
      throw new YarnException(
          "User [" + remoteUser
              + "] is not authorized to view the logs for application "
              + application.getAppId());
    }
  }
  
  private static void checkState(ContainerState state) {
    if (state == ContainerState.NEW || state == ContainerState.LOCALIZING ||
        state == ContainerState.SCHEDULED) {
      throw new NotFoundException("Container is not yet running. Current state is "
          + state);
    }
    if (state == ContainerState.LOCALIZATION_FAILED) {
      throw new NotFoundException("Container wasn't started. Localization failed.");
    }
  }
  
  public static FileInputStream openLogFileForRead(String containerIdStr, File logFile,
      Context context) throws IOException {
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    ApplicationId applicationId = containerId.getApplicationAttemptId()
        .getApplicationId();
    String user = context.getApplications().get(
        applicationId).getUser();
    
    try {
      return SecureIOUtils.openForRead(logFile, user, null);
    } catch (IOException e) {
      if (e.getMessage().contains(
        "did not match expected owner '" + user
            + "'")) {
        LOG.error(
            "Exception reading log file " + logFile.getAbsolutePath(), e);
        throw new IOException("Exception reading log file. Application submitted by '"
            + user
            + "' doesn't own requested log file : "
            + logFile.getName(), e);
      } else {
        throw new IOException("Exception reading log file. It might be because log "
            + "file was aggregated : " + logFile.getName(), e);
      }
    }
  }
}
