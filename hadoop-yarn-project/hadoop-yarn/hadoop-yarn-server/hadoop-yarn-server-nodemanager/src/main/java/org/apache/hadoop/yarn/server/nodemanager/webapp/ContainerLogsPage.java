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

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.PRE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class ContainerLogsPage extends NMView {
  
  public static final String REDIRECT_URL = "redirect.url";
  
  @Override protected void preHead(Page.HTML<_> html) {
    String redirectUrl = $(REDIRECT_URL);
    if (redirectUrl == null || redirectUrl.isEmpty()) {
      set(TITLE, join("Logs for ", $(CONTAINER_ID)));
    } else {
      if (redirectUrl.equals("false")) {
        set(TITLE, join("Failed redirect for ", $(CONTAINER_ID)));
        //Error getting redirect url. Fall through.
      } else {
        set(TITLE, join("Redirecting to log server for ", $(CONTAINER_ID)));
        html.meta_http("refresh", "1; url=" + redirectUrl);
      }
    }
    
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return ContainersLogsBlock.class;
  }

  public static class ContainersLogsBlock extends HtmlBlock implements
      YarnWebParams {    
    private final Configuration conf;
    private final Context nmContext;
    private final ApplicationACLsManager aclsManager;
    private final LocalDirsHandlerService dirsHandler;

    @Inject
    public ContainersLogsBlock(Configuration conf, Context context,
        ApplicationACLsManager aclsManager,
        LocalDirsHandlerService dirsHandler) {
      this.conf = conf;
      this.nmContext = context;
      this.aclsManager = aclsManager;
      this.dirsHandler = dirsHandler;
    }

    @Override
    protected void render(Block html) {

      String redirectUrl = $(REDIRECT_URL);
      if (redirectUrl !=null && redirectUrl.equals("false")) {
        html.h1("Failed while trying to construct the redirect url to the log" +
        		" server. Log Server url may not be configured");
        //Intentional fallthrough.
      }
      
      ContainerId containerId;
      try {
        containerId = ConverterUtils.toContainerId($(CONTAINER_ID));
      } catch (IllegalArgumentException e) {
        html.h1("Invalid containerId " + $(CONTAINER_ID));
        return;
      }

      ApplicationId applicationId = containerId.getApplicationAttemptId()
          .getApplicationId();
      Application application = this.nmContext.getApplications().get(
          applicationId);
      Container container = this.nmContext.getContainers().get(containerId);

      if (application == null) {
        html.h1(
            "Unknown container. Container either has not started or "
                + "has already completed or "
                + "doesn't belong to this node at all.");
        return;
      }
      if (container == null) {
        // Container may have alerady completed, but logs not aggregated yet.
        printLogs(html, containerId, applicationId, application);
        return;
      }

      if (EnumSet.of(ContainerState.NEW, ContainerState.LOCALIZING,
          ContainerState.LOCALIZED).contains(container.getContainerState())) {
        html.h1("Container is not yet running. Current state is "
                + container.getContainerState());
        return;
      }

      if (container.getContainerState() == ContainerState.LOCALIZATION_FAILED) {
        html.h1("Container wasn't started. Localization failed.");
        return;
      }

      if (EnumSet.of(ContainerState.RUNNING,
          ContainerState.EXITED_WITH_FAILURE,
          ContainerState.EXITED_WITH_SUCCESS).contains(
          container.getContainerState())) {
        printLogs(html, containerId, applicationId, application);
        return;
      }
      if (EnumSet.of(ContainerState.KILLING,
          ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
          ContainerState.CONTAINER_RESOURCES_CLEANINGUP).contains(
          container.getContainerState())) {
        //Container may have generated some logs before being killed.
        printLogs(html, containerId, applicationId, application);
        return;
      }
      if (container.getContainerState().equals(ContainerState.DONE)) {
        // Prev state unknown. Logs may be available.
        printLogs(html, containerId, applicationId, application);
        return;
      } else {
        html.h1("Container is no longer running...");
        return;
      }
    }

    private void printLogs(Block html, ContainerId containerId,
        ApplicationId applicationId, Application application) {
      // Check for the authorization.
      String remoteUser = request().getRemoteUser();
      UserGroupInformation callerUGI = null;

      if (remoteUser != null) {
        callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
      }
      if (callerUGI != null
          && !this.aclsManager.checkAccess(callerUGI,
              ApplicationAccessType.VIEW_APP, application.getUser(),
              applicationId)) {
        html.h1(
            "User [" + remoteUser
                + "] is not authorized to view the logs for application "
                + applicationId);
        return;
      }

      if (!$(CONTAINER_LOG_TYPE).isEmpty()) {
        File logFile = null;
        try {
          logFile =
              new File(this.dirsHandler.getLogPathToRead(
                  ContainerLaunch.getRelativeContainerLogDir(
                  applicationId.toString(), containerId.toString())
                  + Path.SEPARATOR + $(CONTAINER_LOG_TYPE))
                  .toUri().getPath());
        } catch (Exception e) {
          html.h1("Cannot find this log on the local disk.");
          return;
        }
        long start =
            $("start").isEmpty() ? -4 * 1024 : Long.parseLong($("start"));
        start = start < 0 ? logFile.length() + start : start;
        start = start < 0 ? 0 : start;
        long end =
            $("end").isEmpty() ? logFile.length() : Long.parseLong($("end"));
        end = end < 0 ? logFile.length() + end : end;
        end = end < 0 ? logFile.length() : end;
        if (start > end) {
          html.h1("Invalid start and end values. Start: [" + start + "]"
              + ", end[" + end + "]");
          return;
        } else {
          FileInputStream logByteStream = null;
          try {
            long toRead = end - start;
            if (toRead < logFile.length()) {
              html.p()._("Showing " + toRead + " bytes. Click ")
                  .a(url("containerlogs", $(CONTAINER_ID), $(APP_OWNER), 
                      logFile.getName(), "?start=0"), "here").
                      _(" for full log")._();
            }
            // TODO: Use secure IO Utils to avoid symlink attacks.
            // TODO Fix findBugs close warning along with IOUtils change
            logByteStream = new FileInputStream(logFile);
            IOUtils.skipFully(logByteStream, start);

            InputStreamReader reader = new InputStreamReader(logByteStream);
            int bufferSize = 65536;
            char[] cbuf = new char[bufferSize];

            int len = 0;
            int currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
            PRE<Hamlet> pre = html.pre();

            while ((len = reader.read(cbuf, 0, currentToRead)) > 0
                && toRead > 0) {
              pre._(new String(cbuf, 0, len));
              toRead = toRead - len;
              currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
            }

            pre._();
            reader.close();

          } catch (IOException e) {
            html.h1("Exception reading log-file. Log file was likely aggregated. "
                + StringUtils.stringifyException(e));
          } finally {
            if (logByteStream != null) {
              try {
                logByteStream.close();
              } catch (IOException e) {
                // Ignore
              }
            }
          }
        }
      } else {
        // Print out log types in lexical order
        List<File> containerLogsDirs = getContainerLogDirs(containerId,
            dirsHandler);
        Collections.sort(containerLogsDirs);
        boolean foundLogFile = false;
        for (File containerLogsDir : containerLogsDirs) {
          File[] logFiles = containerLogsDir.listFiles();
          Arrays.sort(logFiles);
          for (File logFile : logFiles) {
            foundLogFile = true;
            html.p()
                .a(url("containerlogs", $(CONTAINER_ID), $(APP_OWNER), 
                    logFile.getName(), "?start=-4096"),
                    logFile.getName() + " : Total file length is "
                        + logFile.length() + " bytes.")._();
          }
        }
        if (!foundLogFile) {
          html.h1("No logs available for container " + containerId.toString());
          return;
        }
      }
      return;
    }

    static List<File> getContainerLogDirs(ContainerId containerId,
            LocalDirsHandlerService dirsHandler) {
      List<String> logDirs = dirsHandler.getLogDirs();
      List<File> containerLogDirs = new ArrayList<File>(logDirs.size());
      for (String logDir : logDirs) {
        String appIdStr = 
            ConverterUtils.toString(
                containerId.getApplicationAttemptId().getApplicationId());
        File appLogDir = new File(logDir, appIdStr);
        String containerIdStr = ConverterUtils.toString(containerId);
        containerLogDirs.add(new File(appLogDir, containerIdStr));
      }
      return containerLogDirs;
    }
  }
}
