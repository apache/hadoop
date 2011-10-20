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
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class ContainerLogsPage extends NMView {
  @Override
  protected Class<? extends SubView> content() {
    return ContainersLogsBlock.class;
  }

  public static class ContainersLogsBlock extends HtmlBlock implements
      NMWebParams {

    private final Configuration conf;
    private final LocalDirAllocator logsSelector;
    private final Context nmContext;
    private final ApplicationACLsManager aclsManager;

    @Inject
    public ContainersLogsBlock(Configuration conf, Context context,
        ApplicationACLsManager aclsManager) {
      this.conf = conf;
      this.logsSelector = new LocalDirAllocator(YarnConfiguration.NM_LOG_DIRS);
      this.nmContext = context;
      this.aclsManager = aclsManager;
    }

    @Override
    protected void render(Block html) {
      DIV<Hamlet> div = html.div("#content");

      ContainerId containerId;
      try {
        containerId = ConverterUtils.toContainerId($(CONTAINER_ID));
      } catch (IllegalArgumentException e) {
        div.h1("Invalid containerId " + $(CONTAINER_ID))._();
        return;
      }

      ApplicationId applicationId = containerId.getApplicationAttemptId()
          .getApplicationId();
      Application application = this.nmContext.getApplications().get(
          applicationId);
      Container container = this.nmContext.getContainers().get(containerId);

      if (application == null || container == null) {
        div.h1(
            "Unknown container. Container is either not yet running or "
                + "has already completed or "
                + "doesn't belong to this node at all.")._();
        return;
      }

      if (EnumSet.of(ContainerState.NEW, ContainerState.LOCALIZING,
          ContainerState.LOCALIZING).contains(container.getContainerState())) {
        div.h1("Container is not yet running. Current state is "
                + container.getContainerState())
              ._();
        return;
      }

      if (EnumSet.of(ContainerState.RUNNING,
          ContainerState.EXITED_WITH_FAILURE,
          ContainerState.EXITED_WITH_SUCCESS).contains(
          container.getContainerState())) {

        // Check for the authorization.
        String remoteUser = request().getRemoteUser();
        UserGroupInformation callerUGI = null;
        if (remoteUser != null) {
          callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
        }
        if (callerUGI != null && !this.aclsManager.checkAccess(callerUGI,
            ApplicationAccessType.VIEW_APP, application.getUser(),
                applicationId)) {
          div.h1(
              "You (User " + remoteUser
                  + ") are not authorized to view the logs for application "
                  + applicationId)._();
          return;
        }

        if (!$(CONTAINER_LOG_TYPE).isEmpty()) {
          File logFile = null;
          try {
            logFile =
                new File(this.logsSelector
                    .getLocalPathToRead(
                        ConverterUtils.toString(
                            applicationId)
                            + Path.SEPARATOR + $(CONTAINER_ID)
                            + Path.SEPARATOR
                            + $(CONTAINER_LOG_TYPE), this.conf).toUri()
                    .getPath());
          } catch (Exception e) {
            div.h1("Cannot find this log on the local disk.")._();
          }
          div.h1(logFile == null ? "Unknown LogFile" : logFile.getName());
          long start =
              $("start").isEmpty() ? -4 * 1024 : Long.parseLong($("start"));
          start = start < 0 ? logFile.length() + start : start;
          start = start < 0 ? 0 : start;
          long end =
              $("end").isEmpty() ? logFile.length() : Long
                  .parseLong($("end"));
          end = end < 0 ? logFile.length() + end : end;
          end = end < 0 ? logFile.length() : end;
          if (start > end) {
            writer().write("Invalid start and end values!");
          } else {
          try {
            long toRead = end - start;
            if (toRead < logFile.length()) {
                div._("Showing " + toRead + " bytes. Click ")
                    .a(url("containerlogs", $(CONTAINER_ID),
                        logFile.getName()), "here")
                    ._(" for full log").br()._();
            }
            // TODO: Use secure IO Utils to avoid symlink attacks.
            //TODO Fix findBugs close warning along with IOUtils change
            FileReader reader = new FileReader(logFile);
            char[] cbuf = new char[65536];
            reader.skip(start);
            int len = 0;
            int totalRead = 0;
            writer().write("<pre>");
            while ((len = reader.read(cbuf, 0, (int) toRead)) > 0
                && totalRead < (end - start)) {
              writer().write(cbuf, 0, len); // TODO: HTMl Quoting?
              totalRead += len;
              toRead = toRead - totalRead;
            }
            reader.close();
            writer().write("</pre>");
          } catch (IOException e) {
              writer().write(
                  "Exception reading log-file "
                      + StringUtils.stringifyException(e));
          } 
        }
          div._();
        } else {
          // Just print out the log-types
          List<File> containerLogsDirs =
              getContainerLogDirs(this.conf, containerId);
          for (File containerLogsDir : containerLogsDirs) {
            for (File logFile : containerLogsDir.listFiles()) {
              div
                  .p()
                  .a(
                      url("containerlogs", $(CONTAINER_ID),
                          logFile.getName(), "?start=-4076"),
                      logFile.getName() + " : Total file length is "
                          + logFile.length() + " bytes.")
                  ._();
            }
          }
          div._();
        }
      } else {
        div.h1("Container is no longer running..")._();
      }
    }

    static List<File>
        getContainerLogDirs(Configuration conf, ContainerId containerId) {
      String[] logDirs = conf.getStrings(YarnConfiguration.NM_LOG_DIRS,
          YarnConfiguration.DEFAULT_NM_LOG_DIRS);
      List<File> containerLogDirs = new ArrayList<File>(logDirs.length);
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
