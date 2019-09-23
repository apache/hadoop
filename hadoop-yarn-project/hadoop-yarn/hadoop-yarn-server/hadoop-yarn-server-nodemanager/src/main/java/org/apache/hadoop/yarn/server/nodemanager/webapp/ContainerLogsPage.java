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
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.PRE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;

public class ContainerLogsPage extends NMView {
  public static final Logger LOG = LoggerFactory.getLogger(
      ContainerLogsPage.class);

  public static final String REDIRECT_URL = "redirect.url";
  public static final String LOG_AGGREGATION_TYPE = "log.aggregation.type";
  public static final String LOG_AGGREGATION_REMOTE_TYPE = "remote";
  public static final String LOG_AGGREGATION_LOCAL_TYPE = "local";

  @Override protected void preHead(Page.HTML<__> html) {
    String redirectUrl = $(REDIRECT_URL);
    if (redirectUrl == null || redirectUrl.isEmpty()) {
      set(TITLE, join("Logs for ", $(CONTAINER_ID)));
    } else {
      if (redirectUrl.equals("false")) {
        set(TITLE, join("Failed redirect for ", $(CONTAINER_ID)));
        //Error getting redirect url. Fall through.
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
    private final Context nmContext;
    private final LogAggregationFileControllerFactory factory;

    @Inject
    public ContainersLogsBlock(Context context) {
      this.nmContext = context;
      this.factory = new LogAggregationFileControllerFactory(
          context.getConf());
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
      ApplicationId appId;
      try {
        containerId = ContainerId.fromString($(CONTAINER_ID));
        appId = containerId.getApplicationAttemptId().getApplicationId();
      } catch (IllegalArgumentException ex) {
        html.h1("Invalid container ID: " + $(CONTAINER_ID));
        return;
      }

      LogAggregationFileController fileController = null;
      boolean foundAggregatedLogs = false;
      try {
        fileController = this.factory.getFileControllerForRead(
            appId, $(APP_OWNER));
        foundAggregatedLogs = true;
      } catch (IOException fnf) {
        // Do Nothing
      }

      try {
        if ($(CONTAINER_LOG_TYPE).isEmpty()) {
          html.h2("Local Logs:");
          List<File> logFiles = ContainerLogsUtils.getContainerLogDirs(containerId,
              request().getRemoteUser(), nmContext);
          printLocalLogFileDirectory(html, logFiles);
          if (foundAggregatedLogs) {
            // print out the aggregated logs if exists
            try {
              ContainerLogsRequest logRequest = new ContainerLogsRequest();
              logRequest.setAppId(appId);
              logRequest.setAppOwner($(APP_OWNER));
              logRequest.setContainerId($(CONTAINER_ID));
              logRequest.setNodeId(this.nmContext.getNodeId().toString());
              List<ContainerLogMeta> containersLogMeta = fileController
                  .readAggregatedLogsMeta(logRequest);
              if (containersLogMeta != null && !containersLogMeta.isEmpty()) {
                html.h2("Aggregated Logs:");
                printAggregatedLogFileDirectory(html, containersLogMeta);
              }
            } catch (Exception ex) {
              LOG.debug("{}", ex);
            }
          }
        } else {
          String aggregationType = $(LOG_AGGREGATION_TYPE);
          if (aggregationType == null || aggregationType.isEmpty() ||
              aggregationType.trim().toLowerCase().equals(
                  LOG_AGGREGATION_LOCAL_TYPE)) {
            File logFile = ContainerLogsUtils.getContainerLogFile(containerId,
                $(CONTAINER_LOG_TYPE), request().getRemoteUser(), nmContext);
            printLocalLogFile(html, logFile);
          } else if (!LOG_AGGREGATION_LOCAL_TYPE.trim().toLowerCase().equals(
              aggregationType) && !LOG_AGGREGATION_REMOTE_TYPE.trim()
                  .toLowerCase().equals(aggregationType)) {
            html.h1("Invalid value for query parameter: "
                + LOG_AGGREGATION_TYPE + ". "
                + "The valid value could be either "
                + LOG_AGGREGATION_LOCAL_TYPE + " or "
                + LOG_AGGREGATION_REMOTE_TYPE + ".");
          }
        }
      } catch (YarnException ex) {
        html.h1(ex.getMessage());
      } catch (NotFoundException ex) {
        html.h1(ex.getMessage());
      }
    }
    
    private void printLocalLogFile(Block html, File logFile) {
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
          logByteStream = ContainerLogsUtils.openLogFileForRead($(CONTAINER_ID),
              logFile, nmContext);
        } catch (IOException ex) {
          html.h1(ex.getMessage());
          return;
        }
        
        try {
          long toRead = end - start;
          if (toRead < logFile.length()) {
            html.p().__("Showing " + toRead + " bytes. Click ")
                .a(url("containerlogs", $(CONTAINER_ID), $(APP_OWNER), 
                    logFile.getName(), "?start=0"), "here").
                __(" for full log").__();
          }
          
          IOUtils.skipFully(logByteStream, start);
          InputStreamReader reader =
              new InputStreamReader(logByteStream, Charset.forName("UTF-8"));
          int bufferSize = 65536;
          char[] cbuf = new char[bufferSize];

          int len = 0;
          int currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
          PRE<Hamlet> pre = html.pre();

          while ((len = reader.read(cbuf, 0, currentToRead)) > 0
              && toRead > 0) {
            pre.__(new String(cbuf, 0, len));
            toRead = toRead - len;
            currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
          }

          pre.__();
          reader.close();

        } catch (IOException e) {
          LOG.error(
              "Exception reading log file " + logFile.getAbsolutePath(), e);
          html.h1("Exception reading log file. It might be because log "
                + "file was aggregated : " + logFile.getName());
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
    }
    
    private void printLocalLogFileDirectory(Block html,
        List<File> containerLogsDirs) {
      // Print out log types in lexical order
      Collections.sort(containerLogsDirs);
      boolean foundLogFile = false;
      for (File containerLogsDir : containerLogsDirs) {
        File[] logFiles = containerLogsDir.listFiles();
        if (logFiles != null) {
          Arrays.sort(logFiles);
          for (File logFile : logFiles) {
            foundLogFile = true;
            html.p()
                .a(url("containerlogs", $(CONTAINER_ID), $(APP_OWNER),
                    logFile.getName(), "?start=-4096"),
                    logFile.getName() + " : Total file length is "
                        + logFile.length() + " bytes.").__();
          }
        }
      }
      if (!foundLogFile) {
        html.h1("No logs available for container " + $(CONTAINER_ID));
        return;
      }
    }

    private void printAggregatedLogFileDirectory(Block html,
        List<ContainerLogMeta> containersLogMeta) throws ParseException {
      List<ContainerLogFileInfo> filesInfo = new ArrayList<>();
      for (ContainerLogMeta logMeta : containersLogMeta) {
        filesInfo.addAll(logMeta.getContainerLogMeta());
      }

      //sort the list, so we could list the log file in order.
      Collections.sort(filesInfo, new Comparator<ContainerLogFileInfo>() {
        @Override
        public int compare(ContainerLogFileInfo o1,
            ContainerLogFileInfo o2) {
          return createAggregatedLogFileName(o1.getFileName(),
              o1.getLastModifiedTime()).compareTo(
                  createAggregatedLogFileName(o2.getFileName(),
                      o2.getLastModifiedTime()));
        }
      });

      boolean foundLogFile = false;
      for (ContainerLogFileInfo fileInfo : filesInfo) {
        long timestamp = convertDateToTimeStamp(fileInfo.getLastModifiedTime());
        foundLogFile = true;
        String fileName = createAggregatedLogFileName(fileInfo.getFileName(),
            fileInfo.getLastModifiedTime());
        html.p().a(url("containerlogs", $(CONTAINER_ID), $(APP_OWNER),
            fileInfo.getFileName(),
            "?start=-4096&" + LOG_AGGREGATION_TYPE + "="
                + LOG_AGGREGATION_REMOTE_TYPE + "&start.time="
                + (timestamp - 1000) + "&end.time=" + (timestamp + 1000)),
            fileName + " : Total file length is "
                + fileInfo.getFileSize() + " bytes.").__();
      }

      if (!foundLogFile) {
        html.h4("No aggregated logs available for container "
            + $(CONTAINER_ID));
        return;
      }
    }

    private String createAggregatedLogFileName(String fileName,
        String modificationTime) {
      return fileName + "_" + modificationTime;
    }

    private long convertDateToTimeStamp(String dateTime)
        throws ParseException {
      SimpleDateFormat sdf = new SimpleDateFormat(
          "EEE MMM dd HH:mm:ss Z yyyy");
      Date d = sdf.parse(dateTime);

      Calendar c = Calendar.getInstance();
      c.setTime(d);
      return c.getTimeInMillis();
    }
  }
}
