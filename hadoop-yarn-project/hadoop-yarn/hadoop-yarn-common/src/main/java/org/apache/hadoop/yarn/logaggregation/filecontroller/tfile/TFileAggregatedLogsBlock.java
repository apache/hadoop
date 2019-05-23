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

package org.apache.hadoop.yarn.logaggregation.filecontroller.tfile;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_LOG_TYPE;

import com.google.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.HarFs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationHtmlBlock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.PRE;

/**
 * The Aggregated Logs Block implementation for TFile.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class TFileAggregatedLogsBlock extends LogAggregationHtmlBlock {

  private final Configuration conf;
  private Path remoteRootLogDir;
  private String remoteRootLogDirSuffix;

  @Inject
  public TFileAggregatedLogsBlock(ViewContext ctx, Configuration conf,
      Path remoteRootLogDir, String remoteRootLogDirSuffix) {
    super(ctx);
    this.conf = conf;
    this.remoteRootLogDir = remoteRootLogDir;
    this.remoteRootLogDirSuffix = remoteRootLogDirSuffix;
  }

  @Override
  protected void render(Block html) {

    BlockParameters params = verifyAndParseParameters(html);
    if (params == null) {
      return;
    }

    RemoteIterator<FileStatus> nodeFiles;
    try {
      nodeFiles = LogAggregationUtils
          .getRemoteNodeFileDir(conf, params.getAppId(),
          params.getAppOwner(), remoteRootLogDir, remoteRootLogDirSuffix);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception ex) {
      html.h1("No logs available for container "
          + params.getContainerId().toString());
      return;
    }

    NodeId nodeId = params.getNodeId();
    String logEntity = params.getLogEntity();
    ApplicationId appId = params.getAppId();
    ContainerId containerId = params.getContainerId();
    long start = params.getStartIndex();
    long end = params.getEndIndex();
    long startTime = params.getStartTime();
    long endTime = params.getEndTime();

    boolean foundLog = false;
    String desiredLogType = $(CONTAINER_LOG_TYPE);
    try {
      while (nodeFiles.hasNext()) {
        AggregatedLogFormat.LogReader reader = null;
        try {
          FileStatus thisNodeFile = nodeFiles.next();
          if (thisNodeFile.getPath().getName().equals(
              params.getAppId() + ".har")) {
            Path p = new Path("har:///"
                + thisNodeFile.getPath().toUri().getRawPath());
            nodeFiles = HarFs.get(p.toUri(), conf).listStatusIterator(p);
            continue;
          }
          if (!thisNodeFile.getPath().getName()
              .contains(LogAggregationUtils.getNodeString(nodeId))
              || thisNodeFile.getPath().getName()
                  .endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
            continue;
          }
          long logUploadedTime = thisNodeFile.getModificationTime();
          if (logUploadedTime < startTime || logUploadedTime > endTime) {
            continue;
          }
          reader = new AggregatedLogFormat.LogReader(
              conf, thisNodeFile.getPath());

          String owner = null;
          Map<ApplicationAccessType, String> appAcls = null;
          try {
            owner = reader.getApplicationOwner();
            appAcls = reader.getApplicationAcls();
          } catch (IOException e) {
            LOG.error("Error getting logs for " + logEntity, e);
            continue;
          }
          String remoteUser = request().getRemoteUser();

          if (!checkAcls(conf, appId, owner, appAcls, remoteUser)) {
            html.h1().__("User [" + remoteUser
                + "] is not authorized to view the logs for " + logEntity
                + " in log file [" + thisNodeFile.getPath().getName() + "]")
                .__();
            LOG.error("User [" + remoteUser
                + "] is not authorized to view the logs for " + logEntity);
            continue;
          }

          AggregatedLogFormat.ContainerLogsReader logReader = reader
              .getContainerLogsReader(containerId);
          if (logReader == null) {
            continue;
          }

          foundLog = readContainerLogs(html, logReader, start, end,
              desiredLogType, logUploadedTime, startTime, endTime);
        } catch (IOException ex) {
          LOG.error("Error getting logs for " + logEntity, ex);
          continue;
        } finally {
          if (reader != null) {
            reader.close();
          }
        }
      }
      if (!foundLog) {
        if (desiredLogType.isEmpty()) {
          html.h1("No logs available for container "
              + containerId.toString());
        } else {
          html.h1("Unable to locate '" + desiredLogType
              + "' log for container " + containerId.toString());
        }
      }
    } catch (IOException e) {
      html.h1().__("Error getting logs for " + logEntity).__();
      LOG.error("Error getting logs for " + logEntity, e);
    }
  }

  private boolean readContainerLogs(Block html,
      AggregatedLogFormat.ContainerLogsReader logReader, long startIndex,
      long endIndex, String desiredLogType, long logUpLoadTime,
      long startTime, long endTime) throws IOException {
    int bufferSize = 65536;
    byte[] cbuf = new byte[bufferSize];

    boolean foundLog = false;
    String logType = logReader.nextLog();
    while (logType != null) {
      if (desiredLogType == null || desiredLogType.isEmpty()
          || desiredLogType.equals(logType)) {
        long logLength = logReader.getCurrentLogLength();
        if (foundLog) {
          html.pre().__("\n\n").__();
        }

        html.p().__("Log Type: " + logType).__();
        html.p().__("Log Upload Time: " + Times.format(logUpLoadTime)).__();
        html.p().__("Log Length: " + Long.toString(logLength)).__();

        long[] range = checkParseRange(html, startIndex, endIndex, startTime,
            endTime, logLength, logType);

        processContainerLog(html, range, logReader, bufferSize, cbuf);
        foundLog = true;
      }

      logType = logReader.nextLog();
    }

    return foundLog;
  }
}
