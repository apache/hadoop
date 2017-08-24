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

package org.apache.hadoop.yarn.logaggregation.filecontroller;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.Times;

/**
 * The TFile log aggregation file Controller implementation.
 */
@Private
@Unstable
public class LogAggregationTFileController
    extends LogAggregationFileController {

  private static final Log LOG = LogFactory.getLog(
      LogAggregationTFileController.class);

  private LogWriter writer;

  public LogAggregationTFileController(){}

  @Override
  public void initInternal(Configuration conf) {
    this.remoteRootLogDir = new Path(
        conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    this.remoteRootLogDirSuffix =
        conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
  }

  @Override
  public void initializeWriter(LogAggregationFileControllerContext context)
      throws IOException {
    this.writer = new LogWriter();
    writer.initialize(this.conf, context.getRemoteNodeTmpLogFileForApp(),
        context.getUserUgi());
    // Write ACLs once when the writer is created.
    writer.writeApplicationACLs(context.getAppAcls());
    writer.writeApplicationOwner(context.getUserUgi().getShortUserName());
  }

  @Override
  public void closeWriter() {
    this.writer.close();
  }

  @Override
  public void write(LogKey logKey, LogValue logValue) throws IOException {
    this.writer.append(logKey, logValue);
  }

  @Override
  public void postWrite(final LogAggregationFileControllerContext record)
      throws Exception {
    // Before upload logs, make sure the number of existing logs
    // is smaller than the configured NM log aggregation retention size.
    if (record.isUploadedLogsInThisCycle() &&
        record.isLogAggregationInRolling()) {
      cleanOldLogs(record.getRemoteNodeLogFileForApp(), record.getNodeId(),
          record.getUserUgi());
      record.increcleanupOldLogTimes();
    }

    final Path renamedPath = record.getRollingMonitorInterval() <= 0
        ? record.getRemoteNodeLogFileForApp() : new Path(
            record.getRemoteNodeLogFileForApp().getParent(),
            record.getRemoteNodeLogFileForApp().getName() + "_"
            + record.getLogUploadTimeStamp());
    final boolean rename = record.isUploadedLogsInThisCycle();
    try {
      record.getUserUgi().doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          FileSystem remoteFS = record.getRemoteNodeLogFileForApp()
              .getFileSystem(conf);
          if (rename) {
            remoteFS.rename(record.getRemoteNodeTmpLogFileForApp(),
                renamedPath);
          } else {
            remoteFS.delete(record.getRemoteNodeTmpLogFileForApp(), false);
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error(
          "Failed to move temporary log file to final location: ["
          + record.getRemoteNodeTmpLogFileForApp() + "] to ["
          + renamedPath + "]", e);
      throw new Exception("Log uploaded failed for Application: "
          + record.getAppId() + " in NodeManager: "
          + LogAggregationUtils.getNodeString(record.getNodeId()) + " at "
          + Times.format(record.getLogUploadTimeStamp()) + "\n");
    }
  }
}
