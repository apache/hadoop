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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

public class LogCLIHelpers implements Configurable {

  private Configuration conf;

  @Private
  @VisibleForTesting
  public int dumpAContainersLogs(String appId, String containerId,
      String nodeId, String jobOwner) throws IOException {
    Path remoteRootLogDir = new Path(getConf().get(
        YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    String suffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(getConf());
    Path logPath = LogAggregationUtils.getRemoteNodeLogFileForApp(
        remoteRootLogDir, ConverterUtils.toApplicationId(appId), jobOwner,
        ConverterUtils.toNodeId(nodeId), suffix);
    AggregatedLogFormat.LogReader reader;
    try {
      reader = new AggregatedLogFormat.LogReader(getConf(), logPath);
    } catch (FileNotFoundException fnfe) {
      System.out.println("Logs not available at " + logPath.toString());
      System.out
          .println("Log aggregation has not completed or is not enabled.");
      return -1;
    }
    return dumpAContainerLogs(containerId, reader, System.out);
  }

  @Private
  public int dumpAContainerLogs(String containerIdStr,
      AggregatedLogFormat.LogReader reader, PrintStream out) throws IOException {
    DataInputStream valueStream;
    LogKey key = new LogKey();
    valueStream = reader.next(key);

    while (valueStream != null && !key.toString().equals(containerIdStr)) {
      // Next container
      key = new LogKey();
      valueStream = reader.next(key);
    }

    if (valueStream == null) {
      System.out.println("Logs for container " + containerIdStr
          + " are not present in this log-file.");
      return -1;
    }

    while (true) {
      try {
        LogReader.readAContainerLogsForALogType(valueStream, out);
      } catch (EOFException eof) {
        break;
      }
    }
    return 0;
  }

  @Private
  public int dumpAllContainersLogs(ApplicationId appId, String appOwner,
      PrintStream out) throws IOException {
    Path remoteRootLogDir = new Path(getConf().get(
        YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    String user = appOwner;
    String logDirSuffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(getConf());
    // TODO Change this to get a list of files from the LAS.
    Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(
        remoteRootLogDir, appId, user, logDirSuffix);
    RemoteIterator<FileStatus> nodeFiles;
    try {
      nodeFiles = FileContext.getFileContext().listStatus(remoteAppLogDir);
    } catch (FileNotFoundException fnf) {
      System.out.println("Logs not available at " + remoteAppLogDir.toString());
      System.out
          .println("Log aggregation has not completed or is not enabled.");
      return -1;
    }
    while (nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(
          getConf(), new Path(remoteAppLogDir, thisNodeFile.getPath().getName()));
      try {

        DataInputStream valueStream;
        LogKey key = new LogKey();
        valueStream = reader.next(key);

        while (valueStream != null) {
          String containerString = "\n\nContainer: " + key + " on "
              + thisNodeFile.getPath().getName();
          out.println(containerString);
          out.println(StringUtils.repeat("=", containerString.length()));
          while (true) {
            try {
              LogReader.readAContainerLogsForALogType(valueStream, out);
            } catch (EOFException eof) {
              break;
            }
          }

          // Next container
          key = new LogKey();
          valueStream = reader.next(key);
        }
      } finally {
        reader.close();
      }
    }
    return 0;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
