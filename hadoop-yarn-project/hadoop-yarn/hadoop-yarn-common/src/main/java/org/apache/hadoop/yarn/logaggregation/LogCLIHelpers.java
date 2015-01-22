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
    Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(
        remoteRootLogDir, ConverterUtils.toApplicationId(appId), jobOwner,
        suffix);
    RemoteIterator<FileStatus> nodeFiles;
    try {
      Path qualifiedLogDir =
          FileContext.getFileContext(getConf()).makeQualified(
            remoteAppLogDir);
      nodeFiles =
          FileContext.getFileContext(qualifiedLogDir.toUri(), getConf())
            .listStatus(remoteAppLogDir);
    } catch (FileNotFoundException fnf) {
      logDirNotExist(remoteAppLogDir.toString());
      return -1;
    }
    boolean foundContainerLogs = false;
    while (nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      String fileName = thisNodeFile.getPath().getName();
      if (fileName.contains(LogAggregationUtils.getNodeString(nodeId))
          && !fileName.endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
        AggregatedLogFormat.LogReader reader = null;
        try {
          reader =
              new AggregatedLogFormat.LogReader(getConf(),
                thisNodeFile.getPath());
          if (dumpAContainerLogs(containerId, reader, System.out,
              thisNodeFile.getModificationTime()) > -1) {
            foundContainerLogs = true;
          }
        } finally {
          if (reader != null) {
            reader.close();
          }
        }
      }
    }
    if (!foundContainerLogs) {
      containerLogNotFound(containerId);
      return -1;
    }
    return 0;
  }

  @Private
  public int dumpAContainerLogs(String containerIdStr,
      AggregatedLogFormat.LogReader reader, PrintStream out,
      long logUploadedTime) throws IOException {
    DataInputStream valueStream;
    LogKey key = new LogKey();
    valueStream = reader.next(key);

    while (valueStream != null && !key.toString().equals(containerIdStr)) {
      // Next container
      key = new LogKey();
      valueStream = reader.next(key);
    }

    if (valueStream == null) {
      return -1;
    }

    boolean foundContainerLogs = false;
    while (true) {
      try {
        LogReader.readAContainerLogsForALogType(valueStream, out,
          logUploadedTime);
        foundContainerLogs = true;
      } catch (EOFException eof) {
        break;
      }
    }
    if (foundContainerLogs) {
      return 0;
    }
    return -1;
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
      Path qualifiedLogDir =
          FileContext.getFileContext(getConf()).makeQualified(remoteAppLogDir);
      nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(),
          getConf()).listStatus(remoteAppLogDir);
    } catch (FileNotFoundException fnf) {
      logDirNotExist(remoteAppLogDir.toString());
      return -1;
    }
    boolean foundAnyLogs = false;
    while (nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      if (!thisNodeFile.getPath().getName()
        .endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
        AggregatedLogFormat.LogReader reader =
            new AggregatedLogFormat.LogReader(getConf(), thisNodeFile.getPath());
        try {

          DataInputStream valueStream;
          LogKey key = new LogKey();
          valueStream = reader.next(key);

          while (valueStream != null) {

            String containerString =
                "\n\nContainer: " + key + " on " + thisNodeFile.getPath().getName();
            out.println(containerString);
            out.println(StringUtils.repeat("=", containerString.length()));
            while (true) {
              try {
                LogReader.readAContainerLogsForALogType(valueStream, out,
                  thisNodeFile.getModificationTime());
                foundAnyLogs = true;
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
    }
    if (! foundAnyLogs) {
      emptyLogDir(remoteAppLogDir.toString());
      return -1;
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

  private static void containerLogNotFound(String containerId) {
    System.out.println("Logs for container " + containerId
      + " are not present in this log-file.");
  }

  private static void logDirNotExist(String remoteAppLogDir) {
    System.out.println(remoteAppLogDir + " does not exist.");
    System.out.println("Log aggregation has not completed or is not enabled.");
  }

  private static void emptyLogDir(String remoteAppLogDir) {
    System.out.println(remoteAppLogDir + " does not have any log files.");
  }
}
