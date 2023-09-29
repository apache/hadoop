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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.HarFs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Facilitates an extended query of aggregated log file metadata with
 * the help of file controllers.
 */
public class LogAggregationMetaCollector {

  private static final Logger LOG = LoggerFactory.getLogger(
      LogAggregationMetaCollector.class);

  private final ExtendedLogMetaRequest logsRequest;
  private final Configuration conf;

  public LogAggregationMetaCollector(
      ExtendedLogMetaRequest logsRequest, Configuration conf) {
    this.logsRequest = logsRequest;
    this.conf = conf;
  }

  /**
   * Collects all log file metadata based on the complex query defined in
   * {@code UserLogsRequest}.
   * @param fileController log aggregation file format controller
   * @return collection of log file metadata grouped by containers
   * @throws IOException if node file is not reachable
   */
  public List<ContainerLogMeta> collect(
      LogAggregationFileController fileController) throws IOException {
    List<ContainerLogMeta> containersLogMeta = new ArrayList<>();
    RemoteIterator<FileStatus> appDirs = fileController.
        getApplicationDirectoriesOfUser(logsRequest.getUser());

    while (appDirs.hasNext()) {
      FileStatus currentAppDir = appDirs.next();
      if (logsRequest.getAppId() == null ||
          logsRequest.getAppId().equals(currentAppDir.getPath().getName())) {
        ApplicationId appId = ApplicationId.fromString(
            currentAppDir.getPath().getName());
        RemoteIterator<FileStatus> nodeFiles = fileController
            .getNodeFilesOfApplicationDirectory(currentAppDir);

        while (nodeFiles.hasNext()) {
          FileStatus currentNodeFile = nodeFiles.next();
          if (!logsRequest.getNodeId().match(currentNodeFile.getPath()
              .getName())) {
            continue;
          }

          if (currentNodeFile.getPath().getName().equals(
              logsRequest.getAppId() + ".har")) {
            Path p = new Path("har:///"
                + currentNodeFile.getPath().toUri().getRawPath());
            nodeFiles = HarFs.get(p.toUri(), conf).listStatusIterator(p);
            continue;
          }

          try {
            Map<String, List<ContainerLogFileInfo>> metaFiles = fileController
                .getLogMetaFilesOfNode(logsRequest, currentNodeFile, appId);
            if (metaFiles == null) {
              continue;
            }

            metaFiles.entrySet().removeIf(entry ->
                !(logsRequest.getContainerId() == null ||
                    logsRequest.getContainerId().equals(entry.getKey())));

            containersLogMeta.addAll(createContainerLogMetas(
                currentNodeFile.getPath().getName(), metaFiles));
          } catch (IOException ioe) {
            LOG.warn("Can not get log meta from the log file:"
                + currentNodeFile.getPath() + "\n" + ioe.getMessage());
          }

        }
      }

    }
    return containersLogMeta;
  }

  private List<ContainerLogMeta> createContainerLogMetas(
      String nodeId, Map<String, List<ContainerLogFileInfo>> metaFiles) {
    List<ContainerLogMeta> containerLogMetas = new ArrayList<>();
    for (Map.Entry<String, List<ContainerLogFileInfo>> containerLogs
        : metaFiles.entrySet()) {
      ContainerLogMeta containerLogMeta = new ContainerLogMeta(
          containerLogs.getKey(), nodeId);
      for (ContainerLogFileInfo file : containerLogs.getValue()) {
        boolean isFileNameMatches = logsRequest.getFileName()
            .match(file.getFileName());
        boolean fileSizeComparison = logsRequest.getFileSize()
            .match(file.getFileSize());
        boolean modificationTimeComparison = logsRequest.getModificationTime()
            .match(file.getLastModifiedTime());

        if (!isFileNameMatches || !fileSizeComparison ||
            !modificationTimeComparison) {
          continue;
        }
        containerLogMeta.getContainerLogMeta().add(file);
      }
      if (!containerLogMeta.getContainerLogMeta().isEmpty()) {
        containerLogMetas.add(containerLogMeta);
      }
    }
    return containerLogMetas;
  }
}
