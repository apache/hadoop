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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;

/**
 * This class contains several utility functions for log aggregation tests.
 */
public final class TestContainerLogsUtils {

  private TestContainerLogsUtils() {}

  /**
   * Utility function to create container log file and upload
   * it into remote file system.
   * @param conf the configuration
   * @param fs the FileSystem
   * @param rootLogDir the root log directory
   * @param containerId the containerId
   * @param nodeId the nodeId
   * @param fileName the log file name
   * @param user the application user
   * @param content the log context
   * @param deletePreviousRemoteLogDir whether to delete remote log dir.
   * @throws IOException if we can not create log files locally
   *         or we can not upload container logs into RemoteFS.
   */
  public static void createContainerLogFileInRemoteFS(Configuration conf,
      FileSystem fs, String rootLogDir, ContainerId containerId, NodeId nodeId,
      String fileName, String user, String content,
      boolean deleteRemoteLogDir) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    //prepare the logs for remote directory
    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    // create local logs
    List<String> rootLogDirList = new ArrayList<String>();
    rootLogDirList.add(rootLogDir);
    Path rootLogDirPath = new Path(rootLogDir);
    if (fs.exists(rootLogDirPath)) {
      fs.delete(rootLogDirPath, true);
    }
    assertTrue(fs.mkdirs(rootLogDirPath));
    Path appLogsDir = new Path(rootLogDirPath, appId.toString());
    if (fs.exists(appLogsDir)) {
      fs.delete(appLogsDir, true);
    }
    assertTrue(fs.mkdirs(appLogsDir));

    createContainerLogInLocalDir(appLogsDir, containerId, fs, fileName,
        content);
    // upload container logs to remote log dir
    Path path = new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR),
        user + "/logs/" + appId.toString());
    if (fs.exists(path) && deleteRemoteLogDir) {
      fs.delete(path, true);
    }
    assertTrue(fs.mkdirs(path));
    uploadContainerLogIntoRemoteDir(ugi, conf, rootLogDirList, nodeId,
        containerId, path, fs);
  }

  private static void createContainerLogInLocalDir(Path appLogsDir,
      ContainerId containerId, FileSystem fs, String fileName, String content)
      throws IOException{
    Path containerLogsDir = new Path(appLogsDir, containerId.toString());
    if (fs.exists(containerLogsDir)) {
      fs.delete(containerLogsDir, true);
    }
    assertTrue(fs.mkdirs(containerLogsDir));
    Writer writer =
        new FileWriter(new File(containerLogsDir.toString(), fileName));
    writer.write(content);
    writer.close();
  }

  private static void uploadContainerLogIntoRemoteDir(UserGroupInformation ugi,
      Configuration configuration, List<String> rootLogDirs, NodeId nodeId,
      ContainerId containerId, Path appDir, FileSystem fs) throws IOException {
    Path path =
        new Path(appDir, LogAggregationUtils.getNodeString(nodeId));
    LogAggregationFileControllerFactory factory
        = new LogAggregationFileControllerFactory(configuration);
    LogAggregationFileController fileController = factory
        .getFileControllerForWrite();
    try {
      Map<ApplicationAccessType, String> appAcls = new HashMap<>();
      appAcls.put(ApplicationAccessType.VIEW_APP, ugi.getUserName());
      ApplicationId appId = containerId.getApplicationAttemptId()
          .getApplicationId();
      LogAggregationFileControllerContext context
          = new LogAggregationFileControllerContext(
              path, path, true, 1000,
              appId, appAcls, nodeId, ugi);
      fileController.initializeWriter(context);
      fileController.write(new AggregatedLogFormat.LogKey(containerId),
          new AggregatedLogFormat.LogValue(rootLogDirs, containerId,
              ugi.getShortUserName()));
    } finally {
      fileController.closeWriter();
    }
  }
}
