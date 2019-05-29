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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Private
public class LogAggregationUtils {

  public static final String TMP_FILE_SUFFIX = ".tmp";

  /**
   * Constructs the full filename for an application's log file per node.
   * @param remoteRootLogDir the aggregated remote root log dir
   * @param appId the application Id
   * @param user the application owner
   * @param nodeId the node id
   * @param suffix the log dir suffix
   * @return the remote log file.
   */
  public static Path getRemoteNodeLogFileForApp(Path remoteRootLogDir,
      ApplicationId appId, String user, NodeId nodeId, String suffix) {
    return new Path(getRemoteAppLogDir(remoteRootLogDir, appId, user, suffix),
        getNodeString(nodeId));
  }

  /**
   * Gets the remote app log dir.
   * @param remoteRootLogDir the aggregated log remote root log dir
   * @param appId the application id
   * @param user the application owner
   * @param suffix the log directory suffix
   * @return the remote application specific log dir.
   */
  public static Path getRemoteAppLogDir(Path remoteRootLogDir,
      ApplicationId appId, String user, String suffix) {
    return new Path(getRemoteLogSuffixedDir(remoteRootLogDir, user, suffix),
        appId.toString());
  }

  /**
   * Gets the remote suffixed log dir for the user.
   * @param remoteRootLogDir the aggregated log remote root log dir
   * @param user the application owner
   * @param suffix the log dir suffix
   * @return the remote suffixed log dir.
   */
  public static Path getRemoteLogSuffixedDir(Path remoteRootLogDir,
      String user, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return getRemoteLogUserDir(remoteRootLogDir, user);
    }
    // TODO Maybe support suffix to be more than a single file.
    return new Path(getRemoteLogUserDir(remoteRootLogDir, user), suffix);
  }

  /**
   * Gets the remote log user dir.
   * @param remoteRootLogDir the aggregated log remote root log dir
   * @param user the application owner
   * @return the remote per user log dir.
   */
  public static Path getRemoteLogUserDir(Path remoteRootLogDir, String user) {
    return new Path(remoteRootLogDir, user);
  }

  
  /**
   * Converts a nodeId to a form used in the app log file name.
   * @param nodeId the nodeId
   * @return the node string to be used to construct the file name.
   */
  @VisibleForTesting
  public static String getNodeString(NodeId nodeId) {
    return nodeId.toString().replace(":", "_");
  }

  @VisibleForTesting
  public static String getNodeString(String nodeId) {
    return nodeId.toString().replace(":", "_");
  }


  /**
   * Return the remote application log directory.
   * @param conf the configuration
   * @param appId the application
   * @param appOwner the application owner
   * @param remoteRootLogDir the remote root log directory
   * @param suffix the log directory suffix
   * @return the remote application log directory path
   * @throws IOException if we can not find remote application log directory
   */
  public static org.apache.hadoop.fs.Path getRemoteAppLogDir(
      Configuration conf, ApplicationId appId, String appOwner,
      org.apache.hadoop.fs.Path remoteRootLogDir, String suffix)
      throws IOException {
    org.apache.hadoop.fs.Path remoteAppDir = null;
    if (appOwner == null) {
      org.apache.hadoop.fs.Path qualifiedRemoteRootLogDir =
          FileContext.getFileContext(conf).makeQualified(remoteRootLogDir);
      FileContext fc = FileContext.getFileContext(
          qualifiedRemoteRootLogDir.toUri(), conf);
      org.apache.hadoop.fs.Path toMatch = LogAggregationUtils
          .getRemoteAppLogDir(remoteRootLogDir, appId, "*", suffix);
      FileStatus[] matching  = fc.util().globStatus(toMatch);
      if (matching == null || matching.length != 1) {
        throw new IOException("Can not find remote application directory for "
            + "the application:" + appId);
      }
      remoteAppDir = matching[0].getPath();
    } else {
      remoteAppDir = LogAggregationUtils.getRemoteAppLogDir(
          remoteRootLogDir, appId, appOwner, suffix);
    }
    return remoteAppDir;
  }

  /**
   * Get all available log files under remote app log directory.
   * @param conf the configuration
   * @param appId the applicationId
   * @param appOwner the application owner
   * @param remoteRootLogDir the remote root log directory
   * @param suffix the log directory suffix
   * @return the iterator of available log files
   * @throws IOException if there is no log file available
   */
  public static RemoteIterator<FileStatus> getRemoteNodeFileDir(
      Configuration conf, ApplicationId appId, String appOwner,
      org.apache.hadoop.fs.Path remoteRootLogDir, String suffix)
      throws IOException {
    Path remoteAppLogDir = getRemoteAppLogDir(conf, appId, appOwner,
        remoteRootLogDir, suffix);
    RemoteIterator<FileStatus> nodeFiles = null;
    Path qualifiedLogDir =
        FileContext.getFileContext(conf).makeQualified(remoteAppLogDir);
    nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(),
        conf).listStatus(remoteAppLogDir);
    return nodeFiles;
  }

  /**
   * Get all available log files under remote app log directory.
   * @param conf the configuration
   * @param appId the applicationId
   * @param appOwner the application owner
   * @param remoteRootLogDir the remote root log directory
   * @param suffix the log directory suffix
   * @return the list of available log files
   * @throws IOException if there is no log file available
   */
  public static List<FileStatus> getRemoteNodeFileList(
      Configuration conf, ApplicationId appId, String appOwner,
      org.apache.hadoop.fs.Path remoteRootLogDir, String suffix)
      throws IOException {
    Path remoteAppLogDir = getRemoteAppLogDir(conf, appId, appOwner,
        remoteRootLogDir, suffix);
    List<FileStatus> nodeFiles = new ArrayList<>();
    Path qualifiedLogDir =
        FileContext.getFileContext(conf).makeQualified(remoteAppLogDir);
    nodeFiles.addAll(Arrays.asList(FileContext.getFileContext(
        qualifiedLogDir.toUri(), conf).util().listStatus(remoteAppLogDir)));
    return nodeFiles;
  }
}
