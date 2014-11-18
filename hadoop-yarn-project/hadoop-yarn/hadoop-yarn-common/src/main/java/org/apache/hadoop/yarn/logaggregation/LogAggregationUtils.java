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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.annotations.VisibleForTesting;

@Private
public class LogAggregationUtils {

  public static final String TMP_FILE_SUFFIX = ".tmp";

  /**
   * Constructs the full filename for an application's log file per node.
   * @param remoteRootLogDir
   * @param appId
   * @param user
   * @param nodeId
   * @param suffix
   * @return the remote log file.
   */
  public static Path getRemoteNodeLogFileForApp(Path remoteRootLogDir,
      ApplicationId appId, String user, NodeId nodeId, String suffix) {
    return new Path(getRemoteAppLogDir(remoteRootLogDir, appId, user, suffix),
        getNodeString(nodeId));
  }

  /**
   * Gets the remote app log dir.
   * @param remoteRootLogDir
   * @param appId
   * @param user
   * @param suffix
   * @return the remote application specific log dir.
   */
  public static Path getRemoteAppLogDir(Path remoteRootLogDir,
      ApplicationId appId, String user, String suffix) {
    return new Path(getRemoteLogSuffixedDir(remoteRootLogDir, user, suffix),
        appId.toString());
  }

  /**
   * Gets the remote suffixed log dir for the user.
   * @param remoteRootLogDir
   * @param user
   * @param suffix
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

  // TODO Add a utility method to list available log files. Ignore the
  // temporary ones.
  
  /**
   * Gets the remote log user dir.
   * @param remoteRootLogDir
   * @param user
   * @return the remote per user log dir.
   */
  public static Path getRemoteLogUserDir(Path remoteRootLogDir, String user) {
    return new Path(remoteRootLogDir, user);
  }

  /**
   * Returns the suffix component of the log dir.
   * @param conf
   * @return the suffix which will be appended to the user log dir.
   */
  public static String getRemoteNodeLogDirSuffix(Configuration conf) {
    return conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
  }

  
  /**
   * Converts a nodeId to a form used in the app log file name.
   * @param nodeId
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
}
