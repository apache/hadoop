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
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Private
public class LogAggregationUtils {

  public static final String TMP_FILE_SUFFIX = ".tmp";
  private static final String BUCKET_SUFFIX = "bucket-";

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
    return new Path(getRemoteBucketDir(remoteRootLogDir, user, suffix,
        appId), appId.toString());
  }

  /**
   * Gets the older remote app log dir.
   * @param appId the application id
   * @param user the application owner
   * @param remoteRootLogDir the aggregated log remote root log dir
   * @param suffix the log directory suffix
   * @return the remote application specific log dir.
   */
  public static Path getOlderRemoteAppLogDir(ApplicationId appId,
      String user, Path remoteRootLogDir, String suffix) {
    return new Path(getOlderRemoteLogSuffixedDir(remoteRootLogDir, user,
         suffix), appId.toString());
  }

  public static Path getOlderRemoteAppLogDir(Configuration conf,
      ApplicationId appId, String user, Path remoteRootLogDir, String suffix)
      throws IOException {
    org.apache.hadoop.fs.Path remoteAppDir = null;
    if (user == null) {
      org.apache.hadoop.fs.Path qualifiedRemoteRootLogDir =
          FileContext.getFileContext(conf).makeQualified(remoteRootLogDir);
      FileContext fc = FileContext.getFileContext(
          qualifiedRemoteRootLogDir.toUri(), conf);
      org.apache.hadoop.fs.Path toMatch = LogAggregationUtils
          .getOlderRemoteAppLogDir(appId, "*", remoteRootLogDir, suffix);
      FileStatus[] matching  = fc.util().globStatus(toMatch);
      if (matching == null || matching.length != 1) {
        throw new IOException("Can not find remote application directory for "
            + "the application:" + appId);
      }
      remoteAppDir = matching[0].getPath();
    } else {
      remoteAppDir = LogAggregationUtils.getOlderRemoteAppLogDir(
          appId, user, remoteRootLogDir, suffix);
    }
    return remoteAppDir;
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
    suffix = getBucketSuffix() + suffix;
    return new Path(getRemoteLogUserDir(remoteRootLogDir, user), suffix);
  }

  /**
   * Gets the older remote suffixed log dir for the user.
   * @param remoteRootLogDir the aggregated log remote root log dir
   * @param user the application owner
   * @param suffix the log dir suffix
   * @return the older remote suffixed log dir.
   */
  public static Path getOlderRemoteLogSuffixedDir(Path remoteRootLogDir,
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
   * Gets the remote log user's bucket dir.
   * @param remoteRootLogDir the aggregated log remote root log dir
   * @param user the application owner
   * @param suffix the log dir suffix
   * @param appId the application id
   * @return the remote log per user per cluster timestamp per bucket dir.
   */
  public static Path getRemoteBucketDir(Path remoteRootLogDir, String user,
      String suffix, ApplicationId appId) {
    int bucket = appId.getId() % 10000;
    String bucketDir = String.format("%04d", bucket);
    return new Path(getRemoteLogSuffixedDir(remoteRootLogDir,
       user, suffix), bucketDir);
  }

  /**
   * Check if older Application Log Directory has to be included.
   * @param conf the configuration
   * @return Is Older App Log Dir enabled?
   */
  public static boolean isOlderPathEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.
         NM_REMOTE_APP_LOG_DIR_INCLUDE_OLDER,
             YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_INCLUDE_OLDER);
  }

  /**
   * Returns the bucket suffix component of the log dir.
   * @return the bucket suffix which appended to user log dir
   */
  public static String getBucketSuffix() {
    return BUCKET_SUFFIX;
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
   * @param remoteAppLogDir the application log directory
   * @param appId the applicationId
   * @param appOwner the application owner
   * @return the iterator of available log files
   * @throws IOException if there is no log file directory
   */
  public static RemoteIterator<FileStatus> getNodeFiles(Configuration conf,
      Path remoteAppLogDir, ApplicationId appId, String appOwner)
      throws IOException {
    Path qualifiedLogDir =
        FileContext.getFileContext(conf).makeQualified(remoteAppLogDir);
    return FileContext.getFileContext(
        qualifiedLogDir.toUri(), conf).listStatus(remoteAppLogDir);
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
    RemoteIterator<FileStatus> nodeFilesCur= null;
    RemoteIterator<FileStatus> nodeFilesPrev = null;
    StringBuilder diagnosticsMsg = new StringBuilder();

    // Get Node Files from new app log dir
    try {
      Path remoteAppLogDir = getRemoteAppLogDir(conf, appId, appOwner,
          remoteRootLogDir, suffix);
      nodeFilesCur = getNodeFiles(conf, remoteAppLogDir, appId, appOwner);
    } catch (IOException ex) {
      diagnosticsMsg.append(ex.getMessage() + "\n");
    }

    // Get Node Files from old app log dir
    if (isOlderPathEnabled(conf)) {
      try {
        Path remoteAppLogDir = getOlderRemoteAppLogDir(conf, appId, appOwner,
            remoteRootLogDir, suffix);
        nodeFilesPrev = getNodeFiles(conf,
                remoteAppLogDir, appId, appOwner);
      } catch (IOException ex) {
        diagnosticsMsg.append(ex.getMessage() + "\n");
      }

      // Return older files if new app log dir does not exist
      if (nodeFilesCur == null) {
        return nodeFilesPrev;
      } else if (nodeFilesPrev != null) {
        // Return both new and old node files combined
        RemoteIterator<FileStatus> curDir = nodeFilesCur;
        RemoteIterator<FileStatus> prevDir = nodeFilesPrev;
        RemoteIterator<FileStatus> nodeFilesCombined = new
            RemoteIterator<FileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
              return prevDir.hasNext() || curDir.hasNext();
            }

            @Override
            public FileStatus next() throws IOException {
              return prevDir.hasNext() ? prevDir.next() : curDir.next();
            }
        };
        return nodeFilesCombined;
      }
    }

    // Error reading from or new app log dir does not exist
    if (nodeFilesCur == null) {
      throw new IOException(diagnosticsMsg.toString());
    }
    return nodeFilesCur;
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
    StringBuilder diagnosticsMsg = new StringBuilder();
    List<FileStatus> nodeFiles = new ArrayList<>();

    // Get Node Files from new app log dir
    try {
      Path remoteAppLogDir = getRemoteAppLogDir(conf, appId, appOwner,
          remoteRootLogDir, suffix);
      Path qualifiedLogDir =
          FileContext.getFileContext(conf).makeQualified(remoteAppLogDir);
      nodeFiles.addAll(Arrays.asList(FileContext.getFileContext(
          qualifiedLogDir.toUri(), conf).util().listStatus(remoteAppLogDir)));
    } catch (IOException ex) {
      diagnosticsMsg.append(ex.getMessage() + "\n");
    }

    // Get Node Files from old app log dir
    if (isOlderPathEnabled(conf)) {
      try {
        Path remoteAppLogDir = getOlderRemoteAppLogDir(conf, appId, appOwner,
            remoteRootLogDir, suffix);
        Path qualifiedLogDir = FileContext.getFileContext(conf).
            makeQualified(remoteAppLogDir);
        nodeFiles.addAll(Arrays.asList(FileContext.getFileContext(
            qualifiedLogDir.toUri(), conf).util().listStatus(remoteAppLogDir)));
      } catch (IOException ex) {
        diagnosticsMsg.append(ex.getMessage() + "\n");
      }
    }

    // Error reading from or new app log dir does not exist
    if (nodeFiles.isEmpty()) {
      throw new IOException(diagnosticsMsg.toString());
    }
    return nodeFiles;
  }

}
