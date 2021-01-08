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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.webapp.View.ViewContext;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;

/**
 * Base class to implement Log Aggregation File Controller.
 */
@Public
@Unstable
public abstract class LogAggregationFileController {

  private static final Logger LOG = LoggerFactory.getLogger(
      LogAggregationFileController.class);

  /*
   * Expected deployment TLD will be 1777, owner=<NMOwner>, group=<NMGroup -
   * Group to which NMOwner belongs> App dirs will be created as 770,
   * owner=<AppOwner>, group=<NMGroup>: so that the owner and <NMOwner> can
   * access / modify the files.
   * <NMGroup> should obviously be a limited access group.
   */
  /**
   * Permissions for the top level directory under which app directories will be
   * created.
   */
  protected static final FsPermission TLDIR_PERMISSIONS = FsPermission
      .createImmutable((short) 01777);

  /**
   * Permissions for the Application directory.
   */
  protected static final FsPermission APP_DIR_PERMISSIONS = FsPermission
      .createImmutable((short) 0770);

  /**
   * Umask for the log file.
   */
  protected static final FsPermission APP_LOG_FILE_UMASK = FsPermission
      .createImmutable((short) (0640 ^ 0777));

  protected Configuration conf;
  protected Path remoteRootLogDir;
  protected String remoteRootLogDirSuffix;
  protected int retentionSize;
  protected String fileControllerName;

  protected boolean fsSupportsChmod = true;

  public LogAggregationFileController() {}

  /**
   * Initialize the log file controller.
   * @param conf the Configuration
   * @param controllerName the log controller class name
   */
  public void initialize(Configuration conf, String controllerName) {
    this.conf = conf;
    int configuredRetentionSize = conf.getInt(
        YarnConfiguration.NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP,
        YarnConfiguration
            .DEFAULT_NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP);
    if (configuredRetentionSize <= 0) {
      this.retentionSize =
          YarnConfiguration
              .DEFAULT_NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP;
    } else {
      this.retentionSize = configuredRetentionSize;
    }
    this.fileControllerName = controllerName;

    extractRemoteRootLogDir();
    extractRemoteRootLogDirSuffix();

    initInternal(conf);
  }

  /**
   * Derived classes initialize themselves using this method.
   * @param conf the Configuration
   */
  protected abstract void initInternal(Configuration conf);

  /**
   * Get the remote root log directory.
   * @return the remote root log directory path
   */
  public Path getRemoteRootLogDir() {
    return this.remoteRootLogDir;
  }

  /**
   * Get the log aggregation directory suffix.
   * @return the log aggregation directory suffix
   */
  public String getRemoteRootLogDirSuffix() {
    return this.remoteRootLogDirSuffix;
  }

  /**
   * Initialize the writer.
   * @param context the {@link LogAggregationFileControllerContext}
   * @throws IOException if fails to initialize the writer
   */
  public abstract void initializeWriter(
      LogAggregationFileControllerContext context) throws IOException;

  /**
   * Close the writer.
   * @throws LogAggregationDFSException if the closing of the writer fails
   *         (for example due to HDFS quota being exceeded)
   */
  public abstract void closeWriter() throws LogAggregationDFSException;

  /**
   * Write the log content.
   * @param logKey the log key
   * @param logValue the log content
   * @throws IOException if fails to write the logs
   */
  public abstract void write(LogKey logKey, LogValue logValue)
      throws IOException;

  /**
   * Operations needed after write the log content.
   * @param record the {@link LogAggregationFileControllerContext}
   * @throws Exception if anything fails
   */
  public abstract void postWrite(LogAggregationFileControllerContext record)
      throws Exception;

  protected void closePrintStream(OutputStream out) {
    if (out != System.out) {
      IOUtils.cleanupWithLogger(LOG, out);
    }
  }

  /**
   * Output container log.
   * @param logRequest {@link ContainerLogsRequest}
   * @param os the output stream
   * @return true if we can read the aggregated logs successfully
   * @throws IOException if we can not access the log file.
   */
  public abstract boolean readAggregatedLogs(ContainerLogsRequest logRequest,
      OutputStream os) throws IOException;

  /**
   * Return a list of {@link ContainerLogMeta} for an application
   * from Remote FileSystem.
   *
   * @param logRequest {@link ContainerLogsRequest}
   * @return a list of {@link ContainerLogMeta}
   * @throws IOException if there is no available log file
   */
  public abstract List<ContainerLogMeta> readAggregatedLogsMeta(
      ContainerLogsRequest logRequest) throws IOException;

  /**
   * Render Aggregated Logs block.
   * @param html the html
   * @param context the ViewContext
   */
  public abstract void renderAggregatedLogsBlock(Block html,
      ViewContext context);

  /**
   * Returns the owner of the application.
   *
   * @param aggregatedLogPath the aggregatedLog path
   * @param appId the ApplicationId
   * @return the application owner
   * @throws IOException if we can not get the application owner
   */
  public abstract String getApplicationOwner(Path aggregatedLogPath,
      ApplicationId appId)
      throws IOException;

  /**
   * Returns ACLs for the application. An empty map is returned if no ACLs are
   * found.
   *
   * @param aggregatedLogPath the aggregatedLog path.
   * @param appId the ApplicationId
   * @return a map of the Application ACLs.
   * @throws IOException if we can not get the application acls
   */
  public abstract Map<ApplicationAccessType, String> getApplicationAcls(
      Path aggregatedLogPath, ApplicationId appId) throws IOException;

  /**
   * Sets the remoteRootLogDirSuffix class variable extracting
   * {@link YarnConfiguration#LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT}
   * from the configuration, or
   * {@link YarnConfiguration#NM_REMOTE_APP_LOG_DIR_SUFFIX} appended by the
   * FileController's name, if the former is not set.
   */
  private void extractRemoteRootLogDirSuffix() {
    String suffix = String.format(
        YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT,
        fileControllerName);
    remoteRootLogDirSuffix = conf.get(suffix);
    if (remoteRootLogDirSuffix == null
            || remoteRootLogDirSuffix.isEmpty()) {
      remoteRootLogDirSuffix = conf.get(
          YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
          YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX)
          + "-" + fileControllerName.toLowerCase();
    }
  }

  /**
   * Sets the remoteRootLogDir class variable extracting
   * {@link YarnConfiguration#LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT}
   * from the configuration or {@link YarnConfiguration#NM_REMOTE_APP_LOG_DIR},
   * if the former is not set.
   */
  private void extractRemoteRootLogDir() {
    String remoteDirStr = String.format(
        YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT,
        fileControllerName);
    String remoteDir = conf.get(remoteDirStr);
    if (remoteDir == null || remoteDir.isEmpty()) {
      remoteDir = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
          YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR);
    }
    remoteRootLogDir = new Path(remoteDir);
  }

  /**
   * Verify and create the remote log directory.
   */
  public void verifyAndCreateRemoteLogDir() {
    // Checking the existence of the TLD
    FileSystem remoteFS = null;
    try {
      remoteFS = getFileSystem(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException(
          "Unable to get Remote FileSystem instance", e);
    }
    boolean remoteExists = true;
    Path remoteRootLogDir = getRemoteRootLogDir();
    try {
      FsPermission perms =
          remoteFS.getFileStatus(remoteRootLogDir).getPermission();
      if (!perms.equals(TLDIR_PERMISSIONS)) {
        LOG.warn("Remote Root Log Dir [" + remoteRootLogDir
            + "] already exist, but with incorrect permissions. "
            + "Expected: [" + TLDIR_PERMISSIONS + "], Found: [" + perms
            + "]." + " The cluster may have problems with multiple users.");

      }
    } catch (FileNotFoundException e) {
      remoteExists = false;
    } catch (IOException e) {
      throw new YarnRuntimeException(
          "Failed to check permissions for dir ["
              + remoteRootLogDir + "]", e);
    }

    Path qualified =
        remoteRootLogDir.makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory());
    if (!remoteExists) {
      LOG.warn("Remote Root Log Dir [" + remoteRootLogDir
          + "] does not exist. Attempting to create it.");
      try {
        remoteFS.mkdirs(qualified, new FsPermission(TLDIR_PERMISSIONS));

        // Not possible to query FileSystem API to check if it supports
        // chmod, chown etc. Hence resorting to catching exceptions here.
        // Remove when FS APi is ready
        try {
          remoteFS.setPermission(qualified, new FsPermission(TLDIR_PERMISSIONS));
        } catch ( UnsupportedOperationException use) {
          LOG.info("Unable to set permissions for configured filesystem since"
              + " it does not support this", remoteFS.getScheme());
          fsSupportsChmod = false;
        }

        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        String primaryGroupName = conf.get(
            YarnConfiguration.NM_REMOTE_APP_LOG_DIR_GROUPNAME);
        if (primaryGroupName == null || primaryGroupName.isEmpty()) {
          try {
            primaryGroupName = loginUser.getPrimaryGroupName();
          } catch (IOException e) {
            LOG.warn("No primary group found. The remote root log directory" +
                    " will be created with the HDFS superuser being its " +
                    "group owner. JobHistoryServer may be unable to read " +
                    "the directory.");
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("The group of remote root log directory has been " +
                "determined by the configuration and set to " +
                primaryGroupName);
          }
        }
        // set owner on the remote directory only if the primary group exists
        if (primaryGroupName != null) {
          try {
            remoteFS.setOwner(qualified, loginUser.getShortUserName(),
                primaryGroupName);
          } catch (UnsupportedOperationException use) {
            LOG.info(
                "File System does not support setting user/group" + remoteFS
                    .getScheme(), use);
          }
        }
      } catch (IOException e) {
        throw new YarnRuntimeException("Failed to create remoteLogDir ["
            + remoteRootLogDir + "]", e);
      }
    } else{
      //Check if FS has capability to set/modify permissions
      try {
        remoteFS.setPermission(qualified, new FsPermission(TLDIR_PERMISSIONS));
      } catch (UnsupportedOperationException use) {
        LOG.info("Unable to set permissions for configured filesystem since"
            + " it does not support this", remoteFS.getScheme());
        fsSupportsChmod = false;
      } catch (IOException e) {
        LOG.warn("Failed to check if FileSystem suppports permissions on "
            + "remoteLogDir [" + remoteRootLogDir + "]", e);
      }
    }
  }

  /**
   * Create remote Application directory for log aggregation.
   * @param user the user
   * @param appId the application ID
   * @param userUgi the UGI
   */
  public void createAppDir(final String user, final ApplicationId appId,
      UserGroupInformation userUgi) {
    final Path remoteRootLogDir = getRemoteRootLogDir();
    final String remoteRootLogDirSuffix = getRemoteRootLogDirSuffix();
    try {
      userUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          try {
            // TODO: Reuse FS for user?
            FileSystem remoteFS = getFileSystem(conf);

            // Only creating directories if they are missing to avoid
            // unnecessary load on the filesystem from all of the nodes
            Path appDir = LogAggregationUtils.getRemoteAppLogDir(
                remoteRootLogDir, appId, user, remoteRootLogDirSuffix);
            Path curDir = appDir.makeQualified(remoteFS.getUri(),
                remoteFS.getWorkingDirectory());
            Path rootLogDir = remoteRootLogDir.makeQualified(remoteFS.getUri(),
                remoteFS.getWorkingDirectory());

            LinkedList<Path> pathsToCreate = new LinkedList<>();

            while (!curDir.equals(rootLogDir)) {
              if (!checkExists(remoteFS, curDir, APP_DIR_PERMISSIONS)) {
                pathsToCreate.addFirst(curDir);
                curDir = curDir.getParent();
              } else {
                break;
              }
            }

            for (Path path : pathsToCreate) {
              createDir(remoteFS, path, APP_DIR_PERMISSIONS);
            }
          } catch (IOException e) {
            LOG.error("Failed to setup application log directory for "
                + appId, e);
            throw e;
          }
          return null;
        }
      });
    } catch (Exception e) {
      if (e instanceof RemoteException) {
        throw new YarnRuntimeException(((RemoteException) e)
            .unwrapRemoteException(SecretManager.InvalidToken.class));
      }
      throw new YarnRuntimeException(e);
    }
  }

  @VisibleForTesting
  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return getRemoteRootLogDir().getFileSystem(conf);
  }

  protected void createDir(FileSystem fs, Path path, FsPermission fsPerm)
      throws IOException {
    if (fsSupportsChmod) {
      FsPermission dirPerm = new FsPermission(fsPerm);
      fs.mkdirs(path, dirPerm);
      FsPermission umask = FsPermission.getUMask(fs.getConf());
      if (!dirPerm.equals(dirPerm.applyUMask(umask))) {
        fs.setPermission(path, new FsPermission(fsPerm));
      }
    } else {
      fs.mkdirs(path);
    }
  }

  protected boolean checkExists(FileSystem fs, Path path, FsPermission fsPerm)
      throws IOException {
    boolean exists = true;
    try {
      FileStatus appDirStatus = fs.getFileStatus(path);
      if (fsSupportsChmod) {
        if (!APP_DIR_PERMISSIONS.equals(appDirStatus.getPermission())) {
          fs.setPermission(path, APP_DIR_PERMISSIONS);
        }
      }
    } catch (FileNotFoundException fnfe) {
      exists = false;
    }
    return exists;
  }

  /**
   * Get the remote aggregated log path.
   * @param appId the ApplicationId
   * @param user the Application Owner
   * @param nodeId the NodeManager Id
   * @return the remote aggregated log path
   */
  public Path getRemoteNodeLogFileForApp(ApplicationId appId, String user,
      NodeId nodeId) {
    return LogAggregationUtils.getRemoteNodeLogFileForApp(
        getRemoteRootLogDir(), appId, user, nodeId,
        getRemoteRootLogDirSuffix());
  }

  /**
   * Get the remote application directory for log aggregation.
   * @param appId the Application ID
   * @param appOwner the Application Owner
   * @return the remote application directory
   * @throws IOException if can not find the remote application directory
   */
  public Path getRemoteAppLogDir(ApplicationId appId, String appOwner)
      throws IOException {
    return LogAggregationUtils.getRemoteAppLogDir(conf, appId, appOwner,
        this.remoteRootLogDir, this.remoteRootLogDirSuffix);
  }

  /**
   * Get the older remote application directory for log aggregation.
   * @param appId the Application ID
   * @param appOwner the Application Owner
   * @return the older remote application directory
   * @throws IOException if can not find the remote application directory
   */
  public Path getOlderRemoteAppLogDir(ApplicationId appId, String appOwner)
      throws IOException {
    return LogAggregationUtils.getOlderRemoteAppLogDir(conf, appId, appOwner,
        this.remoteRootLogDir, this.remoteRootLogDirSuffix);
  }

  protected void cleanOldLogs(Path remoteNodeLogFileForApp,
      final NodeId nodeId, UserGroupInformation userUgi) {
    try {
      final FileSystem remoteFS = remoteNodeLogFileForApp.getFileSystem(conf);
      Path appDir = remoteNodeLogFileForApp.getParent().makeQualified(
          remoteFS.getUri(), remoteFS.getWorkingDirectory());
      Set<FileStatus> status =
          new HashSet<FileStatus>(Arrays.asList(remoteFS.listStatus(appDir)));

      status = status.stream().filter(
          next -> next.getPath().getName()
              .contains(LogAggregationUtils.getNodeString(nodeId))
              && !next.getPath().getName().endsWith(
              LogAggregationUtils.TMP_FILE_SUFFIX)).collect(
          Collectors.toSet());
      // Normally, we just need to delete one oldest log
      // before we upload a new log.
      // If we can not delete the older logs in this cycle,
      // we will delete them in next cycle.
      if (status.size() >= this.retentionSize) {
        // sort by the lastModificationTime ascending
        List<FileStatus> statusList = new ArrayList<FileStatus>(status);
        Collections.sort(statusList, new Comparator<FileStatus>() {
          public int compare(FileStatus s1, FileStatus s2) {
            return s1.getModificationTime() < s2.getModificationTime() ? -1
                : s1.getModificationTime() > s2.getModificationTime() ? 1 : 0;
          }
        });
        for (int i = 0; i <= statusList.size() - this.retentionSize; i++) {
          final FileStatus remove = statusList.get(i);
          try {
            userUgi.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                remoteFS.delete(remove.getPath(), false);
                return null;
              }
            });
          } catch (Exception e) {
            LOG.error("Failed to delete " + remove.getPath(), e);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to clean old logs", e);
    }
  }

  /**
   * Create the aggregated log suffix. The LogAggregationFileController
   * should call this to get the suffix and append the suffix to the end
   * of each log. This would keep the aggregated log format consistent.
   *
   * @param fileName the File Name
   * @return the aggregated log suffix String
   */
  protected String aggregatedLogSuffix(String fileName) {
    StringBuilder sb = new StringBuilder();
    String endOfFile = "End of LogType:" + fileName;
    sb.append("\n" + endOfFile + "\n")
        .append(StringUtils.repeat("*", endOfFile.length() + 50)
            + "\n\n");
    return sb.toString();
  }

  public boolean isFsSupportsChmod() {
    return fsSupportsChmod;
  }

  protected boolean belongsToAppAttempt(ApplicationAttemptId appAttemptId,
      String containerIdStr) {
    ContainerId containerId = null;
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException exc) {
      LOG.warn("Could not parse container id from aggregated log.", exc);
    }
    if (containerId != null && containerId.getApplicationAttemptId() != null) {
      return containerId.getApplicationAttemptId().equals(appAttemptId);
    }
    return false;
  }
}
