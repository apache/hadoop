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

package org.apache.hadoop.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to manage job submission files.
 */
@InterfaceAudience.Private
public class JobSubmissionFiles {

  private final static Logger LOG =
      LoggerFactory.getLogger(JobSubmissionFiles.class);

  // job submission directory is private!
  final public static FsPermission JOB_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx------

  final public static FsPermission JOB_DIR_PERMISSION_WORLD_EXCUTABLE =
      FsPermission.createImmutable((short) 0701); // rwx-----x
  //job files are world-wide readable and owner writable
  final public static FsPermission JOB_FILE_PERMISSION = 
      FsPermission.createImmutable((short) 0644); // rw-r--r--

  public static Path getJobSplitFile(Path jobSubmissionDir) {
    return new Path(jobSubmissionDir, "job.split");
  }

  public static Path getJobSplitMetaFile(Path jobSubmissionDir) {
    return new Path(jobSubmissionDir, "job.splitmetainfo");
  }
  
  /**
   * Get the job conf path.
   */
  public static Path getJobConfPath(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "job.xml");
  }

  /**
   * Get the job distributed cache files path.
   * @param jobSubmitDir
   */
  public static Path getJobDistCacheFiles(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "files");
  }

  /**
   * Get the job distributed cache path for log4j properties.
   * @param jobSubmitDir
   */
  public static Path getJobLog4jFile(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "log4j");
  }
  /**
   * Get the job distributed cache archives path.
   * @param jobSubmitDir 
   */
  public static Path getJobDistCacheArchives(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "archives");
  }
  /**
   * Get the job distributed cache libjars path.
   * @param jobSubmitDir 
   */
  public static Path getJobDistCacheLibjars(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "libjars");
  }

  public static List<Path> getJobDistCacheLibjarsPaths(Path jobSubmitDir) {
    List<Path> pathList = new ArrayList<>();
    for (MRResourceVisibility visibility : MRResourceVisibility.values()) {
      pathList.add(getJobDistCacheLibjarsPath(jobSubmitDir, visibility));
    }
    return pathList;
  }

  public static Path getJobDistCacheLibjarsPath(
      Path jobSubmitDir, MRResourceVisibility visibility) {
    return new Path(
        jobSubmitDir, visibility.getDirName() + "/"
        + MRResourceType.LIBJAR.getResourceParentDirName());
  }

  /**
   * Initializes the staging directory and returns the path. It also
   * keeps track of all necessary ownership and permissions
   * @param cluster
   * @param conf
   */
  public static Path getStagingDir(Cluster cluster, Configuration conf)
      throws IOException, InterruptedException {
    UserGroupInformation user = UserGroupInformation.getLoginUser();
    return getStagingDir(cluster, conf, user);
  }

  /**
   * Initializes the staging directory and returns the path. It also
   * keeps track of all necessary ownership and permissions.
   * It is kept for unit testing.
   *
   * @param cluster  Information about the map/reduce cluster
   * @param conf     Configuration object
   * @param realUser UserGroupInformation of login user
   * @return staging dir path object
   * @throws IOException          when ownership of staging area directory does
   *                              not match the login user or current user.
   * @throws InterruptedException when getting the staging area directory path
   */
  @VisibleForTesting
  public static Path getStagingDir(Cluster cluster, Configuration conf,
      UserGroupInformation realUser) throws IOException, InterruptedException {
    Path stagingArea = cluster.getStagingAreaDir();
    FileSystem fs = stagingArea.getFileSystem(conf);
    UserGroupInformation currentUser = realUser.getCurrentUser();
    FsPermission stagingDirPermission = getStagingDirPermission(conf);
    try {
      FileStatus fsStatus = fs.getFileStatus(stagingArea);
      String fileOwner = fsStatus.getOwner();
      if (!(fileOwner.equals(currentUser.getShortUserName()) || fileOwner
          .equalsIgnoreCase(currentUser.getUserName()) || fileOwner
          .equals(realUser.getShortUserName()) || fileOwner
          .equalsIgnoreCase(realUser.getUserName()))) {
        String errorMessage = "The ownership on the staging directory " +
            stagingArea + " is not as expected. " +
            "It is owned by " + fileOwner + ". The directory must " +
            "be owned by the submitter " + currentUser.getShortUserName()
            + " or " + currentUser.getUserName();
        if (!realUser.getUserName().equals(currentUser.getUserName())) {
          throw new IOException(
              errorMessage + " or " + realUser.getShortUserName() + " or "
                  + realUser.getUserName());
        } else {
          throw new IOException(errorMessage);
        }
      }
      if (!fsStatus.getPermission().equals(stagingDirPermission)) {
        LOG.info("Permissions on staging directory " + stagingArea + " are " +
            "incorrect: " + fsStatus.getPermission() + ". Fixing permissions " +
            "to correct value " + stagingDirPermission);
        fs.setPermission(stagingArea, stagingDirPermission);
      }
    } catch (FileNotFoundException e) {
      fs.mkdirs(stagingArea, new FsPermission(stagingDirPermission));
    }
    return stagingArea;
  }

  public static FsPermission getStagingDirPermission(Configuration conf) {
    String permStr = conf.get(MRJobConfig.MR_JOB_STAGING_DIR_PERMISSION);
    if (permStr == null) {
      return FsPermission.createImmutable(
          MRJobConfig.DEFAULT_MR_JOB_STAGING_DIR_PERMISSION);
    }
    return FsPermission.createImmutable(Short.decode(permStr));
  }
}
