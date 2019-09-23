/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.submarine.client.cli.param.Localization;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations.needHdfs;
import static org.apache.hadoop.yarn.submarine.utils.EnvironmentUtilities.appendToEnv;

/**
 * This class holds all dependencies in order to localize dependencies
 * for containers.
 */
public class Localizer {
  private static final Logger LOG = LoggerFactory.getLogger(Localizer.class);

  private final FileSystemOperations fsOperations;
  private final RemoteDirectoryManager remoteDirectoryManager;
  private final RunJobParameters parameters;

  public Localizer(FileSystemOperations fsOperations,
      RemoteDirectoryManager remoteDirectoryManager,
      RunJobParameters parameters) {
    this.fsOperations = fsOperations;
    this.remoteDirectoryManager = remoteDirectoryManager;
    this.parameters = parameters;
  }

  /**
   * Localize dependencies for all containers.
   * If remoteUri is a local directory,
   * we'll zip it, upload to HDFS staging dir HDFS.
   * If remoteUri is directory, we'll download it, zip it and upload
   * to HDFS.
   * If localFilePath is ".", we'll use remoteUri's file/dir name
   * */
  public void handleLocalizations(Service service)
      throws IOException {
    // Handle localizations
    Path stagingDir =
        remoteDirectoryManager.getJobStagingArea(
            parameters.getName(), true);
    List<Localization> localizations = parameters.getLocalizations();
    String remoteUri;
    String containerLocalPath;

    // Check to fail fast
    for (Localization loc : localizations) {
      remoteUri = loc.getRemoteUri();
      Path resourceToLocalize = new Path(remoteUri);
      // Check if remoteUri exists
      if (remoteDirectoryManager.isRemote(remoteUri)) {
        // check if exists
        if (!remoteDirectoryManager.existsRemoteFile(resourceToLocalize)) {
          throw new FileNotFoundException(
              "File " + remoteUri + " doesn't exists.");
        }
      } else {
        // Check if exists
        File localFile = new File(remoteUri);
        if (!localFile.exists()) {
          throw new FileNotFoundException(
              "File " + remoteUri + " doesn't exists.");
        }
      }
      // check remote file size
      fsOperations.validFileSize(remoteUri);
    }
    // Start download remote if needed and upload to HDFS
    for (Localization loc : localizations) {
      remoteUri = loc.getRemoteUri();
      containerLocalPath = loc.getLocalPath();
      String srcFileStr = remoteUri;
      ConfigFile.TypeEnum destFileType = ConfigFile.TypeEnum.STATIC;
      Path resourceToLocalize = new Path(remoteUri);
      boolean needUploadToHDFS = true;


      // Special handling of remoteUri directory
      boolean needDeleteTempFile = false;
      if (remoteDirectoryManager.isDir(remoteUri)) {
        destFileType = ConfigFile.TypeEnum.ARCHIVE;
        srcFileStr = fsOperations.downloadAndZip(
            remoteUri, getLastNameFromPath(srcFileStr), true);
      } else if (remoteDirectoryManager.isRemote(remoteUri)) {
        if (!needHdfs(remoteUri)) {
          // Non HDFS remote uri. Non directory, no need to zip
          srcFileStr = fsOperations.downloadAndZip(
              remoteUri, getLastNameFromPath(srcFileStr), false);
          needDeleteTempFile = true;
        } else {
          // HDFS file, no need to upload
          needUploadToHDFS = false;
        }
      }

      // Upload file to HDFS
      if (needUploadToHDFS) {
        resourceToLocalize =
            fsOperations.uploadToRemoteFile(stagingDir, srcFileStr);
      }
      if (needDeleteTempFile) {
        fsOperations.deleteFiles(srcFileStr);
      }
      // Remove .zip from zipped dir name
      if (destFileType == ConfigFile.TypeEnum.ARCHIVE
          && srcFileStr.endsWith(".zip")) {
        // Delete local zip file
        fsOperations.deleteFiles(srcFileStr);
        int suffixIndex = srcFileStr.lastIndexOf('_');
        srcFileStr = srcFileStr.substring(0, suffixIndex);
      }
      // If provided, use the name of local uri
      if (!containerLocalPath.equals(".")
          && !containerLocalPath.equals("./")) {
        // Change the YARN localized file name to what'll used in container
        srcFileStr = getLastNameFromPath(containerLocalPath);
      }
      String localizedName = getLastNameFromPath(srcFileStr);
      LOG.info("The file/dir to be localized is {}",
          resourceToLocalize.toString());
      LOG.info("Its localized file name will be {}", localizedName);
      service.getConfiguration().getFiles().add(new ConfigFile().srcFile(
          resourceToLocalize.toUri().toString()).destFile(localizedName)
          .type(destFileType));
      // set mounts
      // if mount path is absolute, just use it.
      // if relative, no need to mount explicitly
      if (containerLocalPath.startsWith("/")) {
        String mountStr = getLastNameFromPath(srcFileStr) + ":"
            + containerLocalPath + ":" + loc.getMountPermission();
        LOG.info("Add bind-mount string {}", mountStr);
        appendToEnv(service,
            EnvironmentUtilities.ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME,
            mountStr, ",");
      }
    }
  }

  private String getLastNameFromPath(String srcFileStr) {
    return new Path(srcFileStr).getName();
  }
}
