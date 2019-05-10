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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineConfiguration;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.utils.ZipUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Contains methods to perform file system operations. Almost all of the methods
 * are regular non-static methods as the operations are performed with the help
 * of a {@link RemoteDirectoryManager} instance passed in as a constructor
 * dependency. Please note that some operations require to read config settings
 * as well, so that we have Submarine and YARN config objects as dependencies as
 * well.
 */
public class FileSystemOperations {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemOperations.class);
  private final Configuration submarineConfig;
  private final Configuration yarnConfig;

  private Set<Path> uploadedFiles = new HashSet<>();
  private RemoteDirectoryManager remoteDirectoryManager;

  public FileSystemOperations(ClientContext clientContext) {
    this.remoteDirectoryManager = clientContext.getRemoteDirectoryManager();
    this.submarineConfig = clientContext.getSubmarineConfig();
    this.yarnConfig = clientContext.getYarnConfig();
  }

  /**
   * May download a remote uri(file/dir) and zip.
   * Skip download if local dir
   * Remote uri can be a local dir(won't download)
   * or remote HDFS dir, s3 dir/file .etc
   * */
  public String downloadAndZip(String remoteDir, String zipFileName,
      boolean doZip)
      throws IOException {
    //Append original modification time and size to zip file name
    String suffix;
    String srcDir = remoteDir;
    String zipDirPath =
        System.getProperty("java.io.tmpdir") + "/" + zipFileName;
    boolean needDeleteTempDir = false;
    if (remoteDirectoryManager.isRemote(remoteDir)) {
      //Append original modification time and size to zip file name
      FileStatus status =
          remoteDirectoryManager.getRemoteFileStatus(new Path(remoteDir));
      suffix = "_" + status.getModificationTime()
          + "-" + remoteDirectoryManager.getRemoteFileSize(remoteDir);
      // Download them to temp dir
      boolean downloaded =
          remoteDirectoryManager.copyRemoteToLocal(remoteDir, zipDirPath);
      if (!downloaded) {
        throw new IOException("Failed to download files from "
            + remoteDir);
      }
      LOG.info("Downloaded remote: {} to local: {}", remoteDir, zipDirPath);
      srcDir = zipDirPath;
      needDeleteTempDir = true;
    } else {
      File localDir = new File(remoteDir);
      suffix = "_" + localDir.lastModified()
          + "-" + localDir.length();
    }
    if (!doZip) {
      return srcDir;
    }
    // zip a local dir
    String zipFileUri =
        ZipUtilities.zipDir(srcDir, zipDirPath + suffix + ".zip");
    // delete downloaded temp dir
    if (needDeleteTempDir) {
      deleteFiles(srcDir);
    }
    return zipFileUri;
  }

  public void deleteFiles(String localUri) {
    boolean success = FileUtil.fullyDelete(new File(localUri));
    if (!success) {
      LOG.warn("Failed to delete {}", localUri);
    }
    LOG.info("Deleted {}", localUri);
  }

  @VisibleForTesting
  public void uploadToRemoteFileAndLocalizeToContainerWorkDir(Path stagingDir,
      String fileToUpload, String destFilename, Component comp)
      throws IOException {
    Path uploadedFilePath = uploadToRemoteFile(stagingDir, fileToUpload);
    locateRemoteFileToContainerWorkDir(destFilename, comp, uploadedFilePath);
  }

  private void locateRemoteFileToContainerWorkDir(String destFilename,
      Component comp, Path uploadedFilePath)
      throws IOException {
    FileSystem fs = FileSystem.get(yarnConfig);

    FileStatus fileStatus = fs.getFileStatus(uploadedFilePath);
    LOG.info("Uploaded file path = " + fileStatus.getPath());

    // Set it to component's files list
    comp.getConfiguration().getFiles().add(new ConfigFile().srcFile(
        fileStatus.getPath().toUri().toString()).destFile(destFilename)
        .type(ConfigFile.TypeEnum.STATIC));
  }

  public Path uploadToRemoteFile(Path stagingDir, String fileToUpload) throws
      IOException {
    FileSystem fs = remoteDirectoryManager.getDefaultFileSystem();

    // Upload to remote FS under staging area
    File localFile = new File(fileToUpload);
    if (!localFile.exists()) {
      throw new FileNotFoundException(
          "Trying to upload file=" + localFile.getAbsolutePath()
              + " to remote, but couldn't find local file.");
    }
    String filename = new File(fileToUpload).getName();

    Path uploadedFilePath = new Path(stagingDir, filename);
    if (!uploadedFiles.contains(uploadedFilePath)) {
      if (SubmarineLogs.isVerbose()) {
        LOG.info("Copying local file=" + fileToUpload + " to remote="
            + uploadedFilePath);
      }
      fs.copyFromLocalFile(new Path(fileToUpload), uploadedFilePath);
      uploadedFiles.add(uploadedFilePath);
    }
    return uploadedFilePath;
  }

  public void validFileSize(String uri) throws IOException {
    long actualSizeByte;
    String locationType = "Local";
    if (remoteDirectoryManager.isRemote(uri)) {
      actualSizeByte = remoteDirectoryManager.getRemoteFileSize(uri);
      locationType = "Remote";
    } else {
      actualSizeByte = FileUtil.getDU(new File(uri));
    }
    long maxFileSizeMB = submarineConfig
        .getLong(SubmarineConfiguration.LOCALIZATION_MAX_ALLOWED_FILE_SIZE_MB,
            SubmarineConfiguration.DEFAULT_MAX_ALLOWED_REMOTE_URI_SIZE_MB);
    LOG.info("{} fie/dir: {}, size(Byte):{},"
            + " Allowed max file/dir size: {}",
        locationType, uri, actualSizeByte, maxFileSizeMB * 1024 * 1024);

    if (actualSizeByte > maxFileSizeMB * 1024 * 1024) {
      throw new IOException(uri + " size(Byte): "
          + actualSizeByte + " exceeds configured max size:"
          + maxFileSizeMB * 1024 * 1024);
    }
  }

  public void setPermission(Path destPath, FsPermission permission) throws
      IOException {
    FileSystem fs = FileSystem.get(yarnConfig);
    fs.setPermission(destPath, new FsPermission(permission));
  }

  public static boolean needHdfs(List<String> stringsToCheck) {
    for (String content : stringsToCheck) {
      if (content != null && content.contains("hdfs://")) {
        return true;
      }
    }
    return false;
  }

  public static boolean needHdfs(String content) {
    return content != null && content.contains("hdfs://");
  }
}
