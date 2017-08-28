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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;
import org.apache.hadoop.yarn.server.sharedcache.SharedCacheUtil;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksum;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksumFactory;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.FSDownload;

import com.google.common.annotations.VisibleForTesting;

/**
 * The callable class that handles the actual upload to the shared cache.
 */
class SharedCacheUploader implements Callable<Boolean> {
  // rwxr-xr-x
  static final FsPermission DIRECTORY_PERMISSION =
      new FsPermission((short)00755);
  // r-xr-xr-x
  static final FsPermission FILE_PERMISSION =
      new FsPermission((short)00555);

  private static final Logger LOG =
       LoggerFactory.getLogger(SharedCacheUploader.class);

  private final LocalResource resource;
  private final Path localPath;
  private final String user;
  private final Configuration conf;
  private final SCMUploaderProtocol scmClient;
  private final FileSystem fs;
  private final FileSystem localFs;
  private final String sharedCacheRootDir;
  private final int nestedLevel;
  private final SharedCacheChecksum checksum;
  private final RecordFactory recordFactory;

  public SharedCacheUploader(LocalResource resource, Path localPath,
      String user, Configuration conf, SCMUploaderProtocol scmClient)
          throws IOException {
    this(resource, localPath, user, conf, scmClient,
        FileSystem.get(conf), localPath.getFileSystem(conf));
  }

  /**
   * @param resource the local resource that contains the original remote path
   * @param localPath the path in the local filesystem where the resource is
   * localized
   * @param fs the filesystem of the shared cache
   * @param localFs the local filesystem
   */
  public SharedCacheUploader(LocalResource resource, Path localPath,
      String user, Configuration conf, SCMUploaderProtocol scmClient,
      FileSystem fs, FileSystem localFs) {
    this.resource = resource;
    this.localPath = localPath;
    this.user = user;
    this.conf = conf;
    this.scmClient = scmClient;
    this.fs = fs;
    this.sharedCacheRootDir =
        conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
            YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);
    this.nestedLevel = SharedCacheUtil.getCacheDepth(conf);
    this.checksum = SharedCacheChecksumFactory.getChecksum(conf);
    this.localFs = localFs;
    this.recordFactory = RecordFactoryProvider.getRecordFactory(null);
  }

  /**
   * Uploads the file under the shared cache, and notifies the shared cache
   * manager. If it is unable to upload the file because it already exists, it
   * returns false.
   */
  @Override
  public Boolean call() throws Exception {
    Path tempPath = null;
    try {
      if (!verifyAccess()) {
        LOG.warn("User " + user + " is not authorized to upload file " +
            localPath.getName());
        return false;
      }

      // first determine the actual local path that will be used for upload
      Path actualPath = getActualPath();
      // compute the checksum
      String checksumVal = computeChecksum(actualPath);
      // create the directory (if it doesn't exist)
      Path directoryPath =
          new Path(SharedCacheUtil.getCacheEntryPath(nestedLevel,
              sharedCacheRootDir, checksumVal));
      // let's not check if the directory already exists: in the vast majority
      // of the cases, the directory does not exist; as long as mkdirs does not
      // error out if it exists, we should be fine
      fs.mkdirs(directoryPath, DIRECTORY_PERMISSION);
      // create the temporary file
      tempPath = new Path(directoryPath, getTemporaryFileName(actualPath));
      if (!uploadFile(actualPath, tempPath)) {
        LOG.warn("Could not copy the file to the shared cache at " + tempPath);
        return false;
      }

      // set the permission so that it is readable but not writable
      fs.setPermission(tempPath, FILE_PERMISSION);
      // rename it to the final filename
      Path finalPath = new Path(directoryPath, actualPath.getName());
      if (!fs.rename(tempPath, finalPath)) {
        LOG.warn("The file already exists under " + finalPath +
            ". Ignoring this attempt.");
        deleteTempFile(tempPath);
        return false;
      }

      // notify the SCM
      if (!notifySharedCacheManager(checksumVal, actualPath.getName())) {
        // the shared cache manager rejected the upload (as it is likely
        // uploaded under a different name
        // clean up this file and exit
        fs.delete(finalPath, false);
        return false;
      }

      // set the replication factor
      short replication =
          (short)conf.getInt(YarnConfiguration.SHARED_CACHE_NM_UPLOADER_REPLICATION_FACTOR,
              YarnConfiguration.DEFAULT_SHARED_CACHE_NM_UPLOADER_REPLICATION_FACTOR);
      fs.setReplication(finalPath, replication);
      LOG.info("File " + actualPath.getName() +
          " was uploaded to the shared cache at " + finalPath);
      return true;
    } catch (IOException e) {
      LOG.warn("Exception while uploading the file " + localPath.getName(), e);
      // in case an exception is thrown, delete the temp file
      deleteTempFile(tempPath);
      throw e;
    }
  }

  @VisibleForTesting
  Path getActualPath() throws IOException {
    Path path = localPath;
    FileStatus status = localFs.getFileStatus(path);
    if (status != null && status.isDirectory()) {
      // for certain types of resources that get unpacked, the original file may
      // be found under the directory with the same name (see
      // FSDownload.unpack); check if the path is a directory and if so look
      // under it
      path = new Path(path, path.getName());
    }
    return path;
  }

  private void deleteTempFile(Path tempPath) {
    try {
      if (tempPath != null) {
        fs.delete(tempPath, false);
      }
    } catch (IOException ioe) {
      LOG.debug("Exception received while deleting temp files", ioe);
    }
  }

  /**
   * Checks that the (original) remote file is either owned by the user who
   * started the app or public.
   */
  @VisibleForTesting
  boolean verifyAccess() throws IOException {
    // if it is in the public cache, it's trivially OK
    if (resource.getVisibility() == LocalResourceVisibility.PUBLIC) {
      return true;
    }

    final Path remotePath;
    try {
      remotePath = resource.getResource().toPath();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid resource", e);
    }

    // get the file status of the HDFS file
    FileSystem remoteFs = remotePath.getFileSystem(conf);
    FileStatus status = remoteFs.getFileStatus(remotePath);
    // check to see if the file has been modified in any way
    if (status.getModificationTime() != resource.getTimestamp()) {
      LOG.warn("The remote file " + remotePath +
          " has changed since it's localized; will not consider it for upload");
      return false;
    }

    // check for the user ownership
    if (status.getOwner().equals(user)) {
      return true; // the user owns the file
    }
    // check if the file is publicly readable otherwise
    return fileIsPublic(remotePath, remoteFs, status);
  }

  @VisibleForTesting
  boolean fileIsPublic(final Path remotePath, FileSystem remoteFs,
      FileStatus status) throws IOException {
    return FSDownload.isPublic(remoteFs, remotePath, status, null);
  }

  /**
   * Uploads the file to the shared cache under a temporary name, and returns
   * the result.
   */
  @VisibleForTesting
  boolean uploadFile(Path sourcePath, Path tempPath) throws IOException {
    return FileUtil.copy(localFs, sourcePath, fs, tempPath, false, conf);
  }

  @VisibleForTesting
  String computeChecksum(Path path) throws IOException {
    InputStream is = localFs.open(path);
    try {
      return checksum.computeChecksum(is);
    } finally {
      try { is.close(); } catch (IOException ignore) {}
    }
  }

  private String getTemporaryFileName(Path path) {
    return path.getName() + "-" + ThreadLocalRandom.current().nextLong();
  }

  @VisibleForTesting
  boolean notifySharedCacheManager(String checksumVal, String fileName)
      throws IOException {
    try {
      SCMUploaderNotifyRequest request =
          recordFactory.newRecordInstance(SCMUploaderNotifyRequest.class);
      request.setResourceKey(checksumVal);
      request.setFilename(fileName);
      return scmClient.notify(request).getAccepted();
    } catch (YarnException e) {
      throw new IOException(e);
    } catch (UndeclaredThrowableException e) {
      // retrieve the cause of the exception and throw it as an IOException
      throw new IOException(e.getCause() == null ? e : e.getCause());
    }
  }
}
