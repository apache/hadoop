/*
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

package org.apache.hadoop.yarn.service.utils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.service.conf.SliderExitCodes;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.exceptions.BadClusterStateException;
import org.apache.hadoop.yarn.service.exceptions.ErrorStrings;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CoreFileSystem {
  private static final Logger
    log = LoggerFactory.getLogger(CoreFileSystem.class);

  private static final String UTF_8 = "UTF-8";

  protected final FileSystem fileSystem;
  protected final Configuration configuration;

  public CoreFileSystem(FileSystem fileSystem, Configuration configuration) {
    Preconditions.checkNotNull(fileSystem,
                               "Cannot create a CoreFileSystem with a null FileSystem");
    Preconditions.checkNotNull(configuration,
                               "Cannot create a CoreFileSystem with a null Configuration");
    this.fileSystem = fileSystem;
    this.configuration = configuration;
  }

  public CoreFileSystem(Configuration configuration) throws IOException {
    Preconditions.checkNotNull(configuration,
                               "Cannot create a CoreFileSystem with a null Configuration");
    this.fileSystem = FileSystem.get(configuration);
    this.configuration = configuration;
  }
  
  /**
   * Get the temp path for this cluster
   * @param clustername name of the cluster
   * @return path for temp files (is not purged)
   */
  public Path getTempPathForCluster(String clustername) {
    Path clusterDir = buildClusterDirPath(clustername);
    return new Path(clusterDir, YarnServiceConstants.TMP_DIR_PREFIX);
  }

  /**
   * Returns the underlying FileSystem for this object.
   *
   * @return filesystem
   */
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
      new StringBuilder("CoreFileSystem{");
    sb.append("fileSystem=").append(fileSystem.getUri());
    sb.append('}');
    return sb.toString();
  }

  /**
   * Build up the path string for a cluster instance -no attempt to
   * create the directory is made
   *
   * @param clustername name of the cluster
   * @return the path for persistent data
   */
  public Path buildClusterDirPath(String clustername) {
    Preconditions.checkNotNull(clustername);
    Path path = getBaseApplicationPath();
    return new Path(path, YarnServiceConstants.SERVICES_DIRECTORY + "/"
        + clustername);
  }

  /**
   * Build up the upgrade path string for a cluster. No attempt to
   * create the directory is made.
   *
   * @param clusterName name of the cluster
   * @param version version of the cluster
   * @return the upgrade path to the cluster
   */
  public Path buildClusterUpgradeDirPath(String clusterName, String version) {
    Preconditions.checkNotNull(clusterName);
    Preconditions.checkNotNull(version);
    return new Path(buildClusterDirPath(clusterName),
        YarnServiceConstants.UPGRADE_DIR + "/" + version);
  }

  /**
   * Delete the upgrade cluster directory.
   * @param clusterName name of the cluster
   * @param version     version of the cluster
   * @throws IOException
   */
  public void deleteClusterUpgradeDir(String clusterName, String version)
      throws IOException {
    Preconditions.checkNotNull(clusterName);
    Preconditions.checkNotNull(version);
    Path upgradeCluster = buildClusterUpgradeDirPath(clusterName, version);
    fileSystem.delete(upgradeCluster, true);
  }
  /**
   * Build up the path string for keytab install location -no attempt to
   * create the directory is made
   *
   * @return the path for keytab
   */
  public Path buildKeytabInstallationDirPath(String keytabFolder) {
    Preconditions.checkNotNull(keytabFolder);
    Path path = getBaseApplicationPath();
    return new Path(path, YarnServiceConstants.KEYTAB_DIR + "/" + keytabFolder);
  }

  /**
   * Build up the path string for keytab install location -no attempt to
   * create the directory is made
   *
   * @return the path for keytab installation location
   */
  public Path buildKeytabPath(String keytabDir, String keytabName, String clusterName) {
    Path homePath = getHomeDirectory();
    Path baseKeytabDir;
    if (keytabDir != null) {
      baseKeytabDir = new Path(homePath, keytabDir);
    } else {
      baseKeytabDir = new Path(buildClusterDirPath(clusterName),
                               YarnServiceConstants.KEYTAB_DIR);
    }
    return keytabName == null ? baseKeytabDir :
        new Path(baseKeytabDir, keytabName);
  }

  /**
   * Build up the path string for resource install location -no attempt to
   * create the directory is made
   *
   * @return the path for resource
   */
  public Path buildResourcePath(String resourceFolder) {
    Preconditions.checkNotNull(resourceFolder);
    Path path = getBaseApplicationPath();
    return new Path(path, YarnServiceConstants.RESOURCE_DIR + "/" + resourceFolder);
  }

  /**
   * Build up the path string for resource install location -no attempt to
   * create the directory is made
   *
   * @return the path for resource
   */
  public Path buildResourcePath(String dirName, String fileName) {
    Preconditions.checkNotNull(dirName);
    Preconditions.checkNotNull(fileName);
    Path path = getBaseApplicationPath();
    return new Path(path, YarnServiceConstants.RESOURCE_DIR + "/" + dirName + "/" + fileName);
  }

  /**
   * Create a directory with the given permissions.
   *
   * @param dir          directory
   * @param clusterPerms cluster permissions
   * @throws IOException  IO problem
   * @throws BadClusterStateException any cluster state problem
   */
  @SuppressWarnings("deprecation")
  public void createWithPermissions(Path dir, FsPermission clusterPerms) throws
          IOException,
          BadClusterStateException {
    if (fileSystem.isFile(dir)) {
      // HADOOP-9361 shows some filesystems don't correctly fail here
      throw new BadClusterStateException(
              "Cannot create a directory over a file %s", dir);
    }
    log.debug("mkdir {} with perms {}", dir, clusterPerms);
    //no mask whatoever
    fileSystem.getConf().set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "000");
    fileSystem.mkdirs(dir, clusterPerms);
    //and force set it anyway just to make sure
    fileSystem.setPermission(dir, clusterPerms);
  }

  /**
   * Verify that the cluster directory is not present
   *
   * @param clustername      name of the cluster
   * @param clusterDirectory actual directory to look for
   * @throws IOException trouble with FS
   * @throws SliderException If the directory exists
   */
  public void verifyClusterDirectoryNonexistent(String clustername,
                                                Path clusterDirectory)
      throws IOException, SliderException {
    if (fileSystem.exists(clusterDirectory)) {
      throw new SliderException(SliderExitCodes.EXIT_INSTANCE_EXISTS,
              ErrorStrings.PRINTF_E_INSTANCE_ALREADY_EXISTS, clustername,
              clusterDirectory);
    }
  }
  /**
   * Verify that the given directory is not present
   *
   * @param clusterDirectory actual directory to look for
   * @throws IOException    trouble with FS
   * @throws SliderException If the directory exists
   */
  public void verifyDirectoryNonexistent(Path clusterDirectory) throws
          IOException,
      SliderException {
    if (fileSystem.exists(clusterDirectory)) {
      
      log.error("Dir {} exists: {}",
                clusterDirectory,
                listFSDir(clusterDirectory));
      throw new SliderException(SliderExitCodes.EXIT_INSTANCE_EXISTS,
              ErrorStrings.PRINTF_E_INSTANCE_DIR_ALREADY_EXISTS,
              clusterDirectory);
    }
  }

  /**
   * Verify that a user has write access to a directory.
   * It does this by creating then deleting a temp file
   *
   * @param dirPath actual directory to look for
   * @throws FileNotFoundException file not found
   * @throws IOException  trouble with FS
   * @throws BadClusterStateException if the directory is not writeable
   */
  public void verifyDirectoryWriteAccess(Path dirPath) throws IOException,
      SliderException {
    verifyPathExists(dirPath);
    Path tempFile = new Path(dirPath, "tmp-file-for-checks");
    try {
      FSDataOutputStream out ;
      out = fileSystem.create(tempFile, true);
      IOUtils.closeStream(out);
      fileSystem.delete(tempFile, false);
    } catch (IOException e) {
      log.warn("Failed to create file {}: {}", tempFile, e);
      throw new BadClusterStateException(e,
              "Unable to write to directory %s : %s", dirPath, e.toString());
    }
  }

  /**
   * Verify that a path exists
   * @param path path to check
   * @throws FileNotFoundException file not found
   * @throws IOException  trouble with FS
   */
  public void verifyPathExists(Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      throw new FileNotFoundException(path.toString());
    }
  }

  /**
   * Verify that a path exists
   * @param path path to check
   * @throws FileNotFoundException file not found or is not a file
   * @throws IOException  trouble with FS
   */
  public void verifyFileExists(Path path) throws IOException {
    FileStatus status = fileSystem.getFileStatus(path);

    if (!status.isFile()) {
      throw new FileNotFoundException("Not a file: " + path.toString());
    }
  }

  /**
   * Given a path, check if it exists and is a file
   * 
   * @param path
   *          absolute path to the file to check
   * @return true if and only if path exists and is a file, false for all other
   *          reasons including if file check throws IOException
   */
  public boolean isFile(Path path) {
    if (path == null) {
      return false;
    }
    boolean isFile = false;
    try {
      FileStatus status = fileSystem.getFileStatus(path);
      if (status.isFile()) {
        isFile = true;
      }
    } catch (IOException e) {
      // ignore, isFile is already set to false
    }
    return isFile;
  }

  /**
   * Get the base path
   *
   * @return the base path optionally configured by 
   * {@link YarnServiceConf#YARN_SERVICE_BASE_PATH}
   */
  public Path getBaseApplicationPath() {
    String configuredBasePath = configuration
        .get(YarnServiceConf.YARN_SERVICE_BASE_PATH,
            getHomeDirectory() + "/" + YarnServiceConstants.SERVICE_BASE_DIRECTORY);
    return new Path(configuredBasePath);
  }

  /**
   * Get service dependency absolute filepath in HDFS used for application
   * submission.
   * 
   * @return the absolute path to service dependency tarball in HDFS
   */
  public Path getDependencyTarGzip() {
    Path dependencyLibTarGzip = null;
    String configuredDependencyTarballPath = configuration
        .get(YarnServiceConf.DEPENDENCY_TARBALL_PATH);
    if (configuredDependencyTarballPath != null) {
      dependencyLibTarGzip = new Path(configuredDependencyTarballPath);
    }
    if (dependencyLibTarGzip == null) {
      dependencyLibTarGzip = new Path(String.format(YarnServiceConstants
          .DEPENDENCY_DIR, VersionInfo.getVersion()),
          YarnServiceConstants.DEPENDENCY_TAR_GZ_FILE_NAME
              + YarnServiceConstants.DEPENDENCY_TAR_GZ_FILE_EXT);
    }
    return dependencyLibTarGzip;
  }

  public Path getHomeDirectory() {
    return fileSystem.getHomeDirectory();
  }

  /**
   * Create an AM resource from the
   *
   * @param destPath     dest path in filesystem
   * @param resourceType resource type
   * @return the local resource for AM
   */
  public LocalResource createAmResource(Path destPath, LocalResourceType resourceType) throws IOException {
    FileStatus destStatus = fileSystem.getFileStatus(destPath);
    LocalResource amResource = Records.newRecord(LocalResource.class);
    amResource.setType(resourceType);
    // Set visibility of the resource
    // Setting to most private option
    amResource.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    amResource.setResource(
        URL.fromPath(fileSystem.resolvePath(destStatus.getPath())));
    // Set timestamp and length of file so that the framework
    // can do basic sanity checks for the local resource
    // after it has been copied over to ensure it is the same
    // resource the client intended to use with the service
    amResource.setTimestamp(destStatus.getModificationTime());
    amResource.setSize(destStatus.getLen());
    return amResource;
  }

  /**
   * Register all files under a fs path as a directory to push out
   *
   * @param srcDir          src dir
   * @param destRelativeDir dest dir (no trailing /)
   * @return the map of entries
   */
  public Map<String, LocalResource> submitDirectory(Path srcDir, String destRelativeDir) throws IOException {
    //now register each of the files in the directory to be
    //copied to the destination
    FileStatus[] fileset = fileSystem.listStatus(srcDir);
    Map<String, LocalResource> localResources =
            new HashMap<String, LocalResource>(fileset.length);
    for (FileStatus entry : fileset) {

      LocalResource resource = createAmResource(entry.getPath(),
              LocalResourceType.FILE);
      String relativePath = destRelativeDir + "/" + entry.getPath().getName();
      localResources.put(relativePath, resource);
    }
    return localResources;
  }

  /**
   * Submit a JAR containing a specific class, returning
   * the resource to be mapped in
   *
   * @param clazz   class to look for
   * @param subdir  subdirectory (expected to end in a "/")
   * @param jarName <i>At the destination</i>
   * @return the local resource ref
   * @throws IOException trouble copying to HDFS
   */
  public LocalResource submitJarWithClass(Class clazz, Path tempPath, String subdir, String jarName)
          throws IOException, SliderException {
    File localFile = ServiceUtils.findContainingJarOrFail(clazz);
    return submitFile(localFile, tempPath, subdir, jarName);
  }

  /**
   * Submit a local file to the filesystem references by the instance's cluster
   * filesystem
   *
   * @param localFile    filename
   * @param subdir       subdirectory (expected to end in a "/")
   * @param destFileName destination filename
   * @return the local resource ref
   * @throws IOException trouble copying to HDFS
   */
  public LocalResource submitFile(File localFile, Path tempPath, String subdir, String destFileName)
      throws IOException {
    Path src = new Path(localFile.toString());
    Path subdirPath = new Path(tempPath, subdir);
    fileSystem.mkdirs(subdirPath);
    Path destPath = new Path(subdirPath, destFileName);
    log.debug("Copying {} (size={} bytes) to {}", localFile, localFile.length(), destPath);

    fileSystem.copyFromLocalFile(false, true, src, destPath);

    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need the jar file to be untarred for now
    return createAmResource(destPath, LocalResourceType.FILE);
  }

  /**
   * Submit the AM tar.gz resource referenced by the instance's cluster
   * filesystem. Also, update the providerResources object with the new
   * resource.
   * 
   * @param providerResources
   *          the provider resource map to be updated
   * @throws IOException
   *           trouble copying to HDFS
   */
  public void submitTarGzipAndUpdate(
      Map<String, LocalResource> providerResources) throws IOException,
      BadClusterStateException {
    Path dependencyLibTarGzip = getDependencyTarGzip();
    LocalResource lc = createAmResource(dependencyLibTarGzip,
        LocalResourceType.ARCHIVE);
    providerResources.put(YarnServiceConstants.DEPENDENCY_LOCALIZED_DIR_LINK, lc);
  }

  public void copyLocalFileToHdfs(File localPath,
      Path destPath, FsPermission fp)
      throws IOException {
    if (localPath == null || destPath == null) {
      throw new IOException("Either localPath or destPath is null");
    }
    fileSystem.getConf().set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,
        "000");
    fileSystem.mkdirs(destPath.getParent(), fp);
    log.info("Copying file {} to {}", localPath.toURI(), destPath);
    
    fileSystem.copyFromLocalFile(false, true, new Path(localPath.getPath()),
        destPath);
    // set file permissions of the destPath
    fileSystem.setPermission(destPath, fp);
  }

  public void copyHdfsFileToLocal(Path hdfsPath, File destFile)
      throws IOException {
    if (hdfsPath == null || destFile == null) {
      throw new IOException("Either hdfsPath or destPath is null");
    }
    log.info("Copying file {} to {}", hdfsPath.toUri(), destFile.toURI());

    Path destPath = new Path(destFile.getPath());
    fileSystem.copyToLocalFile(hdfsPath, destPath);
  }

  /**
   * list entries in a filesystem directory
   *
   * @param path directory
   * @return a listing, one to a line
   * @throws IOException
   */
  public String listFSDir(Path path) throws IOException {
    FileStatus[] stats = fileSystem.listStatus(path);
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.getPath().toString())
              .append("\t")
              .append(stat.getLen())
              .append("\n");
    }
    return builder.toString();
  }

  public String cat(Path path) throws IOException {
    FileStatus status = fileSystem.getFileStatus(path);
    byte[] b = new byte[(int) status.getLen()];
    FSDataInputStream in = null;
    try {
      in = fileSystem.open(path);
      int count = in.read(b);
      return new String(b, 0, count, UTF_8);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
