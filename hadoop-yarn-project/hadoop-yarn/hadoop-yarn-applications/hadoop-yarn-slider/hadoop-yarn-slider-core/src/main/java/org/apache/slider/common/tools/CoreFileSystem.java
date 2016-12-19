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

package org.apache.slider.common.tools;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException;
import org.apache.slider.core.persist.Filenames;
import org.apache.slider.core.persist.InstancePaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.slider.common.SliderXmlConfKeys.CLUSTER_DIRECTORY_PERMISSIONS;
import static org.apache.slider.common.SliderXmlConfKeys.DEFAULT_CLUSTER_DIRECTORY_PERMISSIONS;

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
    this.configuration = fileSystem.getConf();
  }
  
  /**
   * Get the temp path for this cluster
   * @param clustername name of the cluster
   * @return path for temp files (is not purged)
   */
  public Path getTempPathForCluster(String clustername) {
    Path clusterDir = buildClusterDirPath(clustername);
    return new Path(clusterDir, SliderKeys.TMP_DIR_PREFIX);
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
    return new Path(path, SliderKeys.CLUSTER_DIRECTORY + "/" + clustername);
  }

  /**
   * Build up the path string for app def folder -no attempt to
   * create the directory is made
   *
   * @param clustername name of the cluster
   * @return the path for persistent data
   */
  public Path buildAppDefDirPath(String clustername) {
    Path path = buildClusterDirPath(clustername);
    return new Path(path, SliderKeys.APP_DEF_DIR);
  }

  /**
   * Build up the path string for addon folder -no attempt to
   * create the directory is made
   *
   * @param clustername name of the cluster
   * @return the path for persistent data
   */
  public Path buildAddonDirPath(String clustername, String addonId) {
    Preconditions.checkNotNull(addonId);
    Path path = buildClusterDirPath(clustername);
    return new Path(path, SliderKeys.ADDONS_DIR + "/" + addonId);
  }

  /**
   * Build up the path string for package install location -no attempt to
   * create the directory is made
   *
   * @return the path for persistent app package
   */
  public Path buildPackageDirPath(String packageName, String packageVersion) {
    Preconditions.checkNotNull(packageName);
    Path path = getBaseApplicationPath();
    path = new Path(path, SliderKeys.PACKAGE_DIRECTORY + "/" + packageName);
    if (SliderUtils.isSet(packageVersion)) {
      path = new Path(path, packageVersion);
    }
    return path;
  }

  /**
   * Build up the path string for package install location -no attempt to
   * create the directory is made
   *
   * @return the path for persistent app package
   */
  public Path buildClusterSecurityDirPath(String clusterName) {
    Preconditions.checkNotNull(clusterName);
    Path path = buildClusterDirPath(clusterName);
    return new Path(path, SliderKeys.SECURITY_DIR);
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
    return new Path(path, SliderKeys.KEYTAB_DIR + "/" + keytabFolder);
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
                               SliderKeys.KEYTAB_DIR);
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
    return new Path(path, SliderKeys.RESOURCE_DIR + "/" + resourceFolder);
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
    return new Path(path, SliderKeys.RESOURCE_DIR + "/" + dirName + "/" + fileName);
  }

  /**
   * Build up the path string for cluster resource install location -no
   * attempt to create the directory is made
   *
   * @return the path for resource
   */
  public Path buildClusterResourcePath(String clusterName, String component) {
    Preconditions.checkNotNull(clusterName);
    Path path = buildClusterDirPath(clusterName);
    return new Path(path, SliderKeys.RESOURCE_DIR + "/" + component);
  }

  /**
   * Build up the path string for cluster resource install location -no
   * attempt to create the directory is made
   *
   * @return the path for resource
   */
  public Path buildClusterResourcePath(String clusterName) {
    Preconditions.checkNotNull(clusterName);
    Path path = buildClusterDirPath(clusterName);
    return new Path(path, SliderKeys.RESOURCE_DIR);
  }

  /**
   * Create the Slider cluster path for a named cluster and all its subdirs
   * This is a directory; a mkdirs() operation is executed
   * to ensure that it is there.
   *
   * @param clustername name of the cluster
   * @return the path to the cluster directory
   * @throws java.io.IOException                      trouble
   * @throws SliderException slider-specific exceptions
   */
  public Path createClusterDirectories(String clustername, Configuration conf)
      throws IOException, SliderException {


    Path clusterDirectory = buildClusterDirPath(clustername);
    InstancePaths instancePaths = new InstancePaths(clusterDirectory);
    createClusterDirectories(instancePaths);
    return clusterDirectory;
  }
  
  /**
   * Create the Slider cluster path for a named cluster and all its subdirs
   * This is a directory; a mkdirs() operation is executed
   * to ensure that it is there.
   *
   * @param instancePaths instance paths
   * @throws IOException trouble
   * @throws SliderException slider-specific exceptions
   */
  public void createClusterDirectories(InstancePaths instancePaths) throws
      IOException, SliderException {
    Path instanceDir = instancePaths.instanceDir;

    verifyDirectoryNonexistent(instanceDir);
    FsPermission clusterPerms = getInstanceDirectoryPermissions();
    createWithPermissions(instanceDir, clusterPerms);
    createWithPermissions(instancePaths.snapshotConfPath, clusterPerms);
    createWithPermissions(instancePaths.generatedConfPath, clusterPerms);
    createWithPermissions(instancePaths.historyPath, clusterPerms);
    createWithPermissions(instancePaths.tmpPathAM, clusterPerms);

    // Data Directory
    String dataOpts =
      configuration.get(SliderXmlConfKeys.DATA_DIRECTORY_PERMISSIONS,
               SliderXmlConfKeys.DEFAULT_DATA_DIRECTORY_PERMISSIONS);
    log.debug("Setting data directory permissions to {}", dataOpts);
    createWithPermissions(instancePaths.dataPath, new FsPermission(dataOpts));

  }

  /**
   * Create a directory with the given permissions.
   *
   * @param dir          directory
   * @param clusterPerms cluster permissions
   * @throws IOException  IO problem
   * @throws BadClusterStateException any cluster state problem
   */
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
   * Get the permissions of a path
   *
   * @param path path to check
   * @return the permissions
   * @throws IOException any IO problem (including file not found)
   */
  public FsPermission getPathPermissions(Path path) throws IOException {
    FileStatus status = fileSystem.getFileStatus(path);
    return status.getPermission();
  }

  public FsPermission getInstanceDirectoryPermissions() {
    String clusterDirPermsOct =
      configuration.get(CLUSTER_DIRECTORY_PERMISSIONS,
                        DEFAULT_CLUSTER_DIRECTORY_PERMISSIONS);
    return new FsPermission(clusterDirPermsOct);
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
   * @returns true if and only if path exists and is a file, false for all other
   *          reasons including if file check throws IOException
   */
  public boolean isFile(Path path) {
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
   * Create the application-instance specific temporary directory
   * in the DFS
   *
   * @param clustername name of the cluster
   * @param subdir       application ID
   * @return the path; this directory will already have been created
   */
  public Path createAppInstanceTempPath(String clustername, String subdir)
      throws IOException {
    Path tmp = getTempPathForCluster(clustername);
    Path instancePath = new Path(tmp, subdir);
    fileSystem.mkdirs(instancePath);
    return instancePath;
  }

  /**
   * Create the application-instance specific temporary directory
   * in the DFS
   *
   * @param clustername name of the cluster
   * @return the path; this directory will already have been deleted
   */
  public Path purgeAppInstanceTempFiles(String clustername) throws
          IOException {
    Path tmp = getTempPathForCluster(clustername);
    fileSystem.delete(tmp, true);
    return tmp;
  }

  /**
   * Get the base path
   *
   * @return the base path optionally configured by 
   * {@link SliderXmlConfKeys#KEY_SLIDER_BASE_PATH}
   */
  public Path getBaseApplicationPath() {
    String configuredBasePath = configuration.get(SliderXmlConfKeys.KEY_SLIDER_BASE_PATH);
    return configuredBasePath != null ? new Path(configuredBasePath) :
           new Path(getHomeDirectory(), SliderKeys.SLIDER_BASE_DIRECTORY);
  }

  /**
   * Get slider dependency parent dir in HDFS
   * 
   * @return the parent dir path of slider.tar.gz in HDFS
   */
  public Path getDependencyPath() {
    String parentDir = (SliderUtils.isHdp()) ? SliderKeys.SLIDER_DEPENDENCY_HDP_PARENT_DIR
        + SliderKeys.SLIDER_DEPENDENCY_DIR
        : SliderKeys.SLIDER_DEPENDENCY_DIR;
    Path dependencyPath = new Path(String.format(parentDir,
        SliderUtils.getSliderVersion()));
    return dependencyPath;
  }

  /**
   * Get slider.tar.gz absolute filepath in HDFS
   * 
   * @return the absolute path to slider.tar.gz in HDFS
   */
  public Path getDependencyTarGzip() {
    Path dependencyLibAmPath = getDependencyPath();
    Path dependencyLibTarGzip = new Path(
        dependencyLibAmPath.toUri().toString(),
        SliderKeys.SLIDER_DEPENDENCY_TAR_GZ_FILE_NAME
            + SliderKeys.SLIDER_DEPENDENCY_TAR_GZ_FILE_EXT);
    return dependencyLibTarGzip;
  }

  public Path getHomeDirectory() {
    return fileSystem.getHomeDirectory();
  }

  public boolean maybeAddImagePath(Map<String, LocalResource> localResources,
                                   Path imagePath) throws IOException {
    if (imagePath != null) {
      LocalResource resource = createAmResource(imagePath,
          LocalResourceType.ARCHIVE);
      localResources.put(SliderKeys.LOCAL_TARBALL_INSTALL_SUBDIR, resource);
      return true;
    } else {
      return false;
    }
  }

  public boolean maybeAddImagePath(Map<String, LocalResource> localResources,
                                   String imagePath) throws IOException {
    
    return imagePath != null &&
           maybeAddImagePath(localResources, new Path(imagePath));
  }
  
  
  

  /**
   * Create an AM resource from the
   *
   * @param destPath     dest path in filesystem
   * @param resourceType resource type
   * @return the resource set up wih application-level visibility and the
   * timestamp & size set from the file stats.
   */
  public LocalResource createAmResource(Path destPath, LocalResourceType resourceType) throws IOException {
    FileStatus destStatus = fileSystem.getFileStatus(destPath);
    LocalResource amResource = Records.newRecord(LocalResource.class);
    amResource.setType(resourceType);
    // Set visibility of the resource
    // Setting to most private option
    amResource.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    amResource.setResource(ConverterUtils.getYarnUrlFromPath(fileSystem
        .resolvePath(destStatus.getPath())));
    // Set timestamp and length of file so that the framework
    // can do basic sanity checks for the local resource
    // after it has been copied over to ensure it is the same
    // resource the client intended to use with the application
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
    File localFile = SliderUtils.findContainingJarOrFail(clazz);
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
    providerResources.put(SliderKeys.SLIDER_DEPENDENCY_LOCALIZED_DIR_LINK, lc);
  }

  /**
   * Copy local file(s) to destination HDFS directory. If {@code localPath} is a
   * local directory then all files matching the {@code filenameFilter}
   * (optional) are copied, otherwise {@code filenameFilter} is ignored.
   * 
   * @param localPath
   *          a local file or directory path
   * @param filenameFilter
   *          if {@code localPath} is a directory then filenameFilter is used as
   *          a filter (if specified)
   * @param destDir
   *          the destination HDFS directory where the file(s) should be copied
   * @param fp
   *          file permissions of all the directories and files that will be
   *          created in this api
   * @throws IOException
   */
  public void copyLocalFilesToHdfs(File localPath,
      FilenameFilter filenameFilter, Path destDir, FsPermission fp)
      throws IOException {
    if (localPath == null || destDir == null) {
      throw new IOException("Either localPath or destDir is null");
    }
    fileSystem.getConf().set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,
        "000");
    fileSystem.mkdirs(destDir, fp);
    if (localPath.isDirectory()) {
      // copy all local files under localPath to destDir (honoring filename
      // filter if provided
      File[] localFiles = localPath.listFiles(filenameFilter);
      Path[] localFilePaths = new Path[localFiles.length];
      int i = 0;
      for (File localFile : localFiles) {
        localFilePaths[i++] = new Path(localFile.getPath());
      }
      log.info("Copying {} files from {} to {}", i, localPath.toURI(),
          destDir.toUri());
      fileSystem.copyFromLocalFile(false, true, localFilePaths, destDir);
    } else {
      log.info("Copying file {} to {}", localPath.toURI(), destDir.toUri());
      fileSystem.copyFromLocalFile(false, true, new Path(localPath.getPath()),
          destDir);
    }
    // set permissions for all the files created in the destDir
    fileSystem.setPermission(destDir, fp);
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
    log.info("Copying file {} to {}", localPath.toURI(), destPath.toUri());
    
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

  /**
   * List all application instances persisted for this user, giving the 
   * path. The instance name is the last element in the path
   * @return a possibly empty map of application instance names to paths
   */
  public Map<String, Path> listPersistentInstances() throws IOException {
    FileSystem fs = getFileSystem();
    Path path = new Path(getBaseApplicationPath(), SliderKeys.CLUSTER_DIRECTORY);
    log.debug("Looking for all persisted application at {}", path.toString());
    if (!fs.exists(path)) {
      // special case: no instances have ever been created
      return new HashMap<String, Path>(0);
    }
    FileStatus[] statuses = fs.listStatus(path);
    Map<String, Path> instances = new HashMap<String, Path>(statuses.length);

    // enum the child entries
    for (FileStatus status : statuses) {
      if (status.isDirectory()) {
        // for directories, look for an internal.json underneath
        Path child = status.getPath();
        Path internalJson = new Path(child, Filenames.INTERNAL);
        if (fs.exists(internalJson)) {
          // success => this is an instance
          instances.put(child.getName(), child);
        } else {
          log.info("Malformed cluster found at {}. It does not appear to be a valid persisted instance.",
                   child.toString());
        }
      }
    }
    return instances;
  }

  public void touch(Path path, boolean overwrite) throws IOException {
    FSDataOutputStream out = null;
    try {
      out = fileSystem.create(path, overwrite);
    } finally {
      IOUtils.closeStream(out);
    }
  }

  public void cat(Path path, boolean overwrite, String data) throws IOException {
    FSDataOutputStream out = null;
    try {
      out = fileSystem.create(path, overwrite);
      byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
      out.write(bytes);
    } finally {
      IOUtils.closeStream(out);
    }
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

  /**
   * Create a path that must exist in the cluster fs
   * @param uri uri to create
   * @return the path
   * @throws SliderException if the path does not exist
   */
  public Path createPathThatMustExist(String uri) throws
      SliderException, IOException {
    Preconditions.checkNotNull(uri);
    Path path = new Path(uri);
    verifyPathExists(path);
    return path;
  }

  /**
   * Locate an application conf json in the FS. This includes a check to verify
   * that the file is there.
   *
   * @param clustername name of the cluster
   * @return the path to the spec.
   * @throws IOException IO problems
   * @throws SliderException if the path isn't there
   */
  public Path locateInstanceDefinition(String clustername) throws IOException,
      SliderException {
    Path clusterDirectory = buildClusterDirPath(clustername);
    Path appConfPath =
            new Path(clusterDirectory, Filenames.APPCONF);
    verifyClusterSpecExists(clustername, appConfPath);
    return appConfPath;
  }

  /**
   * Verify that a cluster specification exists
   * @param clustername name of the cluster (For errors only)
   * @param clusterSpecPath cluster specification path
   * @throws IOException IO problems
   * @throws SliderException if the cluster specification is not present
   */
  public void verifyClusterSpecExists(String clustername, Path clusterSpecPath)
      throws IOException,
      SliderException {
    if (!fileSystem.isFile(clusterSpecPath)) {
      log.debug("Missing specification file {}", clusterSpecPath);
      throw UnknownApplicationInstanceException.unknownInstance(
          clustername + "\n (definition not found at " + clusterSpecPath);
    }
  }

  
}
