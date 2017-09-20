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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class JobResourceUploader {
  protected static final Log LOG = LogFactory.getLog(JobResourceUploader.class);
  private final boolean useWildcard;
  private final FileSystem jtFs;

  JobResourceUploader(FileSystem submitFs, boolean useWildcard) {
    this.jtFs = submitFs;
    this.useWildcard = useWildcard;
  }

  /**
   * Upload and configure files, libjars, jobjars, and archives pertaining to
   * the passed job.
   * 
   * @param job the job containing the files to be uploaded
   * @param submitJobDir the submission directory of the job
   * @throws IOException
   */
  public void uploadResources(Job job, Path submitJobDir) throws IOException {
    Configuration conf = job.getConfiguration();
    short replication =
        (short) conf.getInt(Job.SUBMIT_REPLICATION,
            Job.DEFAULT_SUBMIT_REPLICATION);

    if (!(conf.getBoolean(Job.USED_GENERIC_PARSER, false))) {
      LOG.warn("Hadoop command-line option parsing not performed. "
          + "Implement the Tool interface and execute your application "
          + "with ToolRunner to remedy this.");
    }

    //
    // Figure out what fs the JobTracker is using. Copy the
    // job to it, under a temporary name. This allows DFS to work,
    // and under the local fs also provides UNIX-like object loading
    // semantics. (that is, if the job file is deleted right after
    // submission, we can still run the submission to completion)
    //

    // Create a number of filenames in the JobTracker's fs namespace
    LOG.debug("default FileSystem: " + jtFs.getUri());
    if (jtFs.exists(submitJobDir)) {
      throw new IOException("Not submitting job. Job directory " + submitJobDir
          + " already exists!! This is unexpected.Please check what's there in"
          + " that directory");
    }
    submitJobDir = jtFs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission mapredSysPerms =
        new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    mkdirs(jtFs, submitJobDir, mapredSysPerms);

    if (!conf.getBoolean(MRJobConfig.MR_AM_STAGING_DIR_ERASURECODING_ENABLED,
        MRJobConfig.DEFAULT_MR_AM_STAGING_ERASURECODING_ENABLED)) {
      disableErasureCodingForPath(jtFs, submitJobDir);
    }

    Collection<String> files = conf.getStringCollection("tmpfiles");
    Collection<String> libjars = conf.getStringCollection("tmpjars");
    Collection<String> archives = conf.getStringCollection("tmparchives");
    String jobJar = job.getJar();

    Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
    checkLocalizationLimits(conf, files, libjars, archives, jobJar, statCache);

    uploadFiles(conf, files, submitJobDir, mapredSysPerms, replication);
    uploadLibJars(conf, libjars, submitJobDir, mapredSysPerms, replication);
    uploadArchives(conf, archives, submitJobDir, mapredSysPerms, replication);
    uploadJobJar(job, jobJar, submitJobDir, replication);
    addLog4jToDistributedCache(job, submitJobDir);

    // set the timestamps of the archives and files
    // set the public/private visibility of the archives and files
    ClientDistributedCacheManager.determineTimestampsAndCacheVisibilities(conf,
        statCache);
    // get DelegationToken for cached file
    ClientDistributedCacheManager.getDelegationTokens(conf,
        job.getCredentials());
  }

  @VisibleForTesting
  void uploadFiles(Configuration conf, Collection<String> files,
      Path submitJobDir, FsPermission mapredSysPerms, short submitReplication)
      throws IOException {
    Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
    if (!files.isEmpty()) {
      mkdirs(jtFs, filesDir, mapredSysPerms);
      for (String tmpFile : files) {
        URI tmpURI = null;
        try {
          tmpURI = new URI(tmpFile);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("Error parsing files argument."
              + " Argument must be a valid URI: " + tmpFile, e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = copyRemoteFiles(filesDir, tmp, conf, submitReplication);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          DistributedCache.addCacheFile(pathURI, conf);
        } catch (URISyntaxException ue) {
          // should not throw a uri exception
          throw new IOException(
              "Failed to create a URI (URISyntaxException) for the remote path "
                  + newPath + ". This was based on the files parameter: "
                  + tmpFile,
              ue);
        }
      }
    }
  }

  // Suppress warning for use of DistributedCache (it is everywhere).
  @SuppressWarnings("deprecation")
  @VisibleForTesting
  void uploadLibJars(Configuration conf, Collection<String> libjars,
      Path submitJobDir, FsPermission mapredSysPerms, short submitReplication)
      throws IOException {
    Path libjarsDir = JobSubmissionFiles.getJobDistCacheLibjars(submitJobDir);
    if (!libjars.isEmpty()) {
      mkdirs(jtFs, libjarsDir, mapredSysPerms);
      Collection<URI> libjarURIs = new LinkedList<>();
      boolean foundFragment = false;
      for (String tmpjars : libjars) {
        URI tmpURI = null;
        try {
          tmpURI = new URI(tmpjars);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("Error parsing libjars argument."
              + " Argument must be a valid URI: " + tmpjars, e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath =
            copyRemoteFiles(libjarsDir, tmp, conf, submitReplication);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          if (!foundFragment) {
            foundFragment = pathURI.getFragment() != null;
          }
          DistributedCache.addFileToClassPath(new Path(pathURI.getPath()), conf,
              jtFs, false);
          libjarURIs.add(pathURI);
        } catch (URISyntaxException ue) {
          // should not throw a uri exception
          throw new IOException(
              "Failed to create a URI (URISyntaxException) for the remote path "
                  + newPath + ". This was based on the libjar parameter: "
                  + tmpjars,
              ue);
        }
      }

      if (useWildcard && !foundFragment) {
        // Add the whole directory to the cache using a wild card
        Path libJarsDirWildcard =
            jtFs.makeQualified(new Path(libjarsDir, DistributedCache.WILDCARD));
        DistributedCache.addCacheFile(libJarsDirWildcard.toUri(), conf);
      } else {
        for (URI uri : libjarURIs) {
          DistributedCache.addCacheFile(uri, conf);
        }
      }
    }
  }

  @VisibleForTesting
  void uploadArchives(Configuration conf, Collection<String> archives,
      Path submitJobDir, FsPermission mapredSysPerms, short submitReplication)
      throws IOException {
    Path archivesDir = JobSubmissionFiles.getJobDistCacheArchives(submitJobDir);
    if (!archives.isEmpty()) {
      mkdirs(jtFs, archivesDir, mapredSysPerms);
      for (String tmpArchives : archives) {
        URI tmpURI;
        try {
          tmpURI = new URI(tmpArchives);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("Error parsing archives argument."
              + " Argument must be a valid URI: " + tmpArchives, e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath =
            copyRemoteFiles(archivesDir, tmp, conf, submitReplication);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          DistributedCache.addCacheArchive(pathURI, conf);
        } catch (URISyntaxException ue) {
          // should not throw an uri excpetion
          throw new IOException(
              "Failed to create a URI (URISyntaxException) for the remote path"
                  + newPath + ". This was based on the archive parameter: "
                  + tmpArchives,
              ue);
        }
      }
    }
  }

  @VisibleForTesting
  void uploadJobJar(Job job, String jobJar, Path submitJobDir,
      short submitReplication) throws IOException {
    if (jobJar != null) { // copy jar to JobTracker's fs
      // use jar name if job is not named.
      if ("".equals(job.getJobName())) {
        job.setJobName(new Path(jobJar).getName());
      }
      Path jobJarPath = new Path(jobJar);
      URI jobJarURI = jobJarPath.toUri();
      // If the job jar is already in a global fs,
      // we don't need to copy it from local fs
      if (jobJarURI.getScheme() == null || jobJarURI.getScheme().equals("file")) {
        copyJar(jobJarPath, JobSubmissionFiles.getJobJar(submitJobDir),
            submitReplication);
        job.setJar(JobSubmissionFiles.getJobJar(submitJobDir).toString());
      }
    } else {
      LOG.warn("No job jar file set.  User classes may not be found. "
          + "See Job or Job#setJar(String).");
    }
  }

  /**
   * Verify that the resources this job is going to localize are within the
   * localization limits.
   */
  @VisibleForTesting
  void checkLocalizationLimits(Configuration conf, Collection<String> files,
      Collection<String> libjars, Collection<String> archives, String jobJar,
      Map<URI, FileStatus> statCache) throws IOException {

    LimitChecker limitChecker = new LimitChecker(conf);
    if (!limitChecker.hasLimits()) {
      // there are no limits set, so we are done.
      return;
    }

    // Get the files and archives that are already in the distributed cache
    Collection<String> dcFiles =
        conf.getStringCollection(MRJobConfig.CACHE_FILES);
    Collection<String> dcArchives =
        conf.getStringCollection(MRJobConfig.CACHE_ARCHIVES);

    for (String uri : dcFiles) {
      explorePath(conf, stringToPath(uri), limitChecker, statCache);
    }

    for (String uri : dcArchives) {
      explorePath(conf, stringToPath(uri), limitChecker, statCache);
    }

    for (String uri : files) {
      explorePath(conf, stringToPath(uri), limitChecker, statCache);
    }

    for (String uri : libjars) {
      explorePath(conf, stringToPath(uri), limitChecker, statCache);
    }

    for (String uri : archives) {
      explorePath(conf, stringToPath(uri), limitChecker, statCache);
    }

    if (jobJar != null) {
      explorePath(conf, stringToPath(jobJar), limitChecker, statCache);
    }
  }

  /**
   * Convert a String to a Path and gracefully remove fragments/queries if they
   * exist in the String.
   */
  @VisibleForTesting
  Path stringToPath(String s) {
    try {
      URI uri = new URI(s);
      return new Path(uri.getScheme(), uri.getAuthority(), uri.getPath());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Error parsing argument." + " Argument must be a valid URI: " + s, e);
    }
  }

  @VisibleForTesting
  protected static final String MAX_RESOURCE_ERR_MSG =
      "This job has exceeded the maximum number of submitted resources";
  @VisibleForTesting
  protected static final String MAX_TOTAL_RESOURCE_MB_ERR_MSG =
      "This job has exceeded the maximum size of submitted resources";
  @VisibleForTesting
  protected static final String MAX_SINGLE_RESOURCE_MB_ERR_MSG =
      "This job has exceeded the maximum size of a single submitted resource";

  private static class LimitChecker {
    LimitChecker(Configuration conf) {
      this.maxNumOfResources =
          conf.getInt(MRJobConfig.MAX_RESOURCES,
              MRJobConfig.MAX_RESOURCES_DEFAULT);
      this.maxSizeMB =
          conf.getLong(MRJobConfig.MAX_RESOURCES_MB,
              MRJobConfig.MAX_RESOURCES_MB_DEFAULT);
      this.maxSizeOfResourceMB =
          conf.getLong(MRJobConfig.MAX_SINGLE_RESOURCE_MB,
              MRJobConfig.MAX_SINGLE_RESOURCE_MB_DEFAULT);
      this.totalConfigSizeBytes = maxSizeMB * 1024 * 1024;
      this.totalConfigSizeOfResourceBytes = maxSizeOfResourceMB * 1024 * 1024;
    }

    private long totalSizeBytes = 0;
    private int totalNumberOfResources = 0;
    private long currentMaxSizeOfFileBytes = 0;
    private final long maxSizeMB;
    private final int maxNumOfResources;
    private final long maxSizeOfResourceMB;
    private final long totalConfigSizeBytes;
    private final long totalConfigSizeOfResourceBytes;

    private boolean hasLimits() {
      return maxNumOfResources > 0 || maxSizeMB > 0 || maxSizeOfResourceMB > 0;
    }

    private void addFile(Path p, long fileSizeBytes) throws IOException {
      totalNumberOfResources++;
      totalSizeBytes += fileSizeBytes;
      if (fileSizeBytes > currentMaxSizeOfFileBytes) {
        currentMaxSizeOfFileBytes = fileSizeBytes;
      }

      if (totalConfigSizeBytes > 0 && totalSizeBytes > totalConfigSizeBytes) {
        throw new IOException(MAX_TOTAL_RESOURCE_MB_ERR_MSG + " (Max: "
            + maxSizeMB + "MB).");
      }

      if (maxNumOfResources > 0 &&
          totalNumberOfResources > maxNumOfResources) {
        throw new IOException(MAX_RESOURCE_ERR_MSG + " (Max: "
            + maxNumOfResources + ").");
      }

      if (totalConfigSizeOfResourceBytes > 0
          && currentMaxSizeOfFileBytes > totalConfigSizeOfResourceBytes) {
        throw new IOException(MAX_SINGLE_RESOURCE_MB_ERR_MSG + " (Max: "
            + maxSizeOfResourceMB + "MB, Violating resource: " + p + ").");
      }
    }
  }

  /**
   * Recursively explore the given path and enforce the limits for resource
   * localization. This method assumes that there are no symlinks in the
   * directory structure.
   */
  private void explorePath(Configuration job, Path p,
      LimitChecker limitChecker, Map<URI, FileStatus> statCache)
      throws IOException {
    Path pathWithScheme = p;
    if (!pathWithScheme.toUri().isAbsolute()) {
      // the path does not have a scheme, so we assume it is a path from the
      // local filesystem
      FileSystem localFs = FileSystem.getLocal(job);
      pathWithScheme = localFs.makeQualified(p);
    }
    FileStatus status = getFileStatus(statCache, job, pathWithScheme);
    if (status.isDirectory()) {
      FileStatus[] statusArray =
          pathWithScheme.getFileSystem(job).listStatus(pathWithScheme);
      for (FileStatus s : statusArray) {
        explorePath(job, s.getPath(), limitChecker, statCache);
      }
    } else {
      limitChecker.addFile(pathWithScheme, status.getLen());
    }
  }

  @VisibleForTesting
  FileStatus getFileStatus(Map<URI, FileStatus> statCache,
      Configuration job, Path p) throws IOException {
    URI u = p.toUri();
    FileStatus status = statCache.get(u);
    if (status == null) {
      status = p.getFileSystem(job).getFileStatus(p);
      statCache.put(u, status);
    }
    return status;
  }

  /**
   * Create a new directory in the passed filesystem. This wrapper method exists
   * so that it can be overridden/stubbed during testing.
   */
  @VisibleForTesting
  boolean mkdirs(FileSystem fs, Path dir, FsPermission permission)
      throws IOException {
    return FileSystem.mkdirs(fs, dir, permission);
  }

  // copies a file to the jobtracker filesystem and returns the path where it
  // was copied to
  @VisibleForTesting
  Path copyRemoteFiles(Path parentDir, Path originalPath,
      Configuration conf, short replication) throws IOException {
    // check if we do not need to copy the files
    // is jt using the same file system.
    // just checking for uri strings... doing no dns lookups
    // to see if the filesystems are the same. This is not optimal.
    // but avoids name resolution.

    FileSystem remoteFs = null;
    remoteFs = originalPath.getFileSystem(conf);
    if (FileUtil.compareFs(remoteFs, jtFs)) {
      return originalPath;
    }
    // this might have name collisions. copy will throw an exception
    // parse the original path to create new path
    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, conf);
    jtFs.setReplication(newPath, replication);
    jtFs.makeQualified(newPath);
    return newPath;
  }

  @VisibleForTesting
  void copyJar(Path originalJarPath, Path submitJarFile,
      short replication) throws IOException {
    jtFs.copyFromLocalFile(originalJarPath, submitJarFile);
    jtFs.setReplication(submitJarFile, replication);
    jtFs.setPermission(submitJarFile, new FsPermission(
        JobSubmissionFiles.JOB_FILE_PERMISSION));
  }

  private void addLog4jToDistributedCache(Job job, Path jobSubmitDir)
      throws IOException {
    Configuration conf = job.getConfiguration();
    String log4jPropertyFile =
        conf.get(MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE, "");
    if (!log4jPropertyFile.isEmpty()) {
      short replication = (short) conf.getInt(Job.SUBMIT_REPLICATION, 10);
      copyLog4jPropertyFile(job, jobSubmitDir, replication);
    }
  }

  private URI getPathURI(Path destPath, String fragment)
      throws URISyntaxException {
    URI pathURI = destPath.toUri();
    if (pathURI.getFragment() == null) {
      if (fragment == null) {
        // no fragment, just return existing pathURI from destPath
      } else {
        pathURI = new URI(pathURI.toString() + "#" + fragment);
      }
    }
    return pathURI;
  }

  // copy user specified log4j.property file in local
  // to HDFS with putting on distributed cache and adding its parent directory
  // to classpath.
  @SuppressWarnings("deprecation")
  private void copyLog4jPropertyFile(Job job, Path submitJobDir,
      short replication) throws IOException {
    Configuration conf = job.getConfiguration();

    String file =
        validateFilePath(
            conf.get(MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE), conf);
    LOG.debug("default FileSystem: " + jtFs.getUri());
    FsPermission mapredSysPerms =
        new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    try {
      jtFs.getFileStatus(submitJobDir);
    } catch (FileNotFoundException e) {
      throw new IOException("Cannot find job submission directory! "
          + "It should just be created, so something wrong here.", e);
    }

    Path fileDir = JobSubmissionFiles.getJobLog4jFile(submitJobDir);

    // first copy local log4j.properties file to HDFS under submitJobDir
    if (file != null) {
      FileSystem.mkdirs(jtFs, fileDir, mapredSysPerms);
      URI tmpURI = null;
      try {
        tmpURI = new URI(file);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
      Path tmp = new Path(tmpURI);
      Path newPath = copyRemoteFiles(fileDir, tmp, conf, replication);
      DistributedCache.addFileToClassPath(new Path(newPath.toUri().getPath()),
          conf);
    }
  }

  /**
   * takes input as a path string for file and verifies if it exist. It defaults
   * for file:/// if the files specified do not have a scheme. it returns the
   * paths uri converted defaulting to file:///. So an input of /home/user/file1
   * would return file:///home/user/file1
   * 
   * @param file
   * @param conf
   * @return
   */
  private String validateFilePath(String file, Configuration conf)
      throws IOException {
    if (file == null) {
      return null;
    }
    if (file.isEmpty()) {
      throw new IllegalArgumentException("File name can't be empty string");
    }
    String finalPath;
    URI pathURI;
    try {
      pathURI = new URI(file);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    Path path = new Path(pathURI);
    FileSystem localFs = FileSystem.getLocal(conf);
    if (pathURI.getScheme() == null) {
      // default to the local file system
      // check if the file exists or not first
      localFs.getFileStatus(path);
      finalPath =
          path.makeQualified(localFs.getUri(), localFs.getWorkingDirectory())
              .toString();
    } else {
      // check if the file exists in this file system
      // we need to recreate this filesystem object to copy
      // these files to the file system ResourceManager is running
      // on.
      FileSystem fs = path.getFileSystem(conf);
      fs.getFileStatus(path);
      finalPath =
          path.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString();
    }
    return finalPath;
  }

  private void disableErasureCodingForPath(FileSystem fs, Path path)
      throws IOException {
    if (jtFs instanceof DistributedFileSystem) {
      LOG.info("Disabling Erasure Coding for path: " + path);
      DistributedFileSystem dfs = (DistributedFileSystem) jtFs;
      dfs.setErasureCodingPolicy(path,
          SystemErasureCodingPolicies.getReplicationPolicy().getName());
    }
  }
}
