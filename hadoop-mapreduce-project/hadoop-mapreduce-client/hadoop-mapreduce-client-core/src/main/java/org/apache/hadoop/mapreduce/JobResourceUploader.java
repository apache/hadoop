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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class JobResourceUploader {
  protected static final Log LOG = LogFactory.getLog(JobResourceUploader.class);
  private FileSystem jtFs;

  JobResourceUploader(FileSystem submitFs) {
    this.jtFs = submitFs;
  }

  /**
   * Upload and configure files, libjars, jobjars, and archives pertaining to
   * the passed job.
   * 
   * @param job the job containing the files to be uploaded
   * @param submitJobDir the submission directory of the job
   * @throws IOException
   */
  public void uploadFiles(Job job, Path submitJobDir) throws IOException {
    Configuration conf = job.getConfiguration();
    short replication =
        (short) conf.getInt(Job.SUBMIT_REPLICATION,
            Job.DEFAULT_SUBMIT_REPLICATION);

    if (!(conf.getBoolean(Job.USED_GENERIC_PARSER, false))) {
      LOG.warn("Hadoop command-line option parsing not performed. "
          + "Implement the Tool interface and execute your application "
          + "with ToolRunner to remedy this.");
    }

    // get all the command line arguments passed in by the user conf
    String files = conf.get("tmpfiles");
    String libjars = conf.get("tmpjars");
    String archives = conf.get("tmparchives");
    String jobJar = job.getJar();

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
    FileSystem.mkdirs(jtFs, submitJobDir, mapredSysPerms);
    Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
    Path archivesDir = JobSubmissionFiles.getJobDistCacheArchives(submitJobDir);
    Path libjarsDir = JobSubmissionFiles.getJobDistCacheLibjars(submitJobDir);
    // add all the command line files/ jars and archive
    // first copy them to jobtrackers filesystem

    if (files != null) {
      FileSystem.mkdirs(jtFs, filesDir, mapredSysPerms);
      String[] fileArr = files.split(",");
      for (String tmpFile : fileArr) {
        URI tmpURI = null;
        try {
          tmpURI = new URI(tmpFile);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = copyRemoteFiles(filesDir, tmp, conf, replication);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          DistributedCache.addCacheFile(pathURI, conf);
        } catch (URISyntaxException ue) {
          // should not throw a uri exception
          throw new IOException("Failed to create uri for " + tmpFile, ue);
        }
      }
    }

    if (libjars != null) {
      FileSystem.mkdirs(jtFs, libjarsDir, mapredSysPerms);
      String[] libjarsArr = libjars.split(",");
      for (String tmpjars : libjarsArr) {
        Path tmp = new Path(tmpjars);
        Path newPath = copyRemoteFiles(libjarsDir, tmp, conf, replication);
        DistributedCache.addFileToClassPath(
            new Path(newPath.toUri().getPath()), conf, jtFs);
      }
    }

    if (archives != null) {
      FileSystem.mkdirs(jtFs, archivesDir, mapredSysPerms);
      String[] archivesArr = archives.split(",");
      for (String tmpArchives : archivesArr) {
        URI tmpURI;
        try {
          tmpURI = new URI(tmpArchives);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = copyRemoteFiles(archivesDir, tmp, conf, replication);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          DistributedCache.addCacheArchive(pathURI, conf);
        } catch (URISyntaxException ue) {
          // should not throw an uri excpetion
          throw new IOException("Failed to create uri for " + tmpArchives, ue);
        }
      }
    }

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
            replication);
        job.setJar(JobSubmissionFiles.getJobJar(submitJobDir).toString());
      }
    } else {
      LOG.warn("No job jar file set.  User classes may not be found. "
          + "See Job or Job#setJar(String).");
    }

    addLog4jToDistributedCache(job, submitJobDir);

    // set the timestamps of the archives and files
    // set the public/private visibility of the archives and files
    ClientDistributedCacheManager.determineTimestampsAndCacheVisibilities(conf);
    // get DelegationToken for cached file
    ClientDistributedCacheManager.getDelegationTokens(conf,
        job.getCredentials());
  }

  // copies a file to the jobtracker filesystem and returns the path where it
  // was copied to
  private Path copyRemoteFiles(Path parentDir, Path originalPath,
      Configuration conf, short replication) throws IOException {
    // check if we do not need to copy the files
    // is jt using the same file system.
    // just checking for uri strings... doing no dns lookups
    // to see if the filesystems are the same. This is not optimal.
    // but avoids name resolution.

    FileSystem remoteFs = null;
    remoteFs = originalPath.getFileSystem(conf);
    if (compareFs(remoteFs, jtFs)) {
      return originalPath;
    }
    // this might have name collisions. copy will throw an exception
    // parse the original path to create new path
    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, conf);
    jtFs.setReplication(newPath, replication);
    return newPath;
  }

  /*
   * see if two file systems are the same or not.
   */
  private boolean compareFs(FileSystem srcFs, FileSystem destFs) {
    URI srcUri = srcFs.getUri();
    URI dstUri = destFs.getUri();
    if (srcUri.getScheme() == null) {
      return false;
    }
    if (!srcUri.getScheme().equals(dstUri.getScheme())) {
      return false;
    }
    String srcHost = srcUri.getHost();
    String dstHost = dstUri.getHost();
    if ((srcHost != null) && (dstHost != null)) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch (UnknownHostException ue) {
        return false;
      }
      if (!srcHost.equals(dstHost)) {
        return false;
      }
    } else if (srcHost == null && dstHost != null) {
      return false;
    } else if (srcHost != null && dstHost == null) {
      return false;
    }
    // check for ports
    if (srcUri.getPort() != dstUri.getPort()) {
      return false;
    }
    return true;
  }

  private void copyJar(Path originalJarPath, Path submitJarFile,
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
        pathURI = new URI(pathURI.toString() + "#" + destPath.getName());
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
    if (!jtFs.exists(submitJobDir)) {
      throw new IOException("Cannot find job submission directory! "
          + "It should just be created, so something wrong here.");
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
      if (!localFs.exists(path)) {
        throw new FileNotFoundException("File " + file + " does not exist.");
      }
      finalPath =
          path.makeQualified(localFs.getUri(), localFs.getWorkingDirectory())
              .toString();
    } else {
      // check if the file exists in this file system
      // we need to recreate this filesystem object to copy
      // these files to the file system ResourceManager is running
      // on.
      FileSystem fs = path.getFileSystem(conf);
      if (!fs.exists(path)) {
        throw new FileNotFoundException("File " + file + " does not exist.");
      }
      finalPath =
          path.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString();
    }
    return finalPath;
  }
}
