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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.util.ReflectionUtils;

class JobSubmitter {
  protected static final Log LOG = LogFactory.getLog(JobSubmitter.class);
  private FileSystem jtFs;
  private ClientProtocol submitClient;
  
  JobSubmitter(FileSystem submitFs, ClientProtocol submitClient) 
  throws IOException {
    this.submitClient = submitClient;
    this.jtFs = submitFs;
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
      } catch(UnknownHostException ue) {
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
    //check for ports
    if (srcUri.getPort() != dstUri.getPort()) {
      return false;
    }
    return true;
  }

  // copies a file to the jobtracker filesystem and returns the path where it
  // was copied to
  private Path copyRemoteFiles(Path parentDir,
      Path originalPath, Configuration conf, short replication) 
      throws IOException {
    //check if we do not need to copy the files
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
    //parse the original path to create new path
    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, conf);
    jtFs.setReplication(newPath, replication);
    return newPath;
  }

  // configures -files, -libjars and -archives.
  private void copyAndConfigureFiles(Job job, Path submitJobDir,
      short replication) throws IOException {
    Configuration conf = job.getConfiguration();
    if (!(conf.getBoolean(Job.USED_GENERIC_PARSER, false))) {
      LOG.warn("Use GenericOptionsParser for parsing the arguments. " +
               "Applications should implement Tool for the same.");
    }

    // get all the command line arguments passed in by the user conf
    String files = conf.get("tmpfiles");
    String libjars = conf.get("tmpjars");
    String archives = conf.get("tmparchives");
      
    /*
     * set this user's id in job configuration, so later job files can be
     * accessed using this user's id
     */
    job.setUGIAndUserGroupNames();

    //
    // Figure out what fs the JobTracker is using.  Copy the
    // job to it, under a temporary name.  This allows DFS to work,
    // and under the local fs also provides UNIX-like object loading 
    // semantics.  (that is, if the job file is deleted right after
    // submission, we can still run the submission to completion)
    //

    // Create a number of filenames in the JobTracker's fs namespace
    LOG.debug("default FileSystem: " + jtFs.getUri());
    jtFs.delete(submitJobDir, true);
    submitJobDir = jtFs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission mapredSysPerms = new FsPermission(JOB_DIR_PERMISSION);
    FileSystem.mkdirs(jtFs, submitJobDir, mapredSysPerms);
    Path filesDir = new Path(submitJobDir, "files");
    Path archivesDir = new Path(submitJobDir, "archives");
    Path libjarsDir = new Path(submitJobDir, "libjars");
    // add all the command line files/ jars and archive
    // first copy them to jobtrackers filesystem 
      
    if (files != null) {
      FileSystem.mkdirs(jtFs, filesDir, mapredSysPerms);
      String[] fileArr = files.split(",");
      for (String tmpFile: fileArr) {
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
        } catch(URISyntaxException ue) {
          //should not throw a uri exception 
          throw new IOException("Failed to create uri for " + tmpFile, ue);
        }
        DistributedCache.createSymlink(conf);
      }
    }
      
    if (libjars != null) {
      FileSystem.mkdirs(jtFs, libjarsDir, mapredSysPerms);
      String[] libjarsArr = libjars.split(",");
      for (String tmpjars: libjarsArr) {
        Path tmp = new Path(tmpjars);
        Path newPath = copyRemoteFiles(libjarsDir, tmp, conf, replication);
        DistributedCache.addFileToClassPath(newPath, conf);
      }
    }
      
    if (archives != null) {
      FileSystem.mkdirs(jtFs, archivesDir, mapredSysPerms); 
      String[] archivesArr = archives.split(",");
      for (String tmpArchives: archivesArr) {
        URI tmpURI;
        try {
          tmpURI = new URI(tmpArchives);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = copyRemoteFiles(archivesDir, tmp, conf,
          replication);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          DistributedCache.addCacheArchive(pathURI, conf);
        } catch(URISyntaxException ue) {
          //should not throw an uri excpetion
          throw new IOException("Failed to create uri for " + tmpArchives, ue);
        }
        DistributedCache.createSymlink(conf);
      }
    }
      
    //  set the timestamps of the archives and files
    TrackerDistributedCacheManager.determineTimestamps(conf);
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
  
  private void copyJar(Path originalJarPath, Path submitJarFile,
      short replication) throws IOException {
    jtFs.copyFromLocalFile(originalJarPath, submitJarFile);
    jtFs.setReplication(submitJarFile, replication);
    jtFs.setPermission(submitJarFile, new FsPermission(JOB_FILE_PERMISSION));
  }
  /**
   * configure the jobconf of the user with the command line options of 
   * -libjars, -files, -archives.
   * @param conf
   * @throws IOException
   */
  private void configureCommandLineOptions(Job job, Path submitJobDir,
      Path submitJarFile) throws IOException {
    Configuration conf = job.getConfiguration();
    short replication = (short)conf.getInt(Job.SUBMIT_REPLICATION, 10);
    copyAndConfigureFiles(job, submitJobDir, replication);
    
    /* set this user's id in job configuration, so later job files can be
     * accessed using this user's id
     */
    String originalJarPath = job.getJar();

    if (originalJarPath != null) {           // copy jar to JobTracker's fs
      // use jar name if job is not named. 
      if ("".equals(job.getJobName())){
        job.setJobName(new Path(originalJarPath).getName());
      }
      job.setJar(submitJarFile.toString());
      copyJar(new Path(originalJarPath), submitJarFile, replication);
    } else {
      LOG.warn("No job jar file set.  User classes may not be found. "+
               "See Job or Job#setJar(String).");
    }

    // Set the working directory
    if (job.getWorkingDirectory() == null) {
      job.setWorkingDirectory(jtFs.getWorkingDirectory());          
    }

  }

  // job files are world-wide readable and owner writable
  final private static FsPermission JOB_FILE_PERMISSION = 
    FsPermission.createImmutable((short) 0644); // rw-r--r--

  // job submission directory is world readable/writable/executable
  final static FsPermission JOB_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0777); // rwx-rwx-rwx
   
  /**
   * Internal method for submitting jobs to the system.
   * 
   * <p>The job submission process involves:
   * <ol>
   *   <li>
   *   Checking the input and output specifications of the job.
   *   </li>
   *   <li>
   *   Computing the {@link InputSplit}s for the job.
   *   </li>
   *   <li>
   *   Setup the requisite accounting information for the 
   *   {@link DistributedCache} of the job, if necessary.
   *   </li>
   *   <li>
   *   Copying the job's jar and configuration to the map-reduce system
   *   directory on the distributed file-system. 
   *   </li>
   *   <li>
   *   Submitting the job to the <code>JobTracker</code> and optionally
   *   monitoring it's status.
   *   </li>
   * </ol></p>
   * @param job the configuration to submit
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  JobStatus submitJobInternal(Job job) throws ClassNotFoundException,
      InterruptedException, IOException {
    
    //configure the command line options correctly on the submitting dfs
    Configuration conf = job.getConfiguration();
    JobID jobId = submitClient.getNewJobID();
    Path submitJobDir = new Path(submitClient.getSystemDir(), jobId.toString());
    Path submitJarFile = new Path(submitJobDir, "job.jar");
    Path submitSplitFile = new Path(submitJobDir, "job.split");
    configureCommandLineOptions(job, submitJobDir, submitJarFile);
    Path submitJobFile = new Path(submitJobDir, "job.xml");
    
    checkSpecs(job);

    // Create the splits for the job
    LOG.info("Creating splits at " + jtFs.makeQualified(submitSplitFile));
    int maps = writeSplits(job, submitSplitFile);
    conf.set("mapred.job.split.file", submitSplitFile.toString());
    conf.setInt("mapred.map.tasks", maps);
    LOG.info("number of splits:" + maps);
    
    // Write job file to JobTracker's fs
    writeConf(conf, submitJobFile);
    
    //
    // Now, actually submit the job (using the submit name)
    //
    JobStatus status = submitClient.submitJob(jobId);
    if (status != null) {
      return status;
    } else {
      throw new IOException("Could not launch job");
    }
  }

  private void checkSpecs(Job job) throws ClassNotFoundException, 
      InterruptedException, IOException {
    JobConf jConf = (JobConf)job.getConfiguration();
    // Check the output specification
    if (jConf.getNumReduceTasks() == 0 ? 
        jConf.getUseNewMapper() : jConf.getUseNewReducer()) {
      org.apache.hadoop.mapreduce.OutputFormat<?, ?> output =
        ReflectionUtils.newInstance(job.getOutputFormatClass(),
          job.getConfiguration());
      output.checkOutputSpecs(job);
    } else {
      jConf.getOutputFormat().checkOutputSpecs(jtFs, jConf);
    }
  }
  
  private void writeConf(Configuration conf, Path jobFile) 
      throws IOException {
    // Write job file to JobTracker's fs        
    FSDataOutputStream out = 
      FileSystem.create(jtFs, jobFile, 
                        new FsPermission(JOB_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }
  
  @SuppressWarnings("unchecked")
  private <T extends InputSplit> 
  int writeNewSplits(JobContext job, Path submitSplitFile) throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = job.getConfiguration();
    InputFormat<?, ?> input =
      ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
    
    List<InputSplit> splits = input.getSplits(job);
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);

    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(array, new SplitComparator());
    DataOutputStream out = writeSplitsFileHeader(conf, submitSplitFile, 
                                                 array.length);
    try {
      if (array.length != 0) {
        DataOutputBuffer buffer = new DataOutputBuffer();
        Job.RawSplit rawSplit = new Job.RawSplit();
        SerializationFactory factory = new SerializationFactory(conf);
        Serializer<T> serializer = 
          factory.getSerializer((Class<T>) array[0].getClass());
        serializer.open(buffer);
        for (T split: array) {
          rawSplit.setClassName(split.getClass().getName());
          buffer.reset();
          serializer.serialize(split);
          rawSplit.setDataLength(split.getLength());
          rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
          rawSplit.setLocations(split.getLocations());
          rawSplit.write(out);
        }
        serializer.close();
      }
    } finally {
      out.close();
    }
    return array.length;
  }

  static final int CURRENT_SPLIT_FILE_VERSION = 0;
  static final byte[] SPLIT_FILE_HEADER = "SPL".getBytes();

  private DataOutputStream writeSplitsFileHeader(Configuration conf,
      Path filename, int length) throws IOException {
    // write the splits to a file for the job tracker
    FileSystem fs = filename.getFileSystem(conf);
    FSDataOutputStream out = 
      FileSystem.create(fs, filename, new FsPermission(JOB_FILE_PERMISSION));
    out.write(SPLIT_FILE_HEADER);
    WritableUtils.writeVInt(out, CURRENT_SPLIT_FILE_VERSION);
    WritableUtils.writeVInt(out, length);
    return out;
  }

  private int writeSplits(org.apache.hadoop.mapreduce.JobContext job,
                  Path submitSplitFile) throws IOException,
      InterruptedException, ClassNotFoundException {
    JobConf jConf = (JobConf)job.getConfiguration();
    // Create the splits for the job
    LOG.debug("Creating splits at " + jtFs.makeQualified(submitSplitFile));
    int maps;
    if (jConf.getUseNewMapper()) {
      maps = writeNewSplits(job, submitSplitFile);
    } else {
      maps = writeOldSplits(jConf, submitSplitFile);
    }
    return maps;
  }

  // method to write splits for old api mapper.
  private int writeOldSplits(JobConf job, 
      Path submitSplitFile) throws IOException {
    org.apache.hadoop.mapred.InputSplit[] splits = 
    job.getInputFormat().getSplits(job, job.getNumMapTasks());
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new Comparator<org.apache.hadoop.mapred.InputSplit>() {
      public int compare(org.apache.hadoop.mapred.InputSplit a,
                         org.apache.hadoop.mapred.InputSplit b) {
        try {
          long left = a.getLength();
          long right = b.getLength();
          if (left == right) {
            return 0;
          } else if (left < right) {
            return 1;
          } else {
            return -1;
          }
        } catch (IOException ie) {
          throw new RuntimeException("Problem getting input split size", ie);
        }
      }
    });
    DataOutputStream out = writeSplitsFileHeader(job, submitSplitFile,
      splits.length);

    try {
      DataOutputBuffer buffer = new DataOutputBuffer();
      Job.RawSplit rawSplit = new Job.RawSplit();
      for (org.apache.hadoop.mapred.InputSplit split: splits) {
        rawSplit.setClassName(split.getClass().getName());
        buffer.reset();
        split.write(buffer);
        rawSplit.setDataLength(split.getLength());
        rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
        rawSplit.setLocations(split.getLocations());
        rawSplit.write(out);
      }
    } finally {
      out.close();
    }
    return splits.length;
  }
  
  private static class SplitComparator implements Comparator<InputSplit> {
    @Override
    public int compare(InputSplit o1, InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("exception in compare", ie);
      } catch (InterruptedException ie) {
        throw new RuntimeException("exception in compare", ie);        
      }
    }
  }
}
