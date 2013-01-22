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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.QueueACL;
import static org.apache.hadoop.mapred.QueueManager.toFullPropertyName;

import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Charsets;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class JobSubmitter {
  protected static final Log LOG = LogFactory.getLog(JobSubmitter.class);
  private FileSystem jtFs;
  private ClientProtocol submitClient;
  private String submitHostName;
  private String submitHostAddress;
  
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
      LOG.warn("Hadoop command-line option parsing not performed. " +
               "Implement the Tool interface and execute your application " +
               "with ToolRunner to remedy this.");
    }

    // get all the command line arguments passed in by the user conf
    String files = conf.get("tmpfiles");
    String libjars = conf.get("tmpjars");
    String archives = conf.get("tmparchives");
    String jobJar = job.getJar();

    //
    // Figure out what fs the JobTracker is using.  Copy the
    // job to it, under a temporary name.  This allows DFS to work,
    // and under the local fs also provides UNIX-like object loading 
    // semantics.  (that is, if the job file is deleted right after
    // submission, we can still run the submission to completion)
    //

    // Create a number of filenames in the JobTracker's fs namespace
    LOG.debug("default FileSystem: " + jtFs.getUri());
    if (jtFs.exists(submitJobDir)) {
      throw new IOException("Not submitting job. Job directory " + submitJobDir
          +" already exists!! This is unexpected.Please check what's there in" +
          " that directory");
    }
    submitJobDir = jtFs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission mapredSysPerms = new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(jtFs, submitJobDir, mapredSysPerms);
    Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
    Path archivesDir = JobSubmissionFiles.getJobDistCacheArchives(submitJobDir);
    Path libjarsDir = JobSubmissionFiles.getJobDistCacheLibjars(submitJobDir);
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
      }
    }
      
    if (libjars != null) {
      FileSystem.mkdirs(jtFs, libjarsDir, mapredSysPerms);
      String[] libjarsArr = libjars.split(",");
      for (String tmpjars: libjarsArr) {
        Path tmp = new Path(tmpjars);
        Path newPath = copyRemoteFiles(libjarsDir, tmp, conf, replication);
        DistributedCache.addFileToClassPath(
            new Path(newPath.toUri().getPath()), conf);
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
      }
    }

    if (jobJar != null) {   // copy jar to JobTracker's fs
      // use jar name if job is not named. 
      if ("".equals(job.getJobName())){
        job.setJobName(new Path(jobJar).getName());
      }
      Path jobJarPath = new Path(jobJar);
      URI jobJarURI = jobJarPath.toUri();
      // If the job jar is already in fs, we don't need to copy it from local fs
      if (jobJarURI.getScheme() == null || jobJarURI.getAuthority() == null
              || !(jobJarURI.getScheme().equals(jtFs.getUri().getScheme()) 
                  && jobJarURI.getAuthority().equals(
                                            jtFs.getUri().getAuthority()))) {
        copyJar(jobJarPath, JobSubmissionFiles.getJobJar(submitJobDir), 
            replication);
        job.setJar(JobSubmissionFiles.getJobJar(submitJobDir).toString());
      }
    } else {
      LOG.warn("No job jar file set.  User classes may not be found. "+
      "See Job or Job#setJar(String).");
    }

    //  set the timestamps of the archives and files
    //  set the public/private visibility of the archives and files
    ClientDistributedCacheManager.determineTimestampsAndCacheVisibilities(conf);
    // get DelegationToken for each cached file
    ClientDistributedCacheManager.getDelegationTokens(conf, job
        .getCredentials());
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
    jtFs.setPermission(submitJarFile, new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
  }
  
  /**
   * configure the jobconf of the user with the command line options of 
   * -libjars, -files, -archives.
   * @param conf
   * @throws IOException
   */
  private void copyAndConfigureFiles(Job job, Path jobSubmitDir) 
  throws IOException {
    Configuration conf = job.getConfiguration();
    short replication = (short)conf.getInt(Job.SUBMIT_REPLICATION, 10);
    copyAndConfigureFiles(job, jobSubmitDir, replication);

    // Set the working directory
    if (job.getWorkingDirectory() == null) {
      job.setWorkingDirectory(jtFs.getWorkingDirectory());          
    }

  }
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
   * @param cluster the handle to the Cluster
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  JobStatus submitJobInternal(Job job, Cluster cluster) 
  throws ClassNotFoundException, InterruptedException, IOException {

    //validate the jobs output specs 
    checkSpecs(job);
    
    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, 
                                                     job.getConfiguration());
    //configure the command line options correctly on the submitting dfs
    Configuration conf = job.getConfiguration();
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      submitHostAddress = ip.getHostAddress();
      submitHostName = ip.getHostName();
      conf.set(MRJobConfig.JOB_SUBMITHOST,submitHostName);
      conf.set(MRJobConfig.JOB_SUBMITHOSTADDR,submitHostAddress);
    }
    JobID jobId = submitClient.getNewJobID();
    job.setJobID(jobId);
    Path submitJobDir = new Path(jobStagingArea, jobId.toString());
    JobStatus status = null;
    try {
      conf.set("hadoop.http.filter.initializers", 
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, submitJobDir.toString());
      LOG.debug("Configuring job " + jobId + " with " + submitJobDir 
          + " as the submit dir");
      // get delegation token for the dir
      TokenCache.obtainTokensForNamenodes(job.getCredentials(),
          new Path[] { submitJobDir }, conf);
      
      populateTokenCache(conf, job.getCredentials());

      copyAndConfigureFiles(job, submitJobDir);
      Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
      
      // Create the splits for the job
      LOG.debug("Creating splits at " + jtFs.makeQualified(submitJobDir));
      int maps = writeSplits(job, submitJobDir);
      conf.setInt(MRJobConfig.NUM_MAPS, maps);
      LOG.info("number of splits:" + maps);

      // write "queue admins of the queue to which job is being submitted"
      // to job file.
      String queue = conf.get(MRJobConfig.QUEUE_NAME,
          JobConf.DEFAULT_QUEUE_NAME);
      AccessControlList acl = submitClient.getQueueAdmins(queue);
      conf.set(toFullPropertyName(queue,
          QueueACL.ADMINISTER_JOBS.getAclName()), acl.getAclString());

      // removing jobtoken referrals before copying the jobconf to HDFS
      // as the tasks don't need this setting, actually they may break
      // because of it if present as the referral will point to a
      // different job.
      TokenCache.cleanUpTokenReferral(conf);

      // Write job file to submit dir
      writeConf(conf, submitJobFile);
      
      //
      // Now, actually submit the job (using the submit name)
      //
      printTokens(jobId, job.getCredentials());
      status = submitClient.submitJob(
          jobId, submitJobDir.toString(), job.getCredentials());
      if (status != null) {
        return status;
      } else {
        throw new IOException("Could not launch job");
      }
    } finally {
      if (status == null) {
        LOG.info("Cleaning up the staging area " + submitJobDir);
        if (jtFs != null && submitJobDir != null)
          jtFs.delete(submitJobDir, true);

      }
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
                        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }
  
  private void printTokens(JobID jobId,
      Credentials credentials) throws IOException {
    LOG.info("Submitting tokens for job: " + jobId);
    for (Token<?> token: credentials.getAllTokens()) {
      LOG.info(token);
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends InputSplit>
  int writeNewSplits(JobContext job, Path jobSubmitDir) throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = job.getConfiguration();
    InputFormat<?, ?> input =
      ReflectionUtils.newInstance(job.getInputFormatClass(), conf);

    List<InputSplit> splits = input.getSplits(job);
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);

    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(array, new SplitComparator());
    JobSplitWriter.createSplitFiles(jobSubmitDir, conf, 
        jobSubmitDir.getFileSystem(conf), array);
    return array.length;
  }
  
  private int writeSplits(org.apache.hadoop.mapreduce.JobContext job,
      Path jobSubmitDir) throws IOException,
      InterruptedException, ClassNotFoundException {
    JobConf jConf = (JobConf)job.getConfiguration();
    int maps;
    if (jConf.getUseNewMapper()) {
      maps = writeNewSplits(job, jobSubmitDir);
    } else {
      maps = writeOldSplits(jConf, jobSubmitDir);
    }
    return maps;
  }
  
  //method to write splits for old api mapper.
  private int writeOldSplits(JobConf job, Path jobSubmitDir) 
  throws IOException {
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
    JobSplitWriter.createSplitFiles(jobSubmitDir, job, 
        jobSubmitDir.getFileSystem(job), splits);
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
  
  @SuppressWarnings("unchecked")
  private void readTokensFromFiles(Configuration conf, Credentials credentials)
  throws IOException {
    // add tokens and secrets coming from a token storage file
    String binaryTokenFilename =
      conf.get("mapreduce.job.credentials.binary");
    if (binaryTokenFilename != null) {
      Credentials binary = Credentials.readTokenStorageFile(
          new Path("file:///" + binaryTokenFilename), conf);
      credentials.addAll(binary);
    }
    // add secret keys coming from a json file
    String tokensFileName = conf.get("mapreduce.job.credentials.json");
    if(tokensFileName != null) {
      LOG.info("loading user's secret keys from " + tokensFileName);
      String localFileName = new Path(tokensFileName).toUri().getPath();

      boolean json_error = false;
      try {
        // read JSON
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> nm = 
          mapper.readValue(new File(localFileName), Map.class);

        for(Map.Entry<String, String> ent: nm.entrySet()) {
          credentials.addSecretKey(new Text(ent.getKey()), ent.getValue()
              .getBytes(Charsets.UTF_8));
        }
      } catch (JsonMappingException e) {
        json_error = true;
      } catch (JsonParseException e) {
        json_error = true;
      }
      if(json_error)
        LOG.warn("couldn't parse Token Cache JSON file with user secret keys");
    }
  }

  //get secret keys and tokens and store them into TokenCache
  @SuppressWarnings("unchecked")
  private void populateTokenCache(Configuration conf, Credentials credentials) 
  throws IOException{
    readTokensFromFiles(conf, credentials);
    // add the delegation tokens from configuration
    String [] nameNodes = conf.getStrings(MRJobConfig.JOB_NAMENODES);
    LOG.debug("adding the following namenodes' delegation tokens:" + 
        Arrays.toString(nameNodes));
    if(nameNodes != null) {
      Path [] ps = new Path[nameNodes.length];
      for(int i=0; i< nameNodes.length; i++) {
        ps[i] = new Path(nameNodes[i]);
      }
      TokenCache.obtainTokensForNamenodes(credentials, ps, conf);
    }
  }
}
