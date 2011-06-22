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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.ClientConstants;
import org.apache.hadoop.mapreduce.v2.MRConstants;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.util.ConverterUtils;


/**
 * This class enables the current JobClient (0.22 hadoop) to run on YARN.
 */
public class YARNRunner implements ClientProtocol {

  private static final Log LOG = LogFactory.getLog(YARNRunner.class);

  public static final String YARN_AM_VMEM_MB =
      "yarn.am.mapreduce.resource.mb";
  private static final int DEFAULT_YARN_AM_VMEM_MB = 2048;
  
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private ResourceMgrDelegate resMgrDelegate;
  private ClientServiceDelegate clientServiceDelegate;
  private YarnConfiguration conf;
  private final FileContext defaultFileContext;

  /**
   * Yarn runner incapsulates the client interface of
   * yarn
   * @param conf the configuration object for the client
   */
  public YARNRunner(Configuration conf) {
    this.conf = new YarnConfiguration(conf);
    try {
      this.resMgrDelegate = new ResourceMgrDelegate(this.conf);
      this.clientServiceDelegate = new ClientServiceDelegate(this.conf,
          resMgrDelegate);
      this.defaultFileContext = FileContext.getFileContext(this.conf);
    } catch (UnsupportedFileSystemException ufe) {
      throw new RuntimeException("Error in instantiating YarnClient", ufe);
    }
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    resMgrDelegate.cancelDelegationToken(arg0);
  }

  @Override
  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
	  return resMgrDelegate.getActiveTrackers();
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
   return resMgrDelegate.getAllJobs();
  }

  @Override
  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    return resMgrDelegate.getBlacklistedTrackers();
  }

  @Override
  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {  
    return resMgrDelegate.getClusterMetrics();
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text arg0)
      throws IOException, InterruptedException {
    return resMgrDelegate.getDelegationToken(arg0);
  }

  @Override
  public String getFilesystemName() throws IOException, InterruptedException {
    return resMgrDelegate.getFilesystemName();
  }

  @Override
  public JobID getNewJobID() throws IOException, InterruptedException {
    return resMgrDelegate.getNewJobID();
  }

  @Override
  public QueueInfo getQueue(String queueName) throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueue(queueName);
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueueAclsForCurrentUser();
  }

  @Override
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getQueues();
  }

  @Override
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getRootQueues();
  }

  @Override
  public QueueInfo[] getChildQueues(String parent) throws IOException,
      InterruptedException {
    return resMgrDelegate.getChildQueues(parent);
  }

  @Override
  public String getStagingAreaDir() throws IOException, InterruptedException {
    return resMgrDelegate.getStagingAreaDir();
  }

  @Override
  public String getSystemDir() throws IOException, InterruptedException {
    return resMgrDelegate.getSystemDir();
  }
  
  @Override
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return resMgrDelegate.getTaskTrackerExpiryInterval();
  }
  
  @Override
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
  throws IOException, InterruptedException {

    // Upload only in security mode: TODO
    Path applicationTokensFile =
        new Path(jobSubmitDir, MRConstants.APPLICATION_TOKENS_FILE);
    try {
      ts.writeTokenStorageFile(applicationTokensFile, conf);
    } catch (IOException e) {
      throw new YarnException(e);
    }

    // XXX Remove
    Path submitJobDir = new Path(jobSubmitDir);
    FileContext defaultFS = FileContext.getFileContext(conf);
    Path submitJobFile =
      defaultFS.makeQualified(JobSubmissionFiles.getJobConfPath(submitJobDir));
    FSDataInputStream in = defaultFS.open(submitJobFile);
    conf.addResource(in);
    // ---

    // Construct necessary information to start the MR AM
    ApplicationSubmissionContext appContext = 
      createApplicationSubmissionContext(conf, jobSubmitDir, ts);
    setupDistributedCache(conf, appContext);
    
    // XXX Remove
    in.close();
    // ---
    
    // Submit to ResourceManager
    ApplicationId applicationId = resMgrDelegate.submitApplication(appContext);
    
    ApplicationMaster appMaster = 
      resMgrDelegate.getApplicationMaster(applicationId);
    if (appMaster.getState() == ApplicationState.FAILED || appMaster.getState() ==
      ApplicationState.KILLED) {
      throw RPCUtil.getRemoteException("failed to run job");
    }
    return clientServiceDelegate.getJobStatus(jobId);
  }

  private LocalResource createApplicationResource(FileContext fs, Path p)
      throws IOException {
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs
        .getDefaultFileSystem().resolvePath(rsrcStat.getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(LocalResourceType.FILE);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    return rsrc;
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(
      Configuration jobConf,
      String jobSubmitDir, Credentials ts) throws IOException {
    ApplicationSubmissionContext appContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    ApplicationId applicationId = resMgrDelegate.getApplicationId();
    appContext.setApplicationId(applicationId);
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    capability.setMemory(conf.getInt(YARN_AM_VMEM_MB, DEFAULT_YARN_AM_VMEM_MB));
    LOG.info("AppMaster capability = " + capability);
    appContext.setMasterCapability(capability);

    Path jobConfPath = new Path(jobSubmitDir, MRConstants.JOB_CONF_FILE);
    
    URL yarnUrlForJobSubmitDir = ConverterUtils
        .getYarnUrlFromPath(defaultFileContext.getDefaultFileSystem()
            .resolvePath(
                defaultFileContext.makeQualified(new Path(jobSubmitDir))));
    LOG.debug("Creating setup context, jobSubmitDir url is "
        + yarnUrlForJobSubmitDir);

    appContext.setResource(MRConstants.JOB_SUBMIT_DIR,
        yarnUrlForJobSubmitDir);

    appContext.setResourceTodo(MRConstants.JOB_CONF_FILE,
        createApplicationResource(defaultFileContext,
            jobConfPath));
    if (jobConf.get(MRJobConfig.JAR) != null) {
      appContext.setResourceTodo(MRConstants.JOB_JAR,
          createApplicationResource(defaultFileContext,
              new Path(jobSubmitDir, MRConstants.JOB_JAR)));
    } else {
      // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
      // mapreduce jar itself which is already on the classpath.
      LOG.info("Job jar is not present. "
          + "Not adding any jar to the list of resources.");
    }
    
    // TODO gross hack
    for (String s : new String[] { "job.split", "job.splitmetainfo",
        MRConstants.APPLICATION_TOKENS_FILE }) {
      appContext.setResourceTodo(
          MRConstants.JOB_SUBMIT_DIR + "/" + s,
          createApplicationResource(defaultFileContext, new Path(jobSubmitDir, s)));
    }

    // TODO: Only if security is on.
    List<String> fsTokens = new ArrayList<String>();
    for (Token<? extends TokenIdentifier> token : ts.getAllTokens()) {
      fsTokens.add(token.encodeToUrlString());
    }
    
    // TODO - Remove this!
    appContext.addAllFsTokens(fsTokens);
    DataOutputBuffer dob = new DataOutputBuffer();
    ts.writeTokenStorageToStream(dob);
    appContext.setFsTokensTodo(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));

    // Add queue information
    appContext.setQueue(jobConf.get(JobContext.QUEUE_NAME, JobConf.DEFAULT_QUEUE_NAME));
    
    // Add job name
    appContext.setApplicationName(jobConf.get(JobContext.JOB_NAME, "N/A"));
    
    // Add the command line
    String javaHome = "$JAVA_HOME";
    Vector<CharSequence> vargs = new Vector<CharSequence>(8);
    vargs.add(javaHome + "/bin/java");
    vargs.add("-Dhadoop.root.logger="
        + conf.get(ClientConstants.MR_APPMASTER_LOG_OPTS,
            ClientConstants.DEFAULT_MR_APPMASTER_LOG_OPTS) + ",console");
    
    vargs.add(conf.get(ClientConstants.MR_APPMASTER_COMMAND_OPTS,
        ClientConstants.DEFAULT_MR_APPMASTER_COMMAND_OPTS));

    // Add { job jar, MR app jar } to classpath.
    Map<String, String> environment = new HashMap<String, String>();
//    appContext.environment = new HashMap<CharSequence, CharSequence>();
    MRApps.setInitialClasspath(environment);
    MRApps.addToClassPath(environment, MRConstants.JOB_JAR);
    MRApps.addToClassPath(environment,
        MRConstants.YARN_MAPREDUCE_APP_JAR_PATH);
    appContext.addAllEnvironment(environment);
    vargs.add("org.apache.hadoop.mapreduce.v2.app.MRAppMaster");
    vargs.add(String.valueOf(applicationId.getClusterTimestamp()));
    vargs.add(String.valueOf(applicationId.getId()));
    vargs.add(ApplicationConstants.AM_FAIL_COUNT_STRING);
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    Vector<String> vargsFinal = new Vector<String>(8);
    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    vargsFinal.add(mergedCommand.toString());

    LOG.info("Command to launch container for ApplicationMaster is : "
        + mergedCommand);

    appContext.addAllCommands(vargsFinal);
    // TODO: RM should get this from RPC.
    appContext.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    return appContext;
  }

  /**
   * TODO: Copied for now from TaskAttemptImpl.java ... fixme
   * 
   * TODO: This is currently needed in YarnRunner as user code like setupJob,
   * cleanupJob may need access to dist-cache. Once we separate distcache for
   * maps, reduces, setup etc, this can include only a subset of artificats.
   * This is also needed for uberAM case where we run everything inside AM.
   */
  private void setupDistributedCache(Configuration conf, 
      ApplicationSubmissionContext container) throws IOException {
    
    // Cache archives
    parseDistributedCacheArtifacts(conf, container, LocalResourceType.ARCHIVE, 
        DistributedCache.getCacheArchives(conf), 
        DistributedCache.getArchiveTimestamps(conf), 
        getFileSizes(conf, MRJobConfig.CACHE_ARCHIVES_SIZES), 
        DistributedCache.getArchiveVisibilities(conf), 
        DistributedCache.getArchiveClassPaths(conf));
    
    // Cache files
    parseDistributedCacheArtifacts(conf, container, LocalResourceType.FILE, 
        DistributedCache.getCacheFiles(conf),
        DistributedCache.getFileTimestamps(conf),
        getFileSizes(conf, MRJobConfig.CACHE_FILES_SIZES),
        DistributedCache.getFileVisibilities(conf),
        DistributedCache.getFileClassPaths(conf));
  }

  // TODO - Move this to MR!
  // Use TaskDistributedCacheManager.CacheFiles.makeCacheFiles(URI[], long[], boolean[], Path[], FileType)
  private void parseDistributedCacheArtifacts(Configuration conf,
      ApplicationSubmissionContext container, LocalResourceType type,
      URI[] uris, long[] timestamps, long[] sizes, boolean visibilities[], 
      Path[] pathsToPutOnClasspath) throws IOException {

    if (uris != null) {
      // Sanity check
      if ((uris.length != timestamps.length) || (uris.length != sizes.length) ||
          (uris.length != visibilities.length)) {
        throw new IllegalArgumentException("Invalid specification for " +
            "distributed-cache artifacts of type " + type + " :" +
            " #uris=" + uris.length +
            " #timestamps=" + timestamps.length +
            " #visibilities=" + visibilities.length
            );
      }
      
      Map<String, Path> classPaths = new HashMap<String, Path>();
      if (pathsToPutOnClasspath != null) {
        for (Path p : pathsToPutOnClasspath) {
          FileSystem fs = p.getFileSystem(conf);
          p = p.makeQualified(fs.getUri(), fs.getWorkingDirectory());
          classPaths.put(p.toUri().getPath().toString(), p);
        }
      }
      for (int i = 0; i < uris.length; ++i) {
        URI u = uris[i];
        Path p = new Path(u);
        FileSystem fs = p.getFileSystem(conf);
        p = fs.resolvePath(
            p.makeQualified(fs.getUri(), fs.getWorkingDirectory()));
        // Add URI fragment or just the filename
        Path name = new Path((null == u.getFragment())
          ? p.getName()
          : u.getFragment());
        if (name.isAbsolute()) {
          throw new IllegalArgumentException("Resource name must be relative");
        }
        String linkName = name.toUri().getPath();
        container.setResourceTodo(
            linkName,
            createLocalResource(
                p.toUri(), type, 
                visibilities[i]
                  ? LocalResourceVisibility.PUBLIC
                  : LocalResourceVisibility.PRIVATE,
                sizes[i], timestamps[i])
        );
        if (classPaths.containsKey(u.getPath())) {
          Map<String, String> environment = container.getAllEnvironment();
          MRApps.addToClassPath(environment, linkName);
        }
      }
    }
  }

  // TODO - Move this to MR!
  private static long[] getFileSizes(Configuration conf, String key) {
    String[] strs = conf.getStrings(key);
    if (strs == null) {
      return null;
    }
    long[] result = new long[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Long.parseLong(strs[i]);
    }
    return result;
  }
  
  private LocalResource createLocalResource(URI uri, 
      LocalResourceType type, LocalResourceVisibility visibility, 
      long size, long timestamp) throws IOException {
    LocalResource resource = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(LocalResource.class);
    resource.setResource(ConverterUtils.getYarnUrlFromURI(uri));
    resource.setType(type);
    resource.setVisibility(visibility);
    resource.setSize(size);
    resource.setTimestamp(timestamp);
    return resource;
  }
  
  @Override
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    resMgrDelegate.setJobPriority(arg0, arg1);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return resMgrDelegate.getProtocolVersion(arg0, arg1);
  }
  
  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    return resMgrDelegate.renewDelegationToken(arg0);
  }

  
  @Override
  public Counters getJobCounters(JobID arg0) throws IOException,
      InterruptedException {
    return clientServiceDelegate.getJobCounters(arg0);
  }

  @Override
  public String getJobHistoryDir() throws IOException, InterruptedException {
    return clientServiceDelegate.getJobHistoryDir();
  }

  @Override
  public JobStatus getJobStatus(JobID jobID) throws IOException,
      InterruptedException {
    JobStatus status = clientServiceDelegate.getJobStatus(jobID);
    return status;
  }
  
  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1,
      int arg2) throws IOException, InterruptedException {
    return clientServiceDelegate.getTaskCompletionEvents(arg0, arg1, arg2);
  }

  @Override
  public String[] getTaskDiagnostics(TaskAttemptID arg0) throws IOException,
      InterruptedException {
    return clientServiceDelegate.getTaskDiagnostics(arg0);
  }

  @Override
  public TaskReport[] getTaskReports(JobID jobID, TaskType taskType)
  throws IOException, InterruptedException {
    return clientServiceDelegate
        .getTaskReports(jobID, taskType);
  }

  @Override
  public void killJob(JobID arg0) throws IOException, InterruptedException {
    if (!clientServiceDelegate.killJob(arg0)) {
    resMgrDelegate.killApplication(TypeConverter.toYarn(arg0).getAppId());
  }
  }

  @Override
  public boolean killTask(TaskAttemptID arg0, boolean arg1) throws IOException,
      InterruptedException {
    return clientServiceDelegate.killTask(arg0, arg1);
  }

  @Override
  public AccessControlList getQueueAdmins(String arg0) throws IOException {
    return new AccessControlList("*");
  }
  
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSigature(this, protocol, clientVersion,
        clientMethodsHash);
  }
}
