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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.ConverterUtils;


/**
 * This class enables the current JobClient (0.22 hadoop) to run on YARN.
 */
public class YARNRunner implements ClientProtocol {

  private static final Log LOG = LogFactory.getLog(YARNRunner.class);

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private ResourceMgrDelegate resMgrDelegate;
  private ClientCache clientCache;
  private Configuration conf;
  private final FileContext defaultFileContext;

  /**
   * Yarn runner incapsulates the client interface of
   * yarn
   * @param conf the configuration object for the client
   */
  public YARNRunner(Configuration conf) {
   this(conf, new ResourceMgrDelegate(new YarnConfiguration(conf)));
  }

  /**
   * Similar to {@link #YARNRunner(Configuration)} but allowing injecting 
   * {@link ResourceMgrDelegate}. Enables mocking and testing.
   * @param conf the configuration object for the client
   * @param resMgrDelegate the resourcemanager client handle.
   */
  public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate) {
    this.conf = conf;
    try {
      this.resMgrDelegate = resMgrDelegate;
      this.clientCache = new ClientCache(this.conf, resMgrDelegate);
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
        new Path(jobSubmitDir, MRJobConfig.APPLICATION_TOKENS_FILE);
    try {
      ts.writeTokenStorageFile(applicationTokensFile, conf);
    } catch (IOException e) {
      throw new YarnException(e);
    }

    // Construct necessary information to start the MR AM
    ApplicationSubmissionContext appContext = 
      createApplicationSubmissionContext(conf, jobSubmitDir, ts);
    
    // Submit to ResourceManager
    ApplicationId applicationId = resMgrDelegate.submitApplication(appContext);
    
    ApplicationReport appMaster = resMgrDelegate
        .getApplicationReport(applicationId);
    String diagnostics = 
        (appMaster == null ? 
            "application report is null" : appMaster.getDiagnostics());
    if (appMaster == null || appMaster.getState() == ApplicationState.FAILED 
        || appMaster.getState() == ApplicationState.KILLED) {
      throw new IOException("Failed to run job : " + 
        diagnostics);
    }
    return clientCache.getClient(jobId).getJobStatus(jobId);
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

  public ApplicationSubmissionContext createApplicationSubmissionContext(
      Configuration jobConf,
      String jobSubmitDir, Credentials ts) throws IOException {
    ApplicationId applicationId = resMgrDelegate.getApplicationId();
    
    // Setup resource requirements
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    capability.setMemory(conf.getInt(MRJobConfig.MR_AM_VMEM_MB,
        MRJobConfig.DEFAULT_MR_AM_VMEM_MB));
    LOG.info("AppMaster capability = " + capability);

    // Setup LocalResources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    
    Path jobConfPath = new Path(jobSubmitDir, MRJobConfig.JOB_CONF_FILE);
    
    URL yarnUrlForJobSubmitDir = ConverterUtils
        .getYarnUrlFromPath(defaultFileContext.getDefaultFileSystem()
            .resolvePath(
                defaultFileContext.makeQualified(new Path(jobSubmitDir))));
    LOG.debug("Creating setup context, jobSubmitDir url is "
        + yarnUrlForJobSubmitDir);

    localResources.put(MRJobConfig.JOB_CONF_FILE,
        createApplicationResource(defaultFileContext,
            jobConfPath));
    if (jobConf.get(MRJobConfig.JAR) != null) {
      localResources.put(MRJobConfig.JOB_JAR,
          createApplicationResource(defaultFileContext,
              new Path(jobSubmitDir, MRJobConfig.JOB_JAR)));
    } else {
      // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
      // mapreduce jar itself which is already on the classpath.
      LOG.info("Job jar is not present. "
          + "Not adding any jar to the list of resources.");
    }
    
    // TODO gross hack
    for (String s : new String[] { 
        MRJobConfig.JOB_SPLIT, 
        MRJobConfig.JOB_SPLIT_METAINFO,
        MRJobConfig.APPLICATION_TOKENS_FILE }) {
      localResources.put(
          MRJobConfig.JOB_SUBMIT_DIR + "/" + s,
          createApplicationResource(defaultFileContext, 
              new Path(jobSubmitDir, s)));
    }
    
    // Setup security tokens
    ByteBuffer securityTokens = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      DataOutputBuffer dob = new DataOutputBuffer();
      ts.writeTokenStorageToStream(dob);
      securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    // Setup the command to run the AM
    Vector<CharSequence> vargs = new Vector<CharSequence>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    vargs.add("-Dhadoop.root.logger="
        + conf.get(MRJobConfig.MR_AM_LOG_OPTS,
            MRJobConfig.DEFAULT_MR_AM_LOG_OPTS) + ",console");
    
    vargs.add(conf.get(MRJobConfig.MR_AM_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_COMMAND_OPTS));

    vargs.add(MRJobConfig.APPLICATION_MASTER_CLASS);
    vargs.add(String.valueOf(applicationId.getClusterTimestamp()));
    vargs.add(String.valueOf(applicationId.getId()));
    vargs.add(ApplicationConstants.AM_FAIL_COUNT_STRING);
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + 
        Path.SEPARATOR + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + 
        Path.SEPARATOR + ApplicationConstants.STDERR);


    Vector<String> vargsFinal = new Vector<String>(8);
    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    vargsFinal.add(mergedCommand.toString());

    LOG.info("Command to launch container for ApplicationMaster is : "
        + mergedCommand);
    
    // Setup the CLASSPATH in environment 
    // i.e. add { job jar, CWD, Hadoop jars} to classpath.
    Map<String, String> environment = new HashMap<String, String>();
    MRApps.setClasspath(environment);
    
    // Parse distributed cache
    MRApps.setupDistributedCache(jobConf, localResources);

    // Setup ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    amContainer.setResource(capability);             // Resource (mem) required
    amContainer.setLocalResources(localResources);   // Local resources
    amContainer.setEnvironment(environment);         // Environment
    amContainer.setCommands(vargsFinal);             // Command for AM
    amContainer.setContainerTokens(securityTokens);  // Security tokens

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    appContext.setApplicationId(applicationId);                // ApplicationId
    appContext.setUser(                                        // User name
        UserGroupInformation.getCurrentUser().getShortUserName());
    appContext.setQueue(                                       // Queue name
        jobConf.get(JobContext.QUEUE_NAME,     
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    appContext.setApplicationName(                             // Job name
        jobConf.get(JobContext.JOB_NAME, 
        YarnConfiguration.DEFAULT_APPLICATION_NAME));              
    appContext.setAMContainerSpec(amContainer);         // AM Container 

    return appContext;
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
    return clientCache.getClient(arg0).getJobCounters(arg0);
  }

  @Override
  public String getJobHistoryDir() throws IOException, InterruptedException {
    return JobHistoryUtils.getConfiguredHistoryServerDoneDirPrefix(conf);
  }

  @Override
  public JobStatus getJobStatus(JobID jobID) throws IOException,
      InterruptedException {
    JobStatus status = clientCache.getClient(jobID).getJobStatus(jobID);
    return status;
  }
  
  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1,
      int arg2) throws IOException, InterruptedException {
    return clientCache.getClient(arg0).getTaskCompletionEvents(arg0, arg1, arg2);
  }

  @Override
  public String[] getTaskDiagnostics(TaskAttemptID arg0) throws IOException,
      InterruptedException {
    return clientCache.getClient(arg0.getJobID()).getTaskDiagnostics(arg0);
  }

  @Override
  public TaskReport[] getTaskReports(JobID jobID, TaskType taskType)
  throws IOException, InterruptedException {
    return clientCache.getClient(jobID)
        .getTaskReports(jobID, taskType);
  }

  @Override
  public void killJob(JobID arg0) throws IOException, InterruptedException {
    if (!clientCache.getClient(arg0).killJob(arg0)) {
    resMgrDelegate.killApplication(TypeConverter.toYarn(arg0).getAppId());
  }
  }

  @Override
  public boolean killTask(TaskAttemptID arg0, boolean arg1) throws IOException,
      InterruptedException {
    return clientCache.getClient(arg0.getJobID()).killTask(arg0, arg1);
  }

  @Override
  public AccessControlList getQueueAdmins(String arg0) throws IOException {
    return new AccessControlList("*");
  }

  @Override
  public JobTrackerStatus getJobTrackerStatus() throws IOException,
      InterruptedException {
    return JobTrackerStatus.RUNNING;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion,
        clientMethodsHash);
  }
}
