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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.mapreduce.MRJobConfig.MR_AM_RESOURCE_PREFIX;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;
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
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenSelector;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class enables the current JobClient (0.22 hadoop) to run on YARN.
 */
@SuppressWarnings("unchecked")
public class YARNRunner implements ClientProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(YARNRunner.class);

  private static final String RACK_GROUP = "rack";
  private static final String NODE_IF_RACK_GROUP = "node1";
  private static final String NODE_IF_NO_RACK_GROUP = "node2";

  /**
   * Matches any of the following patterns with capturing groups:
   * <ul>
   *  <li>/rack</li>
   *  <li>/rack/node</li>
   *  <li>node (assumes /default-rack)</li>
   * </ul>
   * The groups can be retrieved using the RACK_GROUP, NODE_IF_RACK_GROUP,
   * and/or NODE_IF_NO_RACK_GROUP group keys.
   */
  private static final Pattern RACK_NODE_PATTERN =
      Pattern.compile(
          String.format("(?<%s>[^/]+?)|(?<%s>/[^/]+?)(?:/(?<%s>[^/]+?))?",
          NODE_IF_NO_RACK_GROUP, RACK_GROUP, NODE_IF_RACK_GROUP));

  private final static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public final static Priority AM_CONTAINER_PRIORITY = recordFactory
      .newRecordInstance(Priority.class);
  static {
    AM_CONTAINER_PRIORITY.setPriority(0);
  }

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
   this(conf, resMgrDelegate, new ClientCache(conf, resMgrDelegate));
  }

  /**
   * Similar to {@link YARNRunner#YARNRunner(Configuration, ResourceMgrDelegate)}
   * but allowing injecting {@link ClientCache}. Enable mocking and testing.
   * @param conf the configuration object
   * @param resMgrDelegate the resource manager delegate
   * @param clientCache the client cache object.
   */
  public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate,
      ClientCache clientCache) {
    this.conf = conf;
    try {
      this.resMgrDelegate = resMgrDelegate;
      this.clientCache = clientCache;
      this.defaultFileContext = FileContext.getFileContext(this.conf);
    } catch (UnsupportedFileSystemException ufe) {
      throw new RuntimeException("Error in instantiating YarnClient", ufe);
    }
  }
  
  @Private
  /**
   * Used for testing mostly.
   * @param resMgrDelegate the resource manager delegate to set to.
   */
  public void setResourceMgrDelegate(ResourceMgrDelegate resMgrDelegate) {
    this.resMgrDelegate = resMgrDelegate;
  }
  
  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Use Token.renew instead");
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

  @VisibleForTesting
  void addHistoryToken(Credentials ts) throws IOException, InterruptedException {
    /* check if we have a hsproxy, if not, no need */
    MRClientProtocol hsProxy = clientCache.getInitializedHSProxy();
    if (UserGroupInformation.isSecurityEnabled() && (hsProxy != null)) {
      /*
       * note that get delegation token was called. Again this is hack for oozie
       * to make sure we add history server delegation tokens to the credentials
       */
      RMDelegationTokenSelector tokenSelector = new RMDelegationTokenSelector();
      Text service = resMgrDelegate.getRMDelegationTokenService();
      if (tokenSelector.selectToken(service, ts.getAllTokens()) != null) {
        Text hsService = SecurityUtil.buildTokenService(hsProxy
            .getConnectAddress());
        if (ts.getToken(hsService) == null) {
          ts.addToken(hsService, getDelegationTokenFromHS(hsProxy));
        }
      }
    }
  }
  
  @VisibleForTesting
  Token<?> getDelegationTokenFromHS(MRClientProtocol hsProxy)
      throws IOException, InterruptedException {
    GetDelegationTokenRequest request = recordFactory
      .newRecordInstance(GetDelegationTokenRequest.class);
    request.setRenewer(Master.getMasterPrincipal(conf));
    org.apache.hadoop.yarn.api.records.Token mrDelegationToken;
    mrDelegationToken = hsProxy.getDelegationToken(request)
        .getDelegationToken();
    return ConverterUtils.convertFromYarn(mrDelegationToken,
        hsProxy.getConnectAddress());
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException, InterruptedException {
    // The token is only used for serialization. So the type information
    // mismatch should be fine.
    return resMgrDelegate.getDelegationToken(renewer);
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
    
    addHistoryToken(ts);

    ApplicationSubmissionContext appContext =
      createApplicationSubmissionContext(conf, jobSubmitDir, ts);

    // Submit to ResourceManager
    try {
      ApplicationId applicationId =
          resMgrDelegate.submitApplication(appContext);

      ApplicationReport appMaster = resMgrDelegate
          .getApplicationReport(applicationId);
      String diagnostics =
          (appMaster == null ?
              "application report is null" : appMaster.getDiagnostics());
      if (appMaster == null
          || appMaster.getYarnApplicationState() == YarnApplicationState.FAILED
          || appMaster.getYarnApplicationState() == YarnApplicationState.KILLED) {
        throw new IOException("Failed to run job : " +
            diagnostics);
      }
      return clientCache.getClient(jobId).getJobStatus(jobId);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  private LocalResource createApplicationResource(FileContext fs, Path p,
      LocalResourceType type) throws IOException {
    return createApplicationResource(fs, p, null, type,
        LocalResourceVisibility.APPLICATION, false);
  }

  private LocalResource createApplicationResource(FileContext fs, Path p,
      String fileSymlink, LocalResourceType type, LocalResourceVisibility viz,
      Boolean uploadToSharedCache) throws IOException {
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    // We need to be careful when converting from path to URL to add a fragment
    // so that the symlink name when localized will be correct.
    Path qualifiedPath =
        fs.getDefaultFileSystem().resolvePath(rsrcStat.getPath());
    URI uriWithFragment = null;
    boolean useFragment = fileSymlink != null && !fileSymlink.equals("");
    try {
      if (useFragment) {
        uriWithFragment = new URI(qualifiedPath.toUri() + "#" + fileSymlink);
      } else {
        uriWithFragment = qualifiedPath.toUri();
      }
    } catch (URISyntaxException e) {
      throw new IOException(
          "Error parsing local resource path."
              + " Path was not able to be converted to a URI: " + qualifiedPath,
          e);
    }
    rsrc.setResource(URL.fromURI(uriWithFragment));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(viz);
    rsrc.setShouldBeUploadedToSharedCache(uploadToSharedCache);
    return rsrc;
  }

  private Map<String, LocalResource> setupLocalResources(Configuration jobConf,
      String jobSubmitDir) throws IOException {
    Map<String, LocalResource> localResources = new HashMap<>();

    Path jobConfPath = new Path(jobSubmitDir, MRJobConfig.JOB_CONF_FILE);

    URL yarnUrlForJobSubmitDir = URL.fromPath(defaultFileContext
        .getDefaultFileSystem().resolvePath(
            defaultFileContext.makeQualified(new Path(jobSubmitDir))));
    LOG.debug("Creating setup context, jobSubmitDir url is "
        + yarnUrlForJobSubmitDir);

    localResources.put(MRJobConfig.JOB_CONF_FILE,
        createApplicationResource(defaultFileContext,
            jobConfPath, LocalResourceType.FILE));
    if (jobConf.get(MRJobConfig.JAR) != null) {
      Path jobJarPath = new Path(jobConf.get(MRJobConfig.JAR));
      // We hard code the job.jar symlink because mapreduce code expects the
      // job.jar to be named that way.
      FileContext fccc =
          FileContext.getFileContext(jobJarPath.toUri(), jobConf);
      LocalResourceVisibility jobJarViz =
          jobConf.getBoolean(MRJobConfig.JOBJAR_VISIBILITY,
              MRJobConfig.JOBJAR_VISIBILITY_DEFAULT)
                  ? LocalResourceVisibility.PUBLIC
                  : LocalResourceVisibility.APPLICATION;
      LocalResource rc = createApplicationResource(
          FileContext.getFileContext(jobJarPath.toUri(), jobConf), jobJarPath,
          MRJobConfig.JOB_JAR, LocalResourceType.PATTERN, jobJarViz,
          jobConf.getBoolean(
                  MRJobConfig.JOBJAR_SHARED_CACHE_UPLOAD_POLICY,
                  MRJobConfig.JOBJAR_SHARED_CACHE_UPLOAD_POLICY_DEFAULT));
      String pattern = conf.getPattern(JobContext.JAR_UNPACK_PATTERN,
          JobConf.UNPACK_JAR_PATTERN_DEFAULT).pattern();
      rc.setPattern(pattern);
      localResources.put(MRJobConfig.JOB_JAR, rc);
    } else {
      // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
      // mapreduce jar itself which is already on the classpath.
      LOG.info("Job jar is not present. "
          + "Not adding any jar to the list of resources.");
    }

    // TODO gross hack
    for (String s : new String[] {
        MRJobConfig.JOB_SPLIT,
        MRJobConfig.JOB_SPLIT_METAINFO }) {
      localResources.put(
          MRJobConfig.JOB_SUBMIT_DIR + "/" + s,
          createApplicationResource(defaultFileContext,
              new Path(jobSubmitDir, s), LocalResourceType.FILE));
    }

    return localResources;
  }

  private List<String> setupAMCommand(Configuration jobConf) {
    List<String> vargs = new ArrayList<>(8);
    vargs.add(MRApps.crossPlatformifyMREnv(jobConf, Environment.JAVA_HOME)
        + "/bin/java");

    Path amTmpDir =
        new Path(MRApps.crossPlatformifyMREnv(conf, Environment.PWD),
            YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + amTmpDir);
    MRApps.addLog4jSystemProperties(null, vargs, conf);

    // Check for Java Lib Path usage in MAP and REDUCE configs
    warnForJavaLibPath(conf.get(MRJobConfig.MAP_JAVA_OPTS, ""),
        "map",
        MRJobConfig.MAP_JAVA_OPTS,
        MRJobConfig.MAP_ENV);
    warnForJavaLibPath(conf.get(MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS, ""),
        "map",
        MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS,
        MRJobConfig.MAPRED_ADMIN_USER_ENV);
    warnForJavaLibPath(conf.get(MRJobConfig.REDUCE_JAVA_OPTS, ""),
        "reduce",
        MRJobConfig.REDUCE_JAVA_OPTS,
        MRJobConfig.REDUCE_ENV);
    warnForJavaLibPath(conf.get(MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS, ""),
        "reduce",
        MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS,
        MRJobConfig.MAPRED_ADMIN_USER_ENV);

    // Add AM admin command opts before user command opts
    // so that it can be overridden by user
    String mrAppMasterAdminOptions = conf.get(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_ADMIN_COMMAND_OPTS);
    warnForJavaLibPath(mrAppMasterAdminOptions, "app master",
        MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS, MRJobConfig.MR_AM_ADMIN_USER_ENV);
    vargs.add(mrAppMasterAdminOptions);

    // Add AM user command opts
    String mrAppMasterUserOptions = conf.get(MRJobConfig.MR_AM_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_COMMAND_OPTS);
    warnForJavaLibPath(mrAppMasterUserOptions, "app master",
        MRJobConfig.MR_AM_COMMAND_OPTS, MRJobConfig.MR_AM_ENV);
    vargs.add(mrAppMasterUserOptions);

    if (jobConf.getBoolean(MRJobConfig.MR_AM_PROFILE,
        MRJobConfig.DEFAULT_MR_AM_PROFILE)) {
      final String profileParams = jobConf.get(MRJobConfig.MR_AM_PROFILE_PARAMS,
          MRJobConfig.DEFAULT_TASK_PROFILE_PARAMS);
      if (profileParams != null) {
        vargs.add(String.format(profileParams,
            ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR
                + TaskLog.LogName.PROFILE));
      }
    }

    vargs.add(MRJobConfig.APPLICATION_MASTER_CLASS);
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        Path.SEPARATOR + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        Path.SEPARATOR + ApplicationConstants.STDERR);
    return vargs;
  }

  private ContainerLaunchContext setupContainerLaunchContextForAM(
      Configuration jobConf, Map<String, LocalResource> localResources,
      ByteBuffer securityTokens, List<String> vargs) throws IOException {

    Vector<String> vargsFinal = new Vector<>(8);
    // Final command
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    vargsFinal.add(mergedCommand.toString());

    LOG.debug("Command to launch container for ApplicationMaster is : "
        + mergedCommand);

    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    Map<String, String> environment = new HashMap<>();
    MRApps.setClasspath(environment, conf);

    // Shell
    environment.put(Environment.SHELL.name(),
        conf.get(MRJobConfig.MAPRED_ADMIN_USER_SHELL,
            MRJobConfig.DEFAULT_SHELL));

    // Add the container working directory in front of LD_LIBRARY_PATH
    MRApps.addToEnvironment(environment, Environment.LD_LIBRARY_PATH.name(),
        MRApps.crossPlatformifyMREnv(conf, Environment.PWD), conf);

    // Setup the environment variables for Admin first
    MRApps.setEnvFromInputProperty(environment,
        MRJobConfig.MR_AM_ADMIN_USER_ENV,
        MRJobConfig.DEFAULT_MR_AM_ADMIN_USER_ENV,
        conf);
    // Setup the environment variables (LD_LIBRARY_PATH, etc)
    MRApps.setEnvFromInputProperty(environment, MRJobConfig.MR_AM_ENV, null,
        conf);

    // Parse distributed cache
    MRApps.setupDistributedCache(jobConf, localResources);

    Map<ApplicationAccessType, String> acls = new HashMap<>(2);
    acls.put(ApplicationAccessType.VIEW_APP, jobConf.get(
        MRJobConfig.JOB_ACL_VIEW_JOB, MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));
    acls.put(ApplicationAccessType.MODIFY_APP, jobConf.get(
        MRJobConfig.JOB_ACL_MODIFY_JOB,
        MRJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));

    return ContainerLaunchContext.newInstance(localResources, environment,
        vargsFinal, null, securityTokens, acls);
  }

  /**
   * Constructs all the necessary information to start the MR AM.
   * @param jobConf the configuration for the MR job
   * @param jobSubmitDir the directory path for the job
   * @param ts the security credentials for the job
   * @return ApplicationSubmissionContext
   * @throws IOException on IO error (e.g. path resolution)
   */
  public ApplicationSubmissionContext createApplicationSubmissionContext(
      Configuration jobConf, String jobSubmitDir, Credentials ts)
      throws IOException {
    ApplicationId applicationId = resMgrDelegate.getApplicationId();

    // Setup LocalResources
    Map<String, LocalResource> localResources =
        setupLocalResources(jobConf, jobSubmitDir);

    // Setup security tokens
    DataOutputBuffer dob = new DataOutputBuffer();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens =
        ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Setup ContainerLaunchContext for AM container
    List<String> vargs = setupAMCommand(jobConf);
    ContainerLaunchContext amContainer = setupContainerLaunchContextForAM(
        jobConf, localResources, securityTokens, vargs);

    String regex = conf.get(MRJobConfig.MR_JOB_SEND_TOKEN_CONF);
    if (regex != null && !regex.isEmpty()) {
      setTokenRenewerConf(amContainer, conf, regex);
    }


    Collection<String> tagsFromConf =
        jobConf.getTrimmedStringCollection(MRJobConfig.JOB_TAGS);

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    appContext.setApplicationId(applicationId);                // ApplicationId
    appContext.setQueue(                                       // Queue name
        jobConf.get(JobContext.QUEUE_NAME,
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    // add reservationID if present
    ReservationId reservationID = null;
    try {
      reservationID =
          ReservationId.parseReservationId(jobConf
              .get(JobContext.RESERVATION_ID));
    } catch (NumberFormatException e) {
      // throw exception as reservationid as is invalid
      String errMsg =
          "Invalid reservationId: " + jobConf.get(JobContext.RESERVATION_ID)
              + " specified for the app: " + applicationId;
      LOG.warn(errMsg);
      throw new IOException(errMsg);
    }
    if (reservationID != null) {
      appContext.setReservationID(reservationID);
      LOG.info("SUBMITTING ApplicationSubmissionContext app:" + applicationId
          + " to queue:" + appContext.getQueue() + " with reservationId:"
          + appContext.getReservationID());
    }
    appContext.setApplicationName(                             // Job name
        jobConf.get(JobContext.JOB_NAME,
        YarnConfiguration.DEFAULT_APPLICATION_NAME));
    appContext.setCancelTokensWhenComplete(
        conf.getBoolean(MRJobConfig.JOB_CANCEL_DELEGATION_TOKEN, true));
    appContext.setAMContainerSpec(amContainer);         // AM Container
    appContext.setMaxAppAttempts(
        conf.getInt(MRJobConfig.MR_AM_MAX_ATTEMPTS,
            MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS));

    // Setup the AM ResourceRequests
    List<ResourceRequest> amResourceRequests = generateResourceRequests();
    appContext.setAMContainerResourceRequests(amResourceRequests);

    // set labels for the AM container requests if present
    String amNodelabelExpression = conf.get(MRJobConfig.AM_NODE_LABEL_EXP);
    if (null != amNodelabelExpression
        && amNodelabelExpression.trim().length() != 0) {
      for (ResourceRequest amResourceRequest : amResourceRequests) {
        amResourceRequest.setNodeLabelExpression(amNodelabelExpression.trim());
      }
    }
    // set labels for the Job containers
    appContext.setNodeLabelExpression(jobConf
        .get(JobContext.JOB_NODE_LABEL_EXP));

    appContext.setApplicationType(MRJobConfig.MR_APPLICATION_TYPE);
    if (tagsFromConf != null && !tagsFromConf.isEmpty()) {
      appContext.setApplicationTags(new HashSet<>(tagsFromConf));
    }

    String jobPriority = jobConf.get(MRJobConfig.PRIORITY);
    if (jobPriority != null) {
      int iPriority;
      try {
        iPriority = TypeConverter.toYarnApplicationPriority(jobPriority);
      } catch (IllegalArgumentException e) {
        iPriority = Integer.parseInt(jobPriority);
      }
      appContext.setPriority(Priority.newInstance(iPriority));
    }

    return appContext;
  }

  private List<ResourceRequest> generateResourceRequests() throws IOException {
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    boolean memorySet = false;
    boolean cpuVcoresSet = false;
    List<ResourceInformation> resourceRequests = ResourceUtils
        .getRequestedResourcesFromConfig(conf, MR_AM_RESOURCE_PREFIX);
    for (ResourceInformation resourceReq : resourceRequests) {
      String resourceName = resourceReq.getName();
      if (MRJobConfig.RESOURCE_TYPE_NAME_MEMORY.equals(resourceName) ||
          MRJobConfig.RESOURCE_TYPE_ALTERNATIVE_NAME_MEMORY.equals(
              resourceName)) {
        if (memorySet) {
          throw new IllegalArgumentException(
              "Only one of the following keys " +
                  "can be specified for a single job: " +
                  MRJobConfig.RESOURCE_TYPE_ALTERNATIVE_NAME_MEMORY + ", " +
                  MRJobConfig.RESOURCE_TYPE_NAME_MEMORY);
        }
        String units = isEmpty(resourceReq.getUnits()) ?
            ResourceUtils.getDefaultUnit(ResourceInformation.MEMORY_URI) :
              resourceReq.getUnits();
        capability.setMemorySize(
            UnitsConversionUtil.convert(units, "Mi", resourceReq.getValue()));
        memorySet = true;
        if (conf.get(MRJobConfig.MR_AM_VMEM_MB) != null) {
          LOG.warn("Configuration " + MR_AM_RESOURCE_PREFIX +
              resourceName + "=" + resourceReq.getValue() +
              resourceReq.getUnits() + " is overriding the " +
              MRJobConfig.MR_AM_VMEM_MB + "=" +
              conf.get(MRJobConfig.MR_AM_VMEM_MB) + " configuration");
        }
      } else if (MRJobConfig.RESOURCE_TYPE_NAME_VCORE.equals(resourceName)) {
        capability.setVirtualCores(
            (int) UnitsConversionUtil.convert(resourceReq.getUnits(), "",
                resourceReq.getValue()));
        cpuVcoresSet = true;
        if (conf.get(MRJobConfig.MR_AM_CPU_VCORES) != null) {
          LOG.warn("Configuration " + MR_AM_RESOURCE_PREFIX +
              resourceName + "=" + resourceReq.getValue() +
              resourceReq.getUnits() + " is overriding the " +
              MRJobConfig.MR_AM_CPU_VCORES + "=" +
              conf.get(MRJobConfig.MR_AM_CPU_VCORES) + " configuration");
        }
      } else if (!MRJobConfig.MR_AM_VMEM_MB.equals(
          MR_AM_RESOURCE_PREFIX + resourceName) &&
          !MRJobConfig.MR_AM_CPU_VCORES.equals(
              MR_AM_RESOURCE_PREFIX + resourceName)) {
        // the "mb", "cpu-vcores" resource types are not processed here
        // since the yarn.app.mapreduce.am.resource.mb,
        // yarn.app.mapreduce.am.resource.cpu-vcores keys are used for
        // backward-compatibility - which is handled after this loop
        ResourceInformation resourceInformation = capability
            .getResourceInformation(resourceName);
        resourceInformation.setUnits(resourceReq.getUnits());
        resourceInformation.setValue(resourceReq.getValue());
        capability.setResourceInformation(resourceName, resourceInformation);
      }
    }
    if (!memorySet) {
      capability.setMemorySize(
          conf.getInt(
              MRJobConfig.MR_AM_VMEM_MB, MRJobConfig.DEFAULT_MR_AM_VMEM_MB
          )
      );
    }
    if (!cpuVcoresSet) {
      capability.setVirtualCores(
          conf.getInt(
              MRJobConfig.MR_AM_CPU_VCORES, MRJobConfig.DEFAULT_MR_AM_CPU_VCORES
          )
      );
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("AppMaster capability = " + capability);
    }

    List<ResourceRequest> amResourceRequests = new ArrayList<>();
    // Always have an ANY request
    ResourceRequest amAnyResourceRequest =
        createAMResourceRequest(ResourceRequest.ANY, capability);
    Map<String, ResourceRequest> rackRequests = new HashMap<>();
    amResourceRequests.add(amAnyResourceRequest);
    Collection<String> amStrictResources = conf.getStringCollection(
        MRJobConfig.AM_STRICT_LOCALITY);
    for (String amStrictResource : amStrictResources) {
      amAnyResourceRequest.setRelaxLocality(false);
      Matcher matcher = RACK_NODE_PATTERN.matcher(amStrictResource);
      if (matcher.matches()) {
        String nodeName;
        String rackName = matcher.group(RACK_GROUP);
        if (rackName == null) {
          rackName = "/default-rack";
          nodeName = matcher.group(NODE_IF_NO_RACK_GROUP);
        } else {
          nodeName = matcher.group(NODE_IF_RACK_GROUP);
        }
        ResourceRequest amRackResourceRequest = rackRequests.get(rackName);
        if (amRackResourceRequest == null) {
          amRackResourceRequest = createAMResourceRequest(rackName, capability);
          amResourceRequests.add(amRackResourceRequest);
          rackRequests.put(rackName, amRackResourceRequest);
        }
        if (nodeName != null) {
          amRackResourceRequest.setRelaxLocality(false);
          ResourceRequest amNodeResourceRequest =
              createAMResourceRequest(nodeName, capability);
          amResourceRequests.add(amNodeResourceRequest);
        }
      } else {
        String errMsg =
            "Invalid resource name: " + amStrictResource + " specified.";
        LOG.warn(errMsg);
        throw new IOException(errMsg);
      }
    }
    if (LOG.isDebugEnabled()) {
      for (ResourceRequest amResourceRequest : amResourceRequests) {
        LOG.debug("ResourceRequest: resource = "
            + amResourceRequest.getResourceName() + ", locality = "
            + amResourceRequest.getRelaxLocality());
      }
    }
    return amResourceRequests;
  }

  private ResourceRequest createAMResourceRequest(String resource,
      Resource capability) {
    ResourceRequest resourceRequest =
        recordFactory.newRecordInstance(ResourceRequest.class);
    resourceRequest.setPriority(AM_CONTAINER_PRIORITY);
    resourceRequest.setResourceName(resource);
    resourceRequest.setCapability(capability);
    resourceRequest.setNumContainers(1);
    resourceRequest.setRelaxLocality(true);
    return resourceRequest;
  }

  private void setTokenRenewerConf(ContainerLaunchContext context,
      Configuration conf, String regex) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    Configuration copy = new Configuration(false);
    copy.clear();
    int count = 0;
    for (Map.Entry<String, String> map : conf) {
      String key = map.getKey();
      String val = map.getValue();
      if (key.matches(regex)) {
        copy.set(key, val);
        count++;
      }
    }
    copy.write(dob);
    ByteBuffer appConf = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    LOG.info("Send configurations that match regex expression: " + regex
        + " , total number of configs: " + count + ", total size : " + dob
        .getLength() + " bytes.");
    if (LOG.isDebugEnabled()) {
      for (Iterator<Map.Entry<String, String>> itor = copy.iterator(); itor
          .hasNext(); ) {
        Map.Entry<String, String> entry = itor.next();
        LOG.info(entry.getKey() + " ===> " + entry.getValue());
      }
    }
    context.setTokensConf(appConf);
  }

  @Override
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    ApplicationId appId = TypeConverter.toYarn(arg0).getAppId();
    try {
      resMgrDelegate.updateApplicationPriority(appId,
          Priority.newInstance(Integer.parseInt(arg1)));
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return resMgrDelegate.getProtocolVersion(arg0, arg1);
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Use Token.renew instead");
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

  private void killUnFinishedApplication(ApplicationId appId)
      throws IOException {
    ApplicationReport application = null;
    try {
      application = resMgrDelegate.getApplicationReport(appId);
    } catch (YarnException e) {
      throw new IOException(e);
    }
    if (application.getYarnApplicationState() == YarnApplicationState.FINISHED
        || application.getYarnApplicationState() == YarnApplicationState.FAILED
        || application.getYarnApplicationState() == YarnApplicationState.KILLED) {
      return;
    }
    killApplication(appId);
  }

  private void killApplication(ApplicationId appId) throws IOException {
    try {
      resMgrDelegate.killApplication(appId);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  private boolean isJobInTerminalState(JobStatus status) {
    return status.getState() == JobStatus.State.KILLED
        || status.getState() == JobStatus.State.FAILED
        || status.getState() == JobStatus.State.SUCCEEDED;
  }

  @Override
  public void killJob(JobID arg0) throws IOException, InterruptedException {
    /* check if the status is not running, if not send kill to RM */
    JobStatus status = clientCache.getClient(arg0).getJobStatus(arg0);
    ApplicationId appId = TypeConverter.toYarn(arg0).getAppId();

    // get status from RM and return
    if (status == null) {
      killUnFinishedApplication(appId);
      return;
    }

    if (status.getState() != JobStatus.State.RUNNING) {
      killApplication(appId);
      return;
    }

    try {
      /* send a kill to the AM */
      clientCache.getClient(arg0).killJob(arg0);
      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      long killTimeOut =
          conf.getLong(MRJobConfig.MR_AM_HARD_KILL_TIMEOUT_MS,
                       MRJobConfig.DEFAULT_MR_AM_HARD_KILL_TIMEOUT_MS);
      while ((currentTimeMillis < timeKillIssued + killTimeOut)
          && !isJobInTerminalState(status)) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException ie) {
          /** interrupted, just break */
          break;
        }
        currentTimeMillis = System.currentTimeMillis();
        status = clientCache.getClient(arg0).getJobStatus(arg0);
        if (status == null) {
          killUnFinishedApplication(appId);
          return;
        }
      }
    } catch(IOException io) {
      LOG.debug("Error when checking for application status", io);
    }
    if (status != null && !isJobInTerminalState(status)) {
      killApplication(appId);
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

  @Override
  public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID)
      throws IOException {
    return clientCache.getClient(jobID).getLogFilePath(jobID, taskAttemptID);
  }

  private static void warnForJavaLibPath(String opts, String component, 
      String javaConf, String envConf) {
    if (opts != null && opts.contains("-Djava.library.path")) {
      LOG.warn("Usage of -Djava.library.path in " + javaConf + " can cause " +
               "programs to no longer function if hadoop native libraries " +
               "are used. These values should be set as part of the " +
               "LD_LIBRARY_PATH in the " + component + " JVM env using " +
               envConf + " config settings.");
    }
  }

  public void close() throws IOException {
    if (resMgrDelegate != null) {
      resMgrDelegate.close();
      resMgrDelegate = null;
    }
    if (clientCache != null) {
      clientCache.close();
      clientCache = null;
    }
  }
}
