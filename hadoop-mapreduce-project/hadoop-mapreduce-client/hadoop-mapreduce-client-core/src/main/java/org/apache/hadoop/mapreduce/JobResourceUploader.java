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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcNoSuchMethodException;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.util.MRResourceUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.SharedCacheClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is responsible for uploading resources from the client to HDFS
 * that are associated with a MapReduce job.
 */
@Private
@Unstable
class JobResourceUploader {
  protected static final Logger LOG =
      LoggerFactory.getLogger(JobResourceUploader.class);
  private final boolean useWildcard;
  private final FileSystem jtFs;
  private SharedCacheClient scClient = null;
  private SharedCacheConfig scConfig = new SharedCacheConfig();
  private ApplicationId appId = null;
  private Map<URI, FileStatus> statCache = new HashMap<>();

  JobResourceUploader(FileSystem submitFs, boolean useWildcard) {
    this.jtFs = submitFs;
    this.useWildcard = useWildcard;
  }

  public static class MRResourceInfo {
    private MRResource resource;
    private URI uriToUseInJob;
    private FileStatus fileToUseStatus;
    private boolean isFromShareCache;

    private MRResourceInfo() {}

    MRResourceInfo(
        MRResource mrResource, URI resourceToUse, boolean fromShareCache) {
      resource = mrResource;
      isFromShareCache = fromShareCache;
      uriToUseInJob = resourceToUse;
      fileToUseStatus = null;
    }

    @VisibleForTesting
    void initFileStatus(Configuration conf, Map<URI, FileStatus> fileStatusMap)
        throws IOException {
      // No lock here as we assume one instance won't be accessed concurrently.
      if (fileToUseStatus == null) {
        fileToUseStatus = ClientDistributedCacheManager.getFileStatus(
            conf, uriToUseInJob, fileStatusMap);
      }
    }

    public long getUriToUseTimestamp(
        Configuration conf, Map<URI, FileStatus> fileStatusMap)
        throws IOException {
      initFileStatus(conf, fileStatusMap);
      return fileToUseStatus.getModificationTime();
    }

    public long getUriToUseFileSize(
        Configuration conf, Map<URI, FileStatus> fileStatusMap)
        throws IOException {
      initFileStatus(conf, fileStatusMap);
      return fileToUseStatus.getLen();
    }

    public boolean getUploadPolicy(SharedCacheConfig sharedCacheConfig) {
      return determineResourceUploadPolicy(
          sharedCacheConfig, resource, isFromShareCache);
    }

    public LocalResourceVisibility getYarnResourceVisibility() {
      return resource.getYarnLocalResourceVisibility();
    }

    void removeFragmentInUri() {
      String fragment = uriToUseInJob.getFragment();
      if (fragment == null) {
        return;
      }
      String uriStr = uriToUseInJob.toString();
      // hdfs://nn/jar#class1 => hdfs://nn/jar
      uriStr = uriStr.substring(0, uriStr.length() - fragment.length() - 1);
      try {
        uriToUseInJob = new URI(uriStr);
      } catch (URISyntaxException ex) {
        throw new IllegalArgumentException("Should be impossible," +
            "Wrong uri " + uriToUseInJob);
      }
    }

    @Override
    public String toString() {
      return resource.toString() + ", uriToUseInJob:" + uriToUseInJob
          + ", uriToUseInJobStatus" + fileToUseStatus.toString();
    }
  }

  public static boolean determineResourceUploadPolicy(
      SharedCacheConfig scConf, MRResource resource, boolean fromSharedCache) {
    if (!isSharedCacheEnabled(scConf, resource)
        || resource.getResourceVisibility() != MRResourceVisibility.PUBLIC
        || fromSharedCache) {
      // Only public resource is support for the moment.
      return false;
    }
    return true;
  }

  private void initSharedCache(JobID jobid, Configuration conf) {
    this.scConfig.init(conf);
    if (this.scConfig.isSharedCacheEnabled()) {
      this.scClient = createSharedCacheClient(conf);
      appId = jobIDToAppId(jobid);
    }
  }

  /*
   * We added this method so that we could do the conversion between JobId and
   * ApplicationId for the shared cache client. This logic is very similar to
   * the org.apache.hadoop.mapreduce.TypeConverter#toYarn method. We don't use
   * that because mapreduce-client-core can not depend on
   * mapreduce-client-common.
   */
  private ApplicationId jobIDToAppId(JobID jobId) {
    return ApplicationId.newInstance(Long.parseLong(jobId.getJtIdentifier()),
        jobId.getId());
  }

  private void stopSharedCache() {
    if (scClient != null) {
      scClient.stop();
      scClient = null;
    }
  }

  /**
   * Create, initialize and start a new shared cache client.
   */
  @VisibleForTesting
  protected SharedCacheClient createSharedCacheClient(Configuration conf) {
    SharedCacheClient scc = SharedCacheClient.createSharedCacheClient();
    scc.init(conf);
    scc.start();
    return scc;
  }

  /**
   * Upload and configure files, libjars, jobjars, and archives pertaining to
   * the passed job.
   * <p>
   * This client will use the shared cache for libjars, files, archives and
   * jobjars if it is enabled. When shared cache is enabled, it will try to use
   * the shared cache and fall back to the default behavior when the scm isn't
   * available.
   * <p>
   * 1. For the resources that have been successfully shared, we will continue
   * to use them in a shared fashion.
   * <p>
   * 2. For the resources that weren't in the cache and need to be uploaded by
   * NM, we won't ask NM to upload them.
   *
   * @param job the job containing the files to be uploaded
   * @param submitJobDir the submission directory of the job
   * @throws IOException
   */
  public void uploadResources(Job job, Path submitJobDir) throws IOException {
    try {
      initSharedCache(job.getJobID(), job.getConfiguration());
      uploadResourcesInternal(job, submitJobDir);
    } finally {
      stopSharedCache();
    }
  }

  private void initJobAndResourceDirs(Job job, Path submitJobDir)
      throws IOException {
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
    /**
     * Current job staging dir structure is
     * ${yarn.app.mapreduce.am.staging-dir}
     * |  -- $username
     *   | -- .staging  rwx------
     *     | -- $jobID rwx-----x
     *       | -- application  rwx-----
     *         | -- files rwx------
     *         | -- libjars rwx------
     *         | -- archives rwx------
     *       | -- private  rwx-----
     *         | -- files rwx------
     *         | -- libjars rwx------
     *         | -- archives rwx------
     *       | -- public  rwxr-xr-x
     *         | -- files rwxr-xr-x
     *         | -- libjars rwxr-xr-x
     *         | -- archives rwxr-xr-x
     * Job jar will be directly under one of {application, private, public}
     * based on visibility configuration
     */
    // Create the submission directory for the MapReduce job.
    submitJobDir = jtFs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission mapredSysPerms =
        new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION_WORLD_EXCUTABLE);
    mkdirs(jtFs, submitJobDir, mapredSysPerms);
    for (MRResourceVisibility visibility : MRResourceVisibility.values()) {
      FsPermission permission = visibility.getDirPermission();
      Path visibilityPath = new Path(submitJobDir, visibility.getDirName());
      mkdirs(jtFs, visibilityPath, permission);
      for (MRResourceType resourceType : MRResourceType.values()) {
        if (resourceType.getResourceParentDirName().isEmpty()) {
          continue;
        }
        Path resourceParentPath =
            new Path(visibilityPath, resourceType.getResourceParentDirName());
        mkdirs(jtFs, resourceParentPath, permission);
      }
    }
    // Clear stat cache as the size and timestamp are not correct
    statCache.clear();
  }

  private void uploadResourcesInternal(Job job, Path submitJobDir)
      throws IOException {
    Configuration conf = job.getConfiguration();
    if (!(conf.getBoolean(Job.USED_GENERIC_PARSER, false))) {
      LOG.warn("Hadoop command-line option parsing not performed. "
          + "Implement the Tool interface and execute your application "
          + "with ToolRunner to remedy this.");
    }

    initJobAndResourceDirs(job, submitJobDir);

    if (!conf.getBoolean(MRJobConfig.MR_AM_STAGING_DIR_ERASURECODING_ENABLED,
        MRJobConfig.DEFAULT_MR_AM_STAGING_ERASURECODING_ENABLED)) {
      disableErasureCodingForPath(submitJobDir);
    }

    // The method serve three purposes
    // 1. Get the resources that have been added via command line arguments
    // in the GenericOptionsParser (i.e. files, libjars, archives).
    // 2. Merge resources that have been programmatically specified
    // for the shared cache via the Job API.
    // 3. Handle files which are added to via
    // {@link org.apache.hadoop.mapreduce.Job#addCacheFile}.
    // We need to determine file sizes, visibilities, and modification time.
    // Note the API is deprecated, people shouldn't use it any more.
    // We need to handle it for compatibility reason.
    // We should think of fixing it in the future

    // Handle case 3 first as it will clear and reset flags used
    // by following logic. Note the resource visibility is determined
    // by resource's visibility here.
    ClientDistributedCacheManager.determineTimestampsAndCacheVisibilities(
        conf, statCache);

    List<MRResource> filesList =
        MRResourceUtil.getResourceFromMRConfig(MRResourceType.FILE, conf);
    List<MRResource> libjarsList =
        MRResourceUtil.getResourceFromMRConfig(MRResourceType.LIBJAR, conf);
    List<MRResource> archivesList =
        MRResourceUtil.getResourceFromMRConfig(MRResourceType.ARCHIVE, conf);
    MRResource jobJarResource = MRResourceUtil.getJobJar(conf);

    checkLocalizationLimits(
        conf, filesList, libjarsList, archivesList, jobJarResource);
    checkVisibilitySettings(
        conf, filesList, libjarsList, archivesList, jobJarResource);

    short replication =
        (short) conf.getInt(Job.SUBMIT_REPLICATION,
            Job.DEFAULT_SUBMIT_REPLICATION);

    // Note, we do not consider resources in the distributed cache for the
    // shared cache at this time. Only resources specified via the
    // GenericOptionsParser or the jobjar.
    uploadJobJar(job, submitJobDir, jobJarResource, replication);
    uploadFiles(job, submitJobDir, filesList, replication);
    uploadLibJars(job, submitJobDir, libjarsList, replication);
    uploadArchives(job, submitJobDir, archivesList, replication);
    addLog4jToDistributedCache(job, submitJobDir);

    // get DelegationToken for cached file
    ClientDistributedCacheManager.getDelegationTokens(conf,
        job.getCredentials());
  }

  /**
   *
   * @param job
   * @param submitJobDir
   * @param resourceList
   * @param submitReplication
   * @return <ResourceURI, isFromSharedCache> pair
   * @throws IOException
   */
  private List<MRResourceInfo> checkAndUploadMRResources(
      Job job, Path submitJobDir,
      List<MRResource> resourceList, short submitReplication)
      throws IOException {
    List<MRResourceInfo> mrResourceInfos = new ArrayList<>();
    for (MRResource resource : resourceList) {
      mrResourceInfos.add(
          checkAndUploadMRResource(
              job, submitJobDir, resource, submitReplication));
    }
    return mrResourceInfos;
  }

  /**
   *
   * @param job
   * @param submitJobDir
   * @param resource
   * @param submitReplication
   * @return <ResourceURI, isFromSharedCache> pair
   * @throws IOException
   */
  @VisibleForTesting
   MRResourceInfo checkAndUploadMRResource(
       Job job, Path submitJobDir,
       MRResource resource, short submitReplication) throws IOException {
    Configuration conf = job.getConfiguration();
    URI resourceUri = resource.getResourceUri(conf);
    LOG.debug("Check and upload resource:" + resource.toString()
        + ", resource uri:" + resourceUri);
    // TODO: clean up the code of URI and path, now just copy the old logic
    Path jobJarPath = new Path(resourceUri);
    URI uriToUse = null;
    boolean uriFromSharedCache = false;
    if (isSharedCacheEnabled(resource)) {
      uriToUse = useSharedCache(resourceUri, jobJarPath.getName(), conf, true);
      uriFromSharedCache = uriToUse == null ? false : true;
    }

    if (uriToUse == null) {
      Path submissionParentDir =
          resource.getResourceSubmissionParentDir(submitJobDir);
      // We must have a qualified path for the shared cache client. We can
      // assume this is for the local filesystem
      Path newPath =
          copyRemoteFiles(
              submissionParentDir, jobJarPath, conf, submitReplication);
      LOG.debug("New path is:" + newPath
          + ", submissionParentDir:" + submissionParentDir);
      try {
        uriToUse = getPathURI(newPath, resourceUri.getFragment());
      } catch (URISyntaxException ue) {
        // should not throw a uri exception
        throw new IOException(
            "Failed to create a URI (URISyntaxException) for the"
                + " remote path " + newPath
                + ". This was based on the files parameter: " + resource,
            ue);
      }
    }
    return new MRResourceInfo(resource, uriToUse, uriFromSharedCache);
  }

  public boolean isSharedCacheEnabled(MRResource resource) {
    return isSharedCacheEnabled(scConfig, resource);
  }

  public static boolean isSharedCacheEnabled(
      SharedCacheConfig scConf, MRResource resource) {
    if (resource.getResourceVisibility() != MRResourceVisibility.PUBLIC) {
      // Only public resource is supported for the moment.
      return false;
    }
    switch (resource.getResourceType()) {
    case JOBJAR:
      return scConf.isSharedCacheJobjarEnabled();
    case FILE:
      return scConf.isSharedCacheFilesEnabled();
    case LIBJAR:
      return scConf.isSharedCacheLibjarsEnabled();
    case ARCHIVE:
      return scConf.isSharedCacheArchivesEnabled();
    default:
      return false;
    }
  }

  @VisibleForTesting
  void uploadFiles(
      Job job, Path submitJobDir,
      List<MRResource> resourceList, short submitReplication)
      throws IOException {
    if (resourceList.isEmpty()) {
      return;
    }
    Map<String, Boolean> uploadPolicyMap = new HashMap<>();
    Configuration conf = job.getConfiguration();
    for (MRResource resource : resourceList) {
      MRResourceInfo resourceInfo =
          checkAndUploadMRResource(
              job, submitJobDir, resource, submitReplication);
      addFileToJobCache(job, resourceInfo);
      uploadPolicyMap.put(
          resourceInfo.uriToUseInJob.toString(),
          resourceInfo.getUploadPolicy(scConfig));
    }
    job.appendFileSharedCacheUploadPolicies(conf, uploadPolicyMap);
  }

  @VisibleForTesting
  void uploadArchives(
      Job job, Path submitJobDir,
      List<MRResource> resourceList, short submitReplication)
      throws IOException {
    if (resourceList.isEmpty()) {
      return;
    }
    Map<String, Boolean> uploadPolicyMap = new HashMap<>();
    Configuration conf = job.getConfiguration();
    for (MRResource resource : resourceList) {
      MRResourceInfo resourceInfo =
          checkAndUploadMRResource(
              job, submitJobDir, resource, submitReplication);
      DistributedCache.addCacheArchiveWithMeta(
          resourceInfo.uriToUseInJob,
          resourceInfo.getYarnResourceVisibility(),
          resourceInfo.getUriToUseFileSize(conf, statCache),
          resourceInfo.getUriToUseTimestamp(conf, statCache),
          conf);
      uploadPolicyMap.put(
          resourceInfo.uriToUseInJob.toString(),
          resourceInfo.getUploadPolicy(scConfig));
    }
    job.setArchiveSharedCacheUploadPolicies(conf, uploadPolicyMap);
  }

  @VisibleForTesting
  void uploadLibJars(
      Job job, Path submitJobDir,
      List<MRResource> resourceList, short submitReplication)
      throws IOException {
    if (resourceList.isEmpty()) {
      return;
    }
    Configuration conf = job.getConfiguration();

    boolean foundFragment = false;
    List<MRResourceInfo> notFromSharedCacheJars = new LinkedList<>();
    Map<String, Boolean> uploadPolicies = new HashMap<>();
    for (MRResource resource : resourceList) {
      MRResourceInfo resourceInfo =
          checkAndUploadMRResource(
              job, submitJobDir, resource, submitReplication);
      URI resourceUri = resourceInfo.uriToUseInJob;
      if (!foundFragment) {
        // We do not count shared cache paths containing fragments as a
        // "foundFragment." This is because these resources are not in the
        // staging directory and will be added to the distributed cache
        // separately.
        foundFragment = (resourceInfo.uriToUseInJob.getFragment() != null)
            && !resourceInfo.isFromShareCache;
      }
      DistributedCache.addFileToClassPath(
          new Path(resourceUri.getPath()), conf, jtFs, false);
      if (resourceInfo.isFromShareCache) {
        // We simply add this URI to the distributed cache. It will not come
        // from the staging directory (it is in the shared cache), so we
        // must add it to the cache regardless of the wildcard feature.
        addFileToJobCache(job, resourceInfo);
      } else {
        notFromSharedCacheJars.add(resourceInfo);
      }
      uploadPolicies.put(
          resourceInfo.uriToUseInJob.toString(),
          resourceInfo.getUploadPolicy(scConfig));
    }

    if (useWildcard && !foundFragment) {
      // Add the whole directory to the cache using a wild card
      // addLibJarDirs(job, submitJobDir);

      // TODO: Share cache manger got a problem while dealing with wildcards.
      // So will add jar file by file here.
      for (MRResourceInfo resourceInfo : notFromSharedCacheJars) {
        addFileToJobCache(job, resourceInfo);
      }
    } else {
      for (MRResourceInfo resourceInfo : notFromSharedCacheJars) {
        addFileToJobCache(job, resourceInfo);
      }
    }
    job.appendFileSharedCacheUploadPolicies(conf, uploadPolicies);
  }

  @VisibleForTesting
  void addLibJarDirs(Job job, Path submitJobDir) throws IOException {
    Configuration conf = job.getConfiguration();
    Map<String, Boolean> uploadPolicies = new HashMap<>();
    for (MRResourceVisibility visibility : MRResourceVisibility.values()) {
      Path libJarsDirWildcard =
          jtFs.makeQualified(
              new Path(
                  JobSubmissionFiles.getJobDistCacheLibjarsPath(
                      submitJobDir, visibility), DistributedCache.WILDCARD));
      MRResource resource =  new MRResource(
          MRResourceType.LIBJAR, libJarsDirWildcard.toString(), visibility);
      MRResourceInfo resourceInfo = new MRResourceInfo(
          resource, libJarsDirWildcard.toUri(), false);
      addFileToJobCache(job, resourceInfo);
      uploadPolicies.put(libJarsDirWildcard.toString(),
          resourceInfo.getUploadPolicy(scConfig));
      LOG.info("Upload policy for " + libJarsDirWildcard.toString() + " is:"
          + resourceInfo.getUploadPolicy(scConfig));
    }
    job.appendFileSharedCacheUploadPolicies(conf, uploadPolicies);
  }

  private void addFileToJobCache(Job job, MRResourceInfo resourceInfo)
      throws IOException {
    DistributedCache.addCacheFileWithMeta(
        resourceInfo.uriToUseInJob,
        resourceInfo.getYarnResourceVisibility(),
        resourceInfo.getUriToUseTimestamp(job.getConfiguration(), statCache),
        resourceInfo.getUriToUseFileSize(job.getConfiguration(), statCache),
        job.getConfiguration());
  }

  void uploadJobJar(
      Job job, Path submitJobDir,
      MRResource jobJarResource, short submitReplication)
      throws IOException {
    if (jobJarResource == null || jobJarResource.getResourcePathStr() == null) {
      LOG.warn("No job jar file set.  User classes may not be found. "
          + "See Job or Job#setJar(String).");
      return;
    }
    if (job.getJobName().trim().isEmpty()) {
      // use jar name if job is not named.
      job.setJobName(new Path(jobJarResource.getResourcePathStr()).getName());
    }
    MRResourceInfo resourceInfo
        = checkAndUploadMRResource(
            job, submitJobDir, jobJarResource, submitReplication);
    // Remove fragment in uri to keep the old behavior
    resourceInfo.removeFragmentInUri();
    Configuration conf = job.getConfiguration();
    LOG.debug("Jar jar:" + jobJarResource.toString()
        + ", URIToUse:" + resourceInfo.uriToUseInJob
        + ", isFromSharedcache:" + resourceInfo.isFromShareCache);
    // {@link MRJobConfig.CACHE_JOBJAR_VISIBILITY} will be used
    // after 2.9 to replace {@link MRJobConfig.JOBJAR_VISIBILITY}.
    // JOBJAR_VISIBILITY will be user facing only.
    conf.set(MRJobConfig.CACHE_JOBJAR_VISIBILITY,
        jobJarResource.getResourceVisibility().name());
    conf.setBoolean(MRJobConfig.JOBJAR_SHARED_CACHE_UPLOAD_POLICY,
        determineResourceUploadPolicy(
            scConfig, jobJarResource, resourceInfo.isFromShareCache));
    job.setJar(resourceInfo.uriToUseInJob.toString());
  }

  /**
   * Verify the visibility settings to avoid permission violation.
   * For the moment, we only check to make sure resource with
   * public visibilities are public accessible.
   */
  @VisibleForTesting
  void checkVisibilitySettings(
      Configuration conf, List<MRResource> files, List<MRResource> libjars,
      List<MRResource> archives, MRResource jobJar) throws IOException {
    if (jobJar != null) {
      checkResourceVisibilitySettings(conf, jobJar);
    }
    checkResourceVisibilitySettings(conf, files);
    checkResourceVisibilitySettings(conf, libjars);
    checkResourceVisibilitySettings(conf, archives);
  }

  private void checkResourceVisibilitySettings(
      Configuration conf, List<MRResource> resourceList) throws IOException {
    for (MRResource resource : resourceList) {
      checkResourceVisibilitySettings(conf, resource);
    }
  }

  private void checkResourceVisibilitySettings(
      Configuration conf, MRResource resource) throws IOException {
    if (resource.getResourceVisibility() != MRResourceVisibility.PUBLIC) {
      // We will only check public resources for the moment.
      return;
    }
    URI uri = resource.getResourceUri(conf);
    if (uri == null || uri.toString().isEmpty()) {
      throw new IOException(resource.toString() +
          " is illegal, path shouldn't be null or empty");
    }
    // We won't enforce local files to be public accessible to use share cache.
    // For files on HDFS, we have to check whether it's public accessible as
    // node manager will enforce it.
    if (!isLocalFile(uri) && !ClientDistributedCacheManager.isPublic(
        conf, uri, statCache)) {
      throw new IOException(resource.toString() +
          " is not public accessible and can't set to be public");
    }
  }

  private static boolean isLocalFile(URI uri) {
    if (uri.getScheme() == null || uri.getScheme().isEmpty()
        || uri.getScheme().startsWith("file")) {
      return true;
    }
    return false;
  }

  /**
   * Verify that the resources this job is going to localize are within the
   * localization limits. We count all resources towards these limits regardless
   * of where they are coming from (i.e. local, distributed cache, or shared
   * cache).
   * @param
   * @return return
   */
  @VisibleForTesting
  void checkLocalizationLimits(
      Configuration conf, List<MRResource> files, List<MRResource> libjars,
      List<MRResource> archives, MRResource jobJar)
      throws IOException {
    Map<URI, FileStatus> statsCache;
    LimitChecker limitChecker = new LimitChecker(conf);
    if (!limitChecker.hasLimits()) {
      // there are no limits set, so we are done.
      return;
    }
    checkExistingResourceLimits(conf, limitChecker);
    if (jobJar != null) {
      // Keep the behavior compatible with old logic
      exploreResource(conf, jobJar, limitChecker);
    }
    exploreResources(conf, files, limitChecker);
    exploreResources(conf, libjars, limitChecker);
    exploreResources(conf, archives, limitChecker);
  }

  private void checkExistingResourceLimits(
      Configuration conf, LimitChecker limitChecker)
      throws IOException {
    // Get the files and archives that are already in the distributed cache
    Collection<String> dcFiles =
        conf.getStringCollection(MRJobConfig.CACHE_FILES);
    explorePaths(conf, dcFiles, limitChecker);
    Collection<String> dcArchives =
        conf.getStringCollection(MRJobConfig.CACHE_ARCHIVES);
    explorePaths(conf, dcArchives, limitChecker);
  }

  private void explorePaths(
      Configuration conf, Collection<String> paths, LimitChecker limitChecker)
      throws IOException {
    for (String path : paths) {
      explorePath(conf, stringToPath(path), limitChecker);
    }
  }

  private void exploreResources(
      Configuration conf,
      List<MRResource> resourceList,
      LimitChecker limitChecker)
      throws IOException{
    for (MRResource resource : resourceList) {
      exploreResource(conf, resource, limitChecker);
    }
  }

  private void exploreResource(
      Configuration jobConf, MRResource resource, LimitChecker limitChecker)
      throws IOException {
    Path path = stringToPath(resource.getResourcePathStr());
    explorePath(jobConf, path, limitChecker);
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
      LimitChecker limitChecker)
      throws IOException {
    Path pathWithScheme = p;
    if (!pathWithScheme.toUri().isAbsolute()) {
      // the path does not have a scheme, so we assume it is a path from the
      // local filesystem
      FileSystem localFs = FileSystem.getLocal(job);
      pathWithScheme = localFs.makeQualified(p);
    }
    FileStatus status = getFileStatus(job, pathWithScheme);
    if (status.isDirectory()) {
      FileStatus[] statusArray =
          pathWithScheme.getFileSystem(job).listStatus(pathWithScheme);
      for (FileStatus s : statusArray) {
        explorePath(job, s.getPath(), limitChecker);
      }
    } else {
      limitChecker.addFile(pathWithScheme, status.getLen());
    }
  }

  @VisibleForTesting
  FileStatus getFileStatus(Configuration job, Path p) throws IOException {
    URI u = p.toUri();
    FileStatus status = statCache.get(u);
    if (status == null) {
      status = p.getFileSystem(job).getFileStatus(p);
      if (!status.isDirectory()) {
        // Buffer dir could be tricky as we don't change the cache
        // while add files to the dir. So disable it here.
        statCache.put(u, status);
      }
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

  /**
   * Checksum a local resource file and call use for that resource with the scm.
   */
  private URI useSharedCache(URI sourceFile, String resourceName,
      Configuration conf, boolean honorFragment)
      throws IOException {
    if (scClient == null) {
      return null;
    }
    Path filePath = new Path(sourceFile);
    if (getFileStatus(conf, filePath).isDirectory()) {
      LOG.warn("Shared cache does not support directories"
          + " (see YARN-6097)." + " Will not upload " + filePath
          + " to the shared cache.");
      return null;
    }

    String rn = resourceName;
    if (honorFragment) {
      if (sourceFile.getFragment() != null) {
        rn = sourceFile.getFragment();
      }
    }

    // If for whatever reason, we can't even calculate checksum for
    // a resource, something is really wrong with the file system;
    // even non-SCM approach won't work. Let us just throw the exception.
    String checksum = scClient.getFileChecksum(filePath);
    URL url = null;
    try {
      url = scClient.use(this.appId, checksum);
    } catch (YarnException e) {
      LOG.warn("Error trying to contact the shared cache manager,"
          + " disabling the SCMClient for the rest of this job submission", e);
      /*
       * If we fail to contact the SCM, we do not use it for the rest of this
       * JobResourceUploader's life. This prevents us from having to timeout
       * each time we try to upload a file while the SCM is unavailable. Instead
       * we timeout/error the first time and quickly revert to the default
       * behavior without the shared cache. We do this by stopping the shared
       * cache client and setting it to null.
       */
      stopSharedCache();
    }

    if (url != null) {
      // Because we deal with URI's in mapreduce, we need to convert the URL to
      // a URI and add a fragment if necessary.
      URI uri = null;
      try {
        String name = new Path(url.getFile()).getName();
        if (rn != null && !name.equals(rn)) {
          // A name was specified that is different then the URL in the shared
          // cache. Therefore, we need to set the fragment portion of the URI to
          // preserve the user's desired name. We assume that there is no
          // existing fragment in the URL since the shared cache manager does
          // not use fragments.
          uri = new URI(url.getScheme(), url.getUserInfo(), url.getHost(),
              url.getPort(), url.getFile(), null, rn);
        } else {
          uri = new URI(url.getScheme(), url.getUserInfo(), url.getHost(),
              url.getPort(), url.getFile(), null, null);
        }
        return uri;
      } catch (URISyntaxException e) {
        LOG.warn("Error trying to convert URL received from shared cache to"
            + " a URI: " + url.toString());
        return null;
      }
    } else {
      return null;
    }
  }

  private MRResource getLog4jResource(Configuration conf) {
    String log4jPropertyFile =
        conf.get(MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE, "");
    if (log4jPropertyFile.isEmpty()) {
      return null;
    }
    MRResourceVisibility vis =
        MRResourceVisibility.getVisibility(
            conf.get(
                MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE_VISIBILITY));
    if (vis == null) {
      vis = MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE_VISIBILITY_DEFAULT;
    }
    return new MRResource(MRResourceType.FILE, log4jPropertyFile, vis);
  }

  private void addLog4jToDistributedCache(Job job, Path jobSubmitDir)
      throws IOException {
    Configuration conf = job.getConfiguration();
    MRResource log4jPropertyFile = getLog4jResource(conf);
    if (log4jPropertyFile == null) {
      return;
    }
    short replication = (short) conf.getInt(Job.SUBMIT_REPLICATION, 10);
    MRResourceInfo resourceInfo =
        checkAndUploadMRResource(
            job, jobSubmitDir, log4jPropertyFile, replication);
    addFileToJobCache(job, resourceInfo);
    FileSystem fs = FileSystem.get(resourceInfo.uriToUseInJob, conf);
    DistributedCache.addFileToClassPath(
        new Path(resourceInfo.uriToUseInJob), conf, fs, false);
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
          conf, FileSystem.get(newPath.toUri(), conf), false);
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
    if (pathURI.getScheme() == null) {
      FileSystem localFs = FileSystem.getLocal(conf);
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

  private void disableErasureCodingForPath(Path path)
      throws IOException {
    try {
      if (jtFs instanceof DistributedFileSystem) {
        LOG.info("Disabling Erasure Coding for path: " + path);
        DistributedFileSystem dfs = (DistributedFileSystem) jtFs;
        dfs.setErasureCodingPolicy(path,
            SystemErasureCodingPolicies.getReplicationPolicy().getName());
      }
    } catch (RemoteException e) {
      if (!RpcNoSuchMethodException.class.getName().equals(e.getClassName())) {
        throw e;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Ignore disabling erasure coding for path {} because method "
                  + "disableErasureCodingForPath doesn't exist, probably "
                  + "talking to a lower version HDFS.", path.toString(), e);
        }
      }
    }
  }
}
