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

package org.apache.hadoop.mapreduce.v2.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.ContainerLogAppender;
import org.apache.hadoop.yarn.ContainerRollingLogAppender;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * Helper class for MR applications
 */
@Private
@Unstable
public class MRApps extends Apps {
  public static final Log LOG = LogFactory.getLog(MRApps.class);

  public static String toString(JobId jid) {
    return jid.toString();
  }

  public static JobId toJobID(String jid) {
    return TypeConverter.toYarn(JobID.forName(jid));
  }

  public static String toString(TaskId tid) {
    return tid.toString();
  }

  public static TaskId toTaskID(String tid) {
    return TypeConverter.toYarn(TaskID.forName(tid));
  }

  public static String toString(TaskAttemptId taid) {
    return taid.toString(); 
  }

  public static TaskAttemptId toTaskAttemptID(String taid) {
    return TypeConverter.toYarn(TaskAttemptID.forName(taid));
  }

  public static String taskSymbol(TaskType type) {
    switch (type) {
      case MAP:           return "m";
      case REDUCE:        return "r";
    }
    throw new YarnRuntimeException("Unknown task type: "+ type.toString());
  }

  public static enum TaskAttemptStateUI {
    NEW(
        new TaskAttemptState[] { TaskAttemptState.NEW,
            TaskAttemptState.STARTING }),
    RUNNING(
        new TaskAttemptState[] { TaskAttemptState.RUNNING,
            TaskAttemptState.COMMIT_PENDING }),
    SUCCESSFUL(new TaskAttemptState[] { TaskAttemptState.SUCCEEDED}),
    FAILED(new TaskAttemptState[] { TaskAttemptState.FAILED}),
    KILLED(new TaskAttemptState[] { TaskAttemptState.KILLED});

    private final List<TaskAttemptState> correspondingStates;

    private TaskAttemptStateUI(TaskAttemptState[] correspondingStates) {
      this.correspondingStates = Arrays.asList(correspondingStates);
    }

    public boolean correspondsTo(TaskAttemptState state) {
      return this.correspondingStates.contains(state);
    }
  }

  public static enum TaskStateUI {
    RUNNING(
        new TaskState[]{TaskState.RUNNING}),
    PENDING(new TaskState[]{TaskState.SCHEDULED}),
    COMPLETED(new TaskState[]{TaskState.SUCCEEDED, TaskState.FAILED, TaskState.KILLED});

    private final List<TaskState> correspondingStates;

    private TaskStateUI(TaskState[] correspondingStates) {
      this.correspondingStates = Arrays.asList(correspondingStates);
    }

    public boolean correspondsTo(TaskState state) {
      return this.correspondingStates.contains(state);
    }
  }

  public static TaskType taskType(String symbol) {
    // JDK 7 supports switch on strings
    if (symbol.equals("m")) return TaskType.MAP;
    if (symbol.equals("r")) return TaskType.REDUCE;
    throw new YarnRuntimeException("Unknown task symbol: "+ symbol);
  }

  public static TaskAttemptStateUI taskAttemptState(String attemptStateStr) {
    return TaskAttemptStateUI.valueOf(attemptStateStr);
  }

  public static TaskStateUI taskState(String taskStateStr) {
    return TaskStateUI.valueOf(taskStateStr);
  }

  // gets the base name of the MapReduce framework or null if no
  // framework was configured
  private static String getMRFrameworkName(Configuration conf) {
    String frameworkName = null;
    String framework =
        conf.get(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, "");
    if (!framework.isEmpty()) {
      URI uri;
      try {
        uri = new URI(framework);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Unable to parse '" + framework
            + "' as a URI, check the setting for "
            + MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, e);
      }

      frameworkName = uri.getFragment();
      if (frameworkName == null) {
        frameworkName = new Path(uri).getName();
      }
    }
    return frameworkName;
  }

  private static void setMRFrameworkClasspath(
      Map<String, String> environment, Configuration conf) throws IOException {
    // Propagate the system classpath when using the mini cluster
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      MRApps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          System.getProperty("java.class.path"), conf);
    }
    boolean crossPlatform =
        conf.getBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM,
          MRConfig.DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM);

    // if the framework is specified then only use the MR classpath
    String frameworkName = getMRFrameworkName(conf);
    if (frameworkName == null) {
      // Add standard Hadoop classes
      for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          crossPlatform
              ? YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH
              : YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        MRApps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          c.trim(), conf);
      }
    }

    boolean foundFrameworkInClasspath = (frameworkName == null);
    for (String c : conf.getStrings(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
        crossPlatform ?
            StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_CROSS_PLATFORM_APPLICATION_CLASSPATH)
            : StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH))) {
      MRApps.addToEnvironment(environment, Environment.CLASSPATH.name(),
        c.trim(), conf);
      if (!foundFrameworkInClasspath) {
        foundFrameworkInClasspath = c.contains(frameworkName);
      }
    }

    if (!foundFrameworkInClasspath) {
      throw new IllegalArgumentException(
          "Could not locate MapReduce framework name '" + frameworkName
          + "' in " + MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH);
    }
    // TODO: Remove duplicates.
  }

  @SuppressWarnings("deprecation")
  public static void setClasspath(Map<String, String> environment,
      Configuration conf) throws IOException {
    boolean userClassesTakesPrecedence = 
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);

    String classpathEnvVar =
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false)
        ? Environment.APP_CLASSPATH.name() : Environment.CLASSPATH.name();

    String hadoopClasspathEnvVar = Environment.HADOOP_CLASSPATH.name();

    MRApps.addToEnvironment(environment,
      classpathEnvVar, crossPlatformifyMREnv(conf, Environment.PWD), conf);

    MRApps.addToEnvironment(environment,
        hadoopClasspathEnvVar, crossPlatformifyMREnv(conf, Environment.PWD),
        conf);

    if (!userClassesTakesPrecedence) {
      MRApps.setMRFrameworkClasspath(environment, conf);
    }

    addClasspathToEnv(environment, classpathEnvVar, conf);
    addClasspathToEnv(environment, hadoopClasspathEnvVar, conf);

    // MAPREDUCE-6619, retain $HADOOP_CLASSPATH
    MRApps.addToEnvironment(environment, hadoopClasspathEnvVar,
        System.getenv(hadoopClasspathEnvVar), conf);

    if (userClassesTakesPrecedence) {
      MRApps.setMRFrameworkClasspath(environment, conf);
    }
  }

  @SuppressWarnings("deprecation")
  public static void addClasspathToEnv(Map<String, String> environment,
      String classpathEnvVar, Configuration conf) throws IOException {
    MRApps.addToEnvironment(
        environment,
        classpathEnvVar,
        MRJobConfig.JOB_JAR + Path.SEPARATOR + MRJobConfig.JOB_JAR, conf);
    MRApps.addToEnvironment(
        environment,
        classpathEnvVar,
        MRJobConfig.JOB_JAR + Path.SEPARATOR + "classes" + Path.SEPARATOR,
        conf);

    MRApps.addToEnvironment(
        environment,
        classpathEnvVar,
        MRJobConfig.JOB_JAR + Path.SEPARATOR + "lib" + Path.SEPARATOR + "*",
        conf);

    MRApps.addToEnvironment(
        environment,
        classpathEnvVar,
        crossPlatformifyMREnv(conf, Environment.PWD) + Path.SEPARATOR + "*",
        conf);

    // a * in the classpath will only find a .jar, so we need to filter out
    // all .jars and add everything else
    addToClasspathIfNotJar(DistributedCache.getFileClassPaths(conf),
        DistributedCache.getCacheFiles(conf),
        conf,
        environment, classpathEnvVar);
    addToClasspathIfNotJar(DistributedCache.getArchiveClassPaths(conf),
        DistributedCache.getCacheArchives(conf),
        conf,
        environment, classpathEnvVar);
  }

  /**
   * Add the paths to the classpath if they are not jars
   * @param paths the paths to add to the classpath
   * @param withLinks the corresponding paths that may have a link name in them
   * @param conf used to resolve the paths
   * @param environment the environment to update CLASSPATH in
   * @throws IOException if there is an error resolving any of the paths.
   */
  private static void addToClasspathIfNotJar(Path[] paths,
      URI[] withLinks, Configuration conf,
      Map<String, String> environment,
      String classpathEnvVar) throws IOException {
    if (paths != null) {
      HashMap<Path, String> linkLookup = new HashMap<Path, String>();
      if (withLinks != null) {
        for (URI u: withLinks) {
          Path p = new Path(u);
          FileSystem remoteFS = p.getFileSystem(conf);
          p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
              remoteFS.getWorkingDirectory()));
          String name = (null == u.getFragment())
              ? p.getName() : u.getFragment();
          if (!StringUtils.toLowerCase(name).endsWith(".jar")) {
            linkLookup.put(p, name);
          }
        }
      }
      
      for (Path p : paths) {
        FileSystem remoteFS = p.getFileSystem(conf);
        p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory()));
        String name = linkLookup.get(p);
        if (name == null) {
          name = p.getName();
        }
        if(!StringUtils.toLowerCase(name).endsWith(".jar")) {
          MRApps.addToEnvironment(
              environment,
              classpathEnvVar,
              crossPlatformifyMREnv(conf, Environment.PWD) + Path.SEPARATOR + name, conf);
        }
      }
    }
  }

  /**
   * Creates and sets a {@link ApplicationClassLoader} on the given
   * configuration and as the thread context classloader, if
   * {@link MRJobConfig#MAPREDUCE_JOB_CLASSLOADER} is set to true, and
   * the APP_CLASSPATH environment variable is set.
   * @param conf
   * @throws IOException
   */
  public static void setJobClassLoader(Configuration conf)
      throws IOException {
    setClassLoader(createJobClassLoader(conf), conf);
  }

  /**
   * Creates a {@link ApplicationClassLoader} if
   * {@link MRJobConfig#MAPREDUCE_JOB_CLASSLOADER} is set to true, and
   * the APP_CLASSPATH environment variable is set.
   * @param conf
   * @return the created job classloader, or null if the job classloader is not
   * enabled or the APP_CLASSPATH environment variable is not set
   * @throws IOException
   */
  public static ClassLoader createJobClassLoader(Configuration conf)
      throws IOException {
    ClassLoader jobClassLoader = null;
    if (conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false)) {
      String appClasspath = System.getenv(Environment.APP_CLASSPATH.key());
      if (appClasspath == null) {
        LOG.warn("Not creating job classloader since APP_CLASSPATH is not set.");
      } else {
        LOG.info("Creating job classloader");
        if (LOG.isDebugEnabled()) {
          LOG.debug("APP_CLASSPATH=" + appClasspath);
        }
        String[] systemClasses = getSystemClasses(conf);
        jobClassLoader = createJobClassLoader(appClasspath,
            systemClasses);
      }
    }
    return jobClassLoader;
  }

  /**
   * Sets the provided classloader on the given configuration and as the thread
   * context classloader if the classloader is not null.
   * @param classLoader
   * @param conf
   */
  public static void setClassLoader(ClassLoader classLoader,
      Configuration conf) {
    if (classLoader != null) {
      LOG.info("Setting classloader " + classLoader.getClass().getName() +
          " on the configuration and as the thread context classloader");
      conf.setClassLoader(classLoader);
      Thread.currentThread().setContextClassLoader(classLoader);
    }
  }

  @VisibleForTesting
  static String[] getSystemClasses(Configuration conf) {
    return conf.getTrimmedStrings(
        MRJobConfig.MAPREDUCE_JOB_CLASSLOADER_SYSTEM_CLASSES);
  }

  private static ClassLoader createJobClassLoader(final String appClasspath,
      final String[] systemClasses) throws IOException {
    try {
      return AccessController.doPrivileged(
        new PrivilegedExceptionAction<ClassLoader>() {
          @Override
          public ClassLoader run() throws MalformedURLException {
            return new ApplicationClassLoader(appClasspath,
                MRApps.class.getClassLoader(), Arrays.asList(systemClasses));
          }
      });
    } catch (PrivilegedActionException e) {
      Throwable t = e.getCause();
      if (t instanceof MalformedURLException) {
        throw (MalformedURLException) t;
      }
      throw new IOException(e);
    }
  }

  private static final String STAGING_CONSTANT = ".staging";
  public static Path getStagingAreaDir(Configuration conf, String user) {
    return new Path(conf.get(MRJobConfig.MR_AM_STAGING_DIR,
        MRJobConfig.DEFAULT_MR_AM_STAGING_DIR)
        + Path.SEPARATOR + user + Path.SEPARATOR + STAGING_CONSTANT);
  }

  public static String getJobFile(Configuration conf, String user, 
      org.apache.hadoop.mapreduce.JobID jobId) {
    Path jobFile = new Path(MRApps.getStagingAreaDir(conf, user),
        jobId.toString() + Path.SEPARATOR + MRJobConfig.JOB_CONF_FILE);
    return jobFile.toString();
  }
  
  public static Path getEndJobCommitSuccessFile(Configuration conf, String user,
      JobId jobId) {
    Path endCommitFile = new Path(MRApps.getStagingAreaDir(conf, user),
        jobId.toString() + Path.SEPARATOR + "COMMIT_SUCCESS");
    return endCommitFile;
  }
  
  public static Path getEndJobCommitFailureFile(Configuration conf, String user,
      JobId jobId) {
    Path endCommitFile = new Path(MRApps.getStagingAreaDir(conf, user),
        jobId.toString() + Path.SEPARATOR + "COMMIT_FAIL");
    return endCommitFile;
  }
  
  public static Path getStartJobCommitFile(Configuration conf, String user,
      JobId jobId) {
    Path startCommitFile = new Path(MRApps.getStagingAreaDir(conf, user),
        jobId.toString() + Path.SEPARATOR + "COMMIT_STARTED");
    return startCommitFile;
  }

  @SuppressWarnings("deprecation")
  public static void setupDistributedCache( 
      Configuration conf, 
      Map<String, LocalResource> localResources) 
  throws IOException {
    
    // Cache archives
    parseDistributedCacheArtifacts(conf, localResources,  
        LocalResourceType.ARCHIVE, 
        DistributedCache.getCacheArchives(conf), 
        DistributedCache.getArchiveTimestamps(conf),
        getFileSizes(conf, MRJobConfig.CACHE_ARCHIVES_SIZES), 
        DistributedCache.getArchiveVisibilities(conf));
    
    // Cache files
    parseDistributedCacheArtifacts(conf, 
        localResources,  
        LocalResourceType.FILE, 
        DistributedCache.getCacheFiles(conf),
        DistributedCache.getFileTimestamps(conf),
        getFileSizes(conf, MRJobConfig.CACHE_FILES_SIZES),
        DistributedCache.getFileVisibilities(conf));
  }

  /**
   * Set up the DistributedCache related configs to make
   * {@link DistributedCache#getLocalCacheFiles(Configuration)}
   * and
   * {@link DistributedCache#getLocalCacheArchives(Configuration)}
   * working.
   * @param conf
   * @throws java.io.IOException
   */
  @SuppressWarnings("deprecation")
  public static void setupDistributedCacheLocal(Configuration conf)
      throws IOException {

    String localWorkDir = System.getenv("PWD");
    //        ^ ^ all symlinks are created in the current work-dir

    // Update the configuration object with localized archives.
    URI[] cacheArchives = DistributedCache.getCacheArchives(conf);
    if (cacheArchives != null) {
      List<String> localArchives = new ArrayList<String>();
      for (int i = 0; i < cacheArchives.length; ++i) {
        URI u = cacheArchives[i];
        Path p = new Path(u);
        Path name =
            new Path((null == u.getFragment()) ? p.getName()
                : u.getFragment());
        String linkName = name.toUri().getPath();
        localArchives.add(new Path(localWorkDir, linkName).toUri().getPath());
      }
      if (!localArchives.isEmpty()) {
        conf.set(MRJobConfig.CACHE_LOCALARCHIVES, StringUtils
            .arrayToString(localArchives.toArray(new String[localArchives
                .size()])));
      }
    }

    // Update the configuration object with localized files.
    URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
    if (cacheFiles != null) {
      List<String> localFiles = new ArrayList<String>();
      for (int i = 0; i < cacheFiles.length; ++i) {
        URI u = cacheFiles[i];
        Path p = new Path(u);
        Path name =
            new Path((null == u.getFragment()) ? p.getName()
                : u.getFragment());
        String linkName = name.toUri().getPath();
        localFiles.add(new Path(localWorkDir, linkName).toUri().getPath());
      }
      if (!localFiles.isEmpty()) {
        conf.set(MRJobConfig.CACHE_LOCALFILES,
            StringUtils.arrayToString(localFiles
                .toArray(new String[localFiles.size()])));
      }
    }
  }

  private static String getResourceDescription(LocalResourceType type) {
    if(type == LocalResourceType.ARCHIVE || type == LocalResourceType.PATTERN) {
      return "cache archive (" + MRJobConfig.CACHE_ARCHIVES + ") ";
    }
    return "cache file (" + MRJobConfig.CACHE_FILES + ") ";
  }
  
  private static String toString(org.apache.hadoop.yarn.api.records.URL url) {
    StringBuffer b = new StringBuffer();
    b.append(url.getScheme()).append("://").append(url.getHost());
    if(url.getPort() >= 0) {
      b.append(":").append(url.getPort());
    }
    b.append(url.getFile());
    return b.toString();
  }
  
  // TODO - Move this to MR!
  // Use TaskDistributedCacheManager.CacheFiles.makeCacheFiles(URI[], 
  // long[], boolean[], Path[], FileType)
  @SuppressWarnings("deprecation")
  private static void parseDistributedCacheArtifacts(
      Configuration conf,
      Map<String, LocalResource> localResources,
      LocalResourceType type,
      URI[] uris, long[] timestamps, long[] sizes, boolean visibilities[])
  throws IOException {

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
      
      for (int i = 0; i < uris.length; ++i) {
        URI u = uris[i];
        Path p = new Path(u);
        FileSystem remoteFS = p.getFileSystem(conf);
        p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory()));
        // Add URI fragment or just the filename
        Path name = new Path((null == u.getFragment())
          ? p.getName()
          : u.getFragment());
        if (name.isAbsolute()) {
          throw new IllegalArgumentException("Resource name must be relative");
        }
        String linkName = name.toUri().getPath();
        LocalResource orig = localResources.get(linkName);
        org.apache.hadoop.yarn.api.records.URL url = 
          ConverterUtils.getYarnUrlFromURI(p.toUri());
        if(orig != null && !orig.getResource().equals(url)) {
          LOG.warn(
              getResourceDescription(orig.getType()) + 
              toString(orig.getResource()) + " conflicts with " + 
              getResourceDescription(type) + toString(url) + 
              " This will be an error in Hadoop 2.0");
          continue;
        }
        localResources.put(linkName, LocalResource.newInstance(ConverterUtils
          .getYarnUrlFromURI(p.toUri()), type, visibilities[i]
            ? LocalResourceVisibility.PUBLIC : LocalResourceVisibility.PRIVATE,
          sizes[i], timestamps[i]));
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

  public static String getChildLogLevel(Configuration conf, boolean isMap) {
    if (isMap) {
      return conf.get(
          MRJobConfig.MAP_LOG_LEVEL,
          JobConf.DEFAULT_LOG_LEVEL.toString()
      );
    } else {
      return conf.get(
          MRJobConfig.REDUCE_LOG_LEVEL,
          JobConf.DEFAULT_LOG_LEVEL.toString()
      );
    }
  }
  
  /**
   * Add the JVM system properties necessary to configure
   *  {@link ContainerLogAppender} or
   *  {@link ContainerRollingLogAppender}.
   *
   * @param task for map/reduce, or null for app master
   * @param vargs the argument list to append to
   * @param conf configuration of MR job
   */
  public static void addLog4jSystemProperties(Task task,
      List<String> vargs, Configuration conf) {
    String log4jPropertyFile =
        conf.get(MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE, "");
    if (log4jPropertyFile.isEmpty()) {
      vargs.add("-Dlog4j.configuration=container-log4j.properties");
    } else {
      URI log4jURI = null;
      try {
        log4jURI = new URI(log4jPropertyFile);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
      Path log4jPath = new Path(log4jURI);
      vargs.add("-Dlog4j.configuration="+log4jPath.getName());
    }

    long logSize;
    String logLevel;
    int numBackups;

    if (task == null) {
      logSize = conf.getLong(MRJobConfig.MR_AM_LOG_KB,
          MRJobConfig.DEFAULT_MR_AM_LOG_KB) << 10;
      logLevel = conf.get(
          MRJobConfig.MR_AM_LOG_LEVEL, MRJobConfig.DEFAULT_MR_AM_LOG_LEVEL);
      numBackups = conf.getInt(MRJobConfig.MR_AM_LOG_BACKUPS,
          MRJobConfig.DEFAULT_MR_AM_LOG_BACKUPS);
    } else {
      logSize = TaskLog.getTaskLogLimitBytes(conf);
      logLevel = getChildLogLevel(conf, task.isMapTask());
      numBackups = conf.getInt(MRJobConfig.TASK_LOG_BACKUPS,
          MRJobConfig.DEFAULT_TASK_LOG_BACKUPS);
    }

    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add(
        "-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_SIZE + "=" + logSize);

    if (logSize > 0L && numBackups > 0) {
      // log should be rolled
      vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_BACKUPS + "="
          + numBackups);
      vargs.add("-Dhadoop.root.logger=" + logLevel + ",CRLA");
    } else {
      vargs.add("-Dhadoop.root.logger=" + logLevel + ",CLA");
    }
    vargs.add("-Dhadoop.root.logfile=" + TaskLog.LogName.SYSLOG);

    if (   task != null
        && !task.isMapTask()
        && conf.getBoolean(MRJobConfig.REDUCE_SEPARATE_SHUFFLE_LOG,
               MRJobConfig.DEFAULT_REDUCE_SEPARATE_SHUFFLE_LOG)) {
      final int numShuffleBackups = conf.getInt(MRJobConfig.SHUFFLE_LOG_BACKUPS,
          MRJobConfig.DEFAULT_SHUFFLE_LOG_BACKUPS);
      final long shuffleLogSize = conf.getLong(MRJobConfig.SHUFFLE_LOG_KB,
          MRJobConfig.DEFAULT_SHUFFLE_LOG_KB) << 10;
      final String shuffleLogger = logLevel
          + (shuffleLogSize > 0L && numShuffleBackups > 0
                 ? ",shuffleCRLA"
                 : ",shuffleCLA");

      vargs.add("-D" + MRJobConfig.MR_PREFIX
          + "shuffle.logger=" + shuffleLogger);
      vargs.add("-D" + MRJobConfig.MR_PREFIX
          + "shuffle.logfile=" + TaskLog.LogName.SYSLOG + ".shuffle");
      vargs.add("-D" + MRJobConfig.MR_PREFIX
          + "shuffle.log.filesize=" + shuffleLogSize);
      vargs.add("-D" + MRJobConfig.MR_PREFIX
          + "shuffle.log.backups=" + numShuffleBackups);
    }
  }

  public static void setEnvFromInputString(Map<String, String> env,
      String envString, Configuration conf) {
    String classPathSeparator =
        conf.getBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM,
          MRConfig.DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM)
            ? ApplicationConstants.CLASS_PATH_SEPARATOR : File.pathSeparator;
    Apps.setEnvFromInputString(env, envString, classPathSeparator);
  }

  @Public
  @Unstable
  public static void addToEnvironment(Map<String, String> environment,
      String variable, String value, Configuration conf) {
    String classPathSeparator =
        conf.getBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM,
          MRConfig.DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM)
            ? ApplicationConstants.CLASS_PATH_SEPARATOR : File.pathSeparator;
    Apps.addToEnvironment(environment, variable, value, classPathSeparator);
  }

  public static String crossPlatformifyMREnv(Configuration conf, Environment env) {
    boolean crossPlatform =
        conf.getBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM,
            MRConfig.DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM);
    return crossPlatform ? env.$$() : env.$();
  }
}
