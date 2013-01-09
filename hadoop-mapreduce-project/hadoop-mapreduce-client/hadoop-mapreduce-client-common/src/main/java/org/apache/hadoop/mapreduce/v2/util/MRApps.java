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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.yarn.ContainerLogAppender;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ApplicationClassLoader;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.BuilderUtils;
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
    throw new YarnException("Unknown task type: "+ type.toString());
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

  public static TaskType taskType(String symbol) {
    // JDK 7 supports switch on strings
    if (symbol.equals("m")) return TaskType.MAP;
    if (symbol.equals("r")) return TaskType.REDUCE;
    throw new YarnException("Unknown task symbol: "+ symbol);
  }

  public static TaskAttemptStateUI taskAttemptState(String attemptStateStr) {
    return TaskAttemptStateUI.valueOf(attemptStateStr);
  }

  private static void setMRFrameworkClasspath(
      Map<String, String> environment, Configuration conf) throws IOException {
    // Propagate the system classpath when using the mini cluster
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          System.getProperty("java.class.path"));
    }

    // Add standard Hadoop classes
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), c
          .trim());
    }
    for (String c : conf.getStrings(
        MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
        MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), c
          .trim());
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

    Apps.addToEnvironment(environment,
      classpathEnvVar,
      Environment.PWD.$());
    if (!userClassesTakesPrecedence) {
      MRApps.setMRFrameworkClasspath(environment, conf);
    }
    Apps.addToEnvironment(
        environment,
        classpathEnvVar,
        MRJobConfig.JOB_JAR + Path.SEPARATOR + MRJobConfig.JOB_JAR);
    Apps.addToEnvironment(
        environment,
        classpathEnvVar,
        MRJobConfig.JOB_JAR + Path.SEPARATOR + "classes" + Path.SEPARATOR);
    Apps.addToEnvironment(
        environment,
        classpathEnvVar,
        MRJobConfig.JOB_JAR + Path.SEPARATOR + "lib" + Path.SEPARATOR + "*");
    Apps.addToEnvironment(
        environment,
        classpathEnvVar,
        Environment.PWD.$() + Path.SEPARATOR + "*");
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
    if (userClassesTakesPrecedence) {
      MRApps.setMRFrameworkClasspath(environment, conf);
    }
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
          if (!name.toLowerCase().endsWith(".jar")) {
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
        if(!name.toLowerCase().endsWith(".jar")) {
          Apps.addToEnvironment(
              environment,
              classpathEnvVar,
              Environment.PWD.$() + Path.SEPARATOR + name);
        }
      }
    }
  }

  /**
   * Sets a {@link ApplicationClassLoader} on the given configuration and as
   * the context classloader, if
   * {@link MRJobConfig#MAPREDUCE_JOB_CLASSLOADER} is set to true, and
   * the APP_CLASSPATH environment variable is set.
   * @param conf
   * @throws IOException
   */
  public static void setJobClassLoader(Configuration conf)
      throws IOException {
    if (conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false)) {
      String appClasspath = System.getenv(Environment.APP_CLASSPATH.key());
      if (appClasspath == null) {
        LOG.warn("Not using job classloader since APP_CLASSPATH is not set.");
      } else {
        LOG.info("Using job classloader");
        if (LOG.isDebugEnabled()) {
          LOG.debug("APP_CLASSPATH=" + appClasspath);
        }
        String[] systemClasses = conf.getStrings(
            MRJobConfig.MAPREDUCE_JOB_CLASSLOADER_SYSTEM_CLASSES);
        ClassLoader jobClassLoader = createJobClassLoader(appClasspath,
            systemClasses);
        if (jobClassLoader != null) {
          conf.setClassLoader(jobClassLoader);
          Thread.currentThread().setContextClassLoader(jobClassLoader);
        }
      }
    }
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

  private static long[] parseTimeStamps(String[] strs) {
    if (null == strs) {
      return null;
    }
    long[] result = new long[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Long.parseLong(strs[i]);
    }
    return result;
  }

  public static void setupDistributedCache( 
      Configuration conf, 
      Map<String, LocalResource> localResources) 
  throws IOException {
    
    // Cache archives
    parseDistributedCacheArtifacts(conf, localResources,  
        LocalResourceType.ARCHIVE, 
        DistributedCache.getCacheArchives(conf), 
        parseTimeStamps(DistributedCache.getArchiveTimestamps(conf)), 
        getFileSizes(conf, MRJobConfig.CACHE_ARCHIVES_SIZES), 
        DistributedCache.getArchiveVisibilities(conf));
    
    // Cache files
    parseDistributedCacheArtifacts(conf, 
        localResources,  
        LocalResourceType.FILE, 
        DistributedCache.getCacheFiles(conf),
        parseTimeStamps(DistributedCache.getFileTimestamps(conf)),
        getFileSizes(conf, MRJobConfig.CACHE_FILES_SIZES),
        DistributedCache.getFileVisibilities(conf));
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
        localResources.put(
            linkName,
            BuilderUtils.newLocalResource(
                p.toUri(), type, 
                visibilities[i]
                  ? LocalResourceVisibility.PUBLIC
                  : LocalResourceVisibility.PRIVATE,
                sizes[i], timestamps[i])
        );
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
  
  /**
   * Add the JVM system properties necessary to configure {@link ContainerLogAppender}.
   * @param logLevel the desired log level (eg INFO/WARN/DEBUG)
   * @param logSize See {@link ContainerLogAppender#setTotalLogFileSize(long)}
   * @param vargs the argument list to append to
   */
  public static void addLog4jSystemProperties(
      String logLevel, long logSize, List<String> vargs) {
    vargs.add("-Dlog4j.configuration=container-log4j.properties");
    vargs.add("-D" + MRJobConfig.TASK_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add("-D" + MRJobConfig.TASK_LOG_SIZE + "=" + logSize);
    vargs.add("-Dhadoop.root.logger=" + logLevel + ",CLA"); 
  }
}
