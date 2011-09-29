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

import static org.apache.hadoop.yarn.util.StringHelper._join;
import static org.apache.hadoop.yarn.util.StringHelper._split;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.BuilderUtils;

/**
 * Helper class for MR applications
 */
@Private
@Unstable
public class MRApps extends Apps {
  public static final String JOB = "job";
  public static final String TASK = "task";
  public static final String ATTEMPT = "attempt";

  public static String toString(JobId jid) {
    return _join(JOB, jid.getAppId().getClusterTimestamp(), jid.getAppId().getId(), jid.getId());
  }

  public static JobId toJobID(String jid) {
    Iterator<String> it = _split(jid).iterator();
    return toJobID(JOB, jid, it);
  }

  // mostly useful for parsing task/attempt id like strings
  public static JobId toJobID(String prefix, String s, Iterator<String> it) {
    ApplicationId appId = toAppID(prefix, s, it);
    shouldHaveNext(prefix, s, it);
    JobId jobId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class);
    jobId.setAppId(appId);
    jobId.setId(Integer.parseInt(it.next()));
    return jobId;
  }

  public static String toString(TaskId tid) {
    return _join("task", tid.getJobId().getAppId().getClusterTimestamp(), tid.getJobId().getAppId().getId(),
                 tid.getJobId().getId(), taskSymbol(tid.getTaskType()), tid.getId());
  }

  public static TaskId toTaskID(String tid) {
    Iterator<String> it = _split(tid).iterator();
    return toTaskID(TASK, tid, it);
  }

  public static TaskId toTaskID(String prefix, String s, Iterator<String> it) {
    JobId jid = toJobID(prefix, s, it);
    shouldHaveNext(prefix, s, it);
    TaskId tid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskId.class);
    tid.setJobId(jid);
    tid.setTaskType(taskType(it.next()));
    shouldHaveNext(prefix, s, it);
    tid.setId(Integer.parseInt(it.next()));
    return tid;
  }

  public static String toString(TaskAttemptId taid) {
    return _join("attempt", taid.getTaskId().getJobId().getAppId().getClusterTimestamp(),
                 taid.getTaskId().getJobId().getAppId().getId(), taid.getTaskId().getJobId().getId(),
                 taskSymbol(taid.getTaskId().getTaskType()), taid.getTaskId().getId(), taid.getId());
  }

  public static TaskAttemptId toTaskAttemptID(String taid) {
    Iterator<String> it = _split(taid).iterator();
    TaskId tid = toTaskID(ATTEMPT, taid, it);
    shouldHaveNext(ATTEMPT, taid, it);
    TaskAttemptId taId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskAttemptId.class);
    taId.setTaskId(tid);
    taId.setId(Integer.parseInt(it.next()));
    return taId;
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
        TaskAttemptState.UNASSIGNED, TaskAttemptState.ASSIGNED }),
    RUNNING(
        new TaskAttemptState[] { TaskAttemptState.RUNNING,
            TaskAttemptState.COMMIT_PENDING,
            TaskAttemptState.SUCCESS_CONTAINER_CLEANUP,
            TaskAttemptState.FAIL_CONTAINER_CLEANUP,
            TaskAttemptState.FAIL_TASK_CLEANUP,
            TaskAttemptState.KILL_CONTAINER_CLEANUP,
            TaskAttemptState.KILL_TASK_CLEANUP }),
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
      Map<String, String> environment) throws IOException {
    InputStream classpathFileStream = null;
    BufferedReader reader = null;
    try {
      // Get yarn mapreduce-app classpath from generated classpath
      // Works if compile time env is same as runtime. Mainly tests.
      ClassLoader thisClassLoader =
          Thread.currentThread().getContextClassLoader();
      String mrAppGeneratedClasspathFile = "mrapp-generated-classpath";
      classpathFileStream =
          thisClassLoader.getResourceAsStream(mrAppGeneratedClasspathFile);
      reader = new BufferedReader(new InputStreamReader(classpathFileStream));
      String cp = reader.readLine();
      if (cp != null) {
        addToEnvironment(environment, Environment.CLASSPATH.name(), cp.trim());
      }
      // Put the file itself on classpath for tasks.
      addToEnvironment(
          environment,
          Environment.CLASSPATH.name(),
          thisClassLoader.getResource(mrAppGeneratedClasspathFile).getFile());

      // Add standard Hadoop classes
      for (String c : ApplicationConstants.APPLICATION_CLASSPATH) {
        addToEnvironment(environment, Environment.CLASSPATH.name(), c);
      }
    } finally {
      if (classpathFileStream != null) {
        classpathFileStream.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
    // TODO: Remove duplicates.
  }
  
  private static final String SYSTEM_PATH_SEPARATOR = 
      System.getProperty("path.separator");

  public static void addToEnvironment(
      Map<String, String> environment, 
      String variable, String value) {
    String val = environment.get(variable);
    if (val == null) {
      val = value;
    } else {
      val = val + SYSTEM_PATH_SEPARATOR + value;
    }
    environment.put(variable, val);
  }

  public static void setClasspath(Map<String, String> environment) 
      throws IOException {
    MRApps.addToEnvironment(
        environment, 
        Environment.CLASSPATH.name(), 
        MRJobConfig.JOB_JAR);
    MRApps.addToEnvironment(
        environment, 
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + Path.SEPARATOR + "*");
    MRApps.setMRFrameworkClasspath(environment);
  }
  
  private static final String STAGING_CONSTANT = ".staging";
  public static Path getStagingAreaDir(Configuration conf, String user) {
    return new Path(
        conf.get(MRJobConfig.MR_AM_STAGING_DIR) + 
        Path.SEPARATOR + user + Path.SEPARATOR + STAGING_CONSTANT);
  }

  public static String getJobFile(Configuration conf, String user, 
      org.apache.hadoop.mapreduce.JobID jobId) {
    Path jobFile = new Path(MRApps.getStagingAreaDir(conf, user),
        jobId.toString() + Path.SEPARATOR + MRJobConfig.JOB_CONF_FILE);
    return jobFile.toString();
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
        DistributedCache.getArchiveVisibilities(conf), 
        DistributedCache.getArchiveClassPaths(conf));
    
    // Cache files
    parseDistributedCacheArtifacts(conf, 
        localResources,  
        LocalResourceType.FILE, 
        DistributedCache.getCacheFiles(conf),
        parseTimeStamps(DistributedCache.getFileTimestamps(conf)),
        getFileSizes(conf, MRJobConfig.CACHE_FILES_SIZES),
        DistributedCache.getFileVisibilities(conf),
        DistributedCache.getFileClassPaths(conf));
  }

  // TODO - Move this to MR!
  // Use TaskDistributedCacheManager.CacheFiles.makeCacheFiles(URI[], 
  // long[], boolean[], Path[], FileType)
  private static void parseDistributedCacheArtifacts(
      Configuration conf,
      Map<String, LocalResource> localResources,
      LocalResourceType type,
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
          FileSystem remoteFS = p.getFileSystem(conf);
          p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
              remoteFS.getWorkingDirectory()));
          classPaths.put(p.toUri().getPath().toString(), p);
        }
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

  public static void setEnvFromInputString(Map<String, String> env,
      String envString) {
    if (envString != null && envString.length() > 0) {
      String childEnvs[] = envString.split(",");
      for (String cEnv : childEnvs) {
        String[] parts = cEnv.split("="); // split on '='
        String value = env.get(parts[0]);
  
        if (value != null) {
          // Replace $env with the child's env constructed by NM's
          // For example: LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp
          value = parts[1].replace("$" + parts[0], value);
        } else {
          // example PATH=$PATH:/tmp
          value = System.getenv(parts[0]);
          if (value != null) {
            // the env key is present in the tt's env
            value = parts[1].replace("$" + parts[0], value);
          } else {
            // check for simple variable substitution
            // for e.g. ROOT=$HOME
            String envValue = System.getenv(parts[1].substring(1)); 
            if (envValue != null) {
              value = envValue;
            } else {
              // the env key is note present anywhere .. simply set it
              // example X=$X:/tmp or X=/tmp
              value = parts[1].replace("$" + parts[0], "");
            }
          }
        }
        addToEnvironment(env, parts[0], value);
      }
    }
  }
  


}
