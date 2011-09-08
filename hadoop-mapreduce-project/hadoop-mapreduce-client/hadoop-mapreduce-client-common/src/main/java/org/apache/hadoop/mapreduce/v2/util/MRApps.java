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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.MRConstants;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.Apps;

/**
 * Helper class for MR applications
 */
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

  public static void setInitialClasspath(
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
        addToClassPath(environment, cp.trim());
      }
      // Put the file itself on classpath for tasks.
      addToClassPath(environment,
          thisClassLoader.getResource(mrAppGeneratedClasspathFile).getFile());

      // If runtime env is different.
      if (System.getenv().get("YARN_HOME") != null) {
        ShellCommandExecutor exec =
            new ShellCommandExecutor(new String[] {
                System.getenv().get("YARN_HOME") + "/bin/yarn",
            "classpath" });
        exec.execute();
        addToClassPath(environment, exec.getOutput().trim());
      }

      // Get yarn mapreduce-app classpath
      if (System.getenv().get("HADOOP_MAPRED_HOME")!= null) {
        ShellCommandExecutor exec =
            new ShellCommandExecutor(new String[] {
                System.getenv().get("HADOOP_MAPRED_HOME") + "/bin/mapred",
            "classpath" });
        exec.execute();
        addToClassPath(environment, exec.getOutput().trim());
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

  public static void addToClassPath(
      Map<String, String> environment, String fileName) {
    String classpath = environment.get(CLASSPATH);
    if (classpath == null) {
      classpath = fileName;
    } else {
      classpath = classpath + ":" + fileName;
    }
    environment.put(CLASSPATH, classpath);
  }

  public static final String CLASSPATH = "CLASSPATH";

  private static final String STAGING_CONSTANT = ".staging";
  public static Path getStagingAreaDir(Configuration conf, String user) {
    return new Path(
        conf.get(MRConstants.APPS_STAGING_DIR_KEY) +
        Path.SEPARATOR + user + Path.SEPARATOR + STAGING_CONSTANT);
  }

  public static String getJobFile(Configuration conf, String user, 
      org.apache.hadoop.mapreduce.JobID jobId) {
    Path jobFile = new Path(MRApps.getStagingAreaDir(conf, user),
        jobId.toString() + Path.SEPARATOR + MRConstants.JOB_CONF_FILE);
    return jobFile.toString();
  }
}
