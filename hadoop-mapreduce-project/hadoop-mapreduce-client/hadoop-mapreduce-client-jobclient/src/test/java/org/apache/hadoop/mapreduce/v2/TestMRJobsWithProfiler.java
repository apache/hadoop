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

package org.apache.hadoop.mapreduce.v2;

import java.io.*;
import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.Assert;

import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assume.assumeFalse;

public class TestMRJobsWithProfiler {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMRJobsWithProfiler.class);

  private static final EnumSet<RMAppState> TERMINAL_RM_APP_STATES =
    EnumSet.of(RMAppState.FINISHED, RMAppState.FAILED, RMAppState.KILLED);

  private static final int PROFILED_TASK_ID = 1;

  private static MiniMRYarnCluster mrCluster;

  private static final Configuration CONF = new Configuration();
  private static final FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(CONF);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static final Path TEST_ROOT_DIR =
    new Path("target",  TestMRJobs.class.getName() + "-tmpDir").
      makeQualified(localFs.getUri(), localFs.getWorkingDirectory());

  private static final Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");

  @BeforeClass
  public static void setup() throws InterruptedException, IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(TestMRJobsWithProfiler.class.getName());
      mrCluster.init(CONF);
      mrCluster.start();
    }

    // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
    // workaround the absent public discache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    localFs.setPermission(APP_JAR, new FsPermission("700"));
  }

  @AfterClass
  public static void tearDown() {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (mrCluster != null) {
      mrCluster.stop();
    }
  }

  @Test (timeout = 150000)
  public void testDefaultProfiler() throws Exception {
    assumeFalse("The hprof agent has been removed since Java 9. Skipping.",
        Shell.isJavaVersionAtLeast(9));
    LOG.info("Starting testDefaultProfiler");
    testProfilerInternal(true);
  }

  @Test (timeout = 150000)
  public void testDifferentProfilers() throws Exception {
    LOG.info("Starting testDefaultProfiler");
    testProfilerInternal(false);
  }

  private void testProfilerInternal(boolean useDefault) throws Exception {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
        + " not found. Not running test.");
      return;
    }

    final SleepJob sleepJob = new SleepJob();
    final JobConf sleepConf = new JobConf(mrCluster.getConfig());

    sleepConf.setProfileEnabled(true);
    sleepConf.setProfileTaskRange(true, String.valueOf(PROFILED_TASK_ID));
    sleepConf.setProfileTaskRange(false, String.valueOf(PROFILED_TASK_ID));

    if (!useDefault) {
      if (Shell.isJavaVersionAtLeast(9)) {
        // use JDK Flight Recorder
        sleepConf.set(MRJobConfig.TASK_MAP_PROFILE_PARAMS,
            "-XX:StartFlightRecording=dumponexit=true,filename=%s");
        sleepConf.set(MRJobConfig.TASK_REDUCE_PROFILE_PARAMS,
            "-XX:StartFlightRecording=dumponexit=true,filename=%s");
      } else {
        // use hprof for map to profile.out
        sleepConf.set(MRJobConfig.TASK_MAP_PROFILE_PARAMS,
            "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,"
                + "file=%s");

        // use Xprof for reduce to stdout
        sleepConf.set(MRJobConfig.TASK_REDUCE_PROFILE_PARAMS, "-Xprof");
      }
    }

    sleepJob.setConf(sleepConf);

    // 2-map-2-reduce SleepJob
    final Job job = sleepJob.createJob(2, 2, 500, 1, 500, 1);
    job.setJarByClass(SleepJob.class);
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.waitForCompletion(true);
    final JobId jobId = TypeConverter.toYarn(job.getJobID());
    final ApplicationId appID = jobId.getAppId();
    int pollElapsed = 0;
    while (true) {
      Thread.sleep(1000);
      pollElapsed += 1000;

      if (TERMINAL_RM_APP_STATES.contains(
        mrCluster.getResourceManager().getRMContext().getRMApps().get(appID)
          .getState())) {
        break;
      }

      if (pollElapsed >= 60000) {
        LOG.warn("application did not reach terminal state within 60 seconds");
        break;
      }
    }
    Assert.assertEquals(RMAppState.FINISHED, mrCluster.getResourceManager()
      .getRMContext().getRMApps().get(appID).getState());

    // Job finished, verify logs
    //
    final Configuration nmConf = mrCluster.getNodeManager(0).getConfig();

    final String appIdStr = appID.toString();
    final String appIdSuffix = appIdStr.substring(
      "application_".length(), appIdStr.length());
    final String containerGlob = "container_" + appIdSuffix + "_*_*";

    final Map<TaskAttemptID,Path> taLogDirs = new HashMap<TaskAttemptID,Path>();
    final Pattern taskPattern = Pattern.compile(
        ".*Task:(attempt_"
      + appIdSuffix + "_[rm]_" + "[0-9]+_[0-9]+).*");
    for (String logDir :
         nmConf.getTrimmedStrings(YarnConfiguration.NM_LOG_DIRS))
    {
      // filter out MRAppMaster and create attemptId->logDir map
      //
      for (FileStatus fileStatus :
          localFs.globStatus(new Path(logDir
            + Path.SEPARATOR + appIdStr
            + Path.SEPARATOR + containerGlob
            + Path.SEPARATOR + TaskLog.LogName.SYSLOG)))
      {
        final BufferedReader br = new BufferedReader(
          new InputStreamReader(localFs.open(fileStatus.getPath())));
        String line;
        while ((line = br.readLine()) != null) {
          final Matcher m = taskPattern.matcher(line);
          if (m.matches()) {
            // found Task done message
            taLogDirs.put(TaskAttemptID.forName(m.group(1)),
              fileStatus.getPath().getParent());
            break;
          }
        }
        br.close();
      }
    }

    Assert.assertEquals(4, taLogDirs.size());  // all 4 attempts found

    // Skip checking the contents because the JFR dumps binary files
    if (Shell.isJavaVersionAtLeast(9)) {
      return;
    }

    for (Map.Entry<TaskAttemptID,Path> dirEntry : taLogDirs.entrySet()) {
      final TaskAttemptID tid = dirEntry.getKey();
      final Path profilePath = new Path(dirEntry.getValue(),
        TaskLog.LogName.PROFILE.toString());
      final Path stdoutPath = new Path(dirEntry.getValue(),
        TaskLog.LogName.STDOUT.toString());
      if (useDefault || tid.getTaskType() == TaskType.MAP) {
        if (tid.getTaskID().getId() == PROFILED_TASK_ID) {
          // verify profile.out
          final BufferedReader br = new BufferedReader(new InputStreamReader(
            localFs.open(profilePath)));
          final String line = br.readLine();
          Assert.assertTrue("No hprof content found!",
            line !=null && line.startsWith("JAVA PROFILE"));
          br.close();
          Assert.assertEquals(0L, localFs.getFileStatus(stdoutPath).getLen());
        } else {
          Assert.assertFalse("hprof file should not exist",
            localFs.exists(profilePath));
        }
      } else {
        Assert.assertFalse("hprof file should not exist",
          localFs.exists(profilePath));
        if (tid.getTaskID().getId() == PROFILED_TASK_ID) {
          // reducer is profiled with Xprof
          final BufferedReader br = new BufferedReader(new InputStreamReader(
            localFs.open(stdoutPath)));
          boolean flatProfFound = false;
          String line;
          while ((line = br.readLine()) != null) {
            if (line.startsWith("Flat profile")) {
              flatProfFound = true;
              break;
            }
          }
          br.close();
          Assert.assertTrue("Xprof flat profile not found!", flatProfFound);
        } else {
          Assert.assertEquals(0L, localFs.getFileStatus(stdoutPath).getLen());
        }
      }
    }
  }
}
