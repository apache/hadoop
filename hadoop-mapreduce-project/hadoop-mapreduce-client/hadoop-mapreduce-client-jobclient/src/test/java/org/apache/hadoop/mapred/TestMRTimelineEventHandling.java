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

import java.io.File;
import java.io.IOException;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.TestJobHistoryEventHandler;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import org.junit.Assert;
import org.junit.Test;

public class TestMRTimelineEventHandling {

  private static final String TIMELINE_AUX_SERVICE_NAME = "timeline_collector";
  private static final Log LOG =
    LogFactory.getLog(TestMRTimelineEventHandling.class);
  
  @Test
  public void testTimelineServiceStartInMiniCluster() throws Exception {
    Configuration conf = new YarnConfiguration();

    /*
     * Timeline service should not start if the config is set to false
     * Regardless to the value of MAPREDUCE_JOB_EMIT_TIMELINE_DATA
     */
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, true);
    MiniMRYarnCluster cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
        TestMRTimelineEventHandling.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();

      //verify that the timeline service is not started.
      Assert.assertNull("Timeline Service should not have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, false);
    cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
          TestJobHistoryEventHandler.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();

      //verify that the timeline service is not started.
      Assert.assertNull("Timeline Service should not have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }
  }

  @Test
  public void testMRTimelineEventHandling() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, true);
    MiniMRYarnCluster cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
        TestMRTimelineEventHandling.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();
      conf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          MiniYARNCluster.getHostname() + ":"
          + cluster.getApplicationHistoryServer().getPort());
      TimelineStore ts = cluster.getApplicationHistoryServer()
              .getTimelineStore();
      String localPathRoot = System.getProperty("test.build.data",
          "build/test/data");
      Path inDir = new Path(localPathRoot, "input");
      Path outDir = new Path(localPathRoot, "output");
      RunningJob job =
              UtilsForTests.runJobSucceed(new JobConf(conf), inDir, outDir);
      Assert.assertEquals(JobStatus.SUCCEEDED,
              job.getJobStatus().getState().getValue());
      TimelineEntities entities = ts.getEntities("MAPREDUCE_JOB", null, null,
              null, null, null, null, null, null, null);
      Assert.assertEquals(1, entities.getEntities().size());
      TimelineEntity tEntity = entities.getEntities().get(0);
      Assert.assertEquals(job.getID().toString(), tEntity.getEntityId());
      Assert.assertEquals("MAPREDUCE_JOB", tEntity.getEntityType());
      Assert.assertEquals(EventType.AM_STARTED.toString(),
              tEntity.getEvents().get(tEntity.getEvents().size() - 1)
              .getEventType());
      Assert.assertEquals(EventType.JOB_FINISHED.toString(),
              tEntity.getEvents().get(0).getEventType());

      job = UtilsForTests.runJobFail(new JobConf(conf), inDir, outDir);
      Assert.assertEquals(JobStatus.FAILED,
              job.getJobStatus().getState().getValue());
      entities = ts.getEntities("MAPREDUCE_JOB", null, null, null, null, null,
              null, null, null, null);
      Assert.assertEquals(2, entities.getEntities().size());
      tEntity = entities.getEntities().get(0);
      Assert.assertEquals(job.getID().toString(), tEntity.getEntityId());
      Assert.assertEquals("MAPREDUCE_JOB", tEntity.getEntityType());
      Assert.assertEquals(EventType.AM_STARTED.toString(),
              tEntity.getEvents().get(tEntity.getEvents().size() - 1)
              .getEventType());
      Assert.assertEquals(EventType.JOB_FAILED.toString(),
              tEntity.getEvents().get(0).getEventType());
    } finally {
      if (cluster != null) {
        cluster.stop();
      }
    }
  }
  
  @Test
  public void testMRNewTimelineServiceEventHandling() throws Exception {
    LOG.info("testMRNewTimelineServiceEventHandling start.");
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, true);

    // enable new timeline serivce in MR side
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_NEW_TIMELINE_SERVICE_ENABLED, true);

    // enable aux-service based timeline collectors
    conf.set(YarnConfiguration.NM_AUX_SERVICES, TIMELINE_AUX_SERVICE_NAME);
    conf.set(YarnConfiguration.NM_AUX_SERVICES + "." + TIMELINE_AUX_SERVICE_NAME
      + ".class", PerNodeTimelineCollectorsAuxService.class.getName());
    
    conf.setBoolean(YarnConfiguration.SYSTEM_METRICS_PUBLISHER_ENABLED, true);

    MiniMRYarnCluster cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
          TestMRTimelineEventHandling.class.getSimpleName(), 1, true);
      cluster.init(conf);
      cluster.start();
      LOG.info("A MiniMRYarnCluster get start.");

      Path inDir = new Path("input");
      Path outDir = new Path("output");
      LOG.info("Run 1st job which should be successful.");
      RunningJob job =
          UtilsForTests.runJobSucceed(new JobConf(conf), inDir, outDir);
      Assert.assertEquals(JobStatus.SUCCEEDED,
          job.getJobStatus().getState().getValue());

      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(new Configuration(cluster.getConfig()));
      yarnClient.start();
      EnumSet<YarnApplicationState> appStates = 
          EnumSet.allOf(YarnApplicationState.class);
      
      ApplicationId firstAppId = null;
      List<ApplicationReport> apps = yarnClient.getApplications(appStates);
      Assert.assertEquals(apps.size(), 1);
      ApplicationReport appReport = apps.get(0);
      firstAppId = appReport.getApplicationId();

      checkNewTimelineEvent(firstAppId);

      LOG.info("Run 2nd job which should be failed.");
      job = UtilsForTests.runJobFail(new JobConf(conf), inDir, outDir);
      Assert.assertEquals(JobStatus.FAILED,
          job.getJobStatus().getState().getValue());
      
      apps = yarnClient.getApplications(appStates);
      Assert.assertEquals(apps.size(), 2);
      
      ApplicationId secAppId = null;
      secAppId = apps.get(0).getApplicationId() == firstAppId ? 
          apps.get(1).getApplicationId() : apps.get(0).getApplicationId();
      checkNewTimelineEvent(firstAppId);

    } finally {
      if (cluster != null) {
        cluster.stop();
      }
      // Cleanup test file
      String testRoot =
          FileSystemTimelineWriterImpl.DEFAULT_TIMELINE_SERVICE_STORAGE_DIR_ROOT;
      File testRootFolder = new File(testRoot);
      if(testRootFolder.isDirectory()) {
        FileUtils.deleteDirectory(testRootFolder);
      }
      
    }
  }
  
  private void checkNewTimelineEvent(ApplicationId appId) throws IOException {
    String tmpRoot =
        FileSystemTimelineWriterImpl.DEFAULT_TIMELINE_SERVICE_STORAGE_DIR_ROOT
            + "/entities/";

    File tmpRootFolder = new File(tmpRoot);
    
    Assert.assertTrue(tmpRootFolder.isDirectory());
    String basePath = tmpRoot + YarnConfiguration.DEFAULT_RM_CLUSTER_ID + "/" +
        UserGroupInformation.getCurrentUser().getShortUserName() +
        "/" + TimelineUtils.generateDefaultFlowIdBasedOnAppId(appId) +
        "/1/1/" + appId.toString();
    // for this test, we expect MAPREDUCE_JOB and MAPREDUCE_TASK dirs
    String outputDirJob = basePath + "/MAPREDUCE_JOB/";

    File entityFolder = new File(outputDirJob);
    Assert.assertTrue("Job output directory: " + outputDirJob + " is not exist.",
        entityFolder.isDirectory());

    // check for job event file
    String jobEventFileName = appId.toString().replaceAll("application", "job")
        + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;

    String jobEventFilePath = outputDirJob + jobEventFileName;
    File jobEventFile = new File(jobEventFilePath);
    Assert.assertTrue("jobEventFilePath: " + jobEventFilePath + " is not exist.",
        jobEventFile.exists());

    // check for task event file
    String outputDirTask = basePath + "/MAPREDUCE_TASK/";
    File taskFolder = new File(outputDirTask);
    Assert.assertTrue("Task output directory: " + outputDirTask + " is not exist.",
        taskFolder.isDirectory());
    
    String taskEventFileName = appId.toString().replaceAll("application", "task")
        + "_m_000000" + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;

    String taskEventFilePath = outputDirTask + taskEventFileName;
    File taskEventFile = new File(taskEventFilePath);
    Assert.assertTrue("taskEventFileName: " + taskEventFilePath + " is not exist.",
        taskEventFile.exists());
    
    // check for task attempt event file
    String outputDirTaskAttempt = basePath + "/MAPREDUCE_TASK_ATTEMPT/";
    File taskAttemptFolder = new File(outputDirTaskAttempt);
    Assert.assertTrue("TaskAttempt output directory: " + outputDirTaskAttempt + 
        " is not exist.", taskAttemptFolder.isDirectory());
    
    String taskAttemptEventFileName = appId.toString().replaceAll(
        "application", "attempt") + "_m_000000_0" + 
        FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;

    String taskAttemptEventFilePath = outputDirTaskAttempt +
        taskAttemptEventFileName;
    File taskAttemptEventFile = new File(taskAttemptEventFilePath);
    Assert.assertTrue("taskAttemptEventFileName: " + taskAttemptEventFilePath +
        " is not exist.", taskAttemptEventFile.exists());
  }

  @Test
  public void testMapreduceJobTimelineServiceEnabled()
      throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, false);
    MiniMRYarnCluster cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
        TestMRTimelineEventHandling.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();
      conf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          MiniYARNCluster.getHostname() + ":"
          + cluster.getApplicationHistoryServer().getPort());
      TimelineStore ts = cluster.getApplicationHistoryServer()
          .getTimelineStore();

      String localPathRoot = System.getProperty("test.build.data",
          "build/test/data");
      Path inDir = new Path(localPathRoot, "input");
      Path outDir = new Path(localPathRoot, "output");
      RunningJob job =
          UtilsForTests.runJobSucceed(new JobConf(conf), inDir, outDir);
      Assert.assertEquals(JobStatus.SUCCEEDED,
          job.getJobStatus().getState().getValue());
      TimelineEntities entities = ts.getEntities("MAPREDUCE_JOB", null, null,
          null, null, null, null, null, null, null);
      Assert.assertEquals(0, entities.getEntities().size());

      conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, true);
      job = UtilsForTests.runJobSucceed(new JobConf(conf), inDir, outDir);
      Assert.assertEquals(JobStatus.SUCCEEDED,
          job.getJobStatus().getState().getValue());
      entities = ts.getEntities("MAPREDUCE_JOB", null, null, null, null, null,
          null, null, null, null);
      Assert.assertEquals(1, entities.getEntities().size());
      TimelineEntity tEntity = entities.getEntities().get(0);
      Assert.assertEquals(job.getID().toString(), tEntity.getEntityId());
    } finally {
      if (cluster != null) {
        cluster.stop();
      }
    }

    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, true);
    cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
          TestJobHistoryEventHandler.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();
      conf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          MiniYARNCluster.getHostname() + ":"
          + cluster.getApplicationHistoryServer().getPort());
      TimelineStore ts = cluster.getApplicationHistoryServer()
          .getTimelineStore();

      String localPathRoot = System.getProperty("test.build.data",
          "build/test/data");
      Path inDir = new Path(localPathRoot, "input");
      Path outDir = new Path(localPathRoot, "output");

      conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, false);
      RunningJob job =
          UtilsForTests.runJobSucceed(new JobConf(conf), inDir, outDir);
      Assert.assertEquals(JobStatus.SUCCEEDED,
          job.getJobStatus().getState().getValue());
      TimelineEntities entities = ts.getEntities("MAPREDUCE_JOB", null, null,
          null, null, null, null, null, null, null);
      Assert.assertEquals(0, entities.getEntities().size());

      conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, true);
      job = UtilsForTests.runJobSucceed(new JobConf(conf), inDir, outDir);
      Assert.assertEquals(JobStatus.SUCCEEDED,
          job.getJobStatus().getState().getValue());
      entities = ts.getEntities("MAPREDUCE_JOB", null, null, null, null, null,
          null, null, null, null);
      Assert.assertEquals(1, entities.getEntities().size());
      TimelineEntity tEntity = entities.getEntities().get(0);
      Assert.assertEquals(job.getID().toString(), tEntity.getEntityId());
    } finally {
      if (cluster != null) {
        cluster.stop();
      }
    }
  }
}
