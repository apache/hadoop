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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.TestJobHistoryEventHandler;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

public class TestMRTimelineEventHandling {

  private static final String TIMELINE_AUX_SERVICE_NAME = "timeline_collector";
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMRTimelineEventHandling.class);

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

  @SuppressWarnings("deprecation")
  @Test
  public void testMRNewTimelineServiceEventHandling() throws Exception {
    LOG.info("testMRNewTimelineServiceEventHandling start.");

    String testDir =
        new File("target", getClass().getSimpleName() +
            "-test_dir").getAbsolutePath();
    String storageDir =
        testDir + File.separator + "timeline_service_data";

    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    // enable new timeline service
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
        FileSystemTimelineWriterImpl.class, TimelineWriter.class);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, true);
    // set the file system root directory
    conf.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
        storageDir);

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

      Path inDir = new Path(testDir, "input");
      Path outDir = new Path(testDir, "output");
      LOG.info("Run 1st job which should be successful.");
      JobConf successConf = new JobConf(conf);
      successConf.set("dummy_conf1",
          UtilsForTests.createConfigValue(51 * 1024));
      successConf.set("dummy_conf2",
          UtilsForTests.createConfigValue(51 * 1024));
      successConf.set("huge_dummy_conf1",
          UtilsForTests.createConfigValue(101 * 1024));
      successConf.set("huge_dummy_conf2",
          UtilsForTests.createConfigValue(101 * 1024));
      RunningJob job =
          UtilsForTests.runJobSucceed(successConf, inDir, outDir);
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
      UtilsForTests.waitForAppFinished(job, cluster);
      checkNewTimelineEvent(firstAppId, appReport, storageDir);

      LOG.info("Run 2nd job which should be failed.");
      job = UtilsForTests.runJobFail(new JobConf(conf), inDir, outDir);
      Assert.assertEquals(JobStatus.FAILED,
          job.getJobStatus().getState().getValue());

      apps = yarnClient.getApplications(appStates);
      Assert.assertEquals(apps.size(), 2);

      appReport = apps.get(0).getApplicationId().equals(firstAppId) ?
          apps.get(0) : apps.get(1);

      checkNewTimelineEvent(firstAppId, appReport, storageDir);

    } finally {
      if (cluster != null) {
        cluster.stop();
      }
      // Cleanup test file
      File testDirFolder = new File(testDir);
      if(testDirFolder.isDirectory()) {
        FileUtils.deleteDirectory(testDirFolder);
      }

    }
  }

  private void checkNewTimelineEvent(ApplicationId appId,
      ApplicationReport appReport, String storageDir) throws IOException {
    String tmpRoot = storageDir + File.separator + "entities" + File.separator;

    File tmpRootFolder = new File(tmpRoot);

    Assert.assertTrue(tmpRootFolder.isDirectory());
    String basePath = tmpRoot + YarnConfiguration.DEFAULT_RM_CLUSTER_ID +
        File.separator +
        UserGroupInformation.getCurrentUser().getShortUserName() +
        File.separator + appReport.getName() +
        File.separator + TimelineUtils.DEFAULT_FLOW_VERSION +
        File.separator + appReport.getStartTime() +
        File.separator + appId.toString();
    // for this test, we expect MAPREDUCE_JOB and MAPREDUCE_TASK dirs
    String outputDirJob =
        basePath + File.separator + "MAPREDUCE_JOB" + File.separator;

    File entityFolder = new File(outputDirJob);
    Assert.assertTrue("Job output directory: " + outputDirJob +
        " does not exist.",
        entityFolder.isDirectory());

    // check for job event file
    String jobEventFileName = appId.toString().replaceAll("application", "job")
        + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;

    String jobEventFilePath = outputDirJob + jobEventFileName;
    File jobEventFile = new File(jobEventFilePath);
    Assert.assertTrue("jobEventFilePath: " + jobEventFilePath +
        " does not exist.",
        jobEventFile.exists());
    verifyEntity(jobEventFile, EventType.JOB_FINISHED.name(),
        true, false, null, false);
    Set<String> cfgsToCheck = Sets.newHashSet("dummy_conf1", "dummy_conf2",
        "huge_dummy_conf1", "huge_dummy_conf2");
    verifyEntity(jobEventFile, null, false, true, cfgsToCheck, false);

    // for this test, we expect MR job metrics are published in YARN_APPLICATION
    String outputAppDir =
        basePath + File.separator + "YARN_APPLICATION" + File.separator;
    entityFolder = new File(outputAppDir);
    Assert.assertTrue(
        "Job output directory: " + outputAppDir +
        " does not exist.",
        entityFolder.isDirectory());

    // check for job event file
    String appEventFileName = appId.toString()
        + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;

    String appEventFilePath = outputAppDir + appEventFileName;
    File appEventFile = new File(appEventFilePath);
    Assert.assertTrue(
        "appEventFilePath: " + appEventFilePath +
        " does not exist.",
        appEventFile.exists());
    verifyEntity(appEventFile, null, true, false, null, false);
    verifyEntity(appEventFile, null, false, true, cfgsToCheck, false);

    // check for task event file
    String outputDirTask =
        basePath + File.separator + "MAPREDUCE_TASK" + File.separator;
    File taskFolder = new File(outputDirTask);
    Assert.assertTrue("Task output directory: " + outputDirTask +
        " does not exist.",
        taskFolder.isDirectory());

    String taskEventFileName =
        appId.toString().replaceAll("application", "task") +
        "_m_000000" +
        FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;

    String taskEventFilePath = outputDirTask + taskEventFileName;
    File taskEventFile = new File(taskEventFilePath);
    Assert.assertTrue("taskEventFileName: " + taskEventFilePath +
        " does not exist.",
        taskEventFile.exists());
    verifyEntity(taskEventFile, EventType.TASK_FINISHED.name(),
        true, false, null, true);

    // check for task attempt event file
    String outputDirTaskAttempt =
        basePath + File.separator + "MAPREDUCE_TASK_ATTEMPT" + File.separator;
    File taskAttemptFolder = new File(outputDirTaskAttempt);
    Assert.assertTrue("TaskAttempt output directory: " + outputDirTaskAttempt +
        " does not exist.", taskAttemptFolder.isDirectory());

    String taskAttemptEventFileName = appId.toString().replaceAll(
        "application", "attempt") + "_m_000000_0" +
        FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;

    String taskAttemptEventFilePath = outputDirTaskAttempt +
        taskAttemptEventFileName;
    File taskAttemptEventFile = new File(taskAttemptEventFilePath);
    Assert.assertTrue("taskAttemptEventFileName: " + taskAttemptEventFilePath +
        " does not exist.", taskAttemptEventFile.exists());
    verifyEntity(taskAttemptEventFile, EventType.MAP_ATTEMPT_FINISHED.name(),
        true, false, null, true);
  }

  /**
   * Verifies entity by reading the entity file written via FS impl.
   * @param entityFile File to be read.
   * @param eventId Event to be checked.
   * @param chkMetrics If event is not null, this flag determines if metrics
   *     exist when the event is encountered. If event is null, we merely check
   *     if metrics exist in the entity file.
   * @param chkCfg If event is not null, this flag determines if configs
   *     exist when the event is encountered. If event is null, we merely check
   *     if configs exist in the entity file.
   * @param cfgsToVerify a set of configs which should exist in the entity file.
   * @throws IOException
   */
  private void verifyEntity(File entityFile, String eventId,
      boolean chkMetrics, boolean chkCfg, Set<String> cfgsToVerify,
      boolean checkIdPrefix) throws IOException {
    BufferedReader reader = null;
    String strLine;
    try {
      reader = new BufferedReader(new FileReader(entityFile));
      long idPrefix = -1;
      while ((strLine = reader.readLine()) != null) {
        if (strLine.trim().length() > 0) {
          org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
              entity =
                  FileSystemTimelineReaderImpl.getTimelineRecordFromJSON(
                      strLine.trim(),
                      org.apache.hadoop.yarn.api.records.timelineservice.
                          TimelineEntity.class);

          LOG.info("strLine.trim()= " + strLine.trim());
          if (checkIdPrefix) {
            Assert.assertTrue("Entity ID prefix expected to be > 0",
                entity.getIdPrefix() > 0);
            if (idPrefix == -1) {
              idPrefix = entity.getIdPrefix();
            } else {
              Assert.assertEquals("Entity ID prefix should be same across " +
                  "each publish of same entity",
                      idPrefix, entity.getIdPrefix());
            }
          }
          if (eventId == null) {
            // Job metrics are published without any events for
            // ApplicationEntity. There is also possibility that some other
            // ApplicationEntity is published without events, hence loop till
            // its found. Same applies to configs.
            if (chkMetrics && entity.getMetrics().size() > 0) {
              return;
            }
            if (chkCfg && entity.getConfigs().size() > 0) {
              if (cfgsToVerify == null) {
                return;
              } else {
                // Have configs to verify. Keep on removing configs from the set
                // of configs to verify as they are found. When the all the
                // entities have been looped through, we will check if the set
                // is empty or not(indicating if all configs have been found or
                // not).
                for (Iterator<String> itr =
                    cfgsToVerify.iterator(); itr.hasNext();) {
                  String config = itr.next();
                  if (entity.getConfigs().containsKey(config)) {
                    itr.remove();
                  }
                }
                // All the required configs have been verified, so return.
                if (cfgsToVerify.isEmpty()) {
                  return;
                }
              }
            }
          } else {
            for (TimelineEvent event : entity.getEvents()) {
              if (event.getId().equals(eventId)) {
                if (chkMetrics) {
                  assertTrue(entity.getMetrics().size() > 0);
                }
                if (chkCfg) {
                  assertTrue(entity.getConfigs().size() > 0);
                  if (cfgsToVerify != null) {
                    for (String cfg : cfgsToVerify) {
                      assertTrue(entity.getConfigs().containsKey(cfg));
                    }
                  }
                }
                return;
              }
            }
          }
        }
      }
      if (cfgsToVerify != null) {
        assertTrue(cfgsToVerify.isEmpty());
        return;
      }
      fail("Expected event : " + eventId + " not found in the file "
          + entityFile);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMapreduceJobTimelineServiceEnabled()
      throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, false);
    MiniMRYarnCluster cluster = null;
    FileSystem fs = null;
    Path inDir = new Path(GenericTestUtils.getTempPath("input"));
    Path outDir = new Path(GenericTestUtils.getTempPath("output"));
    try {
      fs = FileSystem.get(conf);
      cluster = new MiniMRYarnCluster(
        TestMRTimelineEventHandling.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();
      conf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          MiniYARNCluster.getHostname() + ":"
              + cluster.getApplicationHistoryServer().getPort());
      TimelineStore ts = cluster.getApplicationHistoryServer()
          .getTimelineStore();

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
      deletePaths(fs, inDir, outDir);
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
      deletePaths(fs, inDir, outDir);
    }
  }

  /** Delete input paths recursively. Paths should not be null. */
  private void deletePaths(FileSystem fs, Path... paths) {
    if (fs == null) {
      return;
    }
    for (Path path : paths) {
      try {
        fs.delete(path, true);
      } catch (Exception ignored) {
      }
    }
  }
}
