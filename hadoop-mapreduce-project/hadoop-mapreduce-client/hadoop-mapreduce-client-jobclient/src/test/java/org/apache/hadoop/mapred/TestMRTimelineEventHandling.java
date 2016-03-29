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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.TestJobHistoryEventHandler;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;

import org.junit.Assert;
import org.junit.Test;

public class TestMRTimelineEventHandling {

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
  public void testMapreduceJobTimelineServiceEnabled()
      throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, false);
    MiniMRYarnCluster cluster = null;
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
