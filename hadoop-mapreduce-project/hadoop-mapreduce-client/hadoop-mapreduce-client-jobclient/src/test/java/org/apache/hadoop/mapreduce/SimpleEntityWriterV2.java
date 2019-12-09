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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TimelineServicePerformance.PerfCounters;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.collector.AppLevelTimelineCollector;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds simple entities with random string payload, events, metrics, and
 * configuration.
 */
class SimpleEntityWriterV2 extends EntityWriterV2
    implements SimpleEntityWriterConstants {
  private static final Logger LOG =
      LoggerFactory.getLogger(SimpleEntityWriterV2.class);

  protected void writeEntities(Configuration tlConf,
      TimelineCollectorManager manager, Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    // simulate the app id with the task id
    int taskId = context.getTaskAttemptID().getTaskID().getId();
    long timestamp = conf.getLong(TIMELINE_SERVICE_PERFORMANCE_RUN_ID, 0);
    ApplicationId appId = ApplicationId.newInstance(timestamp, taskId);

    // create the app level timeline collector
    AppLevelTimelineCollector collector =
        new AppLevelTimelineCollector(appId);
    manager.putIfAbsent(appId, collector);

    try {
      // set the context
      // flow id: job name, flow run id: timestamp, user id
      TimelineCollectorContext tlContext =
          collector.getTimelineEntityContext();
      tlContext.setFlowName(context.getJobName());
      tlContext.setFlowRunId(timestamp);
      tlContext.setUserId(context.getUser());

      final int kbs = conf.getInt(KBS_SENT, KBS_SENT_DEFAULT);

      long totalTime = 0;
      final int testtimes = conf.getInt(TEST_TIMES, TEST_TIMES_DEFAULT);
      final Random rand = new Random();
      final TaskAttemptID taskAttemptId = context.getTaskAttemptID();
      final char[] payLoad = new char[kbs * 1024];

      for (int i = 0; i < testtimes; i++) {
        // Generate a fixed length random payload
        for (int xx = 0; xx < kbs * 1024; xx++) {
          int alphaNumIdx =
              rand.nextInt(ALPHA_NUMS.length);
          payLoad[xx] = ALPHA_NUMS[alphaNumIdx];
        }
        String entId = taskAttemptId + "_" + Integer.toString(i);
        final TimelineEntity entity = new TimelineEntity();
        entity.setId(entId);
        entity.setType("FOO_ATTEMPT");
        entity.addInfo("PERF_TEST", payLoad);
        // add an event
        TimelineEvent event = new TimelineEvent();
        event.setId("foo_event_id");
        event.setTimestamp(System.currentTimeMillis());
        event.addInfo("foo_event", "test");
        entity.addEvent(event);
        // add a metric
        TimelineMetric metric = new TimelineMetric();
        metric.setId("foo_metric");
        metric.addValue(System.currentTimeMillis(), 123456789L);
        entity.addMetric(metric);
        // add a config
        entity.addConfig("foo", "bar");

        TimelineEntities entities = new TimelineEntities();
        entities.addEntity(entity);
        // use the current user for this purpose
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        long startWrite = System.nanoTime();
        try {
          collector.putEntities(entities, ugi);
        } catch (Exception e) {
          context.getCounter(PerfCounters.TIMELINE_SERVICE_WRITE_FAILURES).
              increment(1);
          LOG.error("writing to the timeline service failed", e);
        }
        long endWrite = System.nanoTime();
        totalTime += TimeUnit.NANOSECONDS.toMillis(endWrite-startWrite);
      }
      LOG.info("wrote " + testtimes + " entities (" + kbs*testtimes +
          " kB) in " + totalTime + " ms");
      context.getCounter(PerfCounters.TIMELINE_SERVICE_WRITE_TIME).
          increment(totalTime);
      context.getCounter(PerfCounters.TIMELINE_SERVICE_WRITE_COUNTER).
          increment(testtimes);
      context.getCounter(PerfCounters.TIMELINE_SERVICE_WRITE_KBS).
          increment(kbs*testtimes);
    } finally {
      // clean up
      manager.remove(appId);
    }
  }
}
