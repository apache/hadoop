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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TimelineServicePerformance.PerfCounters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
   * Adds simple entities with random string payload, events, metrics, and
   * configuration.
   */
class SimpleEntityWriterV1 extends
    org.apache.hadoop.mapreduce.Mapper<IntWritable,IntWritable,Writable,Writable> {
  private static final Log LOG = LogFactory.getLog(SimpleEntityWriterV1.class);

  // constants for mtype = 1
  static final String KBS_SENT = "kbs sent";
  static final int KBS_SENT_DEFAULT = 1;
  static final String TEST_TIMES = "testtimes";
  static final int TEST_TIMES_DEFAULT = 100;
  static final String TIMELINE_SERVICE_PERFORMANCE_RUN_ID =
      "timeline.server.performance.run.id";
  /**
   *  To ensure that the compression really gets exercised, generate a
   *  random alphanumeric fixed length payload
   */
  private static char[] ALPHA_NUMS = new char[] { 'a', 'b', 'c', 'd', 'e', 'f',
    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
    's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D',
    'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '1', '2',
    '3', '4', '5', '6', '7', '8', '9', '0', ' ' };

  public void map(IntWritable key, IntWritable val, Context context) throws IOException {
    TimelineClient tlc = new TimelineClientImpl();
    Configuration conf = context.getConfiguration();

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
      entity.setEntityId(entId);
      entity.setEntityType("FOO_ATTEMPT");
      entity.addOtherInfo("PERF_TEST", payLoad);
      // add an event
      TimelineEvent event = new TimelineEvent();
      event.setTimestamp(System.currentTimeMillis());
      event.setEventType("foo_event");
      entity.addEvent(event);

      // use the current user for this purpose
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      long startWrite = System.nanoTime();
      try {
        tlc.putEntities(entity);
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
  }
}
