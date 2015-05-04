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

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SleepJob.SleepInputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.collector.AppLevelTimelineCollector;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;

public class TimelineServicePerformanceV2 extends Configured implements Tool {
  private static final Log LOG =
      LogFactory.getLog(TimelineServicePerformanceV2.class);

  static final int NUM_MAPS_DEFAULT = 1;

  static final int SIMPLE_ENTITY_WRITER = 1;
  // constants for mtype = 1
  static final String KBS_SENT = "kbs sent";
  static final int KBS_SENT_DEFAULT = 1;
  static final String TEST_TIMES = "testtimes";
  static final int TEST_TIMES_DEFAULT = 100;
  static final String TIMELINE_SERVICE_PERFORMANCE_RUN_ID =
      "timeline.server.performance.run.id";

  static int mapperType = SIMPLE_ENTITY_WRITER;

  protected static int printUsage() {
    // TODO is there a way to handle mapper-specific options more gracefully?
    System.err.println(
        "Usage: [-m <maps>] number of mappers (default: " + NUM_MAPS_DEFAULT +
            ")\n" +
        "     [-mtype <mapper type in integer>] \n" +
        "          1. simple entity write mapper\n" +
        "     [-s <(KBs)test>] number of KB per put (default: " +
            KBS_SENT_DEFAULT + " KB)\n" +
        "     [-t] package sending iterations per mapper (default: " +
            TEST_TIMES_DEFAULT + ")\n");
    GenericOptionsParser.printGenericCommandUsage(System.err);
    return -1;
  }

  /**
   * Configure a job given argv.
   */
  public static boolean parseArgs(String[] args, Job job) throws IOException {
    // set the defaults
    Configuration conf = job.getConfiguration();
    conf.setInt(MRJobConfig.NUM_MAPS, NUM_MAPS_DEFAULT);
    conf.setInt(KBS_SENT, KBS_SENT_DEFAULT);
    conf.setInt(TEST_TIMES, TEST_TIMES_DEFAULT);

    for (int i = 0; i < args.length; i++) {
      if (args.length == i + 1) {
        System.out.println("ERROR: Required parameter missing from " + args[i]);
        return printUsage() == 0;
      }
      try {
        if ("-m".equals(args[i])) {
          if (Integer.parseInt(args[++i]) > 0) {
            job.getConfiguration()
                .setInt(MRJobConfig.NUM_MAPS, (Integer.parseInt(args[i])));
          }
        } else if ("-mtype".equals(args[i])) {
          mapperType = Integer.parseInt(args[++i]);
          switch (mapperType) {
          case SIMPLE_ENTITY_WRITER:
            job.setMapperClass(SimpleEntityWriter.class);
            break;
          default:
            job.setMapperClass(SimpleEntityWriter.class);
          }
        } else if ("-s".equals(args[i])) {
          if (Integer.parseInt(args[++i]) > 0) {
            conf.setInt(KBS_SENT, (Integer.parseInt(args[i])));
          }
        } else if ("-t".equals(args[i])) {
          if (Integer.parseInt(args[++i]) > 0) {
            conf.setInt(TEST_TIMES, (Integer.parseInt(args[i])));
          }
        } else {
          System.out.println("Unexpected argument: " + args[i]);
          return printUsage() == 0;
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage() == 0;
      } catch (Exception e) {
        throw (IOException)new IOException().initCause(e);
      }
    }

    return true;
  }

  /**
   * TimelineServer Performance counters
   */
  static enum PerfCounters {
    TIMELINE_SERVICE_WRITE_TIME,
    TIMELINE_SERVICE_WRITE_COUNTER,
    TIMELINE_SERVICE_WRITE_FAILURES,
    TIMELINE_SERVICE_WRITE_KBS,
  }

  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf());
    job.setJarByClass(TimelineServicePerformanceV2.class);
    job.setMapperClass(SimpleEntityWriter.class);
    job.setInputFormatClass(SleepInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    if (!parseArgs(args, job)) {
      return -1;
    }

    // for mtype = 1
    // use the current timestamp as the "run id" of the test: this will be used
    // as simulating the cluster timestamp for apps
    Configuration conf = job.getConfiguration();
    conf.setLong(TIMELINE_SERVICE_PERFORMANCE_RUN_ID,
        System.currentTimeMillis());

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    int ret = job.waitForCompletion(true) ? 0 : 1;
    org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
    long writetime =
        counters.findCounter(PerfCounters.TIMELINE_SERVICE_WRITE_TIME).getValue();
    long writecounts =
        counters.findCounter(PerfCounters.TIMELINE_SERVICE_WRITE_COUNTER).getValue();
    long writesize =
        counters.findCounter(PerfCounters.TIMELINE_SERVICE_WRITE_KBS).getValue();
    double transacrate = writecounts * 1000 / (double)writetime;
    double iorate = writesize * 1000 / (double)writetime;
    int numMaps = Integer.parseInt(conf.get(MRJobConfig.NUM_MAPS));

    System.out.println("TRANSACTION RATE (per mapper): " + transacrate +
        " ops/s");
    System.out.println("IO RATE (per mapper): " + iorate + " KB/s");

    System.out.println("TRANSACTION RATE (total): " + transacrate*numMaps +
        " ops/s");
    System.out.println("IO RATE (total): " + iorate*numMaps + " KB/s");

    return ret;
  }

  public static void main(String[] args) throws Exception {
    int res =
        ToolRunner.run(new Configuration(), new TimelineServicePerformanceV2(),
            args);
    System.exit(res);
  }

  /**
   *  To ensure that the compression really gets exercised, generate a
   *  random alphanumeric fixed length payload
   */
  static final char[] alphaNums = new char[] { 'a', 'b', 'c', 'd', 'e', 'f',
    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
    's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D',
    'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '1', '2',
    '3', '4', '5', '6', '7', '8', '9', '0', ' ' };

  /**
   * Adds simple entities with random string payload, events, metrics, and
   * configuration.
   */
  public static class SimpleEntityWriter
      extends org.apache.hadoop.mapreduce.Mapper<IntWritable,IntWritable,Writable,Writable> {
    public void map(IntWritable key, IntWritable val, Context context)
        throws IOException {

      Configuration conf = context.getConfiguration();
      // simulate the app id with the task id
      int taskId = context.getTaskAttemptID().getTaskID().getId();
      long timestamp = conf.getLong(TIMELINE_SERVICE_PERFORMANCE_RUN_ID, 0);
      ApplicationId appId = ApplicationId.newInstance(timestamp, taskId);

      // create the app level timeline collector
      Configuration tlConf = new YarnConfiguration();
      AppLevelTimelineCollector collector =
          new AppLevelTimelineCollector(appId);
      collector.init(tlConf);
      collector.start();

      try {
        // set the context
        // flow id: job name, flow run id: timestamp, user id
        TimelineCollectorContext tlContext =
            collector.getTimelineEntityContext();
        tlContext.setFlowName(context.getJobName());
        tlContext.setFlowRunId(timestamp);
        tlContext.setUserId(context.getUser());

        final int kbs = Integer.parseInt(conf.get(KBS_SENT));

        long totalTime = 0;
        final int testtimes = Integer.parseInt(conf.get(TEST_TIMES));
        final Random rand = new Random();
        final TaskAttemptID taskAttemptId = context.getTaskAttemptID();
        final char[] payLoad = new char[kbs * 1024];

        for (int i = 0; i < testtimes; i++) {
          // Generate a fixed length random payload
          for (int xx = 0; xx < kbs * 1024; xx++) {
            int alphaNumIdx = rand.nextInt(alphaNums.length);
            payLoad[xx] = alphaNums[alphaNumIdx];
          }
          String entId = taskAttemptId + "_" + Integer.toString(i);
          final TimelineEntity entity = new TimelineEntity();
          entity.setId(entId);
          entity.setType("FOO_ATTEMPT");
          entity.addInfo("PERF_TEST", payLoad);
          // add an event
          TimelineEvent event = new TimelineEvent();
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
            e.printStackTrace();
          }
          long endWrite = System.nanoTime();
          totalTime += (endWrite-startWrite)/1000000L;
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
        collector.close();
      }
    }
  }
}
