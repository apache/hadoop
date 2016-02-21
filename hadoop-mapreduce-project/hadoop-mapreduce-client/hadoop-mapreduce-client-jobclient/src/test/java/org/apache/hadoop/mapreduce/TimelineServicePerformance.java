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
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SleepJob.SleepInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TimelineServicePerformance extends Configured implements Tool {
  static final int NUM_MAPS_DEFAULT = 1;

  static final int SIMPLE_ENTITY_WRITER = 1;
  static final int JOB_HISTORY_FILE_REPLAY_MAPPER = 2;
  static int mapperType = SIMPLE_ENTITY_WRITER;
  static final int TIMELINE_SERVICE_VERSION_1 = 1;
  static final int TIMELINE_SERVICE_VERSION_2 = 2;
  static int timeline_service_version = TIMELINE_SERVICE_VERSION_1;

  protected static int printUsage() {
    System.err.println(
        "Usage: [-m <maps>] number of mappers (default: " + NUM_MAPS_DEFAULT +
            ")\n" +
        "     [-v] timeline service version\n" +
        "     [-mtype <mapper type in integer>]\n" +
        "          1. simple entity write mapper (default)\n" +
        "          2. jobhistory files replay mapper\n" +
        "     [-s <(KBs)test>] number of KB per put (mtype=1, default: " +
             SimpleEntityWriterV1.KBS_SENT_DEFAULT + " KB)\n" +
        "     [-t] package sending iterations per mapper (mtype=1, default: " +
             SimpleEntityWriterV1.TEST_TIMES_DEFAULT + ")\n" +
        "     [-d <path>] root path of job history files (mtype=2)\n" +
        "     [-r <replay mode>] (mtype=2)\n" +
        "          1. write all entities for a job in one put (default)\n" +
        "          2. write one entity at a time\n");
    GenericOptionsParser.printGenericCommandUsage(System.err);
    return -1;
  }

  /**
   * Configure a job given argv.
   */
  public static boolean parseArgs(String[] args, Job job) throws IOException {
    // set the common defaults
    Configuration conf = job.getConfiguration();
    conf.setInt(MRJobConfig.NUM_MAPS, NUM_MAPS_DEFAULT);

    for (int i = 0; i < args.length; i++) {
      if (args.length == i + 1) {
        System.out.println("ERROR: Required parameter missing from " + args[i]);
        return printUsage() == 0;
      }
      try {
        if ("-v".equals(args[i])) {
          timeline_service_version = Integer.parseInt(args[++i]);
        }
        if ("-m".equals(args[i])) {
          if (Integer.parseInt(args[++i]) > 0) {
            job.getConfiguration()
                .setInt(MRJobConfig.NUM_MAPS, Integer.parseInt(args[i]));
          }
        } else if ("-mtype".equals(args[i])) {
          mapperType = Integer.parseInt(args[++i]);
        } else if ("-s".equals(args[i])) {
          if (Integer.parseInt(args[++i]) > 0) {
            conf.setInt(SimpleEntityWriterV1.KBS_SENT, Integer.parseInt(args[i]));
          }
        } else if ("-t".equals(args[i])) {
          if (Integer.parseInt(args[++i]) > 0) {
            conf.setInt(SimpleEntityWriterV1.TEST_TIMES,
                Integer.parseInt(args[i]));
          }
        } else if ("-d".equals(args[i])) {
          conf.set(JobHistoryFileReplayHelper.PROCESSING_PATH, args[++i]);
        } else if ("-r".equals(args[i])) {
          conf.setInt(JobHistoryFileReplayHelper.REPLAY_MODE,
          Integer.parseInt(args[++i]));
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

    // handle mapper-specific settings
    switch (timeline_service_version) {
    case TIMELINE_SERVICE_VERSION_1:
    default:
      switch (mapperType) {
      case JOB_HISTORY_FILE_REPLAY_MAPPER:
        job.setMapperClass(JobHistoryFileReplayMapperV1.class);
        String processingPath =
            conf.get(JobHistoryFileReplayHelper.PROCESSING_PATH);
        if (processingPath == null || processingPath.isEmpty()) {
          System.out.println("processing path is missing while mtype = 2");
          return printUsage() == 0;
        }
        break;
      case SIMPLE_ENTITY_WRITER:
      default:
        job.setMapperClass(SimpleEntityWriterV1.class);
        // use the current timestamp as the "run id" of the test: this will
        // be used as simulating the cluster timestamp for apps
        conf.setLong(SimpleEntityWriterV1.TIMELINE_SERVICE_PERFORMANCE_RUN_ID,
          System.currentTimeMillis());
        break;
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
    job.setJarByClass(TimelineServicePerformance.class);
    job.setMapperClass(SimpleEntityWriterV1.class);
    job.setInputFormatClass(SleepInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    if (!parseArgs(args, job)) {
      return -1;
    }

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
    int numMaps =
        Integer.parseInt(job.getConfiguration().get(MRJobConfig.NUM_MAPS));

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
        ToolRunner.run(new Configuration(), new TimelineServicePerformance(),
            args);
    System.exit(res);
  }

}
