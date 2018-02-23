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
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobHistoryFileReplayHelper.JobFiles;
import org.apache.hadoop.mapreduce.TimelineServicePerformance.PerfCounters;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper for TimelineServicePerformanceV1 that replays job history files to the
 * timeline service.
 *
 */
class JobHistoryFileReplayMapperV1 extends
    org.apache.hadoop.mapreduce.
        Mapper<IntWritable,IntWritable,Writable,Writable> {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryFileReplayMapperV1.class);

  public void map(IntWritable key, IntWritable val, Context context) throws IOException {
    // collect the apps it needs to process
    TimelineClient tlc = TimelineClient.createTimelineClient();
    TimelineEntityConverterV1 converter = new TimelineEntityConverterV1();
    JobHistoryFileReplayHelper helper = new JobHistoryFileReplayHelper(context);
    int replayMode = helper.getReplayMode();
    Collection<JobFiles> jobs =
        helper.getJobFiles();
    JobHistoryFileParser parser = helper.getParser();

    if (jobs.isEmpty()) {
      LOG.info(context.getTaskAttemptID().getTaskID() +
          " will process no jobs");
    } else {
      LOG.info(context.getTaskAttemptID().getTaskID() + " will process " +
          jobs.size() + " jobs");
    }
    for (JobFiles job: jobs) {
      // process each job
      String jobIdStr = job.getJobId();
      LOG.info("processing " + jobIdStr + "...");
      JobId jobId = TypeConverter.toYarn(JobID.forName(jobIdStr));
      ApplicationId appId = jobId.getAppId();

      try {
        // parse the job info and configuration
        Path historyFilePath = job.getJobHistoryFilePath();
        Path confFilePath = job.getJobConfFilePath();
        if ((historyFilePath == null) || (confFilePath == null)) {
          continue;
        }
        JobInfo jobInfo =
            parser.parseHistoryFile(historyFilePath);
        Configuration jobConf =
            parser.parseConfiguration(confFilePath);
        LOG.info("parsed the job history file and the configuration file for job "
            + jobIdStr);

        // create entities from job history and write them
        long totalTime = 0;
        Set<TimelineEntity> entitySet =
            converter.createTimelineEntities(jobInfo, jobConf);
        LOG.info("converted them into timeline entities for job " + jobIdStr);
        // use the current user for this purpose
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        long startWrite = System.nanoTime();
        try {
          switch (replayMode) {
          case JobHistoryFileReplayHelper.WRITE_ALL_AT_ONCE:
            writeAllEntities(tlc, entitySet, ugi);
            break;
          case JobHistoryFileReplayHelper.WRITE_PER_ENTITY:
            writePerEntity(tlc, entitySet, ugi);
            break;
          default:
            break;
          }
        } catch (Exception e) {
          context.getCounter(PerfCounters.TIMELINE_SERVICE_WRITE_FAILURES).
              increment(1);
          LOG.error("writing to the timeline service failed", e);
        }
        long endWrite = System.nanoTime();
        totalTime += TimeUnit.NANOSECONDS.toMillis(endWrite-startWrite);
        int numEntities = entitySet.size();
        LOG.info("wrote " + numEntities + " entities in " + totalTime + " ms");

        context.getCounter(PerfCounters.TIMELINE_SERVICE_WRITE_TIME).
            increment(totalTime);
        context.getCounter(PerfCounters.TIMELINE_SERVICE_WRITE_COUNTER).
            increment(numEntities);
      } finally {
        context.progress(); // move it along
      }
    }
  }

  private void writeAllEntities(TimelineClient tlc,
      Set<TimelineEntity> entitySet, UserGroupInformation ugi)
      throws IOException, YarnException {
    tlc.putEntities((TimelineEntity[])entitySet.toArray());
  }

  private void writePerEntity(TimelineClient tlc,
      Set<TimelineEntity> entitySet, UserGroupInformation ugi)
      throws IOException, YarnException {
    for (TimelineEntity entity : entitySet) {
      tlc.putEntities(entity);
      LOG.info("wrote entity " + entity.getEntityId());
    }
  }
}
