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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.JobHistoryFileReplayHelper.JobFiles;
import org.apache.hadoop.mapreduce.TimelineServicePerformance.PerfCounters;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.collector.AppLevelTimelineCollector;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper for TimelineServicePerformance that replays job history files to the
 * timeline service v.2.
 *
 */
class JobHistoryFileReplayMapperV2 extends EntityWriterV2 {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryFileReplayMapperV2.class);

  @Override
  protected void writeEntities(Configuration tlConf,
      TimelineCollectorManager manager, Context context) throws IOException {
    JobHistoryFileReplayHelper helper = new JobHistoryFileReplayHelper(context);
    int replayMode = helper.getReplayMode();
    JobHistoryFileParser parser = helper.getParser();
    TimelineEntityConverterV2 converter = new TimelineEntityConverterV2();

    // collect the apps it needs to process
    Collection<JobFiles> jobs = helper.getJobFiles();
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
      // skip if either of the file is missing
      if (job.getJobConfFilePath() == null ||
          job.getJobHistoryFilePath() == null) {
        LOG.info(jobIdStr + " missing either the job history file or the " +
            "configuration file. Skipping.");
        continue;
      }
      LOG.info("processing " + jobIdStr + "...");
      JobId jobId = TypeConverter.toYarn(JobID.forName(jobIdStr));
      ApplicationId appId = jobId.getAppId();

      // create the app level timeline collector and start it
      AppLevelTimelineCollector collector =
          new AppLevelTimelineCollector(appId);
      manager.putIfAbsent(appId, collector);
      try {
        // parse the job info and configuration
        JobInfo jobInfo =
            parser.parseHistoryFile(job.getJobHistoryFilePath());
        Configuration jobConf =
            parser.parseConfiguration(job.getJobConfFilePath());
        LOG.info("parsed the job history file and the configuration file " +
            "for job " + jobIdStr);

        // set the context
        // flow id: job name, flow run id: timestamp, user id
        TimelineCollectorContext tlContext =
            collector.getTimelineEntityContext();
        tlContext.setFlowName(jobInfo.getJobname());
        tlContext.setFlowRunId(jobInfo.getSubmitTime());
        tlContext.setUserId(jobInfo.getUsername());

        // create entities from job history and write them
        long totalTime = 0;
        List<TimelineEntity> entitySet =
            converter.createTimelineEntities(jobInfo, jobConf);
        LOG.info("converted them into timeline entities for job " + jobIdStr);
        // use the current user for this purpose
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        long startWrite = System.nanoTime();
        try {
          switch (replayMode) {
          case JobHistoryFileReplayHelper.WRITE_ALL_AT_ONCE:
            writeAllEntities(collector, entitySet, ugi);
            break;
          case JobHistoryFileReplayHelper.WRITE_PER_ENTITY:
            writePerEntity(collector, entitySet, ugi);
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
        manager.remove(appId);
        context.progress(); // move it along
      }
    }
  }

  private void writeAllEntities(AppLevelTimelineCollector collector,
      List<TimelineEntity> entitySet, UserGroupInformation ugi)
      throws IOException {
    TimelineEntities entities = new TimelineEntities();
    entities.setEntities(entitySet);
    collector.putEntities(entities, ugi);
  }

  private void writePerEntity(AppLevelTimelineCollector collector,
      List<TimelineEntity> entitySet, UserGroupInformation ugi)
      throws IOException {
    for (TimelineEntity entity : entitySet) {
      TimelineEntities entities = new TimelineEntities();
      entities.addEntity(entity);
      collector.putEntities(entities, ugi);
      LOG.info("wrote entity " + entity.getId());
    }
  }
}
