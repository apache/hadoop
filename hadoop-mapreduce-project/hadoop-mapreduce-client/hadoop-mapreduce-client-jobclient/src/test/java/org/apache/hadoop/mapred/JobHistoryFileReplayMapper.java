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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.TimelineServicePerformanceV2.EntityWriter;
import org.apache.hadoop.mapred.TimelineServicePerformanceV2.PerfCounters;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.collector.AppLevelTimelineCollector;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorManager;

/**
 * Mapper for TimelineServicePerformanceV2 that replays job history files to the
 * timeline service.
 *
 */
class JobHistoryFileReplayMapper extends EntityWriter {
  private static final Log LOG =
      LogFactory.getLog(JobHistoryFileReplayMapper.class);

  static final String PROCESSING_PATH = "processing path";
  static final String REPLAY_MODE = "replay mode";
  static final int WRITE_ALL_AT_ONCE = 1;
  static final int WRITE_PER_ENTITY = 2;
  static final int REPLAY_MODE_DEFAULT = WRITE_ALL_AT_ONCE;

  private static final Pattern JOB_ID_PARSER =
      Pattern.compile("^(job_[0-9]+_([0-9]+)).*");

  public static class JobFiles {
    private final String jobId;
    private Path jobHistoryFilePath;
    private Path jobConfFilePath;

    public JobFiles(String jobId) {
      this.jobId = jobId;
    }

    public String getJobId() {
      return jobId;
    }

    public Path getJobHistoryFilePath() {
      return jobHistoryFilePath;
    }

    public void setJobHistoryFilePath(Path jobHistoryFilePath) {
      this.jobHistoryFilePath = jobHistoryFilePath;
    }

    public Path getJobConfFilePath() {
      return jobConfFilePath;
    }

    public void setJobConfFilePath(Path jobConfFilePath) {
      this.jobConfFilePath = jobConfFilePath;
    }

    @Override
    public int hashCode() {
      return jobId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      JobFiles other = (JobFiles) obj;
      return jobId.equals(other.jobId);
    }
  }

  private enum FileType { JOB_HISTORY_FILE, JOB_CONF_FILE, UNKNOWN }


  @Override
  protected void writeEntities(Configuration tlConf,
      TimelineCollectorManager manager, Context context) throws IOException {
    // collect the apps it needs to process
    Configuration conf = context.getConfiguration();
    int taskId = context.getTaskAttemptID().getTaskID().getId();
    int size = conf.getInt(MRJobConfig.NUM_MAPS,
        TimelineServicePerformanceV2.NUM_MAPS_DEFAULT);
    String processingDir =
        conf.get(JobHistoryFileReplayMapper.PROCESSING_PATH);
    int replayMode =
        conf.getInt(JobHistoryFileReplayMapper.REPLAY_MODE,
        JobHistoryFileReplayMapper.REPLAY_MODE_DEFAULT);
    Path processingPath = new Path(processingDir);
    FileSystem processingFs = processingPath.getFileSystem(conf);
    JobHistoryFileParser parser = new JobHistoryFileParser(processingFs);
    TimelineEntityConverter converter = new TimelineEntityConverter();

    Collection<JobFiles> jobs =
        selectJobFiles(processingFs, processingPath, taskId, size);
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
        LOG.info("parsed the job history file and the configuration file for job"
            + jobIdStr);

        // set the context
        // flow id: job name, flow run id: timestamp, user id
        TimelineCollectorContext tlContext =
            collector.getTimelineEntityContext();
        tlContext.setFlowName(jobInfo.getJobname());
        tlContext.setFlowRunId(jobInfo.getSubmitTime());
        tlContext.setUserId(jobInfo.getUsername());

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
          case JobHistoryFileReplayMapper.WRITE_ALL_AT_ONCE:
            writeAllEntities(collector, entitySet, ugi);
            break;
          case JobHistoryFileReplayMapper.WRITE_PER_ENTITY:
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
      Set<TimelineEntity> entitySet, UserGroupInformation ugi)
      throws IOException {
    TimelineEntities entities = new TimelineEntities();
    entities.setEntities(entitySet);
    collector.putEntities(entities, ugi);
  }

  private void writePerEntity(AppLevelTimelineCollector collector,
      Set<TimelineEntity> entitySet, UserGroupInformation ugi)
      throws IOException {
    for (TimelineEntity entity : entitySet) {
      TimelineEntities entities = new TimelineEntities();
      entities.addEntity(entity);
      collector.putEntities(entities, ugi);
      LOG.info("wrote entity " + entity.getId());
    }
  }

  private Collection<JobFiles> selectJobFiles(FileSystem fs,
      Path processingRoot, int i, int size) throws IOException {
    Map<String,JobFiles> jobs = new HashMap<>();
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(processingRoot, true);
    while (it.hasNext()) {
      LocatedFileStatus status = it.next();
      Path path = status.getPath();
      String fileName = path.getName();
      Matcher m = JOB_ID_PARSER.matcher(fileName);
      if (!m.matches()) {
        continue;
      }
      String jobId = m.group(1);
      int lastId = Integer.parseInt(m.group(2));
      int mod = lastId % size;
      if (mod != i) {
        continue;
      }
      LOG.info("this mapper will process file " + fileName);
      // it's mine
      JobFiles jobFiles = jobs.get(jobId);
      if (jobFiles == null) {
        jobFiles = new JobFiles(jobId);
        jobs.put(jobId, jobFiles);
      }
      setFilePath(fileName, path, jobFiles);
    }
    return jobs.values();
  }

  private void setFilePath(String fileName, Path path,
      JobFiles jobFiles) {
    // determine if we're dealing with a job history file or a job conf file
    FileType type = getFileType(fileName);
    switch (type) {
    case JOB_HISTORY_FILE:
      if (jobFiles.getJobHistoryFilePath() == null) {
        jobFiles.setJobHistoryFilePath(path);
      } else {
        LOG.warn("we already have the job history file " +
            jobFiles.getJobHistoryFilePath() + ": skipping " + path);
      }
      break;
    case JOB_CONF_FILE:
      if (jobFiles.getJobConfFilePath() == null) {
        jobFiles.setJobConfFilePath(path);
      } else {
        LOG.warn("we already have the job conf file " +
            jobFiles.getJobConfFilePath() + ": skipping " + path);
      }
      break;
    case UNKNOWN:
      LOG.warn("unknown type: " + path);
    }
  }

  private FileType getFileType(String fileName) {
    if (fileName.endsWith(".jhist")) {
      return FileType.JOB_HISTORY_FILE;
    }
    if (fileName.endsWith("_conf.xml")) {
      return FileType.JOB_CONF_FILE;
    }
    return FileType.UNKNOWN;
  }
}
