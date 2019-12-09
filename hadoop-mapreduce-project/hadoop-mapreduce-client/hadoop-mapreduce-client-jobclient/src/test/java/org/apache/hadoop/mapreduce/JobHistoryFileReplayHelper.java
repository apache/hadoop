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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JobHistoryFileReplayHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryFileReplayHelper.class);
  static final String PROCESSING_PATH = "processing path";
  static final String REPLAY_MODE = "replay mode";
  static final int WRITE_ALL_AT_ONCE = 1;
  static final int WRITE_PER_ENTITY = 2;
  static final int REPLAY_MODE_DEFAULT = WRITE_ALL_AT_ONCE;

  private static Pattern JOB_ID_PARSER =
      Pattern.compile("^(job_[0-9]+_([0-9]+)).*");
  private enum FileType { JOB_HISTORY_FILE, JOB_CONF_FILE, UNKNOWN };
  JobHistoryFileParser parser;
  int replayMode;
  Collection<JobFiles> jobFiles;

  JobHistoryFileReplayHelper(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    int taskId = context.getTaskAttemptID().getTaskID().getId();
    int size = conf.getInt(MRJobConfig.NUM_MAPS,
        TimelineServicePerformance.NUM_MAPS_DEFAULT);
    replayMode = conf.getInt(JobHistoryFileReplayHelper.REPLAY_MODE,
            JobHistoryFileReplayHelper.REPLAY_MODE_DEFAULT);
    String processingDir =
        conf.get(JobHistoryFileReplayHelper.PROCESSING_PATH);

    Path processingPath = new Path(processingDir);
    FileSystem processingFs = processingPath.getFileSystem(conf);
    parser = new JobHistoryFileParser(processingFs);
    jobFiles = selectJobFiles(processingFs, processingPath, taskId, size);
  }

  public int getReplayMode() {
    return replayMode;
  }

  public Collection<JobFiles> getJobFiles() {
    return jobFiles;
  }

  public JobHistoryFileParser getParser() {
    return parser;
  }

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

  private Collection<JobFiles> selectJobFiles(FileSystem fs,
      Path processingRoot, int i, int size) throws IOException {
    Map<String, JobFiles> jobs = new HashMap<>();
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
