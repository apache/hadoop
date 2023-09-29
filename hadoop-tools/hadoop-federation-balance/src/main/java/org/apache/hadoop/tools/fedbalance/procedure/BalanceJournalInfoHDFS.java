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
package org.apache.hadoop.tools.fedbalance.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.SequentialNumber;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TMP_TAIL;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.JOB_PREFIX;

/**
 * BalanceJournal based on HDFS. This class stores all the journals in the HDFS.
 * The jobs are persisted into the HDFS and recovered from the HDFS.
 */
public class BalanceJournalInfoHDFS implements BalanceJournal {

  public static final Logger LOG = LoggerFactory.getLogger(
      BalanceJournalInfoHDFS.class);

  public static class IdGenerator extends SequentialNumber {
    protected IdGenerator(long initialValue) {
      super(initialValue);
    }
  }

  private URI workUri;
  private Configuration conf;
  private IdGenerator generator;

  /**
   * Save job journal to HDFS.
   *
   * All the journals are saved in the path base-dir. Each job has an individual
   * directory named after the job id.
   * When a job is saved, a new journal file is created. The file's name
   * consists of a prefix 'JOB-' and an incremental sequential id. The file with
   * the largest id is the latest journal of this job.
   *
   * Layout:
   *   base-dir/
   *           /job-3f1da5e5-2a60-48de-8736-418d134edbe9/
   *                                                    /JOB-0
   *                                                    /JOB-3
   *                                                    /JOB-5
   *           /job-ebc19478-2324-46c2-8d1a-2f8c4391dc09/
   *                                                    /JOB-1
   *                                                    /JOB-2
   *                                                    /JOB-4
   */
  public void saveJob(BalanceJob job) throws IOException {
    Path jobFile = getNewStateJobPath(job);
    Path tmpJobFile = new Path(jobFile + TMP_TAIL);
    FSDataOutputStream out = null;
    try {
      FileSystem fs = FileSystem.get(workUri, conf);
      out = fs.create(tmpJobFile);
      job.write(new DataOutputStream(out));
      out.close();
      out = null;
      fs.rename(tmpJobFile, jobFile);
    } finally {
      IOUtils.closeStream(out);
    }
    LOG.debug("Save journal of job={}", job);
  }

  /**
   * Recover job from journal on HDFS.
   */
  public void recoverJob(BalanceJob job) throws IOException {
    FSDataInputStream in = null;
    try {
      Path logPath = getLatestStateJobPath(job);
      FileSystem fs = FileSystem.get(workUri, conf);
      in = fs.open(logPath);
      job.readFields(in);
      LOG.debug("Recover job={} from journal.", job);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  @Override
  public BalanceJob[] listAllJobs() throws IOException {
    FileSystem fs = FileSystem.get(workUri, conf);
    Path workPath = new Path(workUri.getPath());
    FileStatus[] statuses;
    try {
      statuses = fs.listStatus(workPath);
    } catch (FileNotFoundException e) {
      LOG.debug("Create work path {}", workPath);
      fs.mkdirs(workPath);
      return new BalanceJob[0];
    }
    BalanceJob[] jobs = new BalanceJob[statuses.length];
    StringBuilder builder = new StringBuilder();
    builder.append("List all jobs from journal [");
    for (int i = 0; i < statuses.length; i++) {
      if (statuses[i].isDirectory()) {
        jobs[i] = new BalanceJob.Builder<>().build();
        jobs[i].setId(statuses[i].getPath().getName());
        builder.append(jobs[i]);
        if (i < statuses.length -1) {
          builder.append(", ");
        }
      }
    }
    builder.append("]");
    LOG.debug(builder.toString());
    return jobs;
  }

  @Override
  public void clear(BalanceJob job) throws IOException {
    Path jobBase = getJobBaseDir(job);
    FileSystem fs = FileSystem.get(workUri, conf);
    if (fs.exists(jobBase)) {
      fs.delete(jobBase, true);
    }
    LOG.debug("Clear journal of job=" + job);
  }

  @Override
  public void setConf(Configuration conf) {
    try {
      this.workUri = new URI(conf.get(SCHEDULER_JOURNAL_URI));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("URI resolution failed.", e);
    }
    this.conf = conf;
    this.generator = new IdGenerator(Time.monotonicNow());
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private Path getJobBaseDir(BalanceJob job) {
    String jobId = job.getId();
    return new Path(workUri.getPath(), jobId);
  }

  private Path getNewStateJobPath(BalanceJob job) {
    Path basePath = getJobBaseDir(job);
    Path logPath = new Path(basePath, JOB_PREFIX + generator.nextValue());
    return logPath;
  }

  private Path getLatestStateJobPath(BalanceJob job) throws IOException {
    Path latestFile = null;
    Path basePath = getJobBaseDir(job);
    FileSystem fs = FileSystem.get(workUri, conf);
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(basePath, false);
    while (iterator.hasNext()) {
      FileStatus status = iterator.next();
      String fileName = status.getPath().getName();
      if (fileName.startsWith(JOB_PREFIX) && !fileName.contains(TMP_TAIL)) {
        if (latestFile == null) {
          latestFile = status.getPath();
        } else if (latestFile.getName().compareTo(fileName) <= 0) {
          latestFile = status.getPath();
        }
      }
    }
    return latestFile;
  }
}
