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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An {@link OutputCommitter} that commits files specified 
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}. 
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileOutputCommitter extends OutputCommitter {

  public static final Logger LOG = LoggerFactory.getLogger(
      "org.apache.hadoop.mapred.FileOutputCommitter");
  
  /**
   * Temporary directory name 
   */
  public static final String TEMP_DIR_NAME = 
    org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.PENDING_DIR_NAME;
  public static final String SUCCEEDED_FILE_NAME = 
    org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.SUCCEEDED_FILE_NAME;
  static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
    org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;
  
  private static Path getOutputPath(JobContext context) {
    JobConf conf = context.getJobConf();
    return FileOutputFormat.getOutputPath(conf);
  }
  
  private static Path getOutputPath(TaskAttemptContext context) {
    JobConf conf = context.getJobConf();
    return FileOutputFormat.getOutputPath(conf);
  }
  
  private org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter wrapped = null;
  
  private org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter 
  getWrapped(JobContext context) throws IOException {
    if(wrapped == null) {
      wrapped = new org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter(
          getOutputPath(context), context);
    }
    return wrapped;
  }
  
  private org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter 
  getWrapped(TaskAttemptContext context) throws IOException {
    if(wrapped == null) {
      wrapped = new org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter(
          getOutputPath(context), context);
    }
    return wrapped;
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @return the path to store job attempt data.
   */
  @Private
  Path getJobAttemptPath(JobContext context) {
    Path out = getOutputPath(context);
    return out == null ? null : 
      org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
        .getJobAttemptPath(context, out);
  }

  @Private
  public Path getTaskAttemptPath(TaskAttemptContext context) throws IOException {
    Path out = getOutputPath(context);
    return out == null ? null : getTaskAttemptPath(context, out);
  }

  private Path getTaskAttemptPath(TaskAttemptContext context, Path out) throws IOException {
    Path workPath = FileOutputFormat.getWorkOutputPath(context.getJobConf());
    if(workPath == null && out != null) {
      return org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
      .getTaskAttemptPath(context, out);
    }
    return workPath;
  }
  
  /**
   * Compute the path where the output of a committed task is stored until
   * the entire job is committed.
   * @param context the context of the task attempt
   * @return the path where the output of a committed task is stored until
   * the entire job is committed.
   */
  @Private
  Path getCommittedTaskPath(TaskAttemptContext context) {
    Path out = getOutputPath(context);
    return out == null ? null : 
      org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
        .getCommittedTaskPath(context, out);
  }

  public Path getWorkPath(TaskAttemptContext context, Path outputPath) 
  throws IOException {
    return outputPath == null ? null : getTaskAttemptPath(context, outputPath);
  }
  
  @Override
  public void setupJob(JobContext context) throws IOException {
    getWrapped(context).setupJob(context);
  }
  
  @Override
  public void commitJob(JobContext context) throws IOException {
    getWrapped(context).commitJob(context);
  }
  
  @Override
  @Deprecated
  public void cleanupJob(JobContext context) throws IOException {
    getWrapped(context).cleanupJob(context);
  }

  @Override
  public void abortJob(JobContext context, int runState) 
  throws IOException {
    JobStatus.State state;
    if(runState == JobStatus.State.RUNNING.getValue()) {
      state = JobStatus.State.RUNNING;
    } else if(runState == JobStatus.State.SUCCEEDED.getValue()) {
      state = JobStatus.State.SUCCEEDED;
    } else if(runState == JobStatus.State.FAILED.getValue()) {
      state = JobStatus.State.FAILED;
    } else if(runState == JobStatus.State.PREP.getValue()) {
      state = JobStatus.State.PREP;
    } else if(runState == JobStatus.State.KILLED.getValue()) {
      state = JobStatus.State.KILLED;
    } else {
      throw new IllegalArgumentException(runState+" is not a valid runState.");
    }
    getWrapped(context).abortJob(context, state);
  }
  
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).setupTask(context);
  }
  
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).commitTask(context, getTaskAttemptPath(context));
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).abortTask(context, getTaskAttemptPath(context));
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) 
  throws IOException {
    return getWrapped(context).needsTaskCommit(context, getTaskAttemptPath(context));
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return true;
  }

  @Override
  public boolean isCommitJobRepeatable(JobContext context) throws IOException {
    return getWrapped(context).isCommitJobRepeatable(context);
  }

  @Override
  public boolean isRecoverySupported(JobContext context) throws IOException {
    return getWrapped(context).isRecoverySupported(context);
  }

  @Override
  public void recoverTask(TaskAttemptContext context)
      throws IOException {
    getWrapped(context).recoverTask(context);
  }
}
