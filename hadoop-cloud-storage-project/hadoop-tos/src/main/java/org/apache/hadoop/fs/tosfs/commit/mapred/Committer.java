/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.commit.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.commit.CommitUtils;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Committer extends FileOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(Committer.class);
  private OutputCommitter wrapped = null;

  private static Path getOutputPath(JobContext context) {
    JobConf conf = context.getJobConf();
    return FileOutputFormat.getOutputPath(conf);
  }

  private static Path getOutputPath(TaskAttemptContext context) {
    JobConf conf = context.getJobConf();
    return FileOutputFormat.getOutputPath(conf);
  }

  private OutputCommitter getWrapped(JobContext context) throws IOException {
    if (wrapped == null) {
      wrapped = CommitUtils.supportObjectStorageCommit(context.getConfiguration(),
          getOutputPath(context)) ?
          new org.apache.hadoop.fs.tosfs.commit.Committer(getOutputPath(context), context) :
          new FileOutputCommitter();
      LOG.debug("Using OutputCommitter implementation {}", wrapped.getClass().getName());
    }
    return wrapped;
  }

  @InterfaceAudience.Private
  @Override
  public Path getTaskAttemptPath(TaskAttemptContext context) throws IOException {
    Path out = getOutputPath(context);
    return out == null ? null : getTaskAttemptPath(context, out);
  }

  private OutputCommitter getWrapped(TaskAttemptContext context) throws IOException {
    if (wrapped == null) {
      wrapped = CommitUtils.supportObjectStorageCommit(context.getConfiguration(),
          getOutputPath(context)) ?
          new org.apache.hadoop.fs.tosfs.commit.Committer(getOutputPath(context), context) :
          new FileOutputCommitter();
    }
    return wrapped;
  }

  @Override
  public Path getWorkPath(TaskAttemptContext context, Path outputPath)
      throws IOException {
    if (getWrapped(context) instanceof org.apache.hadoop.fs.tosfs.commit.Committer) {
      return ((org.apache.hadoop.fs.tosfs.commit.Committer) getWrapped(context)).getWorkPath();
    }
    return super.getWorkPath(context, outputPath);
  }

  private Path getTaskAttemptPath(TaskAttemptContext context, Path out) throws IOException {
    Path workPath = FileOutputFormat.getWorkOutputPath(context.getJobConf());
    if(workPath == null && out != null) {
      if (getWrapped(context) instanceof org.apache.hadoop.fs.tosfs.commit.Committer) {
        return CommitUtils.magicTaskAttemptPath(context, getOutputPath(context));
      } else {
        return org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
            .getTaskAttemptPath(context, out);
      }
    }
    return workPath;
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
    getWrapped(context).commitTask(context);
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).abortTask(context);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context)
      throws IOException {
    return getWrapped(context).needsTaskCommit(context);
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return false;
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

  public String jobId() {
    Preconditions.checkNotNull(wrapped, "Encountered uninitialized job committer.");
    return wrapped instanceof org.apache.hadoop.fs.tosfs.commit.Committer ?
        ((org.apache.hadoop.fs.tosfs.commit.Committer) wrapped).jobId() : null;
  }

  public Path getWorkPath() {
    Preconditions.checkNotNull(wrapped, "Encountered uninitialized job committer.");
    return wrapped instanceof org.apache.hadoop.fs.tosfs.commit.Committer ?
        ((org.apache.hadoop.fs.tosfs.commit.Committer) wrapped).getWorkPath() : null;
  }
}
