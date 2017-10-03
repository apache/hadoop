/*
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

package org.apache.hadoop.fs.s3a.commit.staging.integration;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjection;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjectionImpl;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** ITest of the low level protocol methods. */
public class ITestDirectoryCommitProtocol extends ITestStagingCommitProtocol {

  @Override
  protected String suitename() {
    return "ITestDirectoryCommitProtocol";
  }

  @Override
  protected AbstractS3GuardCommitter createCommitter(
      Path outputPath,
      TaskAttemptContext context)
      throws IOException {
    return new DirectoryStagingCommitter(outputPath, context);
  }

  @Override
  public AbstractS3GuardCommitter createCommitter(
      Path outputPath,
      JobContext context)
      throws IOException {
    return new DirectoryStagingCommitter(outputPath, context);
  }

  @Override
  protected String getCommitterFactoryName() {
    return CommitConstants.DIRECTORY_COMMITTER_FACTORY;
  }

  @Override
  public AbstractS3GuardCommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException {
    return new CommitterWithFailedThenSucceed(getOutDir(), tContext);
  }

  /**
   * The class provides a overridden implementation of commitJobInternal which
   * causes the commit failed for the first time then succeed.
   */
  private static final class CommitterWithFailedThenSucceed extends
      DirectoryStagingCommitter implements CommitterFaultInjection {

    private final CommitterFaultInjectionImpl injection;

    CommitterWithFailedThenSucceed(Path outputPath,
        JobContext context) throws IOException {
      super(outputPath, context);
      injection = new CommitterFaultInjectionImpl(outputPath, context, true);
    }

    CommitterWithFailedThenSucceed(Path outputPath,
        TaskAttemptContext context) throws IOException {
      this(outputPath, (JobContext)context);
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
      injection.setupJob(context);
      super.setupJob(context);
    }

    @Override
    public void abortJob(JobContext context, JobStatus.State state)
        throws IOException {
      injection.abortJob(context, state);
      super.abortJob(context, state);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void cleanupJob(JobContext context) throws IOException {
      injection.cleanupJob(context);
      super.cleanupJob(context);
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
      injection.setupTask(context);
      super.setupTask(context);
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
      injection.commitTask(context);
      super.commitTask(context);
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
      injection.abortTask(context);
      super.abortTask(context);
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      injection.commitJob(context);
      super.commitJob(context);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context)
        throws IOException {
      injection.needsTaskCommit(context);
      return super.needsTaskCommit(context);
    }

    @Override
    public void setFaults(CommitterFaultInjection.Faults... faults) {
      injection.setFaults(faults);
    }
  }
}
