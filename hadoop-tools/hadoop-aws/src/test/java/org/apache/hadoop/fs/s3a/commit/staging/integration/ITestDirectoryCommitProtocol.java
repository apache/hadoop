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
import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjection;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjectionImpl;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.CONFLICT_MODE_APPEND;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_CONFLICT_MODE;

/** ITest of the low level protocol methods. */
public class ITestDirectoryCommitProtocol extends ITestStagingCommitProtocol {

  @Override
  protected String suitename() {
    return "ITestDirectoryCommitProtocol";
  }

  @Override
  protected String getCommitterName() {
    return CommitConstants.COMMITTER_NAME_DIRECTORY;
  }

  @Override
  protected AbstractS3ACommitter createCommitter(
      Path outputPath,
      TaskAttemptContext context)
      throws IOException {
    return new DirectoryStagingCommitter(outputPath, context);
  }

  @Override
  public AbstractS3ACommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException {
    return new CommitterWithFailedThenSucceed(getOutDir(), tContext);
  }

  /**
   * This is here because somehow test runs were failing with
   * the confict mode being fail. Unsetting per-bucket options
   * in setup made this go away; its retained for regression
   * testing
   */
  @Test
  public void testValidateDefaultConflictMode() throws Throwable {
    describe("Checking default conflict mode adoption");
    Configuration baseConf = new Configuration(true);
    String[] sources = baseConf.getPropertySources(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
    String sourceStr = Arrays.stream(sources)
        .collect(Collectors.joining(","));
    String baseConfVal = baseConf
        .getTrimmed(FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
    assertEquals("conflict mode in core config from "+ sourceStr,
        CONFLICT_MODE_APPEND, baseConfVal);

    Configuration fsConf = getFileSystem().getConf();
    String conflictModeDefVal = fsConf
        .getTrimmed(FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
    assertEquals("conflict mode in filesystem",
        CONFLICT_MODE_APPEND, conflictModeDefVal);
  }

  /**
   * The class provides a overridden implementation of commitJobInternal which
   * causes the commit failed for the first time then succeed.
   */
  private static final class CommitterWithFailedThenSucceed extends
      DirectoryStagingCommitter implements CommitterFaultInjection {

    private final CommitterFaultInjectionImpl injection;

    CommitterWithFailedThenSucceed(Path outputPath,
        TaskAttemptContext context) throws IOException {
      super(outputPath, context);
      injection = new CommitterFaultInjectionImpl(outputPath, context, true);
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
