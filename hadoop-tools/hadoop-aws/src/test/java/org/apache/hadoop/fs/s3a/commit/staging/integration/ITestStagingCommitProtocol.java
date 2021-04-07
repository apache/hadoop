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
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.InconsistentS3ClientFactory;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.commit.AbstractITCommitProtocol;
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjection;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjectionImpl;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.fs.s3a.commit.staging.Paths;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.apache.hadoop.fs.s3a.Constants.S3_CLIENT_FACTORY_IMPL;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/** Test the staging committer's handling of the base protocol operations. */
public class ITestStagingCommitProtocol extends AbstractITCommitProtocol {

  @Override
  protected String suitename() {
    return "ITestStagingCommitProtocol";
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setInt(FS_S3A_COMMITTER_THREADS, 1);
    // switch to the inconsistent filesystem
    conf.setClass(S3_CLIENT_FACTORY_IMPL, InconsistentS3ClientFactory.class,
        S3ClientFactory.class);

    // disable unique filenames so that the protocol tests of FileOutputFormat
    // and this test generate consistent names.
    conf.setBoolean(FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES, false);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();

    // identify working dir for staging and delete
    Configuration conf = getConfiguration();
    String uuid = UUID.randomUUID().toString();
    conf.set(InternalCommitterConstants.SPARK_WRITE_UUID,
        uuid);
    Pair<String, AbstractS3ACommitter.JobUUIDSource> t3 = AbstractS3ACommitter
        .buildJobUUID(conf, JobID.forName("job_" + getJobId()));
    assertEquals("Job UUID", uuid, t3.getLeft());
    assertEquals("Job UUID source: " + t3,
        AbstractS3ACommitter.JobUUIDSource.SparkWriteUUID,
        t3.getRight());
    Path tempDir = Paths.getLocalTaskAttemptTempDir(conf, uuid,
        getTaskAttempt0());
    rmdir(tempDir, conf);
  }

  @Override
  protected String getCommitterName() {
    return InternalCommitterConstants.COMMITTER_NAME_STAGING;
  }

  @Override
  protected AbstractS3ACommitter createCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    return new StagingCommitter(outputPath, context);
  }

  public AbstractS3ACommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException {
    return new CommitterWithFailedThenSucceed(getOutDir(), tContext);
  }

  @Override
  protected boolean shouldExpectSuccessMarker() {
    return false;
  }

  @Override
  protected void expectJobCommitToFail(JobContext jContext,
      AbstractS3ACommitter committer) throws Exception {
    expectJobCommitFailure(jContext, committer,
        IOException.class);
  }

  protected void validateTaskAttemptPathDuringWrite(Path p,
      final long expectedLength) throws IOException {
    // this is expected to be local FS
    ContractTestUtils.assertPathExists(getLocalFS(), "task attempt", p);
  }

  protected void validateTaskAttemptPathAfterWrite(Path p,
      final long expectedLength) throws IOException {
    // this is expected to be local FS
    // this is expected to be local FS
    FileSystem localFS = getLocalFS();
    ContractTestUtils.assertPathExists(localFS, "task attempt", p);
    FileStatus st = localFS.getFileStatus(p);
    assertEquals("file length in " + st, expectedLength, st.getLen());
  }

  protected FileSystem getLocalFS() throws IOException {
    return FileSystem.getLocal(getConfiguration());
  }

  /**
   * The staging committers always have the local FS for their work.
   * @param committer committer instance
   * @param context task attempt context
   * @throws IOException IO failure
   */
  @Override
  protected void validateTaskAttemptWorkingDirectory(final AbstractS3ACommitter committer,
      final TaskAttemptContext context) throws IOException {
    Path wd = context.getWorkingDirectory();
    assertEquals("file", wd.toUri().getScheme());
  }

  /**
   * The class provides a overridden implementation of commitJobInternal which
   * causes the commit failed for the first time then succeed.
   */
  private static final class CommitterWithFailedThenSucceed extends
      StagingCommitter implements CommitterFaultInjection {

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
    public void setFaults(Faults... faults) {
      injection.setFaults(faults);
    }
  }
}
