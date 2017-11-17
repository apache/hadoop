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

package org.apache.hadoop.fs.s3a.commit.magic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.commit.AbstractITCommitProtocol;
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.CommitUtils;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjection;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjectionImpl;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * Test the magic committer's commit protocol.
 */
public class ITestMagicCommitProtocol extends AbstractITCommitProtocol {

  @Override
  protected String suitename() {
    return "ITestMagicCommitProtocol";
  }

  /**
   * Need consistency here.
   * @return false
   */
  @Override
  public boolean useInconsistentClient() {
    return false;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setBoolean(MAGIC_COMMITTER_ENABLED, true);
    return conf;
  }

  @Override
  protected String getCommitterFactoryName() {
    return CommitConstants.S3A_COMMITTER_FACTORY;
  }

  @Override
  protected String getCommitterName() {
    return CommitConstants.COMMITTER_NAME_MAGIC;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    CommitUtils.verifyIsMagicCommitFS(getFileSystem());
  }

  @Override
  public void assertJobAbortCleanedUp(JobData jobData)
      throws Exception {
    // special handling of magic directory; harmless in staging
    Path magicDir = new Path(getOutDir(), MAGIC);
    ContractTestUtils.assertPathDoesNotExist(getFileSystem(),
        "magic dir ", magicDir);
    super.assertJobAbortCleanedUp(jobData);
  }

  @Override
  protected AbstractS3ACommitter createCommitter(
      Path outputPath,
      TaskAttemptContext context)
      throws IOException {
    return new MagicS3GuardCommitter(outputPath, context);
  }

  public AbstractS3ACommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException {
    return new CommitterWithFailedThenSucceed(getOutDir(), tContext);
  }

  protected void validateTaskAttemptPathDuringWrite(Path p) throws IOException {
    String pathStr = p.toString();
    assertTrue("not magic " + pathStr,
        pathStr.contains(MAGIC));
    assertPathDoesNotExist("task attempt visible", p);
  }

  protected void validateTaskAttemptPathAfterWrite(Path p) throws IOException {
    FileStatus st = getFileSystem().getFileStatus(p);
    assertEquals("file length in " + st, 0, st.getLen());
    Path pendingFile = new Path(p.toString() + PENDING_SUFFIX);
    assertPathExists("pending file", pendingFile);
  }

  /**
   * The class provides a overridden implementation of commitJobInternal which
   * causes the commit failed for the first time then succeed.
   */

  private static final class CommitterWithFailedThenSucceed extends
      MagicS3GuardCommitter implements CommitterFaultInjection {
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
