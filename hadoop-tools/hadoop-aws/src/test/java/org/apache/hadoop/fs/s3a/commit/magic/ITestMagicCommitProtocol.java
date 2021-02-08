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

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractITCommitProtocol;
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.CommitOperations;
import org.apache.hadoop.fs.s3a.commit.CommitUtils;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjection;
import org.apache.hadoop.fs.s3a.commit.CommitterFaultInjectionImpl;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import static org.apache.hadoop.fs.s3a.S3AUtils.listAndFilter;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.hamcrest.CoreMatchers.containsString;

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
  protected MagicS3GuardCommitter createCommitter(
      Path outputPath,
      TaskAttemptContext context)
      throws IOException {
    return new MagicS3GuardCommitter(outputPath, context);
  }

  public MagicS3GuardCommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException {
    return new CommitterWithFailedThenSucceed(getOutDir(), tContext);
  }

  protected void validateTaskAttemptPathDuringWrite(Path p,
      final long expectedLength) throws IOException {
    String pathStr = p.toString();
    assertTrue("not magic " + pathStr,
        pathStr.contains(MAGIC));
    assertPathDoesNotExist("task attempt visible", p);
  }

  protected void validateTaskAttemptPathAfterWrite(Path marker,
      final long expectedLength) throws IOException {
    // the pending file exists
    Path pendingFile = new Path(marker.toString() + PENDING_SUFFIX);
    assertPathExists("pending file", pendingFile);
    S3AFileSystem fs = getFileSystem();

    // THIS SEQUENCE MUST BE RUN IN ORDER ON A S3GUARDED
    // STORE
    // if you list the parent dir and find the marker, it
    // is really 0 bytes long
    String name = marker.getName();
    List<LocatedFileStatus> filtered = listAndFilter(fs,
        marker.getParent(), false,
        (path) -> path.getName().equals(name));
    Assertions.assertThat(filtered)
        .hasSize(1);
    Assertions.assertThat(filtered.get(0))
        .matches(lst -> lst.getLen() == 0,
            "Listing should return 0 byte length");

    // marker file is empty
    FileStatus st = fs.getFileStatus(marker);
    assertEquals("file length in " + st, 0, st.getLen());
    // xattr header
    Assertions.assertThat(CommitOperations.extractMagicFileLength(fs,
        marker))
        .describedAs("XAttribute " + XA_MAGIC_MARKER)
        .isNotEmpty()
        .hasValue(expectedLength);
  }

  /**
   * The magic committer paths are always on S3, and always have
   * "__magic" in the path.
   * @param committer committer instance
   * @param context task attempt context
   * @throws IOException IO failure
   */
  @Override
  protected void validateTaskAttemptWorkingDirectory(
      final AbstractS3ACommitter committer,
      final TaskAttemptContext context) throws IOException {
    URI wd = committer.getWorkPath().toUri();
    assertEquals("Wrong schema for working dir " + wd
        + " with committer " + committer,
        "s3a", wd.getScheme());
    assertThat(wd.getPath(),
        containsString('/' + CommitConstants.MAGIC + '/'));
  }

  /**
   * Verify that the __magic path for the application/tasks use the
   * committer UUID to ensure uniqueness in the case of more than
   * one job writing to the same destination path.
   */
  @Test
  public void testCommittersPathsHaveUUID() throws Throwable {
    TaskAttemptContext tContext = new TaskAttemptContextImpl(
        getConfiguration(),
        getTaskAttempt0());
    MagicS3GuardCommitter committer = createCommitter(getOutDir(), tContext);

    String ta0 = getTaskAttempt0().toString();
    // magic path for the task attempt
    Path taskAttemptPath = committer.getTaskAttemptPath(tContext);
    Assertions.assertThat(taskAttemptPath.toString())
        .describedAs("task path of %s", committer)
        .contains(committer.getUUID())
        .contains(MAGIC)
        .doesNotContain(TEMP_DATA)
        .endsWith(BASE)
        .contains(ta0);

    // temp path for files which the TA will create with an absolute path
    // and which need renaming into place.
    Path tempTaskAttemptPath = committer.getTempTaskAttemptPath(tContext);
    Assertions.assertThat(tempTaskAttemptPath.toString())
        .describedAs("Temp task path of %s", committer)
        .contains(committer.getUUID())
        .contains(TEMP_DATA)
        .doesNotContain(MAGIC)
        .doesNotContain(BASE)
        .contains(ta0);
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
