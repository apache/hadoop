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

package org.apache.hadoop.fs.s3a.commit.staging;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.MockS3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.PathCommitException;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/** Mocking test of partitioned committer. */
public class TestStagingPartitionedJobCommit
    extends StagingTestBase.JobCommitterTest<PartitionedStagingCommitter> {

  @Override
  public void setupJob() throws Exception {
    super.setupJob();
    getWrapperFS().setLogEvents(MockS3AFileSystem.LOG_NAME);
  }

  @Override
  PartitionedStagingCommitter newJobCommitter() throws IOException {
    return new PartitionedStagingCommitterForTesting(createTaskAttemptForJob());
  }

  /**
   * Subclass of the Partitioned Staging committer used in the test cases.
   */
  private final class PartitionedStagingCommitterForTesting
      extends PartitionedCommitterForTesting {

    private boolean aborted;

    private PartitionedStagingCommitterForTesting(TaskAttemptContext context)
        throws IOException {
      super(StagingTestBase.outputPath, context);
    }

    /**
     * Generate pending uploads to commit.
     * This is quite complex as the mock pending uploads need to be saved
     * to a filesystem for the next stage of the commit process.
     * To simulate multiple commit, more than one .pendingset file is created,
     * @param context job context
     * @return an active commit containing a list of paths to valid pending set
     * file.
     * @throws IOException IO failure
     */
    @Override
    protected ActiveCommit listPendingUploadsToCommit(
        JobContext context) throws IOException {

      LocalFileSystem localFS = FileSystem.getLocal(getConf());
      ActiveCommit activeCommit = new ActiveCommit(localFS,
          new ArrayList<>(0));
      // need to create some pending entries.
      for (String dateint : Arrays.asList("20161115", "20161116")) {
        PendingSet pendingSet = new PendingSet();
        for (String hour : Arrays.asList("13", "14")) {
          String uploadId = UUID.randomUUID().toString();
          String key = OUTPUT_PREFIX + "/dateint=" + dateint + "/hour=" + hour +
              "/" + uploadId + ".parquet";
          SinglePendingCommit commit = new SinglePendingCommit();
          commit.setBucket(BUCKET);
          commit.setDestinationKey(key);
          commit.setUri("s3a://" + BUCKET + "/" + key);
          commit.setUploadId(uploadId);
          ArrayList<String> etags = new ArrayList<>();
          etags.add("tag1");
          commit.setEtags(etags);
          pendingSet.add(commit);
          // register the upload so commit operations are not rejected
          getMockResults().addUpload(uploadId, key);
        }
        File file = File.createTempFile("staging", ".pendingset");
        file.deleteOnExit();
        Path path = new Path(file.toURI());
        pendingSet.save(localFS, path, true);
        activeCommit.add(localFS.getFileStatus(path));
      }
      return activeCommit;
    }

    @Override
    protected void abortJobInternal(JobContext context,
        boolean suppressExceptions) throws IOException {
      this.aborted = true;
      super.abortJobInternal(context, suppressExceptions);
    }
  }

  @Test
  public void testDefaultFailAndAppend() throws Exception {
    FileSystem mockS3 = getMockS3A();

    // both fail and append don't check. fail is enforced at the task level.
    for (String mode : Arrays.asList(null, CONFLICT_MODE_FAIL,
        CONFLICT_MODE_APPEND)) {
      if (mode != null) {
        getJob().getConfiguration().set(
            FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, mode);
      } else {
        getJob().getConfiguration().unset(
            FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
      }

      PartitionedStagingCommitter committer = newJobCommitter();

      // no directories exist
      committer.commitJob(getJob());

      // parent and peer directories exist
      reset(mockS3);
      pathsExist(mockS3, "dateint=20161116",
          "dateint=20161116/hour=10");
      committer.commitJob(getJob());
      verifyCompletion(mockS3);

      // a leaf directory exists.
      // NOTE: this is not checked during job commit, the commit succeeds.
      reset(mockS3);
      pathsExist(mockS3, "dateint=20161115/hour=14");
      committer.commitJob(getJob());
      verifyCompletion(mockS3);
    }
  }

  @Test
  public void testBadConflictMode() throws Throwable {
    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, "merge");
    intercept(IllegalArgumentException.class,
        "MERGE", "committer conflict",
        this::newJobCommitter);
  }

  @Test
  public void testReplace() throws Exception {
    S3AFileSystem mockS3 = getMockS3A();

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    PartitionedStagingCommitter committer = newJobCommitter();

    committer.commitJob(getJob());
    verifyReplaceCommitActions(mockS3);
    verifyCompletion(mockS3);

    // parent and peer directories exist
    reset(mockS3);
    pathsExist(mockS3, "dateint=20161115",
        "dateint=20161115/hour=12");

    committer.commitJob(getJob());
    verifyReplaceCommitActions(mockS3);
    verifyCompletion(mockS3);

    // partition directories exist and should be removed
    reset(mockS3);
    pathsExist(mockS3, "dateint=20161115/hour=12",
        "dateint=20161115/hour=13");
    canDelete(mockS3, "dateint=20161115/hour=13");

    committer.commitJob(getJob());
    verifyDeleted(mockS3, "dateint=20161115/hour=13");
    verifyReplaceCommitActions(mockS3);
    verifyCompletion(mockS3);

    // partition directories exist and should be removed
    reset(mockS3);
    pathsExist(mockS3, "dateint=20161116/hour=13",
        "dateint=20161116/hour=14");

    canDelete(mockS3, "dateint=20161116/hour=13",
        "dateint=20161116/hour=14");

    committer.commitJob(getJob());
    verifyReplaceCommitActions(mockS3);
    verifyDeleted(mockS3, "dateint=20161116/hour=13");
    verifyDeleted(mockS3, "dateint=20161116/hour=14");
    verifyCompletion(mockS3);
  }


  /**
   * Verify the actions which replace does, essentially: delete the parent
   * partitions.
   * @param mockS3 s3 mock
   */
  protected void verifyReplaceCommitActions(FileSystem mockS3)
      throws IOException {
    verifyDeleted(mockS3, "dateint=20161115/hour=13");
    verifyDeleted(mockS3, "dateint=20161115/hour=14");
    verifyDeleted(mockS3, "dateint=20161116/hour=13");
    verifyDeleted(mockS3, "dateint=20161116/hour=14");
  }

  @Test
  public void testReplaceWithDeleteFailure() throws Exception {
    FileSystem mockS3 = getMockS3A();

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    final PartitionedStagingCommitter committer = newJobCommitter();

    pathsExist(mockS3, "dateint=20161116/hour=14");
    when(mockS3
        .delete(
            new Path(outputPath, "dateint=20161116/hour=14"),
            true))
        .thenThrow(new PathCommitException("fake",
            "Fake IOException for delete"));

    intercept(PathCommitException.class, "Fake IOException for delete",
        "Should throw the fake IOException",
        () -> committer.commitJob(getJob()));

    verifyReplaceCommitActions(mockS3);
    verifyDeleted(mockS3, "dateint=20161116/hour=14");
    assertTrue("Should have aborted",
        ((PartitionedStagingCommitterForTesting) committer).aborted);
    verifyCompletion(mockS3);
  }

}
