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

package org.apache.hadoop.fs.tosfs.commit.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.commit.CommitUtils;
import org.apache.hadoop.fs.tosfs.commit.Pending;
import org.apache.hadoop.fs.tosfs.commit.PendingSet;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class CommitterTestBase {
  private Configuration conf;
  private FileSystem fs;
  private Path outputPath;
  private TaskAttemptID taskAttempt0;
  private Path reportDir;

  @BeforeEach
  public void setup() throws IOException {
    conf = newConf();
    fs = FileSystem.get(conf);
    String uuid = UUIDUtils.random();
    outputPath = fs.makeQualified(new Path("/test/" + uuid));
    taskAttempt0 = JobSuite.createTaskAttemptId(randomTrimmedJobId(), 0);

    reportDir = fs.makeQualified(new Path("/report/" + uuid));
    fs.mkdirs(reportDir);
    conf.set(org.apache.hadoop.fs.tosfs.commit.Committer.COMMITTER_SUMMARY_REPORT_DIR,
        reportDir.toUri().toString());
  }

  protected abstract Configuration newConf();

  @AfterEach
  public void teardown() {
    CommonUtils.runQuietly(() -> fs.delete(outputPath, true));
    IOUtils.closeStream(fs);
  }

  @BeforeAll
  public static void beforeClass() {
    Assumptions.assumeTrue(TestEnv.checkTestEnabled());
  }

  @AfterAll
  public static void afterClass() {
    if (!TestEnv.checkTestEnabled()) {
      return;
    }

    List<String> committerThreads = Thread.getAllStackTraces().keySet()
        .stream()
        .map(Thread::getName)
        .filter(n -> n.startsWith(org.apache.hadoop.fs.tosfs.commit.Committer.THREADS_PREFIX))
        .collect(Collectors.toList());
    assertTrue(committerThreads.isEmpty(), "Outstanding committer threads");
  }

  private static String randomTrimmedJobId() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    return String.format("%s%04d_%04d", formatter.format(new Date()),
        (long) (Math.random() * 1000),
        (long) (Math.random() * 1000));
  }

  private static String randomFormedJobId() {
    return String.format("job_%s", randomTrimmedJobId());
  }

  @Test
  public void testSetupJob() throws IOException {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assumptions.assumeFalse(suite.skipTests());

    // Setup job.
    suite.setupJob();
    suite.dumpObjectStorage();
    suite.assertHasMagicKeys();
  }

  @Test
  public void testSetupJobWithOrphanPaths() throws IOException, InterruptedException {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assumptions.assumeFalse(suite.skipTests());

    // Orphan success marker.
    Path successPath = CommitUtils.successMarker(outputPath);
    CommitUtils.save(fs, successPath, new byte[]{});
    assertTrue(fs.exists(successPath), "The success file should exist.");

    // Orphan job path.
    Path jobPath = CommitUtils.magicJobPath(suite.committer().jobId(), outputPath);
    fs.mkdirs(jobPath);
    assertTrue(fs.exists(jobPath), "The job path should exist.");
    Path subPath = new Path(jobPath, "tmp.pending");
    CommitUtils.save(fs, subPath, new byte[]{});
    assertTrue(fs.exists(subPath), "The sub path under job path should be existing.");
    FileStatus jobPathStatus = fs.getFileStatus(jobPath);

    Thread.sleep(1000L);
    suite.setupJob();
    suite.dumpObjectStorage();
    suite.assertHasMagicKeys();

    assertFalse(fs.exists(successPath), "Should have deleted the success path");
    assertTrue(fs.exists(jobPath), "Should have re-created the job path");
    assertFalse(fs.exists(subPath), "Should have deleted the sub path under the job path");
  }

  @Test
  public void testSetupTask() throws IOException {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assumptions.assumeFalse(suite.skipTests());

    // Remaining attempt task path.
    Path taskAttemptBasePath =
        CommitUtils.magicTaskAttemptBasePath(suite.taskAttemptContext(), outputPath);
    Path subTaskAttemptPath = new Path(taskAttemptBasePath, "tmp.pending");
    CommitUtils.save(fs, subTaskAttemptPath, new byte[]{});
    assertTrue(fs.exists(taskAttemptBasePath));
    assertTrue(fs.exists(subTaskAttemptPath));

    // Setup job.
    suite.setupJob();
    suite.assertHasMagicKeys();
    // It will clear all the job path once we've set up the job.
    assertFalse(fs.exists(taskAttemptBasePath));
    assertFalse(fs.exists(subTaskAttemptPath));

    // Left some the task paths.
    CommitUtils.save(fs, subTaskAttemptPath, new byte[]{});
    assertTrue(fs.exists(taskAttemptBasePath));
    assertTrue(fs.exists(subTaskAttemptPath));

    // Setup task.
    suite.setupTask();
    assertFalse(fs.exists(subTaskAttemptPath));
  }

  @Test
  public void testCommitTask() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assumptions.assumeFalse(suite.skipTests());
    // Setup job
    suite.setupJob();
    suite.dumpObjectStorage();
    suite.assertHasMagicKeys();

    // Setup task
    suite.setupTask();

    // Write records.
    suite.assertNoMagicPendingFile();
    suite.assertMultipartUpload(0);
    suite.writeOutput();
    suite.dumpObjectStorage();
    suite.assertHasMagicPendingFile();
    suite.assertNoMagicMultipartUpload();
    suite.assertMultipartUpload(1);
    // Assert the pending file content.
    Path pendingPath = suite.magicPendingPath();
    byte[] pendingData = CommitUtils.load(suite.fs(), pendingPath);
    Pending pending = Pending.deserialize(pendingData);
    assertEquals(suite.destPartKey(), pending.destKey());
    assertEquals(20, pending.length());
    assertEquals(1, pending.parts().size());

    // Commit the task.
    suite.commitTask();

    // Verify the pending set file.
    suite.assertHasPendingSet();
    // Assert the pending set file content.
    Path pendingSetPath = suite.magicPendingSetPath();
    byte[] pendingSetData = CommitUtils.load(suite.fs(), pendingSetPath);
    PendingSet pendingSet = PendingSet.deserialize(pendingSetData);
    assertEquals(suite.job().getJobID().toString(), pendingSet.jobId());
    assertEquals(1, pendingSet.commits().size());
    assertEquals(pending, pendingSet.commits().get(0));
    assertEquals(pendingSet.extraData(), ImmutableMap.of(CommitUtils.TASK_ATTEMPT_ID,
        suite.taskAttemptContext().getTaskAttemptID().toString()));

    // Complete the multipart upload and verify the results.
    ObjectStorage storage = suite.storage();
    storage.completeUpload(pending.destKey(), pending.uploadId(), pending.parts());
    suite.verifyPartContent();
  }

  @Test
  public void testAbortTask() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assumptions.assumeFalse(suite.skipTests());
    suite.setupJob();
    suite.setupTask();

    // Pre-check before the output write.
    suite.assertNoMagicPendingFile();
    suite.assertMultipartUpload(0);

    // Execute the output write.
    suite.writeOutput();

    // Post-check after the output write.
    suite.assertHasMagicPendingFile();
    suite.assertNoMagicMultipartUpload();
    suite.assertMultipartUpload(1);
    // Assert the pending file content.
    Path pendingPath = suite.magicPendingPath();
    byte[] pendingData = CommitUtils.load(suite.fs(), pendingPath);
    Pending pending = Pending.deserialize(pendingData);
    assertEquals(suite.destPartKey(), pending.destKey());
    assertEquals(20, pending.length());
    assertEquals(1, pending.parts().size());

    // Abort the task.
    suite.abortTask();

    // Verify the state after aborting task.
    suite.assertNoMagicPendingFile();
    suite.assertNoMagicMultipartUpload();
    suite.assertMultipartUpload(0);
    suite.assertNoTaskAttemptPath();
  }

  @Test
  public void testCommitJob() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assumptions.assumeFalse(suite.skipTests());
    suite.setupJob();
    suite.setupTask();
    suite.writeOutput();
    suite.commitTask();

    // Commit the job.
    suite.assertNoPartFiles();
    suite.commitJob();
    // Verify the output.
    suite.assertNoMagicMultipartUpload();
    suite.assertNoMagicObjectKeys();
    suite.assertSuccessMarker();
    suite.assertSummaryReport(reportDir);
    suite.verifyPartContent();
  }


  @Test
  public void testCommitJobFailed() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assumptions.assumeFalse(suite.skipTests());
    suite.setupJob();
    suite.setupTask();
    suite.writeOutput();
    suite.commitTask();

    // Commit the job.
    suite.assertNoPartFiles();
    suite.commitJob();
  }

  @Test
  public void testTaskCommitAfterJobCommit() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assumptions.assumeFalse(suite.skipTests());
    suite.setupJob();
    suite.setupTask();
    suite.writeOutput();
    suite.commitTask();

    // Commit the job
    suite.assertNoPartFiles();
    suite.commitJob();
    // Verify the output.
    suite.assertNoMagicMultipartUpload();
    suite.assertNoMagicObjectKeys();
    suite.assertSuccessMarker();
    suite.verifyPartContent();

    // Commit the task again.
    assertThrows(FileNotFoundException.class, suite::commitTask);
  }

  @Test
  public void testTaskCommitWithConsistentJobId() throws Exception {
    Configuration config = newConf();
    String consistentJobId = randomFormedJobId();
    config.set(CommitUtils.SPARK_WRITE_UUID, consistentJobId);
    JobSuite suite = JobSuite.create(config, taskAttempt0, outputPath);
    Assumptions.assumeFalse(suite.skipTests());

    // By now, we have two "jobId"s, one is spark uuid, and the other is the jobId in taskAttempt.
    // The job committer will adopt the former.
    suite.setupJob();

    // Next, we clear spark uuid, and set the jobId of taskAttempt to another value. In this case,
    // the committer will take the jobId of taskAttempt as the final jobId, which is not consistent
    // with the one that committer holds.
    config.unset(CommitUtils.SPARK_WRITE_UUID);
    JobConf jobConf = new JobConf(config);
    String anotherJobId = randomTrimmedJobId();
    TaskAttemptID taskAttemptId1 =
        JobSuite.createTaskAttemptId(anotherJobId, JobSuite.DEFAULT_APP_ATTEMPT_ID);
    final TaskAttemptContext attemptContext1 =
        JobSuite.createTaskAttemptContext(jobConf, taskAttemptId1, JobSuite.DEFAULT_APP_ATTEMPT_ID);

    assertThrows(IllegalArgumentException.class, () -> suite.setupTask(attemptContext1),
        "JobId set in the context");

    // Even though we use another taskAttempt, as long as we ensure the spark uuid is consistent,
    // the jobId in committer is consistent.
    config.set(CommitUtils.SPARK_WRITE_UUID, consistentJobId);
    config.set(FileOutputFormat.OUTDIR, outputPath.toString());
    jobConf = new JobConf(config);
    anotherJobId = randomTrimmedJobId();
    TaskAttemptID taskAttemptId2 =
        JobSuite.createTaskAttemptId(anotherJobId, JobSuite.DEFAULT_APP_ATTEMPT_ID);
    TaskAttemptContext attemptContext2 =
        JobSuite.createTaskAttemptContext(jobConf, taskAttemptId2, JobSuite.DEFAULT_APP_ATTEMPT_ID);

    suite.setupTask(attemptContext2);
    // Write output must use the same task context with setup task.
    suite.writeOutput(attemptContext2);
    // Commit task must use the same task context with setup task.
    suite.commitTask(attemptContext2);
    suite.assertPendingSetAtRightLocation();

    // Commit the job
    suite.assertNoPartFiles();
    suite.commitJob();

    // Verify the output.
    suite.assertNoMagicMultipartUpload();
    suite.assertNoMagicObjectKeys();
    suite.assertSuccessMarker();
    suite.verifyPartContent();
  }
}
