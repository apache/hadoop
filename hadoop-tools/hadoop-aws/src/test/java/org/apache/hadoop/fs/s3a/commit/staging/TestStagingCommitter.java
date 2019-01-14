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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.google.common.collect.Sets;
import org.hamcrest.core.StringStartsWith;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSClientIOException;
import org.apache.hadoop.fs.s3a.MockS3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.*;
import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.staging.Paths.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * The main unit test suite of the staging committer.
 * Parameterized on thread count and unique filename policy.
 */
@RunWith(Parameterized.class)
public class TestStagingCommitter extends StagingTestBase.MiniDFSTest {

  private static final JobID JOB_ID = new JobID("job", 1);
  private static final TaskAttemptID AID = new TaskAttemptID(
      new TaskID(JOB_ID, TaskType.REDUCE, 2), 3);
  private static final Logger LOG =
      LoggerFactory.getLogger(TestStagingCommitter.class);

  private final int numThreads;
  private final boolean uniqueFilenames;
  private JobContext job = null;
  private TaskAttemptContext tac = null;
  private Configuration conf = null;
  private MockedStagingCommitter jobCommitter = null;
  private MockedStagingCommitter committer = null;

  // created in Before
  private S3AFileSystem mockFS = null;
  private MockS3AFileSystem wrapperFS = null;

  // created in Before
  private StagingTestBase.ClientResults results = null;
  private StagingTestBase.ClientErrors errors = null;
  private AmazonS3 mockClient = null;
  private File tmpDir;

  /**
   * Describe a test in the logs.
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n: {}\n", String.format(text, args));
  }

  /**
   * Test array for parameterized test runs: how many threads and
   * how many files to use.
   * @return a list of parameter tuples.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][] {
        {0, false},
        {1, true},
        {3, true},
    });
  }

  public TestStagingCommitter(int numThreads, boolean uniqueFilenames) {
    this.numThreads = numThreads;
    this.uniqueFilenames = uniqueFilenames;
  }

  @Before
  public void setupCommitter() throws Exception {
    JobConf jobConf = getConfiguration();
    jobConf.setInt(FS_S3A_COMMITTER_THREADS, numThreads);
    jobConf.setBoolean(FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES,
        uniqueFilenames);
    jobConf.set(FS_S3A_COMMITTER_STAGING_UUID,
        UUID.randomUUID().toString());

    this.results = new StagingTestBase.ClientResults();
    this.errors = new StagingTestBase.ClientErrors();
    this.mockClient = newMockS3Client(results, errors);
    this.mockFS = createAndBindMockFSInstance(jobConf,
        Pair.of(results, errors));
    this.wrapperFS = lookupWrapperFS(jobConf);
    // and bind the FS
    wrapperFS.setAmazonS3Client(mockClient);


    this.job = new JobContextImpl(jobConf, JOB_ID);
    this.tac = new TaskAttemptContextImpl(
        new Configuration(job.getConfiguration()), AID);

    this.jobCommitter = new MockedStagingCommitter(outputPath, tac);
    jobCommitter.setupJob(job);

    // get the task's configuration copy so modifications take effect
    this.conf = tac.getConfiguration();
    this.conf.setInt(MULTIPART_SIZE, 100);

    tmpDir = File.createTempFile("testStagingCommitter", "");
    tmpDir.delete();
    tmpDir.mkdirs();

    String tmp = tmpDir.getCanonicalPath();
    this.conf.set(BUFFER_DIR,
        String.format("%s/local-0/, %s/local-1 ", tmp, tmp));

    this.committer = new MockedStagingCommitter(outputPath, tac);
    Paths.resetTempFolderCache();
  }

  @After
  public void cleanup() {
    try {
      if (tmpDir != null) {
        FileUtils.deleteDirectory(tmpDir);
      }
    } catch (IOException ignored) {

    }
  }

  @Test
  public void testUUIDPropagation() throws Exception {
    Configuration config = new Configuration();
    String jobUUID = addUUID(config);
    assertEquals("Upload UUID", jobUUID,
        StagingCommitter.getUploadUUID(config, JOB_ID));
  }

  private String addUUID(Configuration config) {
    String jobUUID = UUID.randomUUID().toString();
    config.set(FS_S3A_COMMITTER_STAGING_UUID, jobUUID);
    return jobUUID;
  }

  @Test
  public void testAttemptPathConstructionNoSchema() throws Exception {
    Configuration config = new Configuration();
    final String jobUUID = addUUID(config);
    config.set(BUFFER_DIR, "/tmp/mr-local-0,/tmp/mr-local-1");
    String commonPath = "file:/tmp/mr-local-";

    assertThat("Missing scheme should produce local file paths",
        getLocalTaskAttemptTempDir(config,
            jobUUID, tac.getTaskAttemptID()).toString(),
        StringStartsWith.startsWith(commonPath));
  }

  @Test
  public void testAttemptPathConstructionWithSchema() throws Exception {
    Configuration config = new Configuration();
    final String jobUUID = addUUID(config);
    String commonPath = "file:/tmp/mr-local-";

    config.set(BUFFER_DIR,
        "file:/tmp/mr-local-0,file:/tmp/mr-local-1");

    assertThat("Path should be the same with file scheme",
        getLocalTaskAttemptTempDir(config,
            jobUUID, tac.getTaskAttemptID()).toString(),
        StringStartsWith.startsWith(commonPath));
  }

  @Test
  public void testAttemptPathConstructionWrongSchema() throws Exception {
    Configuration config = new Configuration();
    final String jobUUID = addUUID(config);
    config.set(BUFFER_DIR,
        "hdfs://nn:8020/tmp/mr-local-0,hdfs://nn:8020/tmp/mr-local-1");
    intercept(IllegalArgumentException.class, "Wrong FS",
        () -> getLocalTaskAttemptTempDir(config, jobUUID,
                tac.getTaskAttemptID()));
  }

  @Test
  public void testCommitPathConstruction() throws Exception {
    Path committedTaskPath = committer.getCommittedTaskPath(tac);
    assertEquals("Path should be in HDFS: " + committedTaskPath,
        "hdfs", committedTaskPath.toUri().getScheme());
    String ending = STAGING_UPLOADS + "/_temporary/0/task_job_0001_r_000002";
    assertTrue("Did not end with \"" + ending +"\" :" + committedTaskPath,
        committedTaskPath.toString().endsWith(ending));
  }

  @Test
  public void testSingleTaskCommit() throws Exception {
    Path file = new Path(commitTask(committer, tac, 1).iterator().next());

    List<String> uploads = results.getUploads();
    assertEquals("Should initiate one upload: " + results, 1, uploads.size());

    Path committedPath = committer.getCommittedTaskPath(tac);
    FileSystem dfs = committedPath.getFileSystem(conf);

    assertEquals("Should commit to HDFS: "+ committer, getDFS(), dfs);

    FileStatus[] stats = dfs.listStatus(committedPath);
    assertEquals("Should produce one commit file: " + results, 1, stats.length);
    assertEquals("Should name the commits file with the task ID: " + results,
        "task_job_0001_r_000002", stats[0].getPath().getName());

    PendingSet pending = PendingSet.load(dfs, stats[0].getPath());
    assertEquals("Should have one pending commit", 1, pending.size());
    SinglePendingCommit commit = pending.getCommits().get(0);
    assertEquals("Should write to the correct bucket:" + results,
        BUCKET, commit.getBucket());
    assertEquals("Should write to the correct key: " + results,
        OUTPUT_PREFIX + "/" + file.getName(), commit.getDestinationKey());

    assertValidUpload(results.getTagsByUpload(), commit);
  }

  /**
   * This originally verified that empty files weren't PUT. They are now.
   * @throws Exception on a failure
   */
  @Test
  public void testSingleTaskEmptyFileCommit() throws Exception {
    committer.setupTask(tac);

    Path attemptPath = committer.getTaskAttemptPath(tac);

    String rand = UUID.randomUUID().toString();
    writeOutputFile(tac.getTaskAttemptID(), attemptPath, rand, 0);

    committer.commitTask(tac);

    List<String> uploads = results.getUploads();
    assertEquals("Should initiate one upload", 1, uploads.size());

    Path committedPath = committer.getCommittedTaskPath(tac);
    FileSystem dfs = committedPath.getFileSystem(conf);

    assertEquals("Should commit to HDFS", getDFS(), dfs);

    assertIsFile(dfs, committedPath);
    FileStatus[] stats = dfs.listStatus(committedPath);
    assertEquals("Should produce one commit file", 1, stats.length);
    assertEquals("Should name the commits file with the task ID",
        "task_job_0001_r_000002", stats[0].getPath().getName());

    PendingSet pending = PendingSet.load(dfs,
        stats[0].getPath());
    assertEquals("Should have one pending commit", 1, pending.size());
  }

  @Test
  public void testSingleTaskMultiFileCommit() throws Exception {
    int numFiles = 3;
    Set<String> files = commitTask(committer, tac, numFiles);

    List<String> uploads = results.getUploads();
    assertEquals("Should initiate multiple uploads", numFiles, uploads.size());

    Path committedPath = committer.getCommittedTaskPath(tac);
    FileSystem dfs = committedPath.getFileSystem(conf);

    assertEquals("Should commit to HDFS", getDFS(), dfs);
    assertIsFile(dfs, committedPath);
    FileStatus[] stats = dfs.listStatus(committedPath);
    assertEquals("Should produce one commit file", 1, stats.length);
    assertEquals("Should name the commits file with the task ID",
        "task_job_0001_r_000002", stats[0].getPath().getName());

    List<SinglePendingCommit> pending =
        PendingSet.load(dfs, stats[0].getPath()).getCommits();
    assertEquals("Should have correct number of pending commits",
        files.size(), pending.size());

    Set<String> keys = Sets.newHashSet();
    for (SinglePendingCommit commit : pending) {
      assertEquals("Should write to the correct bucket: " + commit,
          BUCKET, commit.getBucket());
      assertValidUpload(results.getTagsByUpload(), commit);
      keys.add(commit.getDestinationKey());
    }

    assertEquals("Should write to the correct key",
        files, keys);
  }

  @Test
  public void testTaskInitializeFailure() throws Exception {
    committer.setupTask(tac);

    errors.failOnInit(1);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);
    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);

    intercept(AWSClientIOException.class,
        "Fail on init 1",
        "Should fail during init",
        () -> committer.commitTask(tac));

    assertEquals("Should have initialized one file upload",
        1, results.getUploads().size());
    assertEquals("Should abort the upload",
        new HashSet<>(results.getUploads()),
        getAbortedIds(results.getAborts()));
    assertPathDoesNotExist(fs,
        "Should remove the attempt path",
        attemptPath);
  }

  @Test
  public void testTaskSingleFileUploadFailure() throws Exception {
    describe("Set up a single file upload to fail on upload 2");
    committer.setupTask(tac);

    errors.failOnUpload(2);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);

    intercept((Class<? extends Exception>) AWSClientIOException.class,
        "Fail on upload 2",
        "Should fail during upload",
        () -> {
          committer.commitTask(tac);
          return committer.toString();
        });

    assertEquals("Should have attempted one file upload",
        1, results.getUploads().size());
    assertEquals("Should abort the upload",
        results.getUploads().get(0),
        results.getAborts().get(0).getUploadId());
    assertPathDoesNotExist(fs, "Should remove the attempt path",
        attemptPath);
  }

  @Test
  public void testTaskMultiFileUploadFailure() throws Exception {
    committer.setupTask(tac);

    errors.failOnUpload(5);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);
    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);

    intercept((Class<? extends Exception>) AWSClientIOException.class,
        "Fail on upload 5",
        "Should fail during upload",
        () -> {
          committer.commitTask(tac);
          return committer.toString();
        });

    assertEquals("Should have attempted two file uploads",
        2, results.getUploads().size());
    assertEquals("Should abort the upload",
        new HashSet<>(results.getUploads()),
        getAbortedIds(results.getAborts()));
    assertPathDoesNotExist(fs, "Should remove the attempt path",
        attemptPath);
  }

  @Test
  public void testTaskUploadAndAbortFailure() throws Exception {
    committer.setupTask(tac);

    errors.failOnUpload(5);
    errors.failOnAbort(0);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);
    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);

    intercept((Class<? extends Exception>) AWSClientIOException.class,
        "Fail on upload 5",
        "Should suppress abort failure, propagate upload failure",
        ()-> {
            committer.commitTask(tac);
            return committer.toString();
        });

    assertEquals("Should have attempted two file uploads",
        2, results.getUploads().size());
    assertEquals("Should not have succeeded with any aborts",
        new HashSet<>(),
        getAbortedIds(results.getAborts()));
    assertPathDoesNotExist(fs, "Should remove the attempt path", attemptPath);
  }

  @Test
  public void testSingleTaskAbort() throws Exception {
    committer.setupTask(tac);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    Path outPath = writeOutputFile(
        tac.getTaskAttemptID(), attemptPath, UUID.randomUUID().toString(), 10);

    committer.abortTask(tac);

    assertEquals("Should not upload anything",
        0, results.getUploads().size());
    assertEquals("Should not upload anything",
        0, results.getParts().size());
    assertPathDoesNotExist(fs, "Should remove all attempt data", outPath);
    assertPathDoesNotExist(fs, "Should remove the attempt path", attemptPath);

  }

  @Test
  public void testJobCommit() throws Exception {
    Path jobAttemptPath = jobCommitter.getJobAttemptPath(job);
    FileSystem fs = jobAttemptPath.getFileSystem(conf);

    Set<String> uploads = runTasks(job, 4, 3);
    assertNotEquals(0, uploads.size());

    assertPathExists(fs, "No job attempt path", jobAttemptPath);

    jobCommitter.commitJob(job);
    assertEquals("Should have aborted no uploads",
        0, results.getAborts().size());

    assertEquals("Should have deleted no uploads",
        0, results.getDeletes().size());

    assertEquals("Should have committed all uploads",
        uploads, getCommittedIds(results.getCommits()));

    assertPathDoesNotExist(fs, "jobAttemptPath not deleted", jobAttemptPath);

  }

  @Test
  public void testJobCommitFailure() throws Exception {
    Path jobAttemptPath = jobCommitter.getJobAttemptPath(job);
    FileSystem fs = jobAttemptPath.getFileSystem(conf);

    Set<String> uploads = runTasks(job, 4, 3);

    assertPathExists(fs, "No job attempt path", jobAttemptPath);

    errors.failOnCommit(5);
    setMockLogLevel(MockS3AFileSystem.LOG_NAME);

    intercept(AWSClientIOException.class,
        "Fail on commit 5",
        "Should propagate the commit failure",
        () -> {
          jobCommitter.commitJob(job);
          return jobCommitter.toString();
        });

    assertEquals("Should have succeeded to commit some uploads",
        5, results.getCommits().size());

    assertEquals("Should have deleted the files that succeeded",
        5, results.getDeletes().size());

    Set<String> commits = results.getCommits()
        .stream()
        .map((commit) -> commit.getBucketName() + commit.getKey())
        .collect(Collectors.toSet());

    Set<String> deletes = results.getDeletes()
        .stream()
        .map((delete) -> delete.getBucketName() + delete.getKey())
        .collect(Collectors.toSet());

    assertEquals("Committed and deleted objects should match",
        commits, deletes);

    assertEquals("Mismatch in aborted upload count",
        7, results.getAborts().size());

    Set<String> uploadIds = getCommittedIds(results.getCommits());
    uploadIds.addAll(getAbortedIds(results.getAborts()));

    assertEquals("Should have committed/deleted or aborted all uploads",
        uploads, uploadIds);

    assertPathDoesNotExist(fs, "jobAttemptPath not deleted", jobAttemptPath);
  }

  @Test
  public void testJobAbort() throws Exception {
    Path jobAttemptPath = jobCommitter.getJobAttemptPath(job);
    FileSystem fs = jobAttemptPath.getFileSystem(conf);

    Set<String> uploads = runTasks(job, 4, 3);

    assertPathExists(fs, "No job attempt path", jobAttemptPath);
    jobCommitter.abortJob(job, JobStatus.State.KILLED);
    assertEquals("Should have committed no uploads: " + jobCommitter,
        0, results.getCommits().size());

    assertEquals("Should have deleted no uploads: " + jobCommitter,
        0, results.getDeletes().size());

    assertEquals("Should have aborted all uploads: " + jobCommitter,
        uploads, getAbortedIds(results.getAborts()));

    assertPathDoesNotExist(fs, "jobAttemptPath not deleted", jobAttemptPath);
  }

  /**
   * Run tasks, return the uploaded dataset. The upload data is
   * extracted from the {@link #results} field; this is reset
   * before the operation.
   * @param jobContext job ctx
   * @param numTasks number of tasks to run
   * @param numFiles number of files for each task to generate
   * @return a set of all uploads
   * @throws IOException on a failure.
   */
  private Set<String> runTasks(JobContext jobContext,
      int numTasks, int numFiles)
      throws IOException {
    results.resetUploads();
    Set<String> uploads = Sets.newHashSet();

    for (int taskId = 0; taskId < numTasks; taskId += 1) {
      TaskAttemptID attemptID = new TaskAttemptID(
          new TaskID(JOB_ID, TaskType.REDUCE, taskId),
          (taskId * 37) % numTasks);
      TaskAttemptContext attempt = new TaskAttemptContextImpl(
          new Configuration(jobContext.getConfiguration()), attemptID);
      MockedStagingCommitter taskCommitter = new MockedStagingCommitter(
          outputPath, attempt);
      commitTask(taskCommitter, attempt, numFiles);
    }

    uploads.addAll(results.getUploads());
    return uploads;
  }

  private static Set<String> getAbortedIds(
      List<AbortMultipartUploadRequest> aborts) {
    return aborts.stream()
        .map(AbortMultipartUploadRequest::getUploadId)
        .collect(Collectors.toSet());
  }

  private static Set<String> getCommittedIds(
      List<CompleteMultipartUploadRequest> commits) {
    return commits.stream()
        .map(CompleteMultipartUploadRequest::getUploadId)
        .collect(Collectors.toSet());
  }

  private Set<String> commitTask(StagingCommitter staging,
      TaskAttemptContext attempt,
      int numFiles)
      throws IOException {
    Path attemptPath = staging.getTaskAttemptPath(attempt);

    Set<String> files = Sets.newHashSet();
    for (int i = 0; i < numFiles; i += 1) {
      Path outPath = writeOutputFile(
          attempt.getTaskAttemptID(), attemptPath, UUID.randomUUID().toString(),
          10 * (i + 1));
      files.add(OUTPUT_PREFIX +
          "/" + outPath.getName()
          + (uniqueFilenames ? ("-" + staging.getUUID()) : ""));
    }

    staging.commitTask(attempt);

    return files;
  }

  private static void assertValidUpload(Map<String, List<String>> parts,
                                        SinglePendingCommit commit) {
    assertTrue("Should commit a valid uploadId",
        parts.containsKey(commit.getUploadId()));

    List<String> tags = parts.get(commit.getUploadId());
    assertEquals("Should commit the correct number of file parts",
        tags.size(), commit.getPartCount());

    for (int i = 0; i < tags.size(); i += 1) {
      assertEquals("Should commit the correct part tags",
          tags.get(i), commit.getEtags().get(i));
    }
  }

  private static Path writeOutputFile(TaskAttemptID id, Path dest,
                                      String content, long copies)
      throws IOException {
    String fileName = ((id.getTaskType() == TaskType.REDUCE) ? "r_" : "m_") +
        id.getTaskID().getId() + "_" + id.getId() + "_" +
        UUID.randomUUID().toString();
    Path outPath = new Path(dest, fileName);
    FileSystem fs = outPath.getFileSystem(getConfiguration());

    try (OutputStream out = fs.create(outPath)) {
      byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
      for (int i = 0; i < copies; i += 1) {
        out.write(bytes);
      }
    }

    return outPath;
  }

  /**
   * Used during debugging mock test failures; cranks up logging of method
   * calls.
   * @param level log level
   */
  private void setMockLogLevel(int level) {
    wrapperFS.setLogEvents(level);
  }
}
