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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;

import org.apache.hadoop.util.Sets;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.MockS3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter;
import org.apache.hadoop.fs.s3a.commit.PathCommitException;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.PersistentCommitData;
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
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The main unit test suite of the staging committer.
 * Parameterized on thread count and unique filename policy.
 */
@ParameterizedClass(name="threads-{0}-unique-{1}")
@MethodSource("params")
public class TestStagingCommitter extends StagingTestBase.MiniDFSTest {

  private static final JobID JOB_ID = new JobID("job", 1);

  public static final TaskID TASK_ID = new TaskID(JOB_ID, TaskType.REDUCE, 2);

  private static final TaskAttemptID AID = new TaskAttemptID(
      TASK_ID, 1);
  private static final TaskAttemptID AID2 = new TaskAttemptID(
      TASK_ID, 2);
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
  private S3Client mockClient = null;
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
   * whether or not filenames are unique.
   * @return a list of parameter tuples.
   */
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

  @BeforeEach
  public void setupCommitter() throws Exception {
    JobConf jobConf = getConfiguration();
    jobConf.setInt(FS_S3A_COMMITTER_THREADS, numThreads);
    jobConf.setBoolean(FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES,
        uniqueFilenames);
    jobConf.set(FS_S3A_COMMITTER_UUID,
        uuid());
    jobConf.set(RETRY_INTERVAL, "100ms");
    jobConf.setInt(RETRY_LIMIT, 1);

    this.results = new StagingTestBase.ClientResults();
    this.errors = new StagingTestBase.ClientErrors();
    this.mockClient = newMockS3Client(results, errors);
    this.mockFS = createAndBindMockFSInstance(jobConf,
        Pair.of(results, errors), mockClient);
    this.wrapperFS = lookupWrapperFS(jobConf);
    // and bind the FS
    wrapperFS.setAmazonS3Client(mockClient);


    this.job = new JobContextImpl(jobConf, JOB_ID);
    this.tac = new TaskAttemptContextImpl(
        new Configuration(job.getConfiguration()), AID);

    this.jobCommitter = new MockedStagingCommitter(getOutputPath(), tac);
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

    this.committer = new MockedStagingCommitter(getOutputPath(), tac);
    Paths.resetTempFolderCache();
  }

  @AfterEach
  public void cleanup() {
    try {
      if (tmpDir != null) {
        FileUtils.deleteDirectory(tmpDir);
      }
    } catch (IOException ignored) {

    }
  }

  private Configuration newConfig() {
    return new Configuration(false);
  }

  @Test
  public void testMockFSclientWiredUp() throws Throwable {
    final S3Client client = mockFS.getS3AInternals().getAmazonS3Client("test");
    assertThat(client)
        .describedAs("S3Client from FS")
        .isNotNull()
        .isSameAs(mockClient);
  }

  @Test
  public void testUUIDPropagation() throws Exception {
    Configuration config = newConfig();
    String uuid = uuid();
    config.set(SPARK_WRITE_UUID, uuid);
    config.setBoolean(FS_S3A_COMMITTER_REQUIRE_UUID, true);
    Pair<String, AbstractS3ACommitter.JobUUIDSource> t3 = AbstractS3ACommitter
        .buildJobUUID(config, JOB_ID);
    assertEquals(uuid, t3.getLeft(), "Job UUID");
    assertEquals(AbstractS3ACommitter.JobUUIDSource.SparkWriteUUID,
        t3.getRight(), "Job UUID source: " + t3);
  }

  /**
   * If the Spark UUID is required, then binding will fail
   * if a UUID did not get passed in.
   */
  @Test
  public void testUUIDValidation() throws Exception {
    Configuration config = newConfig();
    config.setBoolean(FS_S3A_COMMITTER_REQUIRE_UUID, true);
    intercept(PathCommitException.class, E_NO_SPARK_UUID, () ->
        AbstractS3ACommitter.buildJobUUID(config, JOB_ID));
  }

  /**
   * Validate ordering of UUID retrieval.
   */
  @Test
  public void testUUIDLoadOrdering() throws Exception {
    Configuration config = newConfig();
    config.setBoolean(FS_S3A_COMMITTER_REQUIRE_UUID, true);
    String uuid = uuid();
    // MUST be picked up
    config.set(FS_S3A_COMMITTER_UUID, uuid);
    config.set(SPARK_WRITE_UUID, "something");
    Pair<String, AbstractS3ACommitter.JobUUIDSource> t3 = AbstractS3ACommitter
        .buildJobUUID(config, JOB_ID);
    assertEquals(uuid, t3.getLeft(), "Job UUID");
    assertEquals(AbstractS3ACommitter.JobUUIDSource.CommitterUUIDProperty,
        t3.getRight(), "Job UUID source: " + t3);
  }

  /**
   * Verify that unless the config enables self-generation, JobIDs
   * are used.
   */
  @Test
  public void testJobIDIsUUID() throws Exception {
    Configuration config = newConfig();
    Pair<String, AbstractS3ACommitter.JobUUIDSource> t3 = AbstractS3ACommitter
        .buildJobUUID(config, JOB_ID);
    assertEquals(AbstractS3ACommitter.JobUUIDSource.JobID,
        t3.getRight(), "Job UUID source: " + t3);
    // parse it as a JobID
    JobID.forName(t3.getLeft());
  }

  /**
   * Verify self-generated UUIDs are supported when enabled,
   * and come before JobID.
   */
  @Test
  public void testSelfGeneratedUUID() throws Exception {
    Configuration config = newConfig();
    config.setBoolean(FS_S3A_COMMITTER_GENERATE_UUID, true);
    Pair<String, AbstractS3ACommitter.JobUUIDSource> t3 = AbstractS3ACommitter
        .buildJobUUID(config, JOB_ID);
    assertEquals(AbstractS3ACommitter.JobUUIDSource.GeneratedLocally,
        t3.getRight(), "Job UUID source: " + t3);
    // parse it
    UUID.fromString(t3.getLeft());
  }

  /**
   * Create a UUID and add it as the staging UUID.
   * @param config config to patch
   * @return the UUID
   */
  private String addUUID(Configuration config) {
    String jobUUID = uuid();
    config.set(FS_S3A_COMMITTER_UUID, jobUUID);
    return jobUUID;
  }

  /**
   * Create a new UUID.
   * @return a uuid as a string.
   */
  private String uuid() {
    return UUID.randomUUID().toString();
  }

  @Test
  public void testAttemptPathConstructionNoSchema() throws Exception {
    Configuration config = newConfig();
    final String jobUUID = addUUID(config);
    config.set(BUFFER_DIR, "/tmp/mr-local-0,/tmp/mr-local-1");
    String commonPath = "file:/tmp/mr-local-";
    assertThat(getLocalTaskAttemptTempDir(config,
        jobUUID, tac.getTaskAttemptID()).toString())
        .describedAs("Missing scheme should produce local file paths")
        .startsWith(commonPath)
        .contains(jobUUID);
  }

  @Test
  public void testAttemptPathsDifferentByTaskAttempt() throws Exception {
    Configuration config = newConfig();
    final String jobUUID = addUUID(config);
    config.set(BUFFER_DIR, "file:/tmp/mr-local-0");
    String attempt1Path = getLocalTaskAttemptTempDir(config,
        jobUUID, AID).toString();
    String attempt2Path = getLocalTaskAttemptTempDir(config,
        jobUUID, AID2).toString();
    assertThat(attempt2Path)
        .describedAs("local task attempt dir of TA1 must not match that of TA2")
        .isNotEqualTo(attempt1Path);
  }

  @Test
  public void testAttemptPathConstructionWithSchema() throws Exception {
    Configuration config = newConfig();
    final String jobUUID = addUUID(config);
    String commonPath = "file:/tmp/mr-local-";

    config.set(BUFFER_DIR,
        "file:/tmp/mr-local-0,file:/tmp/mr-local-1");

    assertThat(
        getLocalTaskAttemptTempDir(config,
            jobUUID, tac.getTaskAttemptID()).toString())
        .describedAs("Path should be the same with file scheme")
        .startsWith(commonPath);
  }

  @Test
  public void testAttemptPathConstructionWrongSchema() throws Exception {
    Configuration config = newConfig();
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
    assertEquals("hdfs", committedTaskPath.toUri().getScheme(),
        "Path should be in HDFS: " + committedTaskPath);
    String ending = STAGING_UPLOADS + "/_temporary/0/task_job_0001_r_000002";
    assertTrue(committedTaskPath.toString().endsWith(ending),
        "Did not end with \"" + ending +"\" :" + committedTaskPath);
  }

  @Test
  public void testSingleTaskCommit() throws Exception {
    Path file = new Path(commitTask(committer, tac, 1).iterator().next());

    List<String> uploads = results.getUploads();
    assertEquals(1, uploads.size(), "Should initiate one upload: " + results);

    Path committedPath = committer.getCommittedTaskPath(tac);
    FileSystem dfs = committedPath.getFileSystem(conf);

    assertEquals(getDFS(), dfs, "Should commit to HDFS: "+ committer);

    FileStatus[] stats = dfs.listStatus(committedPath);
    assertEquals(1, stats.length, "Should produce one commit file: " + results);
    assertEquals("task_job_0001_r_000002", stats[0].getPath().getName(),
        "Should name the commits file with the task ID: " + results);

    PendingSet pending = PersistentCommitData.load(dfs, stats[0], PendingSet.serializer());
    assertEquals(1, pending.size(), "Should have one pending commit");
    SinglePendingCommit commit = pending.getCommits().get(0);
    assertEquals(BUCKET, commit.getBucket(), "Should write to the correct bucket:" + results);
    assertEquals(OUTPUT_PREFIX + "/" + file.getName(), commit.getDestinationKey(),
        "Should write to the correct key: " + results);

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
    assertEquals(1, uploads.size(), "Should initiate one upload");

    Path committedPath = committer.getCommittedTaskPath(tac);
    FileSystem dfs = committedPath.getFileSystem(conf);

    assertEquals(getDFS(), dfs, "Should commit to HDFS");

    assertIsFile(dfs, committedPath);
    FileStatus[] stats = dfs.listStatus(committedPath);
    assertEquals(1, stats.length, "Should produce one commit file");
    assertEquals("task_job_0001_r_000002", stats[0].getPath().getName(),
        "Should name the commits file with the task ID");

    PendingSet pending = PersistentCommitData.load(dfs, stats[0], PendingSet.serializer());
    assertEquals(1, pending.size(), "Should have one pending commit");
  }

  @Test
  public void testSingleTaskMultiFileCommit() throws Exception {
    int numFiles = 3;
    Set<String> files = commitTask(committer, tac, numFiles);

    List<String> uploads = results.getUploads();
    assertEquals(numFiles, uploads.size(), "Should initiate multiple uploads");

    Path committedPath = committer.getCommittedTaskPath(tac);
    FileSystem dfs = committedPath.getFileSystem(conf);

    assertEquals(getDFS(), dfs, "Should commit to HDFS");
    assertIsFile(dfs, committedPath);
    FileStatus[] stats = dfs.listStatus(committedPath);
    assertEquals(1, stats.length, "Should produce one commit file");
    assertEquals("task_job_0001_r_000002", stats[0].getPath().getName(),
        "Should name the commits file with the task ID");

    List<SinglePendingCommit> pending =
        PersistentCommitData.load(dfs, stats[0], PendingSet.serializer()).getCommits();
    assertEquals(files.size(), pending.size(),
        "Should have correct number of pending commits");

    Set<String> keys = Sets.newHashSet();
    for (SinglePendingCommit commit : pending) {
      assertEquals(BUCKET, commit.getBucket(),
          "Should write to the correct bucket: " + commit);
      assertValidUpload(results.getTagsByUpload(), commit);
      keys.add(commit.getDestinationKey());
    }

    assertEquals(files, keys, "Should write to the correct key");
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

    intercept(IOException.class,
        "Fail on init 1",
        "Should fail during init",
        () -> committer.commitTask(tac));

    assertEquals(1, results.getUploads().size(),
        "Should have initialized one file upload");
    assertEquals(new HashSet<>(results.getUploads()),
        getAbortedIds(results.getAborts()), "Should abort the upload");
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

    intercept(IOException.class,
        "Fail on upload 2",
        "Should fail during upload",
        () -> {
          committer.commitTask(tac);
          return committer.toString();
        });

    assertEquals(1, results.getUploads().size(),
        "Should have attempted one file upload");
    assertEquals(results.getUploads().get(0),
        results.getAborts().get(0).uploadId(), "Should abort the upload");
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

    intercept(IOException.class,
        "Fail on upload 5",
        "Should fail during upload",
        () -> {
          committer.commitTask(tac);
          return committer.toString();
        });

    assertEquals(2, results.getUploads().size(),
        "Should have attempted two file uploads");
    assertEquals(new HashSet<>(results.getUploads()),
        getAbortedIds(results.getAborts()), "Should abort the upload");
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

    intercept(IOException.class,
        "Fail on upload 5",
        "Should suppress abort failure, propagate upload failure",
        ()-> {
            committer.commitTask(tac);
            return committer.toString();
        });

    assertEquals(2, results.getUploads().size(),
        "Should have attempted two file uploads");
    assertEquals(new HashSet<>(), getAbortedIds(results.getAborts()),
        "Should not have succeeded with any aborts");
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

    assertEquals(0, results.getUploads().size(),
        "Should not upload anything");
    assertEquals(0, results.getParts().size(), "Should not upload anything");
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
    assertEquals(0, results.getAborts().size(),
        "Should have aborted no uploads");

    assertEquals(0, results.getDeletes().size(),
        "Should have deleted no uploads");

    assertEquals(uploads, getCommittedIds(results.getCommits()),
        "Should have committed all uploads");

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

    intercept(IOException.class,
        "Fail on commit 5",
        "Should propagate the commit failure",
        () -> {
          jobCommitter.commitJob(job);
          return jobCommitter.toString();
        });

    Set<String> commits = results.getCommits()
        .stream()
        .map(commit ->
            "s3a://" + commit.bucket() + "/" + commit.key())
        .collect(Collectors.toSet());

    Set<String> deletes = results.getDeletes()
        .stream()
        .map(delete ->
            "s3a://" + delete.bucket() + "/" + delete.key())
        .collect(Collectors.toSet());

    assertThat(commits)
        .describedAs("Committed objects compared to deleted paths %s", results)
        .containsExactlyInAnyOrderElementsOf(deletes);

    assertThat(results.getAborts())
        .describedAs("aborted count in %s", results)
        .hasSize(7);
    Set<String> uploadIds = getCommittedIds(results.getCommits());
    uploadIds.addAll(getAbortedIds(results.getAborts()));
    assertThat(uploadIds)
        .describedAs("Combined commit/delete and aborted upload IDs")
        .containsExactlyInAnyOrderElementsOf(uploads);

    assertPathDoesNotExist(fs, "jobAttemptPath not deleted", jobAttemptPath);
  }

  @Test
  public void testJobAbort() throws Exception {
    Path jobAttemptPath = jobCommitter.getJobAttemptPath(job);
    FileSystem fs = jobAttemptPath.getFileSystem(conf);

    Set<String> uploads = runTasks(job, 4, 3);

    assertPathExists(fs, "No job attempt path", jobAttemptPath);
    jobCommitter.abortJob(job, JobStatus.State.KILLED);
    assertEquals(0, results.getCommits().size(),
        "Should have committed no uploads: " + jobCommitter);

    assertEquals(0, results.getDeletes().size(),
        "Should have deleted no uploads: " + jobCommitter);

    assertEquals(uploads, getAbortedIds(results.getAborts()),
        "Should have aborted all uploads: " + jobCommitter);

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
          getOutputPath(), attempt);
      commitTask(taskCommitter, attempt, numFiles);
    }

    uploads.addAll(results.getUploads());
    return uploads;
  }

  private static Set<String> getAbortedIds(
      List<AbortMultipartUploadRequest> aborts) {
    return aborts.stream()
        .map(AbortMultipartUploadRequest::uploadId)
        .collect(Collectors.toSet());
  }

  private static Set<String> getCommittedIds(
      List<CompleteMultipartUploadRequest> commits) {
    return commits.stream()
        .map(CompleteMultipartUploadRequest::uploadId)
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
    assertTrue(parts.containsKey(commit.getUploadId()),
        "Should commit a valid uploadId");

    List<String> tags = parts.get(commit.getUploadId());
    assertEquals(tags.size(), commit.getPartCount(),
        "Should commit the correct number of file parts");

    for (int i = 0; i < tags.size(); i += 1) {
      assertEquals(tags.get(i), commit.getEtags().get(i).getEtag(),
          "Should commit the correct part tags");
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
