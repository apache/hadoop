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
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.amazonaws.services.s3.model.PartETag;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.CONFLICT_MODE_APPEND;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_CONFLICT_MODE;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.PENDINGSET_SUFFIX;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.BUCKET;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.outputPath;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.outputPathUri;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.pathIsDirectory;

/**
 * Scale test of the directory committer: if there are many, many files
 * does job commit overload.
 * This is a mock test as to avoid the overhead of going near S3;
 * it does use a lot of local filesystem files though so as to
 * simulate real large scale deployment better.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDirectoryCommitterScale
    extends StagingTestBase.JobCommitterTest<DirectoryStagingCommitter> {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDirectoryCommitterScale.class);

  public static final int TASKS = 500;

  public static final int FILES_PER_TASK = 10;

  public static final int TOTAL_COMMIT_COUNT = FILES_PER_TASK * TASKS;

  public static final int BLOCKS_PER_TASK = 1000;

  private static File stagingDir;

  private static LocalFileSystem localFS;

  private static Path stagingPath;

  private static Map<String, String> activeUploads =
      Maps.newHashMap();

  @Override
  DirectoryCommitterForTesting newJobCommitter() throws Exception {
    return new DirectoryCommitterForTesting(outputPath,
        createTaskAttemptForJob());
  }

  @BeforeClass
  public static void setupStaging() throws Exception {
    stagingDir = File.createTempFile("staging", "");
    stagingDir.delete();
    stagingDir.mkdir();
    stagingPath = new Path(stagingDir.toURI());
    localFS = FileSystem.getLocal(new Configuration());
  }


  @AfterClass
  public static void teardownStaging() throws IOException {
    try {
      if (stagingDir != null) {
        FileUtils.deleteDirectory(stagingDir);
      }
    } catch (IOException ignored) {

    }
  }

  @Override
  protected JobConf createJobConf() {
    JobConf conf = super.createJobConf();
    conf.setInt(
        CommitConstants.FS_S3A_COMMITTER_THREADS,
        100);
    return conf;
  }

  protected Configuration getJobConf() {
    return getJob().getConfiguration();
  }

  @Test
  public void test_010_createTaskFiles() throws Exception {
    try (DurationInfo ignored =
             new DurationInfo(LOG, "Creating %d test files in %s",
                 TOTAL_COMMIT_COUNT, stagingDir)) {
      createTasks();
    }
  }

  /**
   * Create the mock uploads of the tasks and save
   * to .pendingset files.
   * @throws IOException failure.
   */
  private void createTasks() throws IOException {
    // create a stub multipart commit containing multiple files.

    // step1: a list of tags.
    // this is the md5sum of hadoop 3.2.1.tar
    String tag = "9062dcf18ffaee254821303bbd11c72b";
    List<PartETag> etags = IntStream.rangeClosed(1, BLOCKS_PER_TASK + 1)
        .mapToObj(i -> new PartETag(i, tag))
        .collect(Collectors.toList());
    SinglePendingCommit base = new SinglePendingCommit();
    base.setBucket(BUCKET);
    base.setJobId("0000");
    base.setLength(914688000);
    base.bindCommitData(etags);
    // these get overwritten
    base.setDestinationKey("/base");
    base.setUploadId("uploadId");
    base.setUri(outputPathUri.toString());

    SinglePendingCommit[] singles = new SinglePendingCommit[FILES_PER_TASK];
    byte[] bytes = base.toBytes();
    for (int i = 0; i < FILES_PER_TASK; i++) {
      singles[i] = SinglePendingCommit.serializer().fromBytes(bytes);
    }
    // now create the files, using this as the template

    int uploadCount = 0;
    for (int task = 0; task < TASKS; task++) {
      PendingSet pending = new PendingSet();
      String taskId = String.format("task-%04d", task);

      for (int i = 0; i < FILES_PER_TASK; i++) {
        String uploadId = String.format("%05d-task-%04d-file-%02d",
            uploadCount, task, i);
        // longer paths to take up more space.
        Path p = new Path(outputPath,
            "datasets/examples/testdirectoryscale/"
                + "year=2019/month=09/day=26/hour=20/second=53"
                + uploadId);
        URI dest = p.toUri();
        SinglePendingCommit commit = singles[i];
        String key = dest.getPath();
        commit.setDestinationKey(key);
        commit.setUri(dest.toString());
        commit.touch(Instant.now().toEpochMilli());
        commit.setTaskId(taskId);
        commit.setUploadId(uploadId);
        pending.add(commit);
        activeUploads.put(uploadId, key);
      }
      Path path = new Path(stagingPath,
          String.format("task-%04d." + PENDINGSET_SUFFIX, task));
      pending.save(localFS, path, true);
    }
  }


  @Test
  public void test_020_loadFilesToAttempt() throws Exception {
    DirectoryStagingCommitter committer = newJobCommitter();

    Configuration jobConf = getJobConf();
    jobConf.set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_APPEND);
    FileSystem mockS3 = getMockS3A();
    pathIsDirectory(mockS3, outputPath);
    try (DurationInfo ignored =
             new DurationInfo(LOG, "listing pending uploads")) {
      AbstractS3ACommitter.ActiveCommit activeCommit
          = committer.listPendingUploadsToCommit(getJob());
      Assertions.assertThat(activeCommit.getSourceFiles())
          .describedAs("Source files of %s", activeCommit)
          .hasSize(TASKS);
    }
  }

  @Test
  public void test_030_commitFiles() throws Exception {
    DirectoryCommitterForTesting committer = newJobCommitter();
    StagingTestBase.ClientResults results = getMockResults();
    results.addUploads(activeUploads);
    Configuration jobConf = getJobConf();
    jobConf.set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_APPEND);
    S3AFileSystem mockS3 = getMockS3A();
    pathIsDirectory(mockS3, outputPath);

    try (DurationInfo ignored =
             new DurationInfo(LOG, "Committing Job")) {
      committer.commitJob(getJob());
    }

    Assertions.assertThat(results.getCommits())
        .describedAs("commit count")
        .hasSize(TOTAL_COMMIT_COUNT);
    AbstractS3ACommitter.ActiveCommit activeCommit = committer.activeCommit;
    Assertions.assertThat(activeCommit.getCommittedObjects())
        .describedAs("committed objects in active commit")
        .hasSize(Math.min(TOTAL_COMMIT_COUNT,
            CommitConstants.SUCCESS_MARKER_FILE_LIMIT));
    Assertions.assertThat(activeCommit.getCommittedFileCount())
        .describedAs("committed objects in active commit")
        .isEqualTo(TOTAL_COMMIT_COUNT);

  }

  @Test
  public void test_040_abortFiles() throws Exception {
    DirectoryStagingCommitter committer = newJobCommitter();
    getMockResults().addUploads(activeUploads);
    Configuration jobConf = getJobConf();
    jobConf.set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_APPEND);
    FileSystem mockS3 = getMockS3A();
    pathIsDirectory(mockS3, outputPath);

    committer.abortJob(getJob(), JobStatus.State.FAILED);
  }


  /**
   * Committer overridden for better testing.
   */
  private static final class DirectoryCommitterForTesting extends
      DirectoryStagingCommitter {
    private ActiveCommit activeCommit;

    private DirectoryCommitterForTesting(Path outputPath,
        TaskAttemptContext context) throws IOException {
      super(outputPath, context);
    }

    @Override
    protected void initOutput(Path out) throws IOException {
      super.initOutput(out);
      setOutputPath(out);
    }

    /**
     * Returns the mock FS without checking FS type.
     * @param out output path
     * @param config job/task config
     * @return a filesystem.
     * @throws IOException failure to get the FS
     */
    @Override
    protected FileSystem getDestinationFS(Path out, Configuration config)
        throws IOException {
      return out.getFileSystem(config);
    }

    @Override
    public Path getJobAttemptPath(JobContext context) {
      return stagingPath;
    }

    @Override
    protected void commitJobInternal(final JobContext context,
        final ActiveCommit pending)
        throws IOException {
      activeCommit = pending;
      super.commitJobInternal(context, pending);
    }
  }
}
