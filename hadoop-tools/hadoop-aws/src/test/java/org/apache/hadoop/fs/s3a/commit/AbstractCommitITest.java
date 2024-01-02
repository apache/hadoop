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

package org.apache.hadoop.fs.s3a.commit;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestPrinter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.listMultipartUploads;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;

/**
 * Base test suite for committer operations.
 *
 */
public abstract class AbstractCommitITest extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCommitITest.class);

  /**
   * Job statistics accrued across all test cases.
   */
  private static final IOStatisticsSnapshot JOB_STATISTICS =
      IOStatisticsSupport.snapshotIOStatistics();

  /**
   * Helper class for commit operations and assertions.
   */
  private CommitterTestHelper testHelper;

  /**
   * Directory for job summary reports.
   * This should be set up in test suites testing against real object stores.
   */
  private File reportDir;

  /**
   * Creates a configuration for commit operations: commit is enabled in the FS
   * and output is multipart to on-heap arrays.
   * @return a configuration to use when creating an FS.
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    disableFilesystemCaching(conf);
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        MAGIC_COMMITTER_ENABLED,
        S3A_COMMITTER_FACTORY_KEY,
        FS_S3A_COMMITTER_NAME,
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE,
        FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES,
        FAST_UPLOAD_BUFFER,
        S3A_COMMITTER_EXPERIMENTAL_COLLECT_IOSTATISTICS);

    conf.setBoolean(MAGIC_COMMITTER_ENABLED, DEFAULT_MAGIC_COMMITTER_ENABLED);
    conf.setLong(MIN_MULTIPART_THRESHOLD, MULTIPART_MIN_SIZE);
    conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    conf.set(FAST_UPLOAD_BUFFER, FAST_UPLOAD_BUFFER_ARRAY);
    // and bind the report dir
    conf.set(OPT_SUMMARY_REPORT_DIR, reportDir.toURI().toString());
    conf.setBoolean(S3A_COMMITTER_EXPERIMENTAL_COLLECT_IOSTATISTICS, true);
    return conf;
  }

  @AfterClass
  public static void printStatistics() {
    LOG.info("Aggregate job statistics {}\n",
        IOStatisticsLogging.ioStatisticsToPrettyString(JOB_STATISTICS));
  }
  /**
   * Get the log; can be overridden for test case log.
   * @return a log.
   */
  public Logger log() {
    return LOG;
  }

  /**
   * Get directory for reports; valid after
   * setup.
   * @return where success/failure reports go.
   */
  protected File getReportDir() {
    return reportDir;
  }

  @Override
  public void setup() throws Exception {
    // set the manifest committer to a localfs path for reports across
    // all threads.
    // do this before superclass setup so reportDir is non-null there
    // and can be used in creating the configuration.
    reportDir = new File(getProjectBuildDir(), "reports");
    reportDir.mkdirs();

    super.setup();
    testHelper = new CommitterTestHelper(getFileSystem());
  }

  /**
   * Get helper class.
   * @return helper; only valid after setup.
   */
  public CommitterTestHelper getTestHelper() {
    return testHelper;
  }

  /***
   * Bind to the named committer.
   *
   * @param conf configuration
   * @param factory factory name
   * @param committerName committer; set if non null/empty
   */
  protected void bindCommitter(Configuration conf, String factory,
      String committerName) {
    conf.set(S3A_COMMITTER_FACTORY_KEY, factory);
    if (StringUtils.isNotEmpty(committerName)) {
      conf.set(FS_S3A_COMMITTER_NAME, committerName);
    }
  }

  /**
   * Clean up a directory.
   * @param dir directory
   * @param conf configuration
   * @throws IOException failure
   */
  public void rmdir(Path dir, Configuration conf) throws IOException {
    if (dir != null) {
      describe("deleting %s", dir);
      FileSystem fs = dir.getFileSystem(conf);

      fs.delete(dir, true);

    }
  }

  /**
   * Create a random Job ID using the fork ID and the current time.
   * @return fork ID string in a format parseable by Jobs
   * @throws Exception failure
   */
  public static String randomJobId() throws Exception {
    String testUniqueForkId = System.getProperty(TEST_UNIQUE_FORK_ID, "0001");
    int l = testUniqueForkId.length();
    String trailingDigits = testUniqueForkId.substring(l - 4, l);
    try {
      int digitValue = Integer.valueOf(trailingDigits);
      DateTimeFormatter formatter = new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendValue(YEAR, 4)
          .appendValue(MONTH_OF_YEAR, 2)
          .appendValue(DAY_OF_MONTH, 2)
          .toFormatter();
      return String.format("%s%04d_%04d",
          LocalDateTime.now().format(formatter),
          (long)(Math.random() * 1000),
          digitValue);
    } catch (NumberFormatException e) {
      throw new Exception("Failed to parse " + trailingDigits, e);
    }
  }

  /**
   * Abort all multipart uploads under a path.
   * @param path path for uploads to abort; may be null
   * @return a count of aborts
   * @throws IOException trouble.
   */
  protected void abortMultipartUploadsUnderPath(Path path) throws IOException {
    getTestHelper()
        .abortMultipartUploadsUnderPath(path);
  }

  /**
   * Assert that there *are* pending MPUs.
   * @param path path to look under
   * @throws IOException IO failure
   */
  protected void assertMultipartUploadsPending(Path path) throws IOException {
    assertTrue("No multipart uploads in progress under " + path,
        countMultipartUploads(path) > 0);
  }

  /**
   * Assert that there *are no* pending MPUs; assertion failure will include
   * the list of pending writes.
   * @param path path to look under
   * @throws IOException IO failure
   */
  protected void assertNoMultipartUploadsPending(Path path) throws IOException {
    List<String> uploads = listMultipartUploads(getFileSystem(),
        pathToPrefix(path));
    Assertions.assertThat(uploads)
        .describedAs("Multipart uploads in progress under " + path)
        .isEmpty();
  }

  /**
   * Count the number of MPUs under a path.
   * @param path path to scan
   * @return count
   * @throws IOException IO failure
   */
  protected int countMultipartUploads(Path path) throws IOException {
    return countMultipartUploads(pathToPrefix(path));
  }

  /**
   * Count the number of MPUs under a prefix; also logs them.
   * @param prefix prefix to scan
   * @return count
   * @throws IOException IO failure
   */
  protected int countMultipartUploads(String prefix) throws IOException {
    return listMultipartUploads(getFileSystem(), prefix).size();
  }

  /**
   * Map from a path to a prefix.
   * @param path path
   * @return the key
   */
  private String pathToPrefix(Path path) {
    return path == null ? "" :
        getFileSystem().pathToKey(path);
  }

  /**
   * Verify that the specified dir has the {@code _SUCCESS} marker
   * and that it can be loaded.
   * The contents will be logged and returned.
   * @param dir directory to scan
   * @param jobId job ID, only verified if non-empty
   * @return the loaded success data
   * @throws IOException IO Failure
   */
  protected SuccessData verifySuccessMarker(Path dir, String jobId)
      throws IOException {
    return validateSuccessFile(dir, "", getFileSystem(), "query", 0, jobId);
  }

  /**
   * Read a UTF-8 file.
   * @param path path to read
   * @return string value
   * @throws IOException IO failure
   */
  protected String readFile(Path path) throws IOException {
    return ContractTestUtils.readUTF8(getFileSystem(), path, -1);
  }

  /**
   * Assert that the given dir does not have the {@code _SUCCESS} marker.
   * @param dir dir to scan
   * @throws IOException IO Failure
   */
  protected void assertSuccessMarkerDoesNotExist(Path dir) throws IOException {
    assertPathDoesNotExist("Success marker",
        new Path(dir, _SUCCESS));
  }

  /**
   * Closeable which can be used to safely close writers in
   * a try-with-resources block..
   */
  protected static class CloseWriter implements AutoCloseable{

    private final RecordWriter writer;
    private final TaskAttemptContext context;

    public CloseWriter(RecordWriter writer,
        TaskAttemptContext context) {
      this.writer = writer;
      this.context = context;
    }

    @Override
    public void close() {
      try {
        writer.close(context);
      } catch (IOException | InterruptedException e) {
        LOG.error("When closing {} on context {}",
            writer, context, e);

      }
    }
  }

  /**
   * Create a task attempt for a Job. This is based on the code
   * run in the MR AM, creating a task (0) for the job, then a task
   * attempt (0).
   * @param jobId job ID
   * @param jContext job context
   * @return the task attempt.
   */
  public static TaskAttemptContext taskAttemptForJob(JobId jobId,
      JobContext jContext) {
    org.apache.hadoop.mapreduce.v2.api.records.TaskId taskID =
        MRBuilderUtils.newTaskId(jobId, 0,
            org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP);
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        MRBuilderUtils.newTaskAttemptId(taskID, 0);
    return new TaskAttemptContextImpl(
        jContext.getConfiguration(),
        TypeConverter.fromYarn(attemptID));
  }


  /**
   * Load in the success data marker: this guarantees that an S3A
   * committer was used.
   * @param outputPath path of job
   * @param committerName name of committer to match, or ""
   * @param fs filesystem
   * @param origin origin (e.g. "teragen" for messages)
   * @param minimumFileCount minimum number of files to have been created
   * @param jobId job ID, only verified if non-empty
   * @return the success data
   * @throws IOException IO failure
   */
  public static SuccessData validateSuccessFile(final Path outputPath,
      final String committerName,
      final S3AFileSystem fs,
      final String origin,
      final int minimumFileCount,
      final String jobId) throws IOException {
    SuccessData successData = loadSuccessFile(fs, outputPath, origin);
    String commitDetails = successData.toString();
    LOG.info("Committer name " + committerName + "\n{}",
        commitDetails);
    LOG.info("Committer statistics: \n{}",
        successData.dumpMetrics("  ", " = ", "\n"));
    LOG.info("Job IOStatistics: \n{}",
        ioStatisticsToString(successData.getIOStatistics()));
    LOG.info("Diagnostics\n{}",
        successData.dumpDiagnostics("  ", " = ", "\n"));
    if (!committerName.isEmpty()) {
      assertEquals("Wrong committer in " + commitDetails,
          committerName, successData.getCommitter());
    }
    Assertions.assertThat(successData.getFilenames())
        .describedAs("Files committed in " + commitDetails)
        .hasSizeGreaterThanOrEqualTo(minimumFileCount);
    if (StringUtils.isNotEmpty(jobId)) {
      Assertions.assertThat(successData.getJobId())
          .describedAs("JobID in " + commitDetails)
          .isEqualTo(jobId);
    }
    // also load as a manifest success data file
    // to verify consistency and that the CLI tool works.
    Path success = new Path(outputPath, _SUCCESS);
    final ManifestPrinter showManifest = new ManifestPrinter();
    ManifestSuccessData manifestSuccessData =
        showManifest.loadAndPrintManifest(fs, success);

    return successData;
  }

  /**
   * Load a success file; fail if the file is empty/nonexistent.
   * The statistics in {@link #JOB_STATISTICS} are updated with
   * the statistics from the success file
   * @param fs filesystem
   * @param outputPath directory containing the success file.
   * @param origin origin of the file
   * @return the loaded file.
   * @throws IOException failure to find/load the file
   * @throws AssertionError file is 0-bytes long,
   */
  public static SuccessData loadSuccessFile(final FileSystem fs,
      final Path outputPath, final String origin) throws IOException {
    ContractTestUtils.assertPathExists(fs,
        "Output directory " + outputPath
            + " from " + origin
            + " not found: Job may not have executed",
        outputPath);
    Path success = new Path(outputPath, _SUCCESS);
    FileStatus status = ContractTestUtils.verifyPathExists(fs,
        "job completion marker " + success
            + " from " + origin
            + " not found: Job may have failed",
        success);
    assertTrue("_SUCCESS outout from " + origin + " is not a file " + status,
        status.isFile());
    assertTrue("0 byte success file "
            + success + " from " + origin
            + "; an S3A committer was not used",
        status.getLen() > 0);
    String body = ContractTestUtils.readUTF8(fs, success, -1);
    LOG.info("Loading committer success file {}. Actual contents=\n{}", success,
        body);
    SuccessData successData = SuccessData.load(fs, success);
    JOB_STATISTICS.aggregate(successData.getIOStatistics());
    return successData;
  }
}
