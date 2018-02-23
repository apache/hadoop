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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.AmazonS3;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.InconsistentAmazonS3Client;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.listMultipartUploads;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * Base test suite for committer operations.
 *
 * By default, these tests enable the inconsistent committer, with
 * a delay of {@link #CONSISTENCY_DELAY}; they may also have throttling
 * enabled/disabled.
 *
 * <b>Important:</b> all filesystem probes will have to wait for
 * the FS inconsistency delays and handle things like throttle exceptions,
 * or disable throttling and fault injection before the probe.
 *
 */
public abstract class AbstractCommitITest extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCommitITest.class);

  protected static final int CONSISTENCY_DELAY = 500;
  protected static final int CONSISTENCY_PROBE_INTERVAL = 500;
  protected static final int CONSISTENCY_WAIT = CONSISTENCY_DELAY * 2;

  private InconsistentAmazonS3Client inconsistentClient;

  /**
   * Should the inconsistent S3A client be used?
   * Default value: true.
   * @return true for inconsistent listing
   */
  public boolean useInconsistentClient() {
    return true;
  }

  /**
   * switch to an inconsistent path if in inconsistent mode.
   * {@inheritDoc}
   */
  @Override
  protected Path path(String filepath) throws IOException {
    return useInconsistentClient() ?
           super.path(InconsistentAmazonS3Client.DEFAULT_DELAY_KEY_SUBSTRING
               + "/" + filepath)
           : super.path(filepath);
  }

  /**
   * Creates a configuration for commit operations: commit is enabled in the FS
   * and output is multipart to on-heap arrays.
   * @return a configuration to use when creating an FS.
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setBoolean(MAGIC_COMMITTER_ENABLED, true);
    conf.setLong(MIN_MULTIPART_THRESHOLD, MULTIPART_MIN_SIZE);
    conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    conf.set(FAST_UPLOAD_BUFFER, FAST_UPLOAD_BUFFER_ARRAY);
    if (useInconsistentClient()) {
      enableInconsistentS3Client(conf, CONSISTENCY_DELAY);
    }
    return conf;
  }

  /**
   * Get the log; can be overridden for test case log.
   * @return a log.
   */
  public Logger log() {
    return LOG;
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
   * Waits for consistency if needed
   * @param dir directory
   * @param conf configuration
   * @throws IOException failure
   */
  public void rmdir(Path dir, Configuration conf) throws IOException {
    if (dir != null) {
      describe("deleting %s", dir);
      FileSystem fs = dir.getFileSystem(conf);
      waitForConsistency();
      fs.delete(dir, true);
      waitForConsistency();
    }
  }

  /**
   * Setup will use inconsistent client if {@link #useInconsistentClient()}
   * is true.
   * @throws Exception failure.
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    if (useInconsistentClient()) {
      AmazonS3 client = getFileSystem()
          .getAmazonS3ClientForTesting("fault injection");
      Assert.assertTrue(
          "AWS client is not inconsistent, even though the test requirees it "
          + client,
          client instanceof InconsistentAmazonS3Client);
      inconsistentClient = (InconsistentAmazonS3Client) client;
    }
  }

  /**
   * Teardown waits for the consistency delay and resets failure count, so
   * FS is stable, before the superclass teardown is called. This
   * should clean things up better.
   * @throws Exception failure.
   */
  @Override
  public void teardown() throws Exception {
    waitForConsistency();
    // make sure there are no failures any more
    resetFailures();
    super.teardown();
  }

  /**
   * Wait a multiple of the inconsistency delay for things to stabilize;
   * no-op if the consistent client is used.
   * @throws InterruptedIOException if the sleep is interrupted
   */
  protected void waitForConsistency() throws InterruptedIOException {
    if (useInconsistentClient() && inconsistentClient != null) {
      try {
        Thread.sleep(2* inconsistentClient.getDelayKeyMsec());
      } catch (InterruptedException e) {
        throw (InterruptedIOException)
            (new InterruptedIOException("while waiting for consistency: " + e)
                .initCause(e));
      }
    }
  }

  /**
   * Set the throttling factor on requests.
   * @param p probability of a throttling occurring: 0-1.0
   */
  protected void setThrottling(float p) {
    if (inconsistentClient != null) {
      inconsistentClient.setThrottleProbability(p);
    }
  }

  /**
   * Set the throttling factor on requests and number of calls to throttle.
   * @param p probability of a throttling occurring: 0-1.0
   * @param limit limit to number of calls which fail
   */
  protected void setThrottling(float p, int limit) {
    if (inconsistentClient != null) {
      inconsistentClient.setThrottleProbability(p);
    }
    setFailureLimit(limit);
  }

  /**
   * Turn off throttling.
   */
  protected void resetFailures() {
    if (inconsistentClient != null) {
      setThrottling(0, 0);
    }
  }

  /**
   * Set failure limit.
   * @param limit limit to number of calls which fail
   */
  private void setFailureLimit(int limit) {
    if (inconsistentClient != null) {
      inconsistentClient.setFailureLimit(limit);
    }
  }

  /**
   * Abort all multipart uploads under a path.
   * @param path path for uploads to abort; may be null
   * @return a count of aborts
   * @throws IOException trouble.
   */
  protected int abortMultipartUploadsUnderPath(Path path) throws IOException {
    S3AFileSystem fs = getFileSystem();
    if (fs != null && path != null) {
      String key = fs.pathToKey(path);
      WriteOperationHelper writeOps = fs.getWriteOperationHelper();
      int count = writeOps.abortMultipartUploadsUnderPath(key);
      if (count > 0) {
        log().info("Multipart uploads deleted: {}", count);
      }
      return count;
    } else {
      return 0;
    }
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
    if (!uploads.isEmpty()) {
      String result = uploads.stream().collect(Collectors.joining("\n"));
      fail("Multipart uploads in progress under " + path + " \n" + result);
    }
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
   * @return the loaded success data
   * @throws IOException IO Failure
   */
  protected SuccessData verifySuccessMarker(Path dir) throws IOException {
    assertPathExists("Success marker",
        new Path(dir, _SUCCESS));
    SuccessData successData = loadSuccessMarker(dir);
    log().info("Success data {}", successData.toString());
    log().info("Metrics\n{}",
        successData.dumpMetrics("  ", " = ", "\n"));
    log().info("Diagnostics\n{}",
        successData.dumpDiagnostics("  ", " = ", "\n"));
    return successData;
  }

  /**
   * Load the success marker and return the data inside it.
   * @param dir directory containing the marker
   * @return the loaded data
   * @throws IOException on any failure to load or validate the data
   */
  protected SuccessData loadSuccessMarker(Path dir) throws IOException {
    return SuccessData.load(getFileSystem(), new Path(dir, _SUCCESS));
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
}
