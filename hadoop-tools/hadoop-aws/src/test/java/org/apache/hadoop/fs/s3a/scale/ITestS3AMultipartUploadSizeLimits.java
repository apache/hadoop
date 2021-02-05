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

package org.apache.hadoop.fs.s3a.scale;

import java.io.File;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.auth.ProgressCounter;
import org.apache.hadoop.fs.s3a.commit.CommitOperations;

import static org.apache.hadoop.fs.StreamCapabilities.ABORTABLE_STREAM;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_ABORT;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_ABORTED;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.assertCompleteAbort;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.assertNoopAbort;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Testing S3 multipart upload for s3.
 */
public class ITestS3AMultipartUploadSizeLimits extends S3AScaleTestBase {

  public static final int MPU_SIZE = 5 * _1MB;

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration configuration = super.createScaleConfiguration();
    removeBaseAndBucketOverrides(configuration,
        MULTIPART_SIZE,
        UPLOAD_PART_COUNT_LIMIT);
    configuration.setLong(MULTIPART_SIZE, MPU_SIZE);
    // Setting the part count limit to 2 such that we
    // failures.
    configuration.setLong(UPLOAD_PART_COUNT_LIMIT, 2);
    return configuration;
  }

  /**
   * Uploads under the limit are valid.
   */
  @Test
  public void testTwoPartUpload() throws Throwable {
    Path file = path(getMethodName());
    // Creating a file having parts less than configured
    // part count will succeed.
    createFile(getFileSystem(), file, true,
            dataset(6 * _1MB, 'a', 'z' - 'a'));
  }

  /**
   * Tests to validate that exception is thrown during a
   * multi part upload when the number of parts is greater
   * than the allowed limit.
   */
  @Test
  public void testUploadOverLimitFailure() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path file = path(getMethodName());
    // Creating a file with more than configured part count should
    // throw a PathIOE
    intercept(PathIOException.class,
        () -> createFile(fs,
           file,
           false,
           dataset(15 * _1MB, 'a', 'z' - 'a')));
    // and the path does not exist
    assertPathDoesNotExist("upload must not have completed", file);
  }

  @Test
  public void testCommitLimitFailure() throws Throwable {
    describe("verify commit uploads fail-safe when MPU limits exceeded");
    S3AFileSystem fs = getFileSystem();
    CommitOperations actions = new CommitOperations(fs);
    File tempFile = File.createTempFile("commit", ".txt");
    FileUtils.writeByteArrayToFile(tempFile,
        dataset(15 * _1MB, 'a', 'z' - 'a'));
    Path dest = methodPath();
    final S3AInstrumentation instrumentation = fs.getInstrumentation();
    final long initial = instrumentation.getCounterValue(
        Statistic.COMMITTER_COMMITS_ABORTED);

    intercept(PathIOException.class, () ->
        actions.uploadFileToPendingCommit(tempFile,
            dest,
            null,
            MPU_SIZE,
            new ProgressCounter()));
    assertPathDoesNotExist("upload must not have completed", dest);
    final long after = instrumentation.getCounterValue(
        Statistic.COMMITTER_COMMITS_ABORTED);
    Assertions.assertThat(after).
        describedAs("commit abort count")
        .isEqualTo(initial + 1);
  }

  @Test
  public void testAbortAfterTwoPartUpload() throws Throwable {
    Path file = path(getMethodName());

    byte[] data = dataset(6 * _1MB, 'a', 'z' - 'a');

    S3AFileSystem fs = getFileSystem();
    FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);

      // From testTwoPartUpload() we know closing stream will finalize uploads
      // and materialize the path. Here we call abort() to abort the upload,
      // and ensure the path is NOT available. (uploads are aborted)

      assertCompleteAbort(stream.abort());

      // the path should not exist
      assertPathDoesNotExist("upload must not have completed", file);
    } finally {
      IOUtils.closeStream(stream);
      // check the path doesn't exist "after" closing stream
      assertPathDoesNotExist("upload must not have completed", file);
    }
    verifyStreamWasAborted(fs, stream);
    // a second abort is a no-op
    assertNoopAbort(stream.abort());
  }


  @Test
  public void testAbortWhenOverwritingAFile() throws Throwable {
    Path file = path(getMethodName());

    S3AFileSystem fs = getFileSystem();
    // write the original data
    byte[] smallData = writeTextFile(fs, file, "original", true);

    // now attempt a multipart upload
    byte[] data = dataset(6 * _1MB, 'a', 'z' - 'a');
    FSDataOutputStream stream = fs.create(file, true);
    try {
      ContractTestUtils.assertCapabilities(stream,
          new String[]{ABORTABLE_STREAM},
          null);
      stream.write(data);
      assertCompleteAbort(stream.abort());

      verifyFileContents(fs, file, smallData);
    } finally {
      IOUtils.closeStream(stream);
    }
    verifyFileContents(fs, file, smallData);
    verifyStreamWasAborted(fs, stream);
  }

  /**
   * Check up on the IOStatistics of the FS and stream to verify that
   * a stream was aborted -both in invocations of abort() and
   * that the multipart upload itself was aborted.
   * @param fs filesystem
   * @param stream stream
   */
  private void verifyStreamWasAborted(final S3AFileSystem fs,
      final FSDataOutputStream stream) {
    // check the stream
    final IOStatistics iostats = stream.getIOStatistics();
    final String sstr = ioStatisticsToPrettyString(iostats);
    LOG.info("IOStatistics for stream: {}", sstr);
    verifyStatisticCounterValue(iostats, INVOCATION_ABORT.getSymbol(), 1);
    verifyStatisticCounterValue(iostats,
        OBJECT_MULTIPART_UPLOAD_ABORTED.getSymbol(), 1);

    // now the FS.
    final IOStatistics fsIostats = fs.getIOStatistics();
    assertThatStatisticCounter(fsIostats, INVOCATION_ABORT.getSymbol())
        .isGreaterThanOrEqualTo(1);
  }
}
