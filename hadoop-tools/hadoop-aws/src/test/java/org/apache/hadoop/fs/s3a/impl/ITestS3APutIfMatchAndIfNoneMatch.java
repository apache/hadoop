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

package org.apache.hadoop.fs.s3a.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;

import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;
import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_MULTIPART;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_PERFORMANCE_FLAGS;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeConditionalCreateEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_404_NOT_FOUND;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_412_PRECONDITION_FAILED;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.fs.s3a.scale.S3AScaleTestBase._1KB;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests with conditional overwrites.
 * This test class verifies the behavior of "If-Match" and "If-None-Match"
 * conditions while writing files.
 */
public class ITestS3APutIfMatchAndIfNoneMatch extends AbstractS3ATestBase {

  private static final int UPDATED_MULTIPART_THRESHOLD = 100 * _1KB;

  private static final byte[] SMALL_FILE_BYTES = dataset(TEST_FILE_LEN, 0, 255);
  private static final byte[] MULTIPART_FILE_BYTES = dataset(UPDATED_MULTIPART_THRESHOLD * 5, 'a', 'z' - 'a');

  private BlockOutputStreamStatistics statistics;

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();

    S3ATestUtils.disableFilesystemCaching(conf);
    removeBaseAndBucketOverrides(
            conf,
            FS_S3A_CREATE_PERFORMANCE,
            FS_S3A_PERFORMANCE_FLAGS,
            MULTIPART_SIZE,
            MIN_MULTIPART_THRESHOLD,
            UPLOAD_PART_COUNT_LIMIT
    );
    conf.setLong(UPLOAD_PART_COUNT_LIMIT, 2);
    conf.setLong(MIN_MULTIPART_THRESHOLD, UPDATED_MULTIPART_THRESHOLD);
    conf.setInt(MULTIPART_SIZE, UPDATED_MULTIPART_THRESHOLD);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Configuration conf = getConfiguration();
    assumeConditionalCreateEnabled(conf);
  }

  /**
   * Asserts that an S3Exception has the expected HTTP status code.
   *
   * @param code Expected HTTP status code.
   * @param ex   Exception to validate.
   */
  private static void assertS3ExceptionStatusCode(int code, Exception ex) {
    S3Exception s3Exception = (S3Exception) ex.getCause();

    if (s3Exception.statusCode() != code) {
      throw new AssertionError("Expected status code " + code + " from " + ex, ex);
    }
  }

  /**
   * Asserts that the FSDataOutputStream has the conditional create capability enabled.
   *
   * @param stream The output stream to check.
   */
  private static void assertHasCapabilityConditionalCreate(FSDataOutputStream stream) {
    Assertions.assertThat(stream.hasCapability(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE))
            .as("Conditional create capability should be enabled")
            .isTrue();
  }

  /**
   * Asserts that the FSDataOutputStream has the ETag-based conditional create capability enabled.
   *
   * @param stream The output stream to check.
   */
  private static void assertHasCapabilityEtagWrite(FSDataOutputStream stream) {
    Assertions.assertThat(stream.hasCapability(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG))
            .as("ETag-based conditional create capability should be enabled")
            .isTrue();
  }

  protected String getBlockOutputBufferName() {
    return FAST_UPLOAD_BUFFER_ARRAY;
  }

  /**
   * Creates a file with specified flags and writes data to it.
   *
   * @param fs              The FileSystem instance.
   * @param path            Path of the file to create.
   * @param data            Byte data to write into the file.
   * @param ifNoneMatchFlag If true, enforces conditional creation.
   * @param etag            The ETag for conditional writes.
   * @param forceMultipart  If true, forces multipart upload.
   * @return The FileStatus of the created file.
   * @throws Exception If an error occurs during file creation.
   */
  private static FileStatus createFileWithFlags(
          FileSystem fs,
          Path path,
          byte[] data,
          boolean ifNoneMatchFlag,
          String etag,
          boolean forceMultipart) throws Exception {
    try (FSDataOutputStream stream = getStreamWithFlags(fs, path, ifNoneMatchFlag, etag,
            forceMultipart)) {
      if (ifNoneMatchFlag) {
        assertHasCapabilityConditionalCreate(stream);
      }
      if (etag != null) {
        assertHasCapabilityEtagWrite(stream);
      }
      if (data != null && data.length > 0) {
        stream.write(data);
      }
    }
    return fs.getFileStatus(path);
  }

  /**
   * Overloaded method to create a file without forcing multipart upload.
   *
   * @param fs              The FileSystem instance.
   * @param path            Path of the file to create.
   * @param data            Byte data to write into the file.
   * @param ifNoneMatchFlag If true, enforces conditional creation.
   * @param etag            The ETag for conditional writes.
   * @return The FileStatus of the created file.
   * @throws Exception If an error occurs during file creation.
   */
  private static FileStatus createFileWithFlags(
          FileSystem fs,
          Path path,
          byte[] data,
          boolean ifNoneMatchFlag,
          String etag) throws Exception {
    return createFileWithFlags(fs, path, data, ifNoneMatchFlag, etag, false);
  }

  /**
   * Opens a file for writing with specific conditional write flags.
   *
   * @param fs              The FileSystem instance.
   * @param path            Path of the file to open.
   * @param ifNoneMatchFlag If true, enables conditional overwrites.
   * @param etag            The ETag for conditional writes.
   * @param forceMultipart  If true, forces multipart upload.
   * @return The FSDataOutputStream for writing.
   * @throws Exception If an error occurs while opening the file.
   */
  private static FSDataOutputStream getStreamWithFlags(
          FileSystem fs,
          Path path,
          boolean ifNoneMatchFlag,
          String etag,
          boolean forceMultipart) throws Exception {
    FSDataOutputStreamBuilder builder = fs.createFile(path);
    if (ifNoneMatchFlag) {
      builder.must(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE, true);
    }
    if (etag != null) {
      builder.must(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG, etag);
    }
    if (forceMultipart) {
      builder.opt(FS_S3A_CREATE_MULTIPART, true);
    }
    return builder.create().build();
  }

  /**
   * Opens a file for writing with specific conditional write flags and without forcing multipart upload.
   *
   * @param fs              The FileSystem instance.
   * @param path            Path of the file to open.
   * @param ifNoneMatchFlag If true, enables conditional overwrites.
   * @param etag            The ETag for conditional writes.
   * @return The FSDataOutputStream for writing.
   * @throws Exception If an error occurs while opening the file.
   */
  private static FSDataOutputStream getStreamWithFlags(
          FileSystem fs,
          Path path,
          boolean ifNoneMatchFlag,
          String etag) throws Exception {
    return getStreamWithFlags(fs, path, ifNoneMatchFlag, etag, false);
  }

  /**
   * Reads the content of a file as a string.
   *
   * @param fs   The FileSystem instance.
   * @param path The file path to read.
   * @return The content of the file as a string.
   * @throws Throwable If an error occurs while reading the file.
   */
  private static String readFileContent(FileSystem fs, Path path) throws Throwable {
    try (FSDataInputStream inputStream = fs.open(path)) {
      return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }
  }

  /**
   * Updates the statistics of the output stream.
   *
   * @param stream The FSDataOutputStream whose statistics should be updated.
   */
  private void updateStatistics(FSDataOutputStream stream) {
    statistics = S3ATestUtils.getOutputStreamStatistics(stream);
  }

  /**
   * Retrieves the ETag of a file.
   *
   * @param fs   The FileSystem instance.
   * @param path The path of the file.
   * @return The ETag associated with the file.
   * @throws IOException If an error occurs while fetching the file status.
   */
  private static String getEtag(FileSystem fs, Path path) throws IOException {
    String etag = ((S3AFileStatus) fs.getFileStatus(path)).getETag();
    return etag;
  }

  @Test
  public void testIfNoneMatchConflictOnOverwrite() throws Throwable {
    describe("generate conflict on overwrites");
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();
    fs.mkdirs(testFile.getParent());

    // create a file over an empty path: all good
    createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null);

    // attempted overwrite fails
    RemoteFileChangedException firstException = intercept(RemoteFileChangedException.class,
            () -> createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null));
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, firstException);

    // second attempt also fails
    RemoteFileChangedException secondException = intercept(RemoteFileChangedException.class,
            () -> createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null));
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, secondException);

    // Delete file and verify an overwrite works again
    fs.delete(testFile, false);
    createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null);
  }

  @Test
  public void testIfNoneMatchConflictOnMultipartUpload() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();

    // Skip if multipart upload not supported
    assumeThat(fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED))
            .as("Skipping as multipart upload not supported")
            .isTrue();

    createFileWithFlags(fs, testFile, MULTIPART_FILE_BYTES, true, null, true);

    RemoteFileChangedException firstException = intercept(RemoteFileChangedException.class,
            () -> createFileWithFlags(fs, testFile, MULTIPART_FILE_BYTES, true, null, true));
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, firstException);

    RemoteFileChangedException secondException = intercept(RemoteFileChangedException.class,
            () -> createFileWithFlags(fs, testFile, MULTIPART_FILE_BYTES, true, null, true));
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, secondException);
  }

  @Test
  public void testIfNoneMatchMultipartUploadWithRaceCondition() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();

    // Skip test if multipart uploads are not supported
    assumeThat(fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED))
            .as("Skipping as multipart upload not supported")
            .isTrue();

    // Create a file with multipart upload but do not close the stream
    FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null, true);
    assertHasCapabilityConditionalCreate(stream);
    stream.write(MULTIPART_FILE_BYTES);

    // create and close another small file in parallel
    createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null);

    // Closing the first stream should throw RemoteFileChangedException
    RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
  }

  @Test
  public void testIfNoneMatchTwoConcurrentMultipartUploads() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();

    // Skip test if multipart uploads are not supported
    assumeThat(fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED))
            .as("Skipping as multipart upload not supported")
            .isTrue();

    // Create a file with multipart upload but do not close the stream
    FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null, true);
    assertHasCapabilityConditionalCreate(stream);
    stream.write(MULTIPART_FILE_BYTES);

    // create and close another multipart file in parallel
    createFileWithFlags(fs, testFile, MULTIPART_FILE_BYTES, true, null, true);

    // Closing the first stream should throw RemoteFileChangedException
    RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
  }

  @Test
  public void testIfNoneMatchOverwriteWithEmptyFile() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();
    fs.mkdirs(testFile.getParent());

    // create a non-empty file
    createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null);

    // overwrite with zero-byte file (no write)
    FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null);
    assertHasCapabilityConditionalCreate(stream);

    // close the stream, should throw RemoteFileChangedException
    RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
  }

  @Test
  public void testIfNoneMatchOverwriteEmptyFileWithFile() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();
    fs.mkdirs(testFile.getParent());

    // create an empty file (no write)
    FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null);
    assertHasCapabilityConditionalCreate(stream);
    stream.close();

    // overwrite with non-empty file, should throw RemoteFileChangedException
    RemoteFileChangedException exception = intercept(RemoteFileChangedException.class,
            () -> createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null));
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
  }

  @Test
  public void testIfNoneMatchOverwriteEmptyWithEmptyFile() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();
    fs.mkdirs(testFile.getParent());

    // create an empty file (no write)
    FSDataOutputStream stream1 = getStreamWithFlags(fs, testFile, true, null);
    assertHasCapabilityConditionalCreate(stream1);
    stream1.close();

    // overwrite with another empty file, should throw RemoteFileChangedException
    FSDataOutputStream stream2 = getStreamWithFlags(fs, testFile, true, null);
    assertHasCapabilityConditionalCreate(stream2);
    RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream2::close);
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
  }

  @Test
  public void testIfMatchOverwriteWithCorrectEtag() throws Throwable {
    FileSystem fs = getFileSystem();
    Path path = methodPath();
    fs.mkdirs(path.getParent());

    // Create a file
    createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, null);

    // Retrieve the etag from the created file
    String etag = getEtag(fs, path);
    Assertions.assertThat(etag)
            .as("ETag should not be null after file creation")
            .isNotNull();

    String updatedFileContent = "Updated content";
    byte[] updatedData = updatedFileContent.getBytes(StandardCharsets.UTF_8);

    // overwrite file with etag
    createFileWithFlags(fs, path, updatedData, false, etag);

    // read file and verify overwritten content
    String fileContent = readFileContent(fs, path);
    Assertions.assertThat(fileContent)
            .as("File content should be correctly updated after overwriting with the correct ETag")
            .isEqualTo(updatedFileContent);
  }

  @Test
  public void testIfMatchOverwriteWithOutdatedEtag() throws Throwable {
    FileSystem fs = getFileSystem();
    Path path = methodPath();
    fs.mkdirs(path.getParent());

    // Create a file
    createFileWithFlags(fs, path, SMALL_FILE_BYTES, true, null);

    // Retrieve the etag from the created file
    String etag = getEtag(fs, path);
    Assertions.assertThat(etag)
            .as("ETag should not be null after file creation")
            .isNotNull();

    // Overwrite the file. Will update the etag, making the previously fetched etag outdated.
    createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, null);

    // overwrite file with outdated etag. Should throw RemoteFileChangedException
    RemoteFileChangedException exception = intercept(RemoteFileChangedException.class,
            () -> createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, etag));
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
  }

  @Test
  public void testIfMatchOverwriteDeletedFileWithEtag() throws Throwable {
    FileSystem fs = getFileSystem();
    Path path = methodPath();
    fs.mkdirs(path.getParent());

    // Create a file
    createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, null);

    // Retrieve the etag from the created file
    String etag = getEtag(fs, path);
    Assertions.assertThat(etag)
            .as("ETag should not be null after file creation")
            .isNotNull();

    // delete the file
    fs.delete(path);

    // overwrite file with etag. Should throw FileNotFoundException
    FileNotFoundException exception = intercept(FileNotFoundException.class,
            () -> createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, etag));
    assertS3ExceptionStatusCode(SC_404_NOT_FOUND, exception);
  }

  @Test
  public void testIfMatchOverwriteFileWithEmptyEtag() throws Throwable {
    FileSystem fs = getFileSystem();
    Path path = methodPath();
    fs.mkdirs(path.getParent());

    // Create a file
    createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, null);

    // overwrite file with empty etag. Should throw IllegalArgumentException
    intercept(IllegalArgumentException.class,
            () -> createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, ""));
  }

  @Test
  public void testIfMatchTwoMultipartUploadsRaceConditionOneClosesFirst() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();

    // Skip test if multipart uploads are not supported
    assumeThat(fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED))
            .as("Skipping as multipart upload not supported")
            .isTrue();

    // Create a file and retrieve its etag
    createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, false, null);
    String etag = getEtag(fs, testFile);
    Assertions.assertThat(etag)
            .as("ETag should not be null after file creation")
            .isNotNull();

    // Start two multipart uploads with the same etag
    FSDataOutputStream stream1 = getStreamWithFlags(fs, testFile, false, etag, true);
    assertHasCapabilityEtagWrite(stream1);

    FSDataOutputStream stream2 = getStreamWithFlags(fs, testFile, false, etag, true);
    assertHasCapabilityEtagWrite(stream2);

    // Write data to both streams
    stream1.write(MULTIPART_FILE_BYTES);
    stream2.write(MULTIPART_FILE_BYTES);

    // Close the first stream successfully. Will update the etag
    stream1.close();

    // Close second stream, should fail due to etag mismatch
    RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream2::close);
    assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
  }

  @Ignore("conditional_write statistics not yet fully implemented")
  @Test
  public void testConditionalWriteStatisticsWithoutIfNoneMatch() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();

    // write without an If-None-Match
    // conditional_write, conditional_write_statistics should remain 0
    FSDataOutputStream stream = getStreamWithFlags(fs, testFile, false, null, false);
    updateStatistics(stream);
    stream.write(SMALL_FILE_BYTES);
    stream.close();
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE.getSymbol(), 0);
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE_FAILED.getSymbol(), 0);

    // write with overwrite = true
    // conditional_write, conditional_write_statistics should remain 0
    try (FSDataOutputStream outputStream = fs.create(testFile, true)) {
      outputStream.write(SMALL_FILE_BYTES);
      updateStatistics(outputStream);
    }
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE.getSymbol(), 0);
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE_FAILED.getSymbol(), 0);

    // write in path where file already exists with overwrite = false
    // conditional_write, conditional_write_statistics should remain 0
    try (FSDataOutputStream outputStream = fs.create(testFile, false)) {
      outputStream.write(SMALL_FILE_BYTES);
      updateStatistics(outputStream);
    } catch (FileAlreadyExistsException e) {}
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE.getSymbol(), 0);
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE_FAILED.getSymbol(), 0);

    // delete the file
    fs.delete(testFile, false);

    // write in path where file doesn't exist with overwrite = false
    // conditional_write, conditional_write_statistics should remain 0
    try (FSDataOutputStream outputStream = fs.create(testFile, false)) {
      outputStream.write(SMALL_FILE_BYTES);
      updateStatistics(outputStream);
    }
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE.getSymbol(), 0);
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE_FAILED.getSymbol(), 0);
  }

  @Ignore("conditional_write statistics not yet fully implemented")
  @Test
  public void testConditionalWriteStatisticsWithIfNoneMatch() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();

    FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null, false);
    updateStatistics(stream);
    stream.write(SMALL_FILE_BYTES);
    stream.close();

    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE.getSymbol(), 1);
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE_FAILED.getSymbol(), 0);

    intercept(RemoteFileChangedException.class,
            () -> {
              // try again with If-None-Match. should fail
              FSDataOutputStream s = getStreamWithFlags(fs, testFile, true, null, false);
              updateStatistics(s);
              s.write(SMALL_FILE_BYTES);
              s.close();
              return "Second write using If-None-Match should have failed due to existing file." + s;
            }
    );

    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE.getSymbol(), 1);
    verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE_FAILED.getSymbol(), 1);
  }

  /**
   * Tests that a conditional create operation is triggered when the performance flag is enabled
   * and the overwrite option is set to false.
   */
  @Test
  public void testConditionalCreateWhenPerformanceFlagEnabledAndOverwriteDisabled() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();
    fs.mkdirs(testFile.getParent());

    // Create a file
    createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, false, null);

    // Attempt to override the file without overwrite and performance flag.
    // Should throw RemoteFileChangedException (due to conditional write operation)
    intercept(RemoteFileChangedException.class, () -> {
      FSDataOutputStreamBuilder cf = fs.createFile(testFile);
      cf.overwrite(false);
      cf.must(FS_S3A_CREATE_PERFORMANCE, true);
      try (FSDataOutputStream stream = cf.build()) {
        assertHasCapabilityConditionalCreate(stream);
        stream.write(SMALL_FILE_BYTES);
        updateStatistics(stream);
      }
    });

    // TODO: uncomment when statistics are getting initialised
    // verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE.getSymbol(), 0);
    // verifyStatisticCounterValue(statistics.getIOStatistics(), Statistic.CONDITIONAL_CREATE_FAILED.getSymbol(), 1);
  }
}
