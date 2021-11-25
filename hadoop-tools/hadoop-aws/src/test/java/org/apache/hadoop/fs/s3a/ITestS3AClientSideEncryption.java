/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.rm;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_MIN_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBool;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests to verify S3 Client-Side Encryption (CSE).
 */
public abstract class ITestS3AClientSideEncryption extends AbstractS3ATestBase {

  private static final List<Integer> SIZES =
      new ArrayList<>(Arrays.asList(0, 1, 255, 4095));

  private static final int BIG_FILE_SIZE = 15 * 1024 * 1024;
  private static final int SMALL_FILE_SIZE = 1024;

  /**
   * Testing S3 CSE on different file sizes.
   */
  @Test
  public void testEncryption() throws Throwable {
    describe("Test to verify client-side encryption for different file sizes.");
    for (int size : SIZES) {
      validateEncryptionForFileSize(size);
    }
  }

  /**
   * Testing the S3 client side encryption over rename operation.
   */
  @Test
  public void testEncryptionOverRename() throws Throwable {
    describe("Test for AWS CSE on Rename Operation.");
    maybeSkipTest();
    S3AFileSystem fs = getFileSystem();
    Path src = path(getMethodName());
    byte[] data = dataset(SMALL_FILE_SIZE, 'a', 'z');
    writeDataset(fs, src, data, data.length, SMALL_FILE_SIZE,
        true, false);

    ContractTestUtils.verifyFileContents(fs, src, data);
    Path dest = path(src.getName() + "-copy");
    fs.rename(src, dest);
    ContractTestUtils.verifyFileContents(fs, dest, data);
    assertEncrypted(dest);
  }

  /**
   * Test to verify if we get same content length of files in S3 CSE using
   * listStatus and listFiles on the parent directory.
   */
  @Test
  public void testDirectoryListingFileLengths() throws IOException {
    describe("Test to verify directory listing calls gives correct content "
        + "lengths");
    maybeSkipTest();
    S3AFileSystem fs = getFileSystem();
    Path parentDir = path(getMethodName());

    // Creating files in the parent directory that will be used to assert
    // content length.
    for (int i : SIZES) {
      Path child = new Path(parentDir, getMethodName() + i);
      writeThenReadFile(child, i);
    }

    // Getting the content lengths of files inside the directory via FileStatus.
    List<Integer> fileLengthDirListing = new ArrayList<>();
    for (FileStatus fileStatus : fs.listStatus(parentDir)) {
      fileLengthDirListing.add((int) fileStatus.getLen());
    }
    // Assert the file length we got against expected file length for
    // ListStatus.
    Assertions.assertThat(fileLengthDirListing)
        .describedAs("File lengths aren't the same "
            + "as expected from FileStatus dir. listing")
        .containsExactlyInAnyOrderElementsOf(SIZES);

    // Getting the content lengths of files inside the directory via ListFiles.
    RemoteIterator<LocatedFileStatus> listDir = fs.listFiles(parentDir, true);
    List<Integer> fileLengthListLocated = new ArrayList<>();
    while (listDir.hasNext()) {
      LocatedFileStatus fileStatus = listDir.next();
      fileLengthListLocated.add((int) fileStatus.getLen());
    }
    // Assert the file length we got against expected file length for
    // LocatedFileStatus.
    Assertions.assertThat(fileLengthListLocated)
        .describedAs("File lengths isn't same "
            + "as expected from LocatedFileStatus dir. listing")
        .containsExactlyInAnyOrderElementsOf(SIZES);

  }

  /**
   * Test to verify multipart upload through S3ABlockOutputStream and
   * verifying the contents of the uploaded file.
   */
  @Test
  public void testBigFilePutAndGet() throws IOException {
    maybeSkipTest();
    assume("Scale test disabled: to enable set property " +
        KEY_SCALE_TESTS_ENABLED, getTestPropertyBool(
        getConfiguration(),
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED));
    S3AFileSystem fs = getFileSystem();
    Path filePath = path(getMethodName());
    byte[] fileContent = dataset(BIG_FILE_SIZE, 'a', 26);
    int offsetSeek = fileContent[BIG_FILE_SIZE - 4];

    // PUT a 15MB file using CSE to force multipart in CSE.
    createFile(fs, filePath, true, fileContent);
    LOG.info("Multi-part upload successful...");

    try (FSDataInputStream in = fs.open(filePath)) {
      // Verify random IO.
      in.seek(BIG_FILE_SIZE - 4);
      assertEquals("Byte at a specific position not equal to actual byte",
          offsetSeek, in.read());
      in.seek(0);
      assertEquals("Byte at a specific position not equal to actual byte",
          'a', in.read());

      // Verify seek-read between two multipart blocks.
      in.seek(MULTIPART_MIN_SIZE - 1);
      int byteBeforeBlockEnd = fileContent[MULTIPART_MIN_SIZE];
      assertEquals("Byte before multipart block end mismatch",
          byteBeforeBlockEnd - 1, in.read());
      assertEquals("Byte at multipart end mismatch",
          byteBeforeBlockEnd, in.read());
      assertEquals("Byte after multipart end mismatch",
          byteBeforeBlockEnd + 1, in.read());

      // Verify end of file seek read.
      in.seek(BIG_FILE_SIZE + 1);
      assertEquals("Byte at eof mismatch",
          -1, in.read());

      // Verify full read.
      in.readFully(0, fileContent);
      verifyFileContents(fs, filePath, fileContent);
    }
  }

  /**
   * Testing how unencrypted and encrypted data behaves when read through
   * CSE enabled and disabled FS respectively.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testEncryptionEnabledAndDisabledFS() throws Exception {
    maybeSkipTest();
    S3AFileSystem cseDisabledFS = new S3AFileSystem();
    Configuration cseDisabledConf = getConfiguration();
    S3AFileSystem cseEnabledFS = getFileSystem();
    Path unEncryptedFilePath = path(getMethodName());
    Path encryptedFilePath = path(getMethodName() + "cse");

    // Initialize a CSE disabled FS.
    removeBaseAndBucketOverrides(getTestBucketName(cseDisabledConf),
        cseDisabledConf,
        S3_ENCRYPTION_ALGORITHM,
        S3_ENCRYPTION_KEY,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY);
    cseDisabledFS.initialize(getFileSystem().getUri(),
        cseDisabledConf);

    // Verifying both FS instances using an IOStat gauge.
    IOStatistics cseDisabledIOStats = cseDisabledFS.getIOStatistics();
    IOStatistics cseEnabledIOStatistics = cseEnabledFS.getIOStatistics();
    IOStatisticAssertions.assertThatStatisticGauge(cseDisabledIOStats,
        Statistic.CLIENT_SIDE_ENCRYPTION_ENABLED.getSymbol()).isEqualTo(0L);
    IOStatisticAssertions.assertThatStatisticGauge(cseEnabledIOStatistics,
        Statistic.CLIENT_SIDE_ENCRYPTION_ENABLED.getSymbol()).isEqualTo(1L);

    // Unencrypted data written to a path.
    try (FSDataOutputStream out = cseDisabledFS.create(unEncryptedFilePath)) {
      out.write(new byte[SMALL_FILE_SIZE]);
    }

    // CSE enabled FS trying to read unencrypted data would face an exception.
    try (FSDataInputStream in = cseEnabledFS.open(unEncryptedFilePath)) {
      FileStatus encryptedFSFileStatus =
          cseEnabledFS.getFileStatus(unEncryptedFilePath);
      assertEquals("Mismatch in content length bytes", SMALL_FILE_SIZE,
          encryptedFSFileStatus.getLen());

      intercept(SecurityException.class, "",
          "SecurityException should be thrown",
          () -> {
            in.read(new byte[SMALL_FILE_SIZE]);
            return "Exception should be raised if unencrypted data is read by "
                + "a CSE enabled FS";
          });
    }

    // Encrypted data written to a path.
    try (FSDataOutputStream out = cseEnabledFS.create(encryptedFilePath)) {
      out.write('a');
    }

    // CSE disabled FS tries to read encrypted data.
    try (FSDataInputStream in = cseDisabledFS.open(encryptedFilePath)) {
      FileStatus unEncryptedFSFileStatus =
          cseDisabledFS.getFileStatus(encryptedFilePath);
      // Due to padding and encryption, content written and length shouldn't be
      // equal to what a CSE disabled FS would read.
      assertNotEquals("Mismatch in content length", 1,
          unEncryptedFSFileStatus.getLen());
      Assertions.assertThat(in.read())
          .describedAs("Encrypted data shouldn't be equal to actual content "
              + "without deciphering")
          .isNotEqualTo('a');
    }
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf, Constants.MULTIPART_SIZE,
        Constants.MIN_MULTIPART_THRESHOLD);
    // To force multi part put and get in small files, we'll set the
    // threshold and part size to 5MB.
    conf.set(Constants.MULTIPART_SIZE,
        String.valueOf(MULTIPART_MIN_SIZE));
    conf.set(Constants.MIN_MULTIPART_THRESHOLD,
        String.valueOf(MULTIPART_MIN_SIZE));
    return conf;
  }

  /**
   * Method to validate CSE for different file sizes.
   *
   * @param len length of the file.
   */
  protected void validateEncryptionForFileSize(int len) throws IOException {
    maybeSkipTest();
    describe("Create an encrypted file of size " + len);
    // Creating a unique path by adding file length in file name.
    Path path = writeThenReadFile(getMethodName() + len, len);
    assertEncrypted(path);
    rm(getFileSystem(), path, false, false);
  }

  /**
   * Skip tests if certain conditions are met.
   */
  protected abstract void maybeSkipTest() throws IOException;

  /**
   * Assert that at path references an encrypted blob.
   *
   * @param path path
   * @throws IOException on a failure
   */
  protected abstract void assertEncrypted(Path path) throws IOException;

}
