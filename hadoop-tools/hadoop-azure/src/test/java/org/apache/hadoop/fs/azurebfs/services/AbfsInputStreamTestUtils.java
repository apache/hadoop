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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.assertj.core.api.Assertions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

import static org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest.SHORTENED_GUID_LEN;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_OPTIMIZE_FOOTER_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_SMALL_FILES_COMPLETELY;

public class AbfsInputStreamTestUtils {

  public static final int HUNDRED = 100;

  private final AbstractAbfsIntegrationTest abstractAbfsIntegrationTest;

  public AbfsInputStreamTestUtils(AbstractAbfsIntegrationTest abstractAbfsIntegrationTest) {
    this.abstractAbfsIntegrationTest = abstractAbfsIntegrationTest;
  }

  private Path path(String filepath) throws IOException {
    return abstractAbfsIntegrationTest.getFileSystem().makeQualified(
        new Path(getTestPath(), getUniquePath(filepath)));
  }

  private Path getTestPath() {
    Path path = new Path(UriUtils.generateUniqueTestPath());
    return path;
  }

  /**
   * Generate a unique path using the given filepath.
   * @param filepath path string
   * @return unique path created from filepath and a GUID
   */
  private Path getUniquePath(String filepath) {
    if (filepath.equals("/")) {
      return new Path(filepath);
    }
    return new Path(filepath + StringUtils
        .right(UUID.randomUUID().toString(), SHORTENED_GUID_LEN));
  }

  /**
   * Returns AzureBlobFileSystem instance with the required
   * readFullFileOptimization configuration.
   *
   * @param readSmallFilesCompletely whether to read small files completely
   * @return AzureBlobFileSystem instance
   * @throws IOException exception in creating fileSystem
   */
  public AzureBlobFileSystem getFileSystem(boolean readSmallFilesCompletely)
      throws IOException {
    Configuration configuration = new Configuration(
        abstractAbfsIntegrationTest.getRawConfiguration());
    configuration.setBoolean(AZURE_READ_SMALL_FILES_COMPLETELY,
        readSmallFilesCompletely);
    configuration.setBoolean(AZURE_READ_OPTIMIZE_FOOTER_READ, false);
    return (AzureBlobFileSystem) FileSystem.newInstance(configuration);
  }

  /**
   * Return array of random bytes of the given length.
   *
   * @param length length of the byte array
   * @return byte array
   */
  public byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  /**
   * Create a file on the file system with the given file name and content.
   *
   * @param fs fileSystem that stores the file
   * @param fileName name of the file
   * @param fileContent content of the file
   *
   * @return path of the file created
   * @throws IOException exception in writing file on fileSystem
   */
  public Path createFileWithContent(FileSystem fs, String fileName,
      byte[] fileContent) throws IOException {
    Path testFilePath = path(fileName);
    try (FSDataOutputStream oStream = fs.create(testFilePath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    return testFilePath;
  }

  /**
   * Assert that the content read from the subsection of a file is correct.
   *
   * @param actualFileContent actual content of the file
   * @param from start index of the content read
   * @param len length of the content read
   * @param contentRead content read from the file
   * @param testFilePath path of the file
   */
  public void assertContentReadCorrectly(byte[] actualFileContent, int from,
      int len, byte[] contentRead, Path testFilePath) {
    Assertions.assertThat(actualFileContent.length)
        .describedAs("From + len should be less than or equal to "
            + "the actual file content length")
        .isGreaterThanOrEqualTo(from + len);
    Assertions.assertThat(contentRead.length)
        .describedAs("Content read length should be greater than or "
            + "equal to the len")
        .isGreaterThanOrEqualTo(len);
    for (int i = 0; i < len; i++) {
      Assertions.assertThat(contentRead[i])
          .describedAs(
              "The test file path is " + testFilePath + ". Equality failed"
                  + "at index " + i
                  + " of the contentRead array. ActualFileContent is being compared from index "
                  + from)
          .isEqualTo(actualFileContent[i + from]);
    }
  }

  /**
   * Assert that the readBuffer in AbfsInputStream contain the correct starting
   * subsequence of the file content.
   *
   * @param actualContent actual content of the file
   * @param abfsInputStream abfsInputStream whose buffer to be asserted
   * @param conf configuration
   * @param testFilePath path of the file
   */
  public void assertAbfsInputStreamBufferEqualToContentStartSubsequence(byte[] actualContent,
      AbfsInputStream abfsInputStream,
      AbfsConfiguration conf,
      Path testFilePath) {
    Assertions.assertThat(abfsInputStream.getBuffer().length)
        .describedAs("ReadBuffer should be lesser than or equal to "
            + "readBufferSize")
        .isLessThanOrEqualTo(conf.getReadBufferSize());
    assertAbfsInputStreamBufferEqualityWithContentStartingSubSequence(
        actualContent, abfsInputStream.getBuffer(), conf,
        false, testFilePath);
  }

  /**
   * Assert that the readBuffer in AbfsInputStream contain the incorrect starting
   * subsequence of the file content.
   *
   * @param actualContent actual content of the file
   * @param abfsInputStream abfsInputStream whose buffer to be asserted
   * @param conf configuration
   * @param testFilePath path of the file
   */
  public void assertAbfsInputStreamBufferNotEqualToContentStartSubsequence(byte[] actualContent,
      AbfsInputStream abfsInputStream,
      AbfsConfiguration conf,
      Path testFilePath) {
    Assertions.assertThat(abfsInputStream.getBuffer().length)
        .describedAs("ReadBuffer should be lesser than or equal to "
            + "readBufferSize")
        .isLessThanOrEqualTo(conf.getReadBufferSize());
    assertAbfsInputStreamBufferEqualityWithContentStartingSubSequence(
        actualContent, abfsInputStream.getBuffer(), conf, true,
        testFilePath);
  }

  /**
   * Assert the equality or inequality of abfsInputStreamReadBuffer with the
   * starting subsequence of the fileContent.
   *
   * @param actualContent actual content of the file
   * @param abfsInputStreamReadBuffer buffer read from the abfsInputStream
   * @param conf configuration
   * @param assertEqual whether to assert equality or inequality
   * @param testFilePath path of the file
   */
  private void assertAbfsInputStreamBufferEqualityWithContentStartingSubSequence(
      byte[] actualContent,
      byte[] abfsInputStreamReadBuffer,
      AbfsConfiguration conf,
      boolean assertEqual,
      Path testFilePath) {
    int bufferSize = conf.getReadBufferSize();
    int actualContentSize = actualContent.length;
    int n = Math.min(actualContentSize, bufferSize);
    int matches = 0;
    for (int i = 0; i < n && i < abfsInputStreamReadBuffer.length; i++) {
      if (actualContent[i] == abfsInputStreamReadBuffer[i]) {
        matches++;
      }
    }
    if (assertEqual) {
      Assertions.assertThat(matches).describedAs(
          "The test file path is " + testFilePath).isEqualTo(n);
    } else {
      Assertions.assertThat(matches).describedAs(
          "The test file path is " + testFilePath).isNotEqualTo(n);
    }
  }

  /**
   * Seek inputStream to the given seekPos.
   *
   * @param iStream inputStream to seek
   * @param seekPos position to seek
   * @throws IOException exception in seeking inputStream
   */
  public void seek(FSDataInputStream iStream, long seekPos)
      throws IOException {
    AbfsInputStream abfsInputStream
        = (AbfsInputStream) iStream.getWrappedStream();
    verifyAbfsInputStreamBaseStateBeforeSeek(abfsInputStream);
    iStream.seek(seekPos);
    verifyAbsInputStreamStateAfterSeek(abfsInputStream, seekPos);
  }

  /**
   * Verifies that the pointers in AbfsInputStream state are unchanged and are
   * equal to that of a newly created inputStream.
   *
   * @param abfsInputStream inputStream to verify
   */
  public void verifyAbfsInputStreamBaseStateBeforeSeek(AbfsInputStream abfsInputStream) {
    Assertions.assertThat(abfsInputStream.getFCursor())
        .describedAs("FCursor should be 0 at the inputStream open")
        .isEqualTo(0);
    Assertions.assertThat(abfsInputStream.getFCursorAfterLastRead())
        .describedAs(
            "FCursorAfterLastRead should be -1 at the inputStream open")
        .isEqualTo(-1);
    Assertions.assertThat(abfsInputStream.getLimit())
        .describedAs("Limit should be 0 at the inputStream open")
        .isEqualTo(0);
    Assertions.assertThat(abfsInputStream.getBCursor())
        .describedAs("BCursor should be 0 at the inputStream open")
        .isEqualTo(0);
  }

  /**
   * Verifies that only the FCursor is updated after seek and all other pointers
   * are in their initial state.
   *
   * @param abfsInputStream inputStream to verify
   * @param seekPos position to seek
   *
   * @throws IOException exception in inputStream operations
   */
  public void verifyAbsInputStreamStateAfterSeek(AbfsInputStream abfsInputStream,
      long seekPos) throws IOException {
    Assertions.assertThat(abfsInputStream.getPos())
        .describedAs("InputStream's pos should be " + seekPos + " after seek")
        .isEqualTo(seekPos);
    Assertions.assertThat(abfsInputStream.getFCursorAfterLastRead())
        .describedAs("FCursorAfterLastRead should be -1 after seek")
        .isEqualTo(-1);
    Assertions.assertThat(abfsInputStream.getLimit())
        .describedAs("Limit should be 0 after seek")
        .isEqualTo(0);
    Assertions.assertThat(abfsInputStream.getBCursor())
        .describedAs("BCursor should be 0 after seek")
        .isEqualTo(0);
  }
}
