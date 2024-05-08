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
import java.util.List;

import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.MultipartTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyPathExists;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.BASE;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_PATH_PREFIX;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.STREAM_CAPABILITY_MAGIC_OUTPUT;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.XA_MAGIC_MARKER;
import static org.apache.hadoop.fs.s3a.commit.impl.CommitOperations.extractMagicFileLength;

/**
 * Helper for committer tests: extra assertions and the like.
 */
public class CommitterTestHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommitterTestHelper.class);

  /**
   * Filesystem under test.
   */
  private final S3AFileSystem fileSystem;

  public static final String JOB_ID = "123";

  /**
   * Constructor.
   * @param fileSystem filesystem to work with.
   */
  public CommitterTestHelper(S3AFileSystem fileSystem) {
    this.fileSystem = requireNonNull(fileSystem);
  }

  /**
   * Get the filesystem.
   * @return the filesystem.
   */
  public S3AFileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * Assert a path refers to a marker file of an expected length;
   * the length is extracted from the custom header.
   * @param path magic file.
   * @param dataSize expected data size
   * @throws IOException IO failure
   */
  public void assertIsMarkerFile(Path path, long dataSize) throws IOException {
    final S3AFileSystem fs = getFileSystem();
    FileStatus status = verifyPathExists(fs,
        "uploaded file commit", path);
    Assertions.assertThat(status.getLen())
        .describedAs("Marker File file %s: %s", path, status)
        .isEqualTo(0);
    Assertions.assertThat(extractMagicFileLength(fs, path))
        .describedAs("XAttribute " + XA_MAGIC_MARKER + " of " + path)
        .isNotEmpty()
        .hasValue(dataSize);
  }

  /**
   * Assert a file does not have the magic marker header.
   * @param path magic file.
   * @throws IOException IO failure
   */
  public void assertFileLacksMarkerHeader(Path path) throws IOException {
    Assertions.assertThat(extractMagicFileLength(getFileSystem(),
            path))
        .describedAs("XAttribute " + XA_MAGIC_MARKER + " of " + path)
        .isEmpty();
  }

  /**
   * Create a new path which has the same filename as the dest file, but
   * is in a magic directory under the destination dir.
   * @param destFile final destination file
   * @return magic path
   */
  public static Path makeMagic(Path destFile) {
    return new Path(destFile.getParent(),
        MAGIC_PATH_PREFIX + JOB_ID + '/' + BASE + "/" + destFile.getName());
  }

  /**
   * Assert that an output stream is magic.
   * @param stream stream to probe.
   */
  public static void assertIsMagicStream(final FSDataOutputStream stream) {
    Assertions.assertThat(stream.hasCapability(STREAM_CAPABILITY_MAGIC_OUTPUT))
        .describedAs("Stream capability %s in stream %s",
            STREAM_CAPABILITY_MAGIC_OUTPUT, stream)
        .isTrue();
  }

  /**
   * Abort all multipart uploads under a path.
   * @param path path for uploads to abort; may be null
   * @return a count of aborts
   * @throws IOException trouble.
   */
  public void abortMultipartUploadsUnderPath(Path path) {

    MultipartTestUtils.clearAnyUploads(getFileSystem(), path);
  }

  /**
   * Get a list of all pending uploads under a prefix, one which can be printed.
   * @param prefix prefix to look under
   * @return possibly empty list
   * @throws IOException IO failure.
   */
  public List<String> listMultipartUploads(
      String prefix) throws IOException {

    return MultipartTestUtils.listMultipartUploads(getFileSystem(), prefix);
  }

}
