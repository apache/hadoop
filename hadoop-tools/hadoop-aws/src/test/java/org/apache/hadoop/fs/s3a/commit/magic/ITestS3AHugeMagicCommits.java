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

package org.apache.hadoop.fs.s3a.commit.magic;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.CommitUtils;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.impl.CommitContext;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.s3a.scale.AbstractSTestS3AHugeFiles;

import static org.apache.hadoop.fs.s3a.MultipartTestUtils.listMultipartUploads;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.extractXAttrLongValue;


/**
 * Write a huge file via the magic commit mechanism,
 * commit it and verify that it is there. This is needed to
 * verify that the pending-upload mechanism works with multipart files
 * of more than one part.
 *
 * This is a scale test.
 */
public class ITestS3AHugeMagicCommits extends AbstractSTestS3AHugeFiles {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3AHugeMagicCommits.class);
  private static final int COMMITTER_THREADS = 64;

  private Path magicDir;
  private Path jobDir;

  /** file used as the destination for the write;
   *  it is never actually created. */
  private Path magicOutputFile;

  /** The file with the JSON data about the commit. */
  private Path pendingDataFile;

  private Path finalDirectory;

  /**
   * Use fast upload on disk.
   * @return the upload buffer mechanism.
   */
  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_DISK;
  }

  /**
   * The suite name; required to be unique.
   * @return the test suite name
   */
  @Override
  public String getTestSuiteName() {
    return "ITestS3AHugeMagicCommits";
  }

  @Override
  protected boolean expectImmediateFileVisibility() {
    return false;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    CommitUtils.verifyIsMagicCommitFS(getFileSystem());

    // set up the paths for the commit operation
    finalDirectory = new Path(getScaleTestDir(), "commit");
    magicDir = new Path(finalDirectory, MAGIC);
    jobDir = new Path(magicDir, "job_001");
    String filename = "commit.bin";
    setHugefile(new Path(finalDirectory, filename));
    magicOutputFile = new Path(jobDir, filename);
    pendingDataFile = new Path(jobDir, filename + PENDING_SUFFIX);
  }

  /**
   * Returns the path to the commit metadata file, not that of the huge file.
   * @return a file in the job dir
   */
  @Override
  protected Path getPathOfFileToCreate() {
    return magicOutputFile;
  }

  @Override
  public void test_030_postCreationAssertions() throws Throwable {
    describe("Committing file");
    assertPathDoesNotExist("final file exists", getHugefile());
    assertPathExists("No pending file", pendingDataFile);
    S3AFileSystem fs = getFileSystem();

    // as a 0-byte marker is created, there is a file at the end path,
    // it just MUST be 0-bytes long
    FileStatus status = fs.getFileStatus(magicOutputFile);
    assertEquals("Non empty marker file " + status,
        0, status.getLen());
    final Map<String, byte[]> xAttr = fs.getXAttrs(magicOutputFile);
    final String header = XA_MAGIC_MARKER;
    Assertions.assertThat(xAttr)
        .describedAs("Header %s of %s", header, magicOutputFile)
        .containsKey(header);
    Assertions.assertThat(extractXAttrLongValue(xAttr.get(header)))
        .describedAs("Decoded header %s of %s", header, magicOutputFile)
        .get()
        .isEqualTo(getFilesize());
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    CommitOperations operations = new CommitOperations(fs);
    Path destDir = getHugefile().getParent();
    assertPathExists("Magic dir", new Path(destDir, CommitConstants.MAGIC));
    String destDirKey = fs.pathToKey(destDir);

    Assertions.assertThat(listMultipartUploads(fs, destDirKey))
        .describedAs("Pending uploads")
        .hasSize(1);
    assertNotNull("jobDir", jobDir);
    try(CommitContext commitContext
            = operations.createCommitContextForTesting(jobDir, null, COMMITTER_THREADS)) {
      Pair<PendingSet, List<Pair<LocatedFileStatus, IOException>>>
          results = operations.loadSinglePendingCommits(jobDir, false, commitContext
      );
      for (SinglePendingCommit singlePendingCommit :
          results.getKey().getCommits()) {
        commitContext.commitOrFail(singlePendingCommit);
      }
    }
    timer.end("time to commit %s", pendingDataFile);
    // upload is no longer pending
    Assertions.assertThat(operations.listPendingUploadsUnderPath(destDir))
        .describedAs("Pending uploads undedr path")
        .isEmpty();
    // at this point, the huge file exists, so the normal assertions
    // on that file must be valid. Verify.
    super.test_030_postCreationAssertions();
  }

  private void skipQuietly(String text) {
    describe("Skipping: %s", text);
  }

  @Override
  public void test_040_PositionedReadHugeFile() {
    skipQuietly("test_040_PositionedReadHugeFile");
  }

  @Override
  public void test_050_readHugeFile() {
    skipQuietly("readHugeFile");
  }

  @Override
  public void test_100_renameHugeFile() {
    skipQuietly("renameHugeFile");
  }

  @Override
  public void test_800_DeleteHugeFiles() throws IOException {
    if (getFileSystem() != null) {
      try {
        getFileSystem().abortOutstandingMultipartUploads(0);
      } catch (IOException e) {
        LOG.info("Exception while purging old uploads", e);
      }
    }
    try {
      super.test_800_DeleteHugeFiles();
    } finally {
      ContractTestUtils.rm(getFileSystem(), magicDir, true, false);
    }
  }
}
