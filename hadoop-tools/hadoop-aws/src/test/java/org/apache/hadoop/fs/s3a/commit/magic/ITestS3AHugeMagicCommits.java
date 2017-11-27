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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.CommitOperations;
import org.apache.hadoop.fs.s3a.commit.CommitUtils;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.scale.AbstractSTestS3AHugeFiles;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;


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

  private Path magicDir;
  private Path jobDir;

  /** file used as the destination for the write;
   *  it is never actually created. */
  private Path magicOutputFile;

  /** The file with the JSON data about the commit. */
  private Path pendingDataFile;

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

  /**
   * Create the scale IO conf with the committer enabled.
   * @return the configuration to use for the test FS.
   */
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    conf.setBoolean(MAGIC_COMMITTER_ENABLED, true);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    CommitUtils.verifyIsMagicCommitFS(getFileSystem());

    // set up the paths for the commit operation
    Path finalDirectory = new Path(getScaleTestDir(), "commit");
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
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    CommitOperations operations = new CommitOperations(fs);
    Path destDir = getHugefile().getParent();
    assertPathExists("Magic dir", new Path(destDir, CommitConstants.MAGIC));
    String destDirKey = fs.pathToKey(destDir);
    List<String> uploads = listMultipartUploads(fs, destDirKey);

    assertEquals("Pending uploads: "
        + uploads.stream()
        .collect(Collectors.joining("\n")), 1, uploads.size());
    assertNotNull("jobDir", jobDir);
    Pair<PendingSet, List<Pair<LocatedFileStatus, IOException>>>
        results = operations.loadSinglePendingCommits(jobDir, false);
    for (SinglePendingCommit singlePendingCommit :
        results.getKey().getCommits()) {
      operations.commitOrFail(singlePendingCommit);
    }
    timer.end("time to commit %s", pendingDataFile);
    // upload is no longer pending
    uploads = listMultipartUploads(fs, destDirKey);
    assertEquals("Pending uploads"
            + uploads.stream().collect(Collectors.joining("\n")),
        0, operations.listPendingUploadsUnderPath(destDir).size());
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
