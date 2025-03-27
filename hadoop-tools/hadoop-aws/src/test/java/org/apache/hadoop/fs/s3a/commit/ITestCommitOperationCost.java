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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.files.PersistentCommitData;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfAnalyticsAcceleratorEnabled;
import static org.apache.hadoop.fs.s3a.Statistic.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.COMMITTER_MAGIC_FILES_CREATED;
import static org.apache.hadoop.fs.s3a.Statistic.COMMITTER_MAGIC_MARKER_PUT;
import static org.apache.hadoop.fs.s3a.Statistic.DIRECTORIES_CREATED;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_METADATA_REQUESTS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_INITIATED;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_PUT_REQUESTS;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_PATH_PREFIX;
import static org.apache.hadoop.fs.s3a.commit.CommitterTestHelper.JOB_ID;
import static org.apache.hadoop.fs.s3a.commit.CommitterTestHelper.assertIsMagicStream;
import static org.apache.hadoop.fs.s3a.commit.CommitterTestHelper.makeMagic;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.LIST_FILES_LIST_OP;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.LIST_OPERATION;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.NO_HEAD_OR_LIST;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.util.functional.RemoteIterators.toList;

/**
 * Assert cost of commit operations;
 * <ol>
 *   <li>Loading pending files from FileStatus entries skips HEAD checks.</li>
 *   <li>Mkdir under magic dirs doesn't check ancestor or dest type</li>
 * </ol>
 */
public class ITestCommitOperationCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestCommitOperationCost.class);

  /**
   * Helper for the tests.
   */
  private CommitterTestHelper testHelper;

  @Override
  public void setup() throws Exception {
    super.setup();
    testHelper = new CommitterTestHelper(getFileSystem());
  }

  @Override
  public void teardown() throws Exception {
    try {
      if (testHelper != null) {
        testHelper.abortMultipartUploadsUnderPath(methodPath());
      }
    } finally {
      super.teardown();
    }
  }

  /**
   * Get a method-relative path.
   * @param filename filename
   * @return new path
   * @throws IOException failure to create/parse the path.
   */
  private Path methodSubPath(String filename) throws IOException {
    return new Path(methodPath(), filename);
  }

  /**
   * Return the FS IOStats, prettified.
   * @return string for assertions.
   */
  protected String fileSystemIOStats() {
    return ioStatisticsToPrettyString(getFileSystem().getIOStatistics());
  }

  /**
   * When a magic subdir is deleted, parent dirs are not recreated.
   */
  @Test
  public void testMagicMkdirs() throws Throwable {
    describe("Mkdirs __magic_job-<jobId>/subdir always skips dir marker recreation");
    S3AFileSystem fs = getFileSystem();
    Path baseDir = methodPath();
    Path magicDir = new Path(baseDir, MAGIC_PATH_PREFIX + JOB_ID);
    fs.delete(baseDir, true);

    Path magicSubdir = new Path(magicDir, "subdir");
    verifyMetrics(() -> {
      fs.mkdirs(magicSubdir, FsPermission.getDirDefault());
      return "after mkdirs " + fileSystemIOStats();
    },
        always(LIST_OPERATION),
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_REQUEST, 0),
        with(DIRECTORIES_CREATED, 1));
    assertPathExists("magicSubdir", magicSubdir);

    verifyMetrics(() -> {
      fs.delete(magicSubdir, true);
      return "after delete " + fileSystemIOStats();
    },
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_REQUEST, 1),
        with(OBJECT_LIST_REQUEST, 1),
        with(OBJECT_METADATA_REQUESTS, 1),
        with(DIRECTORIES_CREATED, 0));
    // no marker dir creation
    assertPathDoesNotExist("magicDir", magicDir);
    assertPathDoesNotExist("baseDir", baseDir);
  }

  /**
   * Active stream; a field is used so closures can write to
   * it.
   */
  private FSDataOutputStream stream;

  /**
   * Abort any active stream.
   * @throws IOException failure
   */
  private void abortActiveStream() throws IOException {
    if (stream != null) {
      stream.abort();
      stream.close();
    }
  }

  @Test
  public void testCostOfCreatingMagicFile() throws Throwable {
    describe("Files created under magic paths skip existence checks and marker deletes");

    // Analytics accelerator currently does not support IOStatistics, this will be added as
    // part of https://issues.apache.org/jira/browse/HADOOP-19364
    skipIfAnalyticsAcceleratorEnabled(getConfiguration(),
        "Analytics Accelerator currently does not support stream statistics");
    S3AFileSystem fs = getFileSystem();
    Path destFile = methodSubPath("file.txt");
    fs.delete(destFile.getParent(), true);
    Path magicDest = makeMagic(destFile);

    // when the file is created, there is no check for overwrites
    // or the dest being a directory, even if overwrite=false
    try {
      verifyMetrics(() -> {
        stream = fs.create(magicDest, false);
        return stream.toString();
      },
          always(NO_HEAD_OR_LIST),
          with(COMMITTER_MAGIC_FILES_CREATED, 1),
          with(COMMITTER_MAGIC_MARKER_PUT, 0),
          with(OBJECT_MULTIPART_UPLOAD_INITIATED, 1));
      assertIsMagicStream(stream);

      stream.write("hello".getBytes(StandardCharsets.UTF_8));

      // when closing, we expect two PUT requests,
      // because the marker and manifests are both written
      LOG.info("closing magic stream to {}", magicDest);
      verifyMetrics(() -> {
        stream.close();
        return stream.toString();
      },
          always(NO_HEAD_OR_LIST),
          with(OBJECT_PUT_REQUESTS, 2),
          with(COMMITTER_MAGIC_MARKER_PUT, 2),
          with(OBJECT_BULK_DELETE_REQUEST, 0),
          with(OBJECT_DELETE_REQUEST, 0));

    } catch (Exception e) {
      abortActiveStream();
      throw e;
    }
    // list the manifests
    final CommitOperations commitOperations = new CommitOperations(fs);
    List<LocatedFileStatus> pending = verifyMetrics(() ->
            toList(commitOperations.
                locateAllSinglePendingCommits(magicDest.getParent(), false)),
        always(LIST_FILES_LIST_OP));
    Assertions.assertThat(pending)
        .describedAs("pending commits")
        .hasSize(1);

    // load the only pending commit
    SinglePendingCommit singleCommit = verifyMetrics(() ->
            PersistentCommitData.load(fs,
                pending.get(0),
                SinglePendingCommit.serializer()),
        always(NO_HEAD_OR_LIST),
        with(ACTION_HTTP_GET_REQUEST, 1));

    // commit it through the commit operations.
    verifyMetrics(() -> {
      commitOperations.commitOrFail(singleCommit);
      return ioStatisticsToPrettyString(
          commitOperations.getIOStatistics());
    },
        always(NO_HEAD_OR_LIST),  // no probes for the dest path
        with(OBJECT_DELETE_REQUEST, 0)); // no deletes

    LOG.info("Final Statistics {}",
        IOStatisticsLogging.ioStatisticsToPrettyString(stream.getIOStatistics()));
  }

  /**
   * saving pending files MUST NOT trigger HEAD/LIST calls
   * when created under a magic path; when opening
   * with an S3AFileStatus the HEAD will be skipped too.
   */
  @Test
  public void testCostOfSavingLoadingPendingFile() throws Throwable {
    describe("Verify costs of saving .pending file under a magic path");

    // Analytics accelerator currently does not support IOStatistics, this will be added as
    // part of https://issues.apache.org/jira/browse/HADOOP-19364
    skipIfAnalyticsAcceleratorEnabled(getConfiguration(),
        "Analytics Accelerator currently does not support stream statistics");
    S3AFileSystem fs = getFileSystem();
    Path partDir = methodSubPath("file.pending");
    Path destFile = new Path(partDir, "file.pending");
    Path magicDest = makeMagic(destFile);
    // create a pending file with minimal values needed
    // for validation to work
    final SinglePendingCommit commit = new SinglePendingCommit();
    commit.touch(System.currentTimeMillis());
    commit.setUri(destFile.toUri().toString());
    commit.setBucket(fs.getBucket());
    commit.setLength(0);
    commit.setDestinationKey(fs.pathToKey(destFile));
    commit.setUploadId("uploadId");
    commit.setEtags(new ArrayList<>());
    // fail fast if the commit data is incomplete
    commit.validate();

    // save the file: no checks will be made
    verifyMetrics(() -> {
          commit.save(fs, magicDest,
              SinglePendingCommit.serializer());
          return commit.toString();
        },
        with(COMMITTER_MAGIC_FILES_CREATED, 0),
        always(NO_HEAD_OR_LIST),
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_REQUEST, 0)
    );

    LOG.info("File written; Validating");
    testHelper.assertFileLacksMarkerHeader(magicDest);
    FileStatus status = fs.getFileStatus(magicDest);

    LOG.info("Reading file {}", status);
    // opening a file with a status passed in will skip the HEAD
    verifyMetrics(() ->
            PersistentCommitData.load(fs, status, SinglePendingCommit.serializer()),
        always(NO_HEAD_OR_LIST),
        with(ACTION_HTTP_GET_REQUEST, 1));
  }

}
