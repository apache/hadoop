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

package org.apache.hadoop.fs.s3a.scale;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.util.DurationInfo;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.contract.ContractTestUtils.rm;
import static org.apache.hadoop.fs.s3a.Constants.BULK_DELETE_PAGE_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.EXPERIMENTAL_AWS_INTERNAL_THROTTLING;
import static org.apache.hadoop.fs.s3a.Constants.USER_AGENT_PREFIX;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.lsR;
import static org.apache.hadoop.fs.s3a.impl.ITestPartialRenamesDeletes.createFiles;
import static org.apache.hadoop.test.GenericTestUtils.filenameOfIndex;

/**
 * Test some scalable operations related to file renaming and deletion.
 * We set a bulk page size low enough that even the default test scale will
 * issue multiple delete requests during a delete sequence -so test that
 * operation more efficiently.
 */
public class ITestS3ADeleteManyFiles extends S3AScaleTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ADeleteManyFiles.class);

  /**
   * Delete Page size: {@value}.
   */
  static final int DELETE_PAGE_SIZE = 50;


  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
        EXPERIMENTAL_AWS_INTERNAL_THROTTLING,
        BULK_DELETE_PAGE_SIZE,
        USER_AGENT_PREFIX);
    conf.setBoolean(EXPERIMENTAL_AWS_INTERNAL_THROTTLING, false);
    conf.setInt(BULK_DELETE_PAGE_SIZE, DELETE_PAGE_SIZE);
    return conf;
  }

  /**
   * CAUTION: If this test starts failing, please make sure that the
   * {@link org.apache.hadoop.fs.s3a.Constants#MAX_THREADS} configuration is not
   * set too low. Alternatively, consider reducing the
   * <code>scale.test.operation.count</code> parameter in
   * <code>getOperationCount()</code>.
   * If it is slow: look at the size of any S3Guard Table used.
   * @see #getOperationCount()
   */
  @Test
  public void testBulkRenameAndDelete() throws Throwable {
    final int count = getConf().getInt(KEY_FILE_COUNT,
        DEFAULT_FILE_COUNT);
    describe("Testing bulk rename and delete of %d files", count);

    final Path scaleTestDir = path("testBulkRenameAndDelete");
    final Path srcParentDir = new Path(scaleTestDir, "srcParent");
    final Path srcDir = new Path(srcParentDir, "src");
    final Path finalParentDir = new Path(scaleTestDir, "finalParent");
    final Path finalDir = new Path(finalParentDir, "final");
    final S3AFileSystem fs = getFileSystem();
    rm(fs, scaleTestDir, true, false);
    fs.mkdirs(srcDir);
    fs.mkdirs(finalParentDir);

    createFiles(fs, srcDir, 1, count, 0);

    FileStatus[] statuses = fs.listStatus(srcDir);
    int nSrcFiles = statuses.length;
    long sourceSize = Arrays.stream(statuses)
        .mapToLong(FileStatus::getLen)
        .sum();
    assertEquals("Source file Count", count, nSrcFiles);
    ContractTestUtils.NanoTimer renameTimer = new ContractTestUtils.NanoTimer();
    try (DurationInfo ignored = new DurationInfo(LOG,
        "Rename %s to %s", srcDir, finalDir)) {
      rename(srcDir, finalDir);
    }
    renameTimer.end();
    LOG.info("Effective rename bandwidth {} MB/s",
        renameTimer.bandwidthDescription(sourceSize));
    LOG.info(String.format(
        "Time to rename a file: %,03f milliseconds",
        (renameTimer.nanosPerOperation(count) * 1.0f) / 1.0e6));
    Assertions.assertThat(lsR(fs, srcParentDir, true))
        .describedAs("Recursive listing of source dir %s", srcParentDir)
        .isEqualTo(0);

    assertPathDoesNotExist("not deleted after rename",
        new Path(srcDir, filenameOfIndex(0)));
    assertPathDoesNotExist("not deleted after rename",
        new Path(srcDir, filenameOfIndex(count / 2)));
    assertPathDoesNotExist("not deleted after rename",
        new Path(srcDir, filenameOfIndex(count - 1)));

    // audit destination
    Assertions.assertThat(lsR(fs, finalDir, true))
        .describedAs("size of recursive destination listFiles(%s)", finalDir)
        .isEqualTo(count);
    Assertions.assertThat(fs.listStatus(finalDir))
        .describedAs("size of destination listStatus(%s)", finalDir)
        .hasSize(count);

    assertPathExists("not renamed to dest dir",
        new Path(finalDir, filenameOfIndex(0)));
    assertPathExists("not renamed to dest dir",
        new Path(finalDir, filenameOfIndex(count / 2)));
    assertPathExists("not renamed to dest dir",
        new Path(finalDir, filenameOfIndex(count - 1)));

    ContractTestUtils.NanoTimer deleteTimer =
        new ContractTestUtils.NanoTimer();
    try (DurationInfo ignored = new DurationInfo(LOG,
        "Delete subtree %s", finalDir)) {
      assertDeleted(finalDir, true);
    }
    deleteTimer.end();
    LOG.info(String.format(
        "Time to delete an object %,03f milliseconds",
        (deleteTimer.nanosPerOperation(count) * 1.0f) / 1.0e6));
    Assertions.assertThat(lsR(fs, finalParentDir, true))
        .describedAs("Recursive listing of deleted rename destination %s",
            finalParentDir)
        .isEqualTo(0);

  }

}
