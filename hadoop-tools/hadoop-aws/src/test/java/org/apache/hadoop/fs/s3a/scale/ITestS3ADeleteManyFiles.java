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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.impl.ITestPartialRenamesDeletes;
import org.apache.hadoop.util.DurationInfo;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.s3a.impl.ITestPartialRenamesDeletes.createFiles;

/**
 * Test some scalable operations related to file renaming and deletion.
 */
public class ITestS3ADeleteManyFiles extends S3AScaleTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ADeleteManyFiles.class);

  public static final String PREFIX = ITestPartialRenamesDeletes.PREFIX;

  /**
   * CAUTION: If this test starts failing, please make sure that the
   * {@link org.apache.hadoop.fs.s3a.Constants#MAX_THREADS} configuration is not
   * set too low. Alternatively, consider reducing the
   * <code>scale.test.operation.count</code> parameter in
   * <code>getOperationCount()</code>.
   *
   * @see #getOperationCount()
   */
  @Test
  public void testBulkRenameAndDelete() throws Throwable {
    final Path scaleTestDir = path("testBulkRenameAndDelete");
    final Path srcDir = new Path(scaleTestDir, "src");
    final Path finalDir = new Path(scaleTestDir, "final");
    final int count = getConf().getInt(KEY_FILE_COUNT,
        DEFAULT_FILE_COUNT);
    final S3AFileSystem fs = getFileSystem();
    ContractTestUtils.rm(fs, scaleTestDir, true, false);
    fs.mkdirs(srcDir);

    createFiles(fs, srcDir, 1, count, 0);

    FileStatus[] statuses = fs.listStatus(srcDir);
    int nSrcFiles = statuses.length;
    long sourceSize = 0;
    for (FileStatus status : statuses) {
      sourceSize += status.getLen();
    }
    assertEquals("Source file Count", count, nSrcFiles);
    ContractTestUtils.NanoTimer renameTimer = new ContractTestUtils.NanoTimer();
    try (DurationInfo ignored = new DurationInfo(LOG,
        "Rename %s to %s", srcDir, finalDir)) {
      assertTrue("Rename failed", fs.rename(srcDir, finalDir));
    }
    renameTimer.end();
    LOG.info("Effective rename bandwidth {} MB/s",
        renameTimer.bandwidthDescription(sourceSize));
    LOG.info(String.format(
        "Time to rename a file: %,03f milliseconds",
        (renameTimer.nanosPerOperation(count) * 1.0f) / 1.0e6));
    assertEquals(nSrcFiles, fs.listStatus(finalDir).length);
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted after rename",
        new Path(srcDir, PREFIX + 0));
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted after rename",
        new Path(srcDir, PREFIX + count / 2));
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted after rename",
        new Path(srcDir, PREFIX + (count - 1)));
    ContractTestUtils.assertPathExists(fs, "not renamed to dest dir",
        new Path(finalDir, PREFIX + 0));
    ContractTestUtils.assertPathExists(fs, "not renamed to dest dir",
        new Path(finalDir, PREFIX + count/2));
    ContractTestUtils.assertPathExists(fs, "not renamed to dest dir",
        new Path(finalDir, PREFIX + (count-1)));

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
  }

}
