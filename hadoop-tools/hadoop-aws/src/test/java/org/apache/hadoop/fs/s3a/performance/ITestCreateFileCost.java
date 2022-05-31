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

package org.apache.hadoop.fs.s3a.performance;

import javax.annotation.Nonnull;
import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.CREATE_FILE_NO_OVERWRITE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.CREATE_FILE_OVERWRITE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILE_STATUS_ALL_PROBES;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILE_STATUS_DIR_PROBE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.GET_FILE_STATUS_ON_EMPTY_DIR;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.HEAD_OPERATION;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.NO_HEAD_OR_LIST;

/**
 * Assert cost of createFile operations, especially
 * with the FS_S3A_CREATE_PERFORMANCE option.
 */
@SuppressWarnings("resource")
public class ITestCreateFileCost extends AbstractS3ACostTest {

  /**
   * Create with markers kept, always.
   */
  public ITestCreateFileCost() {
    super(false);
  }

  @Test
  public void testWritingPerformanceFile() throws Throwable {
    describe("createFile with performance flag skips safety checks");
    S3AFileSystem fs = getFileSystem();

    Path path = methodPath();
    FSDataOutputStreamBuilder builder = fs.createFile(path)
        .overwrite(false);
    builder.recursive();
    // this has a broken return type; not sure why
    builder.must(FS_S3A_CREATE_PERFORMANCE, true);

    verifyMetrics(() ->build(builder),
        always(NO_HEAD_OR_LIST),
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_REQUEST, 0));
  }

  @Test
  public void tesCreateNormalFileRecursive() throws Throwable {
    describe("createFile without performance flag performs safety checks");
    S3AFileSystem fs = getFileSystem();

    Path path = methodPath();
    FSDataOutputStreamBuilder builder = fs.createFile(path)
        .recursive()
        .overwrite(false);

    verifyMetrics(() -> build(builder),
        always(CREATE_FILE_NO_OVERWRITE));
  }

  @Test
  public void tesCreateNormalFileNonRecursive() throws Throwable {
    describe("nonrecursive createFile without performance flag also checks for parent");
    S3AFileSystem fs = getFileSystem();

    final Path base = methodPath();
    // make the parent dir to ensure that there's a marker, so only a HEAD
    // request is needed to probe it.
    fs.mkdirs(base);
    Path path = new Path(base, "file");
    FSDataOutputStreamBuilder builder = fs.createFile(path)
        .overwrite(false);

    verifyMetrics(() -> build(builder),
        always(CREATE_FILE_NO_OVERWRITE.plus(FILE_STATUS_DIR_PROBE)));

    FSDataOutputStreamBuilder builder2 = fs.createFile(path)
        .overwrite(true);
    verifyMetrics(() -> build(builder2),
        always(CREATE_FILE_OVERWRITE));
  }

  private FSDataOutputStream build(final FSDataOutputStreamBuilder builder)
      throws IOException {
    FSDataOutputStream out = builder.build();
    out.close();
    return out;
  }

  /**
   * Shows how the performance option allows the FS to become ill-formed.
   */
  @Test
  public void testWritingPerformanceFileOverDir() throws Throwable {
    describe("createFile with performance flag over a directory");
    S3AFileSystem fs = getFileSystem();

    Path path = methodPath();
    Path child = new Path(path, "child");
    ContractTestUtils.touch(fs, child);
    try {
      FSDataOutputStreamBuilder builder = fs.createFile(path)
          .overwrite(false);
      // this has a broken return type; not sure why
      builder.must(FS_S3A_CREATE_PERFORMANCE, true);

      verifyMetrics(() -> build(builder),
          always(NO_HEAD_OR_LIST),
          with(OBJECT_BULK_DELETE_REQUEST, 0),
          with(OBJECT_DELETE_REQUEST, 0));
      // the file is there
      assertIsFile(path);
      // the child is there
      assertIsFile(child);

      // delete the path
      fs.delete(path, true);
      // the child is still there
      assertIsFile(child);
      // and the directory exists again
      assertIsDirectory(path);
    } finally {
      // always delete the child, so if the test suite fails, the
      // store is at least well-formed.
      fs.delete(child, true);
    }
  }

}
