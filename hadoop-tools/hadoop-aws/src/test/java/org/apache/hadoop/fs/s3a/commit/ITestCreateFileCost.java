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

import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.CREATE_FILE_NO_OVERWRITE;
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

    verifyMetrics(() -> {
      FSDataOutputStream out = builder.build();
      out.close();
      return out;
    },
        always(NO_HEAD_OR_LIST),
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_REQUEST, 0));
  }

  @Test
  public void testWritingNonPerformanceFile() throws Throwable {
    describe("createFile without performance flag performs safety checks");
    S3AFileSystem fs = getFileSystem();

    Path path = methodPath();
    FSDataOutputStreamBuilder builder = fs.createFile(path)
        .overwrite(false);

    verifyMetrics(() -> {
      FSDataOutputStream out = builder.build();
      out.close();
      return out;
    },
        always(CREATE_FILE_NO_OVERWRITE));
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

      verifyMetrics(() -> {
        FSDataOutputStream out = builder.build();
        out.close();
        return out;
      },
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
