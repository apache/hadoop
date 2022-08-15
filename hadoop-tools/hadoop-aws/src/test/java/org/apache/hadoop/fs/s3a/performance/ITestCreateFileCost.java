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

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;


import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.contract.ContractTestUtils.toChar;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_HEADER;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.Constants.XA_HEADER_PREFIX;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.CREATE_FILE_NO_OVERWRITE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.CREATE_FILE_OVERWRITE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILE_STATUS_DIR_PROBE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILE_STATUS_FILE_PROBE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.GET_FILE_STATUS_ON_DIR_MARKER;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.GET_FILE_STATUS_ON_FILE;
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
  public void testCreateNoOverwrite() throws Throwable {
    describe("Test file creation without overwrite");
    Path testFile = methodPath();
    // when overwrite is false, the path is checked for existence.
    create(testFile, false,
        CREATE_FILE_NO_OVERWRITE);
  }

  @Test
  public void testCreateOverwrite() throws Throwable {
    describe("Test file creation with overwrite");
    Path testFile = methodPath();
    // when overwrite is true: only the directory checks take place.
    create(testFile, true, CREATE_FILE_OVERWRITE);
  }

  @Test
  public void testCreateNoOverwriteFileExists() throws Throwable {
    describe("Test cost of create file failing with existing file");
    Path testFile = file(methodPath());

    // now there is a file there, an attempt with overwrite == false will
    // fail on the first HEAD.
    interceptOperation(FileAlreadyExistsException.class, "",
        FILE_STATUS_FILE_PROBE,
        () -> file(testFile, false));
  }

  @Test
  public void testCreateFileOverDir() throws Throwable {
    describe("Test cost of create file failing with existing dir");
    Path testFile = dir(methodPath());

    // now there is a file there, an attempt with overwrite == false will
    // fail on the first HEAD.
    interceptOperation(FileAlreadyExistsException.class, "",
        GET_FILE_STATUS_ON_DIR_MARKER,
        () -> file(testFile, false));
  }

  /**
   * Use the builder API.
   * on s3a this skips parent checks, always.
   */
  @Test
  public void testCreateBuilderSequence() throws Throwable {
    describe("Test builder file creation cost");
    Path testFile = methodPath();
    dir(testFile.getParent());

    // s3a fs skips the recursive checks to avoid race
    // conditions with other processes/threads deleting
    // files and so briefly the path not being present
    // only make sure the dest path isn't a directory.
    buildFile(testFile, true, false,
        FILE_STATUS_DIR_PROBE);

    // now there is a file there, an attempt with overwrite == false will
    // fail on the first HEAD.
    interceptOperation(FileAlreadyExistsException.class, "",
        GET_FILE_STATUS_ON_FILE,
        () -> buildFile(testFile, false, true,
            GET_FILE_STATUS_ON_FILE));
  }

  @Test
  public void testCreateFilePerformanceFlag() throws Throwable {
    describe("createFile with performance flag skips safety checks");
    S3AFileSystem fs = getFileSystem();

    Path path = methodPath();
    FSDataOutputStreamBuilder builder = fs.createFile(path)
        .overwrite(false)
        .recursive();

    // this has a broken return type; something to do with the return value of
    // the createFile() call. only fixable via risky changes to the FileSystem class
    builder.must(FS_S3A_CREATE_PERFORMANCE, true);

    verifyMetrics(() -> build(builder),
        always(NO_HEAD_OR_LIST),
        with(OBJECT_BULK_DELETE_REQUEST, 0),
        with(OBJECT_DELETE_REQUEST, 0));
  }

  @Test
  public void testCreateFileRecursive() throws Throwable {
    describe("createFile without performance flag performs overwrite safety checks");
    S3AFileSystem fs = getFileSystem();

    final Path path = methodPath();
    FSDataOutputStreamBuilder builder = fs.createFile(path)
        .recursive()
        .overwrite(false);

    // include a custom header to probe for after
    final String custom = "custom";
    builder.must(FS_S3A_CREATE_HEADER + ".h1", custom);

    verifyMetrics(() -> build(builder),
        always(CREATE_FILE_NO_OVERWRITE));

    // the header is there and the probe should be a single HEAD call.
    String header = verifyMetrics(() ->
            toChar(requireNonNull(
              fs.getXAttr(path, XA_HEADER_PREFIX + "h1"),
              "no header")),
        always(HEAD_OPERATION));
    Assertions.assertThat(header)
        .isEqualTo(custom);
  }

  @Test
  public void testCreateFileNonRecursive() throws Throwable {
    describe("nonrecursive createFile does not check parents");
    S3AFileSystem fs = getFileSystem();

    verifyMetrics(() ->
            build(fs.createFile(methodPath()).overwrite(true)),
        always(CREATE_FILE_OVERWRITE));
  }


  @Test
  public void testCreateNonRecursive() throws Throwable {
    describe("nonrecursive createFile does not check parents");
    S3AFileSystem fs = getFileSystem();

    verifyMetrics(() -> {
      fs.createNonRecursive(methodPath(),
              true, 1000, (short)1, 0L, null)
          .close();
      return "";
    },
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
  public void testPerformanceFlagPermitsInvalidStores() throws Throwable {
    describe("createFile with performance flag over a directory");
    S3AFileSystem fs = getFileSystem();

    Path path = methodPath();
    Path child = new Path(path, "child");
    ContractTestUtils.touch(fs, child);
    try {
      FSDataOutputStreamBuilder builder = fs.createFile(path)
          .overwrite(false);
      // this has a broken return type; a java typesystem quirk.
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
