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
package org.apache.hadoop.fs.contract;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathHandleException;
import org.apache.hadoop.fs.Options.HandleOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import static org.apache.hadoop.fs.contract.ContractTestUtils.appendFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyRead;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;

import org.apache.hadoop.fs.RawPathHandle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test {@link PathHandle} operations and semantics.
 * @see ContractOptions#SUPPORTS_FILE_REFERENCE
 * @see ContractOptions#SUPPORTS_CONTENT_CHECK
 * @see org.apache.hadoop.fs.FileSystem#getPathHandle(FileStatus, HandleOpt...)
 * @see org.apache.hadoop.fs.FileSystem#open(PathHandle)
 * @see org.apache.hadoop.fs.FileSystem#open(PathHandle, int)
 */
public abstract class AbstractContractPathHandleTest
    extends AbstractFSContractTestBase {

  private HandleOpt[] opts;
  private boolean serialized;

  private static final byte[] B1 = dataset(TEST_FILE_LEN, 43, 255);
  private static final byte[] B2 = dataset(TEST_FILE_LEN, 44, 255);

  /**
   * Create an instance of the test from {@link #params()}.
   * @param pTestname Name of the set of options under test
   * @param pOpts Set of {@link HandleOpt} params under test.
   * @param pSerialized Serialize the handle before using it.
   */
  public void initAbstractContractPathHandleTest(
      String pTestname, HandleOpt[] pOpts,
      boolean pSerialized) {
    this.opts = pOpts;
    this.serialized = pSerialized;
  }

  /**
   * Run test against all combinations of default options. Also run each
   * after converting the PathHandle to bytes and back.
   * @return
   */
  public static Collection<Object[]> params() {
    return Arrays.asList(
        Arrays.asList("Exact", HandleOpt.exact()),
        Arrays.asList("Content", HandleOpt.content()),
        Arrays.asList("Path", HandleOpt.path()),
        Arrays.asList("Reference", HandleOpt.reference())
    ).stream()
    .flatMap((x) -> Arrays.asList(true, false).stream()
        .map((b) -> {
          ArrayList<Object> y = new ArrayList<>(x);
          y.add(b);
          return y;
        }))
    .map(ArrayList::toArray)
    .collect(Collectors.toList());
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setInt(IO_FILE_BUFFER_SIZE_KEY, 4096);
    return conf;
  }

  @MethodSource("params")
  @ParameterizedTest
  public void testIdent(String pTestname, HandleOpt[] pOpts,
      boolean pSerialized) throws IOException {
    initAbstractContractPathHandleTest(pTestname, pOpts, pSerialized);
    describe("verify simple open, no changes");
    FileStatus stat = testFile(B1);
    PathHandle fd = getHandleOrSkip(stat);
    verifyFileContents(getFileSystem(), stat.getPath(), B1);

    try (FSDataInputStream in = getFileSystem().open(fd)) {
      verifyRead(in, B1, 0, TEST_FILE_LEN);
    }
  }

  @MethodSource("params")
  @ParameterizedTest
  public void testChanged(String pTestname, HandleOpt[] pOpts,
      boolean pSerialized) throws IOException {
    initAbstractContractPathHandleTest(pTestname, pOpts, pSerialized);
    describe("verify open(PathHandle, changed(*))");
    assumeSupportsContentCheck();
    HandleOpt.Data data = HandleOpt.getOpt(HandleOpt.Data.class, opts)
        .orElseThrow(IllegalArgumentException::new);
    FileStatus stat = testFile(B1);
    try {
      // Temporary workaround while RawLocalFS supports only second precision
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    // modify the file by appending data
    appendFile(getFileSystem(), stat.getPath(), B2);
    byte[] b12 = Arrays.copyOf(B1, B1.length + B2.length);
    System.arraycopy(B2, 0, b12, B1.length, B2.length);
    // verify fd entity contains contents of file1 + appended bytes
    verifyFileContents(getFileSystem(), stat.getPath(), b12);
    // get the handle *after* the file has been modified
    PathHandle fd = getHandleOrSkip(stat);

    try (FSDataInputStream in = getFileSystem().open(fd)) {
      assertTrue(data.allowChange(), "Failed to detect content change");
      verifyRead(in, b12, 0, b12.length);
    } catch (InvalidPathHandleException e) {
      assertFalse(data.allowChange(), "Failed to allow content change");
    }
  }

  @MethodSource("params")
  @ParameterizedTest
  public void testMoved(String pTestname, HandleOpt[] pOpts,
      boolean pSerialized) throws IOException {
    initAbstractContractPathHandleTest(pTestname, pOpts, pSerialized);
    describe("verify open(PathHandle, moved(*))");
    assumeSupportsFileReference();
    HandleOpt.Location loc = HandleOpt.getOpt(HandleOpt.Location.class, opts)
        .orElseThrow(IllegalArgumentException::new);
    FileStatus stat = testFile(B1);
    // rename the file after obtaining FileStatus
    ContractTestUtils.rename(getFileSystem(), stat.getPath(),
        path(stat.getPath() + "2"));
    // obtain handle to entity from #getFileStatus call
    PathHandle fd = getHandleOrSkip(stat);

    try (FSDataInputStream in = getFileSystem().open(fd)) {
      assertTrue(loc.allowChange(), "Failed to detect location change");
      verifyRead(in, B1, 0, B1.length);
    } catch (InvalidPathHandleException e) {
      assertFalse(loc.allowChange(), "Failed to allow location change");
    }
  }

  @MethodSource("params")
  @ParameterizedTest
  public void testChangedAndMoved(String pTestname, HandleOpt[] pOpts,
      boolean pSerialized) throws IOException {
    initAbstractContractPathHandleTest(pTestname, pOpts, pSerialized);
    describe("verify open(PathHandle, changed(*), moved(*))");
    assumeSupportsFileReference();
    assumeSupportsContentCheck();
    HandleOpt.Data data = HandleOpt.getOpt(HandleOpt.Data.class, opts)
        .orElseThrow(IllegalArgumentException::new);
    HandleOpt.Location loc = HandleOpt.getOpt(HandleOpt.Location.class, opts)
        .orElseThrow(IllegalArgumentException::new);
    FileStatus stat = testFile(B1);
    Path dst = path(stat.getPath() + "2");
    ContractTestUtils.rename(getFileSystem(), stat.getPath(), dst);
    appendFile(getFileSystem(), dst, B2);
    PathHandle fd = getHandleOrSkip(stat);

    byte[] b12 = Arrays.copyOf(B1, B1.length + B2.length);
    System.arraycopy(B2, 0, b12, B1.length, B2.length);
    try (FSDataInputStream in = getFileSystem().open(fd)) {
      assertTrue(loc.allowChange(), "Failed to detect location change");
      assertTrue(data.allowChange(), "Failed to detect content change");
      verifyRead(in, b12, 0, b12.length);
    } catch (InvalidPathHandleException e) {
      if (data.allowChange()) {
        assertFalse(loc.allowChange(), "Failed to allow location change");
      }
      if (loc.allowChange()) {
        assertFalse(data.allowChange(), "Failed to allow content change");
      }
    }
  }

  private FileStatus testFile(byte[] content) throws IOException {
    Path path = path(methodName.getMethodName());
    createFile(getFileSystem(), path, false, content);
    FileStatus stat = getFileSystem().getFileStatus(path);
    assertNotNull(stat);
    assertEquals(path, stat.getPath());
    return stat;
  }

  /**
   * Skip a test case if the FS doesn't support file references.
   * The feature is assumed to be unsupported unless stated otherwise.
   */
  protected void assumeSupportsFileReference() throws IOException {
    if (getContract().isSupported(SUPPORTS_FILE_REFERENCE, false)) {
      return;
    }
    skip("Skipping as unsupported feature: " + SUPPORTS_FILE_REFERENCE);
  }

  /**
   * Skip a test case if the FS doesn't support content validation.
   * The feature is assumed to be unsupported unless stated otherwise.
   */
  protected void assumeSupportsContentCheck() throws IOException {
    if (getContract().isSupported(SUPPORTS_CONTENT_CHECK, false)) {
      return;
    }
    skip("Skipping as unsupported feature: " + SUPPORTS_CONTENT_CHECK);
  }

  /**
   * Utility method to obtain a handle or skip the test if the set of opts
   * are not supported.
   * @param stat Target file status
   * @return Handle to the indicated entity or skip the test
   */
  protected PathHandle getHandleOrSkip(FileStatus stat) {
    try {
      PathHandle fd = getFileSystem().getPathHandle(stat, opts);
      if (serialized) {
        ByteBuffer sb = fd.bytes();
        return new RawPathHandle(sb);
      }
      return fd;
    } catch (UnsupportedOperationException e) {
      skip("FileSystem does not support " + Arrays.toString(opts));
    }
    // unreachable
    return null;
  }


  @MethodSource("params")
  @ParameterizedTest
  public void testOpenFileApplyRead() throws Throwable {
    describe("use the apply sequence to read a whole file");
    CompletableFuture<Long> readAllBytes = getFileSystem()
        .openFile(
            getHandleOrSkip(
                testFile(B1)))
        .build()
        .thenApply(ContractTestUtils::readStream);
    assertEquals(TEST_FILE_LEN,
        (long) readAllBytes.get(), "Wrong number of bytes read value");
  }

  @MethodSource("params")
  @ParameterizedTest
  public void testOpenFileDelete(String pTestname, HandleOpt[] pOpts,
      boolean pSerialized) throws Throwable {
    initAbstractContractPathHandleTest(pTestname, pOpts, pSerialized);
    describe("use the apply sequence to read a whole file");
    FileStatus testFile = testFile(B1);
    PathHandle handle = getHandleOrSkip(testFile);
    // delete that file
    FileSystem fs = getFileSystem();
    fs.delete(testFile.getPath(), false);
    // now construct the builder.
    // even if the open happens in the build operation,
    // the failure must not surface until later.
    CompletableFuture<FSDataInputStream> builder =
        fs.openFile(handle)
            .opt("fs.test.something", true)
            .build();
    IOException ioe = interceptFuture(IOException.class, "", builder);
    if (!(ioe instanceof FileNotFoundException)
        && !(ioe instanceof InvalidPathHandleException)) {
      // support both FileNotFoundException
      // and InvalidPathHandleException as different implementations
      // support either -and with non-atomic open sequences, possibly
      // both
      throw ioe;
    }
  }

  @MethodSource("params")
  @ParameterizedTest
  public void testOpenFileLazyFail(String pTestname, HandleOpt[] pOpts,
      boolean pSerialized) throws Throwable {
    describe("openFile fails on a misssng file in the get() and not before");
    FileStatus stat = testFile(B1);
    CompletableFuture<Long> readAllBytes = getFileSystem()
        .openFile(
            getHandleOrSkip(
                stat))
        .build()
        .thenApply(ContractTestUtils::readStream);
    assertEquals(TEST_FILE_LEN,
        (long) readAllBytes.get(), "Wrong number of bytes read value");
  }

}
