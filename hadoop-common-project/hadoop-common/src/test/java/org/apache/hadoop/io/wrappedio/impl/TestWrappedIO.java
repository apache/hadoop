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

package org.apache.hadoop.io.wrappedio.impl;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;
import org.apache.hadoop.io.wrappedio.WrappedIO;
import org.apache.hadoop.util.Lists;

import static java.nio.ByteBuffer.allocate;
import static org.apache.hadoop.fs.CommonPathCapabilities.BULK_DELETE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.StreamCapabilities.IOSTATISTICS_CONTEXT;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.file;
import static org.apache.hadoop.util.dynamic.BindingUtils.loadClass;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.util.functional.Tuples.pair;

/**
 * Test WrappedIO operations.
 * <p>
 * This is a contract test; the base class is bonded to the local fs;
 * it is possible for other stores to implement themselves.
 * All classes/constants are referenced here because they are part of the reflected
 * API. If anything changes, application code breaks.
 */
public class TestWrappedIO extends AbstractFSContractTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestWrappedIO.class);

  /**
   * Dynamic wrapped IO.
   */
  private DynamicWrappedIO io;

  /**
   * Dynamically Wrapped IO statistics.
   */
  private DynamicWrappedStatistics statistics;

  @BeforeEach
  public void setup() throws Exception {
    super.setup();

    io = new DynamicWrappedIO();
    statistics = new DynamicWrappedStatistics();
    statistics.iostatisticsContext_reset();
  }

  @AfterEach
  @Override
  public void teardown() throws Exception {
    super.teardown();
    logIOStatisticsContext();
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new LocalFSContract(conf);
  }

  /**
   * Verify the {@link #clazz(String)} method raises an assertion
   * if the class isn't found.
   */
  @Test
  public void testClassResolution() throws Throwable {
    intercept(AssertionError.class, () -> clazz("no.such.class"));
  }

  @Test
  public void testAllMethodsFound() throws Throwable {
    io.requireAllMethodsAvailable();
  }

  /**
   * Test the openFile operation.
   * Lots of calls are made to read the same file to save on setup/teardown
   * overhead and to allow for some statistics collection.
   */
  @Test
  public void testOpenFileOperations() throws Throwable {
    Path path = path("testOpenFileOperations");
    final int len = 100;
    final byte[] data = dataset(len, 'a', 26);
    final FileSystem fs = getFileSystem();
    // create the file and any statistics from it.
    final Serializable iostats = statistics.iostatisticsSnapshot_create(
        file(fs, path, true, data));
    final FileStatus st = fs.getFileStatus(path);
    final boolean ioStatisticsContextCapability;

    describe("reading file " + path);
    try (FSDataInputStream in = DynamicWrappedIO.openFile(fs,
        fs.getFileStatus(path),
        DynamicWrappedIO.PARQUET_READ_POLICIES)) {
      Assertions.assertThat(in.read())
          .describedAs("first byte")
          .isEqualTo('a');
      ioStatisticsContextCapability = supportsIOStatisticsContext(in);
      if (ioStatisticsContextCapability) {
        LOG.info("Stream has IOStatisticsContext support: {}", in);
      } else {
        LOG.info("Stream has no IOStatisticsContext support: {}", in);
      }
      Assertions.assertThat(ioStatisticsContextCapability)
          .describedAs("Retrieved stream capability %s from %s",
              IOSTATISTICS_CONTEXT, in)
          .isEqualTo(WrappedIO.streamCapabilities_hasCapability(in, IOSTATISTICS_CONTEXT));
      Assertions.assertThat(ioStatisticsContextCapability)
          .describedAs("Actual stream capability %s from %s",
              IOSTATISTICS_CONTEXT, in)
          .isEqualTo(in.hasCapability(IOSTATISTICS_CONTEXT));
      retrieveAndAggregate(iostats, in);
    }

    // open with a status
    try (FSDataInputStream s = openFile(path, null, st, null, null)) {
      s.seek(1);
      s.read();

      // and do a small amount of statistics collection
      retrieveAndAggregate(iostats, s);
    }

    // open with a length and random IO passed in the map
    try (FSDataInputStream s = openFile(path, null, null,
        (long) len,
        map(pair(FS_OPTION_OPENFILE_READ_POLICY, "random")))) {
      s.seek(len - 10);
      s.read();
      retrieveAndAggregate(iostats, s);
    }

    // now open a file with a length option greater than the file length

    // this string is used in exception logging to report where in the
    // sequence an IOE was raised.
    String validationPoint = "openfile call";

    // open with a length and random IO passed in via the map
    try (FSDataInputStream s = openFile(path, null, null,
        null,
        map(pair(FS_OPTION_OPENFILE_LENGTH, len * 2),
            pair(FS_OPTION_OPENFILE_READ_POLICY, "random")))) {

      // fails if the file length was determined and fixed in open,
      // and the stream doesn't permit seek() beyond the file length.
      validationPoint = "seek()";
      s.seek(len + 10);

      validationPoint = "readFully()";

      // readFully must fail.
      s.readFully(len + 10, new byte[10], 0, 10);
      Assertions.fail("Expected an EOFException but readFully from %s", s);
    } catch (EOFException expected) {
      // expected
      LOG.info("EOF successfully raised, validation point: {}", validationPoint);
      LOG.debug("stack", expected);
    }

    // if we get this far, do a bulk delete
    Assertions.assertThat(io.pathCapabilities_hasPathCapability(fs, path, BULK_DELETE))
        .describedAs("Path capability %s", BULK_DELETE)
        .isTrue();

    // first assert page size was picked up
    Assertions.assertThat(io.bulkDelete_pageSize(fs, path))
        .describedAs("bulkDelete_pageSize for %s", path)
        .isGreaterThanOrEqualTo(1);

    // then do the delete.
    // pass in the parent path for the bulk delete to avoid HADOOP-19196
    Assertions
        .assertThat(io.bulkDelete_delete(fs, path.getParent(), Lists.newArrayList(path)))
        .describedAs("outcome of bulk delete")
        .isEmpty();
  }

  @Test
  public void testOpenFileNotFound() throws Throwable {
    Path path = path("testOpenFileNotFound");

    intercept(FileNotFoundException.class, () ->
        io.fileSystem_openFile(getFileSystem(), path, null, null, null, null));
  }

  /**
   * Test ByteBufferPositionedReadable.
   * This is implemented by HDFS but not much else; this test skips if the stream
   * doesn't support it.
   */
  @Test
  public void testByteBufferPositionedReadable() throws Throwable {
    Path path = path("testByteBufferPositionedReadable");
    final int len = 100;
    final byte[] data = dataset(len, 'a', 26);
    final FileSystem fs = getFileSystem();
    file(fs, path, true, data);

    describe("reading file " + path);
    try (FSDataInputStream in = openFile(path, "random", null, (long) len, null)) {
      // skip rest of test if API is not found.
      if (io.byteBufferPositionedReadable_readFullyAvailable(in)) {

        LOG.info("ByteBufferPositionedReadable is available in {}", in);
        ByteBuffer buffer = allocate(len);
        io.byteBufferPositionedReadable_readFully(in, 0, buffer);
        Assertions.assertThat(buffer.array())
            .describedAs("Full buffer read of %s", in)
            .isEqualTo(data);


        // read from offset (verifies the offset is passed in)
        final int offset = 10;
        final int range = len - offset;
        buffer = allocate(range);
        io.byteBufferPositionedReadable_readFully(in, offset, buffer);
        byte[] byteArray = new byte[range];
        in.readFully(offset, byteArray);
        Assertions.assertThat(buffer.array())
            .describedAs("Offset buffer read of %s", in)
            .isEqualTo(byteArray);

        // now try to read past the EOF
        // first verify the stream rejects this call directly
        intercept(EOFException.class, () ->
            in.readFully(len + 1, allocate(len)));

        // then do the same through the wrapped API
        intercept(EOFException.class, () ->
            io.byteBufferPositionedReadable_readFully(in, len + 1, allocate(len)));
      } else {
        LOG.info("ByteBufferPositionedReadable is not available in {}", in);

        // expect failures here
        intercept(UnsupportedOperationException.class, () ->
            io.byteBufferPositionedReadable_readFully(in, 0, allocate(len)));
      }
    }
  }

  @Test
  public void testFilesystemIOStatistics() throws Throwable {

    final FileSystem fs = getFileSystem();
    final Serializable iostats = statistics.iostatisticsSnapshot_retrieve(fs);
    if (iostats != null) {
      final String status = statistics.iostatisticsSnapshot_toJsonString(iostats);
      final Serializable roundTripped = statistics.iostatisticsSnapshot_fromJsonString(
          status);

      final Path path = methodPath();
      statistics.iostatisticsSnapshot_save(roundTripped, fs, path, true);
      final Serializable loaded = statistics.iostatisticsSnapshot_load(fs, path);

      Assertions.assertThat(loaded)
          .describedAs("loaded statistics from %s", path)
          .isNotNull()
          .satisfies(statistics::isIOStatisticsSnapshot);
      LOG.info("loaded statistics {}",
          statistics.iostatistics_toPrettyString(loaded));
    }

  }

  /**
   * Retrieve any IOStatistics from a class, and aggregate it to the
   * existing IOStatistics.
   * @param iostats statistics to update
   * @param object statistics source
   */
  private void retrieveAndAggregate(final Serializable iostats, final Object object) {
    statistics.iostatisticsSnapshot_aggregate(iostats,
        statistics.iostatisticsSnapshot_retrieve(object));
  }

  /**
   * Log IOStatisticsContext if enabled.
   */
  private void logIOStatisticsContext() {
    // context IOStats
    if (statistics.iostatisticsContext_enabled()) {
      final Serializable iostats = statistics.iostatisticsContext_snapshot();
      LOG.info("Context: {}",
          toPrettyString(iostats));
    } else {
      LOG.info("IOStatisticsContext disabled");
    }
  }

  private String toPrettyString(final Object iostats) {
    return statistics.iostatistics_toPrettyString(iostats);
  }

  /**
   * Does the object update the thread-local IOStatisticsContext?
   * @param o object to cast to StreamCapabilities and probe for the capability.
   * @return true if the methods were found, the interface implemented and the probe successful.
   */
  private boolean supportsIOStatisticsContext(final Object o) {
    return io.streamCapabilities_hasCapability(o, IOSTATISTICS_CONTEXT);
  }

  /**
   * Open a file through dynamic invocation of {@link FileSystem#openFile(Path)}.
   * @param path path
   * @param policy read policy
   * @param status optional file status
   * @param length file length or null
   * @param options nullable map of other options
   * @return stream of the opened file
   */
  private FSDataInputStream openFile(
      final Path path,
      final String policy,
      final FileStatus status,
      final Long length,
      final Map<String, String> options) throws Throwable {

    final FSDataInputStream stream = io.fileSystem_openFile(
        getFileSystem(), path, policy, status, length, options);
    Assertions.assertThat(stream)
        .describedAs("null stream from openFile(%s)", path)
        .isNotNull();
    return stream;
  }

  /**
   * Build a map from the tuples, which all have the value of
   * their toString() method used.
   * @param tuples object list (must be even)
   * @return a map.
   */
  private Map<String, String> map(Map.Entry<String, Object>... tuples) {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<String, Object> tuple : tuples) {
      map.put(tuple.getKey(), tuple.getValue().toString());
    }
    return map;
  }

  /**
   * Load a class by name; includes an assertion that the class was loaded.
   * @param className classname
   * @return the class.
   */
  private static Class<?> clazz(final String className) {
    final Class<?> clazz = loadClass(className);
    Assertions.assertThat(clazz)
        .describedAs("Class %s not found", className)
        .isNotNull();
    return clazz;
  }

  /**
   * Simulate a no binding and verify that everything downgrades as expected.
   */
  @Test
  public void testNoWrappedClass() throws Throwable {
    final DynamicWrappedIO broken = new DynamicWrappedIO(this.getClass().getName());

    Assertions.assertThat(broken)
        .describedAs("broken dynamic io %s", broken)
        .matches(d -> !d.bulkDelete_available())
        .matches(d -> !d.byteBufferPositionedReadable_available())
        .matches(d -> !d.fileSystem_openFile_available());

    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    // bulk deletes fail
    intercept(UnsupportedOperationException.class, () ->
        broken.bulkDelete_pageSize(fs, path));
    intercept(UnsupportedOperationException.class, () ->
        broken.bulkDelete_delete(fs, path, Lists.newArrayList()));

    // openfile
    intercept(UnsupportedOperationException.class, () ->
        broken.fileSystem_openFile(fs, path, "", null, null, null));

    // hasPathCapability downgrades
    Assertions.assertThat(broken.pathCapabilities_hasPathCapability(fs, path, "anything"))
        .describedAs("hasPathCapability(anything) via %s", broken)
        .isFalse();

    // byte buffer positioned readable
    ContractTestUtils.touch(fs, path);
    try (InputStream in = fs.open(path)) {
      Assertions.assertThat(broken.byteBufferPositionedReadable_readFullyAvailable(in))
          .describedAs("byteBufferPositionedReadable_readFullyAvailable on %s", in)
          .isFalse();
      intercept(UnsupportedOperationException.class, () ->
          broken.byteBufferPositionedReadable_readFully(in, 0, allocate(1)));
    }

  }

  /**
   * Simulate a missing binding and verify that static methods fallback as required.
   */
  @Test
  public void testMissingClassFallbacks() throws Throwable {
    Path path = path("testMissingClassFallbacks");
    final FileSystem fs = getFileSystem();
    file(fs, path, true, dataset(100, 'a', 26));
    final DynamicWrappedIO broken = new DynamicWrappedIO(this.getClass().getName());
    try (FSDataInputStream in = DynamicWrappedIO.openFileOnInstance(broken,
        fs, fs.getFileStatus(path), DynamicWrappedIO.PARQUET_READ_POLICIES)) {
      Assertions.assertThat(in.read())
          .describedAs("first byte")
          .isEqualTo('a');
    }
  }

  /**
   * Verify that if an attempt is made to bond to a class where the methods
   * exist but are not static, that this fails during the object construction rather
   * than on invocation.
   */
  @Test
  public void testNonStaticMethods() throws Throwable {
    intercept(IllegalStateException.class, () ->
        new DynamicWrappedIO(NonStaticBulkDeleteMethods.class.getName()));
  }

  /**
   * This class declares the bulk delete methods, but as non-static; the expectation
   * is that class loading will raise an {@link IllegalStateException}.
   */
  private static final class NonStaticBulkDeleteMethods {

    public int bulkDelete_pageSize(FileSystem ignoredFs, Path ignoredPath) {
      return 0;
    }

    public List<Map.Entry<Path, String>> bulkDelete_delete(
        FileSystem ignoredFs,
        Path ignoredBase,
        Collection<Path> ignoredPaths) {
      return null;
    }
  }
}