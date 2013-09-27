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

package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftOperationFailedException;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystemStore;
import org.apache.hadoop.fs.swift.util.DurationStats;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.assertPathExists;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.cleanupInTeardown;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.getServiceURI;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.noteAction;

/**
 * This is the base class for most of the Swift tests
 */
public class SwiftFileSystemBaseTest extends Assert implements
                                                    SwiftTestConstants {

  protected static final Log LOG =
          LogFactory.getLog(SwiftFileSystemBaseTest.class);
  protected SwiftNativeFileSystem fs;
  protected static SwiftNativeFileSystem lastFs;
  protected byte[] data = SwiftTestUtils.dataset(getBlockSize() * 2, 0, 255);
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    noteAction("setup");
    final URI uri = getFilesystemURI();
    conf = createConfiguration();

    fs = createSwiftFS();
    try {
      fs.initialize(uri, conf);
    } catch (IOException e) {
      //FS init failed, set it to null so that teardown doesn't
      //attempt to use it
      fs = null;
      throw e;
    }
    //remember the last FS
    lastFs = fs;
    noteAction("setup complete");
  }

  /**
   * Configuration generator. May be overridden to inject
   * some custom options
   * @return a configuration with which to create FS instances
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  @After
  public void tearDown() throws Exception {
    cleanupInTeardown(fs, "/test");
  }

  @AfterClass
  public static void classTearDown() throws Exception {
    if (lastFs  != null) {
      List<DurationStats> statistics = lastFs.getOperationStatistics();
      for (DurationStats stat : statistics) {
        LOG.info(stat.toString());
      }
    }
  }

  /**
   * Get the configuration used to set up the FS
   * @return the configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Describe the test, combining some logging with details
   * for people reading the code
   *
   * @param description test description
   */
  protected void describe(String description) {
    noteAction(description);
  }

  protected URI getFilesystemURI() throws URISyntaxException, IOException {
    return getServiceURI(createConfiguration());
  }

  protected SwiftNativeFileSystem createSwiftFS() throws IOException {
    SwiftNativeFileSystem swiftNativeFileSystem =
      new SwiftNativeFileSystem();
    return swiftNativeFileSystem;
  }

  protected int getBlockSize() {
    return 1024;
  }

  /**
   * Is rename supported?
   * @return true
   */
  protected boolean renameSupported() {
    return true;
  }

  /**
   * assume in a test that rename is supported;
   * skip it if not
   */
  protected void assumeRenameSupported() {
    Assume.assumeTrue(renameSupported());
  }

  /**
   * Take an unqualified path, and qualify it w.r.t the
   * current filesystem
   * @param pathString source path
   * @return a qualified path instance
   */
  protected Path path(String pathString) {
    return new Path(pathString).makeQualified(fs);
  }

  /**
   * Get the filesystem
   * @return the current FS
   */
  public SwiftNativeFileSystem getFs() {
    return fs;
  }

  /**
   * Create a file using the standard {@link #data} bytes.
   *
   * @param path path to write
   * @throws IOException on any problem
   */
  protected void createFile(Path path) throws IOException {
    createFile(path, data);
  }

  /**
   * Create a file with the given data.
   *
   * @param path       path to write
   * @param sourceData source dataset
   * @throws IOException on any problem
   */
  protected void createFile(Path path, byte[] sourceData) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.write(sourceData, 0, sourceData.length);
    out.close();
  }

  /**
   * Create and then close a file
   * @param path path to create
   * @throws IOException on a failure
   */
  protected void createEmptyFile(Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.close();
  }

  /**
   * Get the inner store -useful for lower level operations
   *
   * @return the store
   */
  protected SwiftNativeFileSystemStore getStore() {
    return fs.getStore();
  }

  /**
   * Rename a path
   * @param src source
   * @param dst dest
   * @param renameMustSucceed flag to say "this rename must exist"
   * @param srcExists add assert that the source exists afterwards
   * @param dstExists add assert the dest exists afterwards
   * @throws IOException IO trouble
   */
  protected void rename(Path src, Path dst, boolean renameMustSucceed,
                        boolean srcExists, boolean dstExists) throws IOException {
    if (renameMustSucceed) {
      renameToSuccess(src, dst, srcExists, dstExists);
    } else {
      renameToFailure(src, dst);
    }
  }

  /**
   * Get a string describing the outcome of a rename, by listing the dest
   * path and its parent along with some covering text
   * @param src source patj
   * @param dst dest path
   * @return a string for logs and exceptions
   * @throws IOException IO problems
   */
  private String getRenameOutcome(Path src, Path dst) throws IOException {
    String lsDst = ls(dst);
    Path parent = dst.getParent();
    String lsParent = parent != null ? ls(parent) : "";
    return "  result of " + src + " => " + dst
            + " - " + lsDst
            + " \n" + lsParent;
  }

  /**
   * Rename, expecting an exception to be thrown
   *
   * @param src source
   * @param dst dest
   * @throws IOException a failure other than an
   *                     expected SwiftRenameException or FileNotFoundException
   */
  protected void renameToFailure(Path src, Path dst) throws IOException {
    try {
      getStore().rename(src, dst);
      fail("Expected failure renaming " + src + " to " + dst
              + "- but got success");
    } catch (SwiftOperationFailedException e) {
      LOG.debug("Rename failed (expected):" + e);
    } catch (FileNotFoundException e) {
      LOG.debug("Rename failed (expected):" + e);
    }
  }

  /**
   * Rename to success
   *
   * @param src source
   * @param dst dest
   * @param srcExists add assert that the source exists afterwards
   * @param dstExists add assert the dest exists afterwards
   * @throws SwiftOperationFailedException operation failure
   * @throws IOException IO problems
   */
  protected void renameToSuccess(Path src, Path dst,
                                 boolean srcExists, boolean dstExists)
      throws SwiftOperationFailedException, IOException {
    getStore().rename(src, dst);
    String outcome = getRenameOutcome(src, dst);
    assertEquals("Source " + src + "exists: " + outcome,
            srcExists, fs.exists(src));
    assertEquals("Destination " + dstExists + " exists" + outcome,
            dstExists, fs.exists(dst));
  }

  /**
   * List a path in the test FS
   * @param path path to list
   * @return the contents of the path/dir
   * @throws IOException IO problems
   */
  protected String ls(Path path) throws IOException {
    return SwiftTestUtils.ls(fs, path);
  }

  /**
   * assert that a path exists
   * @param message message to use in an assertion
   * @param path path to probe
   * @throws IOException IO problems
   */
  public void assertExists(String message, Path path) throws IOException {
    assertPathExists(fs, message, path);
  }

  /**
   * assert that a path does not
   * @param message message to use in an assertion
   * @param path path to probe
   * @throws IOException IO problems
   */
  public void assertPathDoesNotExist(String message, Path path) throws
          IOException {
    SwiftTestUtils.assertPathDoesNotExist(fs, message, path);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   *
   * @param filename name of the file
   * @throws IOException IO problems during file operations
   */
  protected void assertIsFile(Path filename) throws IOException {
    SwiftTestUtils.assertIsFile(fs, filename);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   *
   * @throws IOException IO problems during file operations
   */
  protected void mkdirs(Path path) throws IOException {
    assertTrue("Failed to mkdir" + path, fs.mkdirs(path));
  }

  /**
   * Assert that a delete succeeded
   * @param path path to delete
   * @param recursive recursive flag
   * @throws IOException IO problems
   */
  protected void assertDeleted(Path path, boolean recursive) throws IOException {
    SwiftTestUtils.assertDeleted(fs, path, recursive);
  }

  /**
   * Assert that a value is not equal to the expected value
   * @param message message if the two values are equal
   * @param expected expected value
   * @param actual actual value
   */
  protected void assertNotEqual(String message, int expected, int actual) {
    assertTrue(message,
               actual != expected);
  }

  /**
   * Get the number of partitions written from the Swift Native FS APIs
   * @param out output stream
   * @return the number of partitioned files written by the stream
   */
  protected int getPartitionsWritten(FSDataOutputStream out) {
    return SwiftNativeFileSystem.getPartitionsWritten(out);
  }

  /**
   * Assert that the no. of partitions written matches expectations
   * @param action operation (for use in the assertions)
   * @param out output stream
   * @param expected expected no. of partitions
   */
  protected void assertPartitionsWritten(String action, FSDataOutputStream out,
                                         long expected) {
    OutputStream nativeStream = out.getWrappedStream();
    int written = getPartitionsWritten(out);
    if(written !=expected) {
    Assert.fail(action + ": " +
                TestSwiftFileSystemPartitionedUploads.WRONG_PARTITION_COUNT 
                + " + expected: " + expected + " actual: " + written
                + " -- " + nativeStream);
    }
  }

  /**
   * Assert that the result value == -1; which implies
   * that a read was successful
   * @param text text to include in a message (usually the operation)
   * @param result read result to validate
   */
  protected void assertMinusOne(String text, int result) {
    assertEquals(text + " wrong read result " + result, -1, result);
  }
}
