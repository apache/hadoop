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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.contract.ContractTestUtils.cleanup;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;

/**
 * This is the base class for all the contract tests.
 */
public abstract class AbstractFSContractTestBase extends Assert
  implements ContractOptions {

  private static final Logger LOG =
    LoggerFactory.getLogger(AbstractFSContractTestBase.class);

  /**
   * Length of files to work with: {@value}.
   */
  public static final int TEST_FILE_LEN = 1024;

  /**
   * standard test timeout: {@value}.
   */
  public static final int DEFAULT_TEST_TIMEOUT = 180 * 1000;

  /**
   * The FS contract used for these tests.
   */
  private AbstractFSContract contract;

  /**
   * The test filesystem extracted from it.
   */
  private FileSystem fileSystem;

  /**
   * The path for tests.
   */
  private Path testPath;

  @Rule
  public TestName methodName = new TestName();


  @BeforeClass
  public static void nameTestThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * This must be implemented by all instantiated test cases.
   * -provide the FS contract
   * @return the FS contract
   */
  protected abstract AbstractFSContract createContract(Configuration conf);

  /**
   * Get the contract.
   * @return the contract, which will be non-null once the setup operation has
   * succeeded
   */
  protected AbstractFSContract getContract() {
    return contract;
  }

  /**
   * Get the filesystem created in startup.
   * @return the filesystem to use for tests
   */
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * Get the log of the base class.
   * @return a logger
   */
  public static Logger getLog() {
    return LOG;
  }

  /**
   * Skip a test if a feature is unsupported in this FS.
   * @param feature feature to look for
   * @throws IOException IO problem
   */
  protected void skipIfUnsupported(String feature) throws IOException {
    if (!isSupported(feature)) {
      skip("Skipping as unsupported feature: " + feature);
    }
  }

  /**
   * Is a feature supported?
   * @param feature feature
   * @return true iff the feature is supported
   * @throws IOException IO problems
   */
  protected boolean isSupported(String feature) throws IOException {
    return contract.isSupported(feature, false);
  }

  /**
   * Include at the start of tests to skip them if the FS is not enabled.
   */
  protected void assumeEnabled() {
    if (!contract.isEnabled())
      throw new AssumptionViolatedException("test cases disabled for " + contract);
  }

  /**
   * Create a configuration. May be overridden by tests/instantiations
   * @return a configuration
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(getTestTimeoutMillis());

  /**
   * Option for tests to override the default timeout value.
   * @return the current test timeout
   */
  protected int getTestTimeoutMillis() {
    return DEFAULT_TEST_TIMEOUT;
  }


  /**
   * Setup: create the contract then init it.
   * @throws Exception on any failure
   */
  @Before
  public void setup() throws Exception {
    LOG.debug("== Setup ==");
    contract = createContract(createConfiguration());
    contract.init();
    //skip tests if they aren't enabled
    assumeEnabled();
    //extract the test FS
    fileSystem = contract.getTestFileSystem();
    assertNotNull("null filesystem", fileSystem);
    URI fsURI = fileSystem.getUri();
    LOG.info("Test filesystem = {} implemented by {}",
        fsURI, fileSystem);
    //sanity check to make sure that the test FS picked up really matches
    //the scheme chosen. This is to avoid defaulting back to the localFS
    //which would be drastic for root FS tests
    assertEquals("wrong filesystem of " + fsURI,
                 contract.getScheme(), fsURI.getScheme());
    //create the test path
    testPath = getContract().getTestPath();
    mkdirs(testPath);
    LOG.debug("== Setup complete ==");
  }

  /**
   * Teardown.
   * @throws Exception on any failure
   */
  @After
  public void teardown() throws Exception {
    LOG.debug("== Teardown ==");
    deleteTestDirInTeardown();
    LOG.debug("== Teardown complete ==");
  }

  /**
   * Delete the test dir in the per-test teardown.
   * @throws IOException
   */
  protected void deleteTestDirInTeardown() throws IOException {
    cleanup("TEARDOWN", getFileSystem(), testPath);
  }

  /**
   * Create a path under the test path provided by
   * the FS contract.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   * @throws IOException IO problems
   */
  protected Path path(String filepath) throws IOException {
    return getFileSystem().makeQualified(
      new Path(getContract().getTestPath(), filepath));
  }

  /**
   * Take a simple path like "/something" and turn it into
   * a qualified path against the test FS.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   * @throws IOException IO problems
   */
  protected Path absolutepath(String filepath) throws IOException {
    return getFileSystem().makeQualified(new Path(filepath));
  }

  /**
   * List a path in the test FS.
   * @param path path to list
   * @return the contents of the path/dir
   * @throws IOException IO problems
   */
  protected String ls(Path path) throws IOException {
    return ContractTestUtils.ls(fileSystem, path);
  }

  /**
   * Describe a test. This is a replacement for javadocs
   * where the tests role is printed in the log output
   * @param text description
   */
  protected void describe(String text) {
    LOG.info(text);
  }

  /**
   * Handle the outcome of an operation not being the strictest
   * exception desired, but one that, while still within the boundary
   * of the contract, is a bit looser.
   *
   * If the FS contract says that they support the strictest exceptions,
   * that is what they must return, and the exception here is rethrown
   * @param action Action
   * @param expectedException what was expected
   * @param e exception that was received
   */
  protected void handleRelaxedException(String action,
                                        String expectedException,
                                        Exception e) throws Exception {
    if (getContract().isSupported(SUPPORTS_STRICT_EXCEPTIONS, false)) {
      throw e;
    }
    LOG.warn("The expected exception {}  was not the exception class" +
             " raised on {}: {}", action , e.getClass(), expectedException, e);
  }

  /**
   * Handle expected exceptions through logging and/or other actions.
   * @param e exception raised.
   */
  protected void handleExpectedException(Exception e) {
    getLog().debug("expected :{}" ,e, e);
  }

  /**
   * assert that a path exists.
   * @param message message to use in an assertion
   * @param path path to probe
   * @throws IOException IO problems
   */
  public void assertPathExists(String message, Path path) throws IOException {
    ContractTestUtils.assertPathExists(fileSystem, message, path);
  }

  /**
   * Assert that a path does not exist.
   * @param message message to use in an assertion
   * @param path path to probe
   * @throws IOException IO problems
   */
  public void assertPathDoesNotExist(String message, Path path) throws
                                                                IOException {
    ContractTestUtils.assertPathDoesNotExist(fileSystem, message, path);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   *
   * @param filename name of the file
   * @throws IOException IO problems during file operations
   */
  protected void assertIsFile(Path filename) throws IOException {
    ContractTestUtils.assertIsFile(fileSystem, filename);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   *
   * @param path name of the file
   * @throws IOException IO problems during file operations
   */
  protected void assertIsDirectory(Path path) throws IOException {
    ContractTestUtils.assertIsDirectory(fileSystem, path);
  }


  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   *
   * @throws IOException IO problems during file operations
   */
  protected void mkdirs(Path path) throws IOException {
    assertTrue("Failed to mkdir " + path, fileSystem.mkdirs(path));
  }

  /**
   * Assert that a delete succeeded.
   * @param path path to delete
   * @param recursive recursive flag
   * @throws IOException IO problems
   */
  protected void assertDeleted(Path path, boolean recursive) throws
                                                             IOException {
    ContractTestUtils.assertDeleted(fileSystem, path, recursive);
  }

  /**
   * Assert that the result value == -1; which implies
   * that a read was successful.
   * @param text text to include in a message (usually the operation)
   * @param result read result to validate
   */
  protected void assertMinusOne(String text, int result) {
    assertEquals(text + " wrong read result " + result, -1, result);
  }

  protected boolean rename(Path src, Path dst) throws IOException {
    return getFileSystem().rename(src, dst);
  }

  protected String generateAndLogErrorListing(Path src, Path dst) throws
                                                                  IOException {
    FileSystem fs = getFileSystem();
    getLog().error(
      "src dir " + ContractTestUtils.ls(fs, src.getParent()));
    String destDirLS = ContractTestUtils.ls(fs, dst.getParent());
    if (fs.isDirectory(dst)) {
      //include the dir into the listing
      destDirLS = destDirLS + "\n" + ContractTestUtils.ls(fs, dst);
    }
    return destDirLS;
  }
}
