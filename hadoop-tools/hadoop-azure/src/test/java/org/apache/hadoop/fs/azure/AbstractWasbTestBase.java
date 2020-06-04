/**
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

package org.apache.hadoop.fs.azure;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.integration.AzureTestConstants;
import org.apache.hadoop.io.IOUtils;

import static org.junit.Assume.assumeNotNull;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.*;

/**
 * Abstract test class that provides basic setup and teardown of testing Azure
 * Storage account.  Each subclass defines a different set of test cases to run
 * and overrides {@link #createTestAccount()} to set up the testing account used
 * to run those tests.  The returned account might integrate with Azure Storage
 * directly or it might be a mock implementation.
 */
public abstract class AbstractWasbTestBase extends AbstractWasbTestWithTimeout
    implements AzureTestConstants {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractWasbTestBase.class);

  protected NativeAzureFileSystem fs;
  protected AzureBlobStorageTestAccount testAccount;

  @Before
  public void setUp() throws Exception {
    AzureBlobStorageTestAccount account = createTestAccount();
    assumeNotNull("test account", account);
    bindToTestAccount(account);
  }

  @After
  public void tearDown() throws Exception {
    describe("closing test account and filesystem");
    testAccount = cleanupTestAccount(testAccount);
    IOUtils.closeStream(fs);
    fs = null;
  }

  /**
   * Create the configuration to use when creating a test account.
   * Subclasses can override this to tune the test account configuration.
   * @return a configuration.
   */
  public Configuration createConfiguration() {
    return AzureBlobStorageTestAccount.createTestConfiguration();
  }

  /**
   * Create the test account.
   * Subclasses must implement this.
   * @return the test account.
   * @throws Exception
   */
  protected abstract AzureBlobStorageTestAccount createTestAccount()
      throws Exception;

  /**
   * Get the test account.
   * @return the current test account.
   */
  protected AzureBlobStorageTestAccount getTestAccount() {
    return testAccount;
  }

  /**
   * Get the filesystem
   * @return the current filesystem.
   */
  protected NativeAzureFileSystem getFileSystem() {
    return fs;
  }

  /**
   * Get the configuration used to create the filesystem
   * @return the configuration of the test FS
   */
  protected Configuration getConfiguration() {
    return getFileSystem().getConf();
  }

  /**
   * Bind to a new test account; closing any existing one.
   * This updates the test account returned in {@link #getTestAccount()}
   * and the filesystem in {@link #getFileSystem()}.
   * @param account new test account
   */
  protected void bindToTestAccount(AzureBlobStorageTestAccount account) {
    // clean any existing test account
    cleanupTestAccount(testAccount);
    IOUtils.closeStream(fs);
    testAccount = account;
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
  }

  /**
   * Return a path to a blob which will be unique for this fork.
   * @param filepath filepath
   * @return a path under the default blob directory
   * @throws IOException
   */
  protected Path blobPath(String filepath) throws IOException {
    return blobPathForTests(getFileSystem(), filepath);
  }

  /**
   * Create a path under the test path provided by
   * the FS contract.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   * @throws IOException IO problems
   */
  protected Path path(String filepath) throws IOException {
    return pathForTests(getFileSystem(), filepath);
  }

  /**
   * Return a path bonded to this method name, unique to this fork during
   * parallel execution.
   * @return a method name unique to (fork, method).
   * @throws IOException IO problems
   */
  protected Path methodPath() throws IOException {
    return path(methodName.getMethodName());
  }

  /**
   * Return a blob path bonded to this method name, unique to this fork during
   * parallel execution.
   * @return a method name unique to (fork, method).
   * @throws IOException IO problems
   */
  protected Path methodBlobPath() throws IOException {
    return blobPath(methodName.getMethodName());
  }

  /**
   * Describe a test in the logs.
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        methodName.getMethodName(),
        String.format(text, args));
  }
}
