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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.sun.tools.javac.util.Assert;
import org.junit.rules.ExpectedException;

import java.io.Console;

import static org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.KEY_USE_SECURE_MODE;

/**
 * Test class to hold all WASB authorization tests.
 */
public class TestNativeAzureFileSystemAuthorization
  extends AbstractWasbTestBase {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    Configuration conf = new Configuration();
    conf.set(NativeAzureFileSystem.KEY_AZURE_AUTHORIZATION, "true");
    conf.set(RemoteWasbAuthorizerImpl.KEY_REMOTE_AUTH_SERVICE_URL, "http://localhost/");
    return AzureBlobStorageTestAccount.create(conf);
  }


  @Before
  public void beforeMethod() {
    boolean useSecureMode = fs.getConf().getBoolean(KEY_USE_SECURE_MODE, false);
    boolean useAuthorization = fs.getConf().getBoolean(NativeAzureFileSystem.KEY_AZURE_AUTHORIZATION, false);
    Assume.assumeTrue("Test valid when both SecureMode and Authorization are enabled .. skipping",
        useSecureMode && useAuthorization);

    Assume.assumeTrue(
        useSecureMode && useAuthorization
    );
  }


  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  /**
   * Positive test to verify Create and delete access check
   * @throws Throwable
   */
  @Test
  public void testCreateAccessCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/testCreateAccessCheckPositive");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    fs.create(testPath);
    ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
    fs.delete(parentDir, true);
  }

  /**
   * Negative test to verify Create access check
   * @throws Throwable
   */

  @Test // (expected=WasbAuthorizationException.class)
  public void testCreateAccessCheckNegative() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("create operation for Path : /testCreateAccessCheckNegative/test.dat not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/testCreateAccessCheckNegative");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(),WasbAuthorizationOperations.WRITE.toString(), false);
    authorizer.addAuthRule(parentDir.toString(),WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
    }
    finally {
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test to verify Create and delete access check
   * @throws Throwable
   */
  @Test
  public void testListAccessCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/testListAccessCheckPositive");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    fs.create(testPath);
    ContractTestUtils.assertPathExists(fs, "testPath does not exist", testPath);

    try {
      fs.listStatus(testPath);
    }
    finally {
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test to verify Create access check
   * @throws Throwable
   */

  @Test //(expected=WasbAuthorizationException.class)
  public void testListAccessCheckNegative() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("getFileStatus operation for Path : /testListAccessCheckNegative/test.dat not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/testListAccessCheckNegative");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.EXECUTE.toString(), false);
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    fs.create(testPath);
    ContractTestUtils.assertPathExists(fs, "testPath does not exist", testPath);

    try {
      fs.listStatus(testPath);
    }
    finally {
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test to verify rename access check.
   * @throws Throwable
   */
  @Test
  public void testRenameAccessCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/testRenameAccessCheckPositive");
    Path testPath = new Path(parentDir, "test.dat");
    Path renamePath = new Path(parentDir, "test2.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(renamePath.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    fs.create(testPath);
    ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", testPath);

    try {
      fs.rename(testPath, renamePath);
      ContractTestUtils.assertPathExists(fs, "destPath does not exist", renamePath);
    }
    finally {
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test to verify rename access check.
   * @throws Throwable
   */
  @Test //(expected=WasbAuthorizationException.class)
  public void testRenameAccessCheckNegative() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("rename operation for Path : /testRenameAccessCheckNegative/test.dat not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    Path parentDir = new Path("/testRenameAccessCheckNegative");
    Path testPath = new Path(parentDir, "test.dat");
    Path renamePath = new Path(parentDir, "test2.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    // set EXECUTE to true for initial assert right after creation.
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    fs.create(testPath);
    ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", testPath);

    // Set EXECUTE to false for actual rename-failure test
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.EXECUTE.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.rename(testPath, renamePath);
      ContractTestUtils.assertPathExists(fs, "destPath does not exist", renamePath);
    } finally {
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test for read access check.
   * @throws Throwable
   */
  @Test
  public void testReadAccessCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    Path parentDir = new Path("/testReadAccessCheckPositive");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    FSDataOutputStream fso = fs.create(testPath);
    String data = "Hello World";
    fso.writeBytes(data);
    fso.close();
    ContractTestUtils.assertPathExists(fs, "testPath does not exist", testPath);

    FSDataInputStream inputStream = null;
    try {
      inputStream = fs.open(testPath);
      ContractTestUtils.verifyRead(inputStream, data.getBytes(), 0, data.length());
    }
    finally {
      if(inputStream != null) {
        inputStream.close();
      }
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test to verify read access check.
   * @throws Throwable
   */

  @Test //(expected=WasbAuthorizationException.class)
  public void testReadAccessCheckNegative() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("read operation for Path : /testReadAccessCheckNegative/test.dat not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    Path parentDir = new Path("/testReadAccessCheckNegative");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), false);
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    FSDataOutputStream fso = fs.create(testPath);
    String data = "Hello World";
    fso.writeBytes(data);
    fso.close();
    ContractTestUtils.assertPathExists(fs, "testPath does not exist", testPath);

    FSDataInputStream inputStream = null;
    try {
      inputStream = fs.open(testPath);
      ContractTestUtils.verifyRead(inputStream, data.getBytes(), 0, data.length());
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
      fs.delete(parentDir, true);
    }
  }
}