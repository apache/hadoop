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

import org.junit.rules.ExpectedException;

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
   * Setup up permissions to allow a recursive delete for cleanup purposes.
   */
  private void allowRecursiveDelete(NativeAzureFileSystem fs, MockWasbAuthorizerImpl authorizer, String path) {

    int index = path.lastIndexOf('/');
    String parent = (index == 0) ? "/" : path.substring(0, index);

    authorizer.init(null);
    authorizer.addAuthRule(parent, WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule((path.endsWith("*") ? path : path+"*"), WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);
  }

  /**
   * Positive test to verify Create access check
   * The file is created directly under an existing folder.
   * No intermediate folders need to be created.
   * @throws Throwable
   */
  @Test
  public void testCreateAccessWithoutCreateIntermediateFoldersCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
    }
    finally {
      fs.delete(testPath, false);
    }
  }

  /**
   * Positive test to verify Create access check
   * The test tries to create a file whose parent is non-existent to ensure that
   * the intermediate folders between ancestor and direct parent are being created
   * when proper ranger policies are configured.
   * @throws Throwable
   */
  @Test
  public void testCreateAccessWithCreateIntermediateFoldersCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/testCreateAccessCheckPositive/1/2/3");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
    }
    finally {
      allowRecursiveDelete(fs, authorizer, "/testCreateAccessCheckPositive");
      fs.delete(new Path("/testCreateAccessCheckPositive"), true);
    }
  }


  /**
   * Negative test to verify that create fails when trying to overwrite an existing file
   * without proper write permissions on the file being overwritten.
   * @throws Throwable
   */
  @Test // (expected=WasbAuthorizationException.class)
  public void testCreateAccessWithOverwriteCheckNegative() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("create operation for Path : /test.dat not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    boolean initialCreateSucceeded = false;
    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
      initialCreateSucceeded = true;
      fs.create(testPath, true);
    }
    finally {
      ContractTestUtils.assertTrue(initialCreateSucceeded);
      fs.delete(testPath, false);
    }
  }

  /**
   * Positive test to verify that create succeeds when trying to overwrite an existing file
   * when proper write permissions on the file being overwritten are provided.
   * @throws Throwable
   */
  @Test
  public void testCreateAccessWithOverwriteCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    boolean initialCreateSucceeded = false;
    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
      initialCreateSucceeded = true;
      fs.create(testPath, true);
    }
    finally {
      ContractTestUtils.assertTrue(initialCreateSucceeded);
      fs.delete(testPath, false);
    }
  }

  /**
   * Negative test to verify that Create fails when appropriate permissions are not provided.
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
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
    }
    finally {
      /* Provide permissions to cleanup in case the file got created */
      allowRecursiveDelete(fs, authorizer, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test to verify listStatus access check
   * @throws Throwable
   */
  @Test
  public void testListAccessCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/testListAccessCheckPositive");
    Path intermediateFolders = new Path(parentDir, "1/2/3/");
    Path testPath = new Path(intermediateFolders, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      fs.listStatus(testPath);
    }
    finally {
      allowRecursiveDelete(fs, authorizer, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test to verify listStatus access check
   * @throws Throwable
   */

  @Test //(expected=WasbAuthorizationException.class)
  public void testListAccessCheckNegative() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("liststatus operation for Path : /testListAccessCheckNegative/test.dat not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/testListAccessCheckNegative");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      fs.listStatus(testPath);
    }
    finally {
      allowRecursiveDelete(fs, authorizer, parentDir.toString());
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
    Path srcPath = new Path(parentDir, "test1.dat");
    Path dstPath = new Path(parentDir, "test2.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); /* to create parentDir */
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true); /* for rename */
    authorizer.addAuthRule(srcPath.toString(), WasbAuthorizationOperations.READ.toString(), true); /* for exists */
    authorizer.addAuthRule(dstPath.toString(), WasbAuthorizationOperations.READ.toString(), true); /* for exists */
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(srcPath);
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", srcPath);
      fs.rename(srcPath, dstPath);
      ContractTestUtils.assertPathExists(fs, "destPath does not exist", dstPath);
      ContractTestUtils.assertPathDoesNotExist(fs, "sourcePath exists after rename!", srcPath);
    }
    finally {
      allowRecursiveDelete(fs, authorizer, parentDir.toString());
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
    expectedEx.expectMessage("rename operation for Path : /testRenameAccessCheckNegative/test1.dat not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    Path parentDir = new Path("/testRenameAccessCheckNegative");
    Path srcPath = new Path(parentDir, "test1.dat");
    Path dstPath = new Path(parentDir, "test2.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); /* to create parent dir */
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.WRITE.toString(), false);
    authorizer.addAuthRule(srcPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    authorizer.addAuthRule(dstPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(srcPath);
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", srcPath);
      fs.rename(srcPath, dstPath);
      ContractTestUtils.assertPathExists(fs, "destPath does not exist", dstPath);
    } finally {
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist after rename failure!", srcPath);

      allowRecursiveDelete(fs, authorizer, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test to verify rename access check - the dstFolder disallows rename
   * @throws Throwable
   */
  @Test //(expected=WasbAuthorizationException.class)
  public void testRenameAccessCheckNegativeOnDstFolder() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("rename operation for Path : /testRenameAccessCheckNegativeDst/test2.dat not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    Path parentSrcDir = new Path("/testRenameAccessCheckNegativeSrc");
    Path srcPath = new Path(parentSrcDir, "test1.dat");
    Path parentDstDir = new Path("/testRenameAccessCheckNegativeDst");
    Path dstPath = new Path(parentDstDir, "test2.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); /* to create parent dir */
    authorizer.addAuthRule(parentSrcDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(parentDstDir.toString(), WasbAuthorizationOperations.WRITE.toString(), false);
    authorizer.addAuthRule(srcPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    authorizer.addAuthRule(dstPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(srcPath);
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", srcPath);
      fs.rename(srcPath, dstPath);
      ContractTestUtils.assertPathDoesNotExist(fs, "destPath does not exist", dstPath);
    } finally {
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist after rename !", srcPath);
      allowRecursiveDelete(fs, authorizer, parentSrcDir.toString());
      fs.delete(parentSrcDir, true);
    }
  }

  /**
   * Positive test to verify rename access check - the dstFolder allows rename
   * @throws Throwable
   */
  @Test //(expected=WasbAuthorizationException.class)
  public void testRenameAccessCheckPositiveOnDstFolder() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    Path parentSrcDir = new Path("/testRenameAccessCheckPositiveSrc");
    Path srcPath = new Path(parentSrcDir, "test1.dat");
    Path parentDstDir = new Path("/testRenameAccessCheckPositiveDst");
    Path dstPath = new Path(parentDstDir, "test2.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); /* to create parent dirs */
    authorizer.addAuthRule(parentSrcDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(parentDstDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(srcPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    authorizer.addAuthRule(dstPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(srcPath);
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", srcPath);
      fs.mkdirs(parentDstDir);
      fs.rename(srcPath, dstPath);
      ContractTestUtils.assertPathDoesNotExist(fs, "sourcePath does not exist", srcPath);
      ContractTestUtils.assertPathExists(fs, "destPath does not exist", dstPath);
    } finally {
      allowRecursiveDelete(fs, authorizer, parentSrcDir.toString());
      fs.delete(parentSrcDir, true);

      allowRecursiveDelete(fs, authorizer, parentDstDir.toString());
      fs.delete(parentDstDir, true);
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
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    FSDataInputStream inputStream = null;
    FSDataOutputStream fso = null;

    try {
      fso = fs.create(testPath);
      String data = "Hello World";
      fso.writeBytes(data);
      fso.close();

      inputStream = fs.open(testPath);
      ContractTestUtils.verifyRead(inputStream, data.getBytes(), 0, data.length());
    }
    finally {
      if (fso != null) {
        fso.close();
      }
      if(inputStream != null) {
        inputStream.close();
      }
      allowRecursiveDelete(fs, authorizer, parentDir.toString());
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
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    FSDataInputStream inputStream = null;
    FSDataOutputStream fso = null;

    try {
      fso = fs.create(testPath);
      String data = "Hello World";
      fso.writeBytes(data);
      fso.close();

      inputStream = fs.open(testPath);
      ContractTestUtils.verifyRead(inputStream, data.getBytes(), 0, data.length());
    } finally {
      if (fso != null) {
        fso.close();
      }
      if (inputStream != null) {
        inputStream.close();
      }
      allowRecursiveDelete(fs, authorizer, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test to verify file delete access check
   * @throws Throwable
   */
  @Test
  public void testFileDeleteAccessCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);
    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
    }
    finally {
      fs.delete(testPath, false);
      ContractTestUtils.assertPathDoesNotExist(fs, "testPath exists after deletion!", testPath);
    }
  }

  /**
   * Negative test to verify file delete access check
   * @throws Throwable
   */
  @Test //(expected=WasbAuthorizationException.class)
  public void testFileDeleteAccessCheckNegative() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("delete operation for Path : /test.dat not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);
    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);


      /* Remove permissions for delete to force failure */
      authorizer.init(null);
      authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), false);
      fs.updateWasbAuthorizer(authorizer);

      fs.delete(testPath, false);
    }
    finally {
      /* Restore permissions to force a successful delete */
      authorizer.init(null);
      authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
      authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
      fs.updateWasbAuthorizer(authorizer);

      fs.delete(testPath, false);
      ContractTestUtils.assertPathDoesNotExist(fs, "testPath exists after deletion!", testPath);
    }
  }

  /**
   * Positive test to verify file delete access check, with intermediate folders
   * Uses wildcard recursive permissions
   * @throws Throwable
   */
  @Test
  public void testFileDeleteAccessWithIntermediateFoldersCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path parentDir = new Path("/testDeleteIntermediateFolder");
    Path testPath = new Path(parentDir, "1/2/test.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); // for create and delete
    authorizer.addAuthRule("/testDeleteIntermediateFolder*",
        WasbAuthorizationOperations.WRITE.toString(), true); // for recursive delete
    authorizer.addAuthRule("/*", WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
      fs.delete(parentDir, true);
      ContractTestUtils.assertPathDoesNotExist(fs, "testPath exists after deletion!", parentDir);
    }
    finally {
      allowRecursiveDelete(fs, authorizer, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test for getFileStatus
   * @throws Throwable
   */
  @Test
  public void testGetFileStatusPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path testPath = new Path("/");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    ContractTestUtils.assertIsDirectory(fs, testPath);
  }

  /**
   * Negative test for getFileStatus
   * @throws Throwable
   */
  @Test //(expected=WasbAuthorizationException.class)
  public void testGetFileStatusNegative() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("getFileStatus operation for Path : / not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path testPath = new Path("/");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.READ.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    ContractTestUtils.assertIsDirectory(fs, testPath);
  }

  /**
   * Positive test for mkdirs access check
   * @throws Throwable
   */
  @Test
  public void testMkdirsCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path testPath = new Path("/testMkdirsAccessCheckPositive/1/2/3");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.mkdirs(testPath);
      ContractTestUtils.assertIsDirectory(fs, testPath);
    }
    finally {
      allowRecursiveDelete(fs, authorizer, "/testMkdirsAccessCheckPositive");
      fs.delete(new Path("/testMkdirsAccessCheckPositive"), true);
    }
  }

  /**
   * Negative test for mkdirs access check
   * @throws Throwable
   */
  @Test //(expected=WasbAuthorizationException.class)
  public void testMkdirsCheckNegative() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("mkdirs operation for Path : /testMkdirsAccessCheckNegative/1/2/3 not allowed");

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path testPath = new Path("/testMkdirsAccessCheckNegative/1/2/3");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), false);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.mkdirs(testPath);
      ContractTestUtils.assertPathDoesNotExist(fs, "testPath was not created", testPath);
    }
    finally {
      allowRecursiveDelete(fs, authorizer, "/testMkdirsAccessCheckNegative");
      fs.delete(new Path("/testMkdirsAccessCheckNegative"), true);
    }
  }
}