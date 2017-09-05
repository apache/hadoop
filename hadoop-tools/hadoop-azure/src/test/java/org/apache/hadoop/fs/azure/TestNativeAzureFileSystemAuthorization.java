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

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.ExpectedException;
import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.KEY_USE_SECURE_MODE;
import static org.junit.Assert.assertEquals;

/**
 * Test class to hold all WASB authorization tests.
 */
public class TestNativeAzureFileSystemAuthorization
  extends AbstractWasbTestBase {

  @VisibleForTesting
  protected MockWasbAuthorizerImpl authorizer;

  @Override
  public Configuration getConfiguration() {
    Configuration conf = super.getConfiguration();
    conf.set(NativeAzureFileSystem.KEY_AZURE_AUTHORIZATION, "true");
    conf.set(RemoteWasbAuthorizerImpl.KEY_REMOTE_AUTH_SERVICE_URLS, "http://localhost/");
    conf.set(NativeAzureFileSystem.AZURE_CHOWN_USERLIST_PROPERTY_NAME, "user1 , user2");
    return conf;
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    Configuration conf = getConfiguration();
    return AzureBlobStorageTestAccount.create(conf);
  }


  @Before
  public void beforeMethod() {
    boolean useSecureMode = fs.getConf().getBoolean(KEY_USE_SECURE_MODE, false);
    boolean useAuthorization = fs.getConf().getBoolean(NativeAzureFileSystem.KEY_AZURE_AUTHORIZATION, false);
    Assume.assumeTrue("Test valid when both SecureMode and Authorization are enabled .. skipping",
        useSecureMode && useAuthorization);

    authorizer = new MockWasbAuthorizerImpl(fs);
    authorizer.init(fs.getConf());
    fs.updateWasbAuthorizer(authorizer);
  }


  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  /**
   * Setup up permissions to allow a recursive delete for cleanup purposes.
   */
  protected void allowRecursiveDelete(NativeAzureFileSystem fs, String path) {

    int index = path.lastIndexOf('/');
    String parent = (index == 0) ? "/" : path.substring(0, index);

    authorizer.deleteAllAuthRules();
    authorizer.addAuthRule(parent, WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule((path.endsWith("*") ? path : path+"*"), WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);
  }

  /**
   * Setup the expected exception class, and exception message that the test is supposed to fail with
   */
  protected void setExpectedFailureMessage(String operation, Path path) {
    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage(String.format("%s operation for Path : %s not allowed",
        operation, path.makeQualified(fs.getUri(), fs.getWorkingDirectory())));
  }

  /**
   * Positive test to verify Create access check
   * The file is created directly under an existing folder.
   * No intermediate folders need to be created.
   * @throws Throwable
   */
  @Test
  public void testCreateAccessWithoutCreateIntermediateFoldersCheckPositive() throws Throwable {

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
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

    Path parentDir = new Path("/testCreateAccessCheckPositive/1/2/3");
    Path testPath = new Path(parentDir, "test.dat");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
    }
    finally {
      allowRecursiveDelete(fs, "/testCreateAccessCheckPositive");
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

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    setExpectedFailureMessage("create", testPath);

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
      fs.create(testPath, true);
    }
    finally {
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

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
      fs.create(testPath, true);
    }
    finally {
      fs.delete(testPath, false);
    }
  }

  /**
   * Negative test to verify that Create fails when appropriate permissions are not provided.
   * @throws Throwable
   */

  @Test // (expected=WasbAuthorizationException.class)
  public void testCreateAccessCheckNegative() throws Throwable {

    Path parentDir = new Path("/testCreateAccessCheckNegative");
    Path testPath = new Path(parentDir, "test.dat");

    setExpectedFailureMessage("create", testPath);

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
    }
    finally {
      /* Provide permissions to cleanup in case the file got created */
      allowRecursiveDelete(fs, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test to verify listStatus access check
   * @throws Throwable
   */
  @Test
  public void testListAccessCheckPositive() throws Throwable {

    Path parentDir = new Path("/testListAccessCheckPositive");
    Path intermediateFolders = new Path(parentDir, "1/2/3/");
    Path testPath = new Path(intermediateFolders, "test.dat");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      fs.listStatus(testPath);
    }
    finally {
      allowRecursiveDelete(fs, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test to verify listStatus access check
   * @throws Throwable
   */

  @Test //(expected=WasbAuthorizationException.class)
  public void testListAccessCheckNegative() throws Throwable {

    Path parentDir = new Path("/testListAccessCheckNegative");
    Path testPath = new Path(parentDir, "test.dat");

    setExpectedFailureMessage("liststatus", testPath);

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      fs.listStatus(testPath);
    }
    finally {
      allowRecursiveDelete(fs, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test to verify rename access check.
   * @throws Throwable
   */
  @Test
  public void testRenameAccessCheckPositive() throws Throwable {

    Path parentDir = new Path("/testRenameAccessCheckPositive");
    Path srcPath = new Path(parentDir, "test1.dat");
    Path dstPath = new Path(parentDir, "test2.dat");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); /* to create parentDir */
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true); /* for rename */
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(srcPath);
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", srcPath);
      fs.rename(srcPath, dstPath);
      ContractTestUtils.assertPathExists(fs, "destPath does not exist", dstPath);
      ContractTestUtils.assertPathDoesNotExist(fs, "sourcePath exists after rename!", srcPath);
    }
    finally {
      allowRecursiveDelete(fs, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test to verify rename access check.
   * @throws Throwable
   */
  @Test //(expected=WasbAuthorizationException.class)
  public void testRenameAccessCheckNegative() throws Throwable {

    Path parentDir = new Path("/testRenameAccessCheckNegative");
    Path srcPath = new Path(parentDir, "test1.dat");
    Path dstPath = new Path(parentDir, "test2.dat");

    setExpectedFailureMessage("rename", srcPath);

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); /* to create parent dir */
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.WRITE.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(srcPath);
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", srcPath);
      fs.rename(srcPath, dstPath);
      ContractTestUtils.assertPathExists(fs, "destPath does not exist", dstPath);
    } finally {
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist after rename failure!", srcPath);

      allowRecursiveDelete(fs, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test to verify rename access check - the dstFolder disallows rename
   * @throws Throwable
   */
  @Test //(expected=WasbAuthorizationException.class)
  public void testRenameAccessCheckNegativeOnDstFolder() throws Throwable {

    Path parentSrcDir = new Path("/testRenameAccessCheckNegativeSrc");
    Path srcPath = new Path(parentSrcDir, "test1.dat");
    Path parentDstDir = new Path("/testRenameAccessCheckNegativeDst");
    Path dstPath = new Path(parentDstDir, "test2.dat");

    setExpectedFailureMessage("rename", dstPath);

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); /* to create parent dir */
    authorizer.addAuthRule(parentSrcDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(parentDstDir.toString(), WasbAuthorizationOperations.WRITE.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(srcPath);
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", srcPath);
      fs.rename(srcPath, dstPath);
      ContractTestUtils.assertPathDoesNotExist(fs, "destPath does not exist", dstPath);
    } finally {
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist after rename !", srcPath);
      allowRecursiveDelete(fs, parentSrcDir.toString());
      fs.delete(parentSrcDir, true);
    }
  }

  /**
   * Positive test to verify rename access check - the dstFolder allows rename
   * @throws Throwable
   */
  @Test
  public void testRenameAccessCheckPositiveOnDstFolder() throws Throwable {

    Path parentSrcDir = new Path("/testRenameAccessCheckPositiveSrc");
    Path srcPath = new Path(parentSrcDir, "test1.dat");
    Path parentDstDir = new Path("/testRenameAccessCheckPositiveDst");
    Path dstPath = new Path(parentDstDir, "test2.dat");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); /* to create parent dirs */
    authorizer.addAuthRule(parentSrcDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(parentDstDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(srcPath);
      ContractTestUtils.assertPathExists(fs, "sourcePath does not exist", srcPath);
      fs.mkdirs(parentDstDir);
      fs.rename(srcPath, dstPath);
      ContractTestUtils.assertPathDoesNotExist(fs, "sourcePath does not exist", srcPath);
      ContractTestUtils.assertPathExists(fs, "destPath does not exist", dstPath);
    } finally {
      allowRecursiveDelete(fs, parentSrcDir.toString());
      fs.delete(parentSrcDir, true);

      allowRecursiveDelete(fs, parentDstDir.toString());
      fs.delete(parentDstDir, true);
    }
  }

  /**
   * Positive test for read access check.
   * @throws Throwable
   */
  @Test
  public void testReadAccessCheckPositive() throws Throwable {

    Path parentDir = new Path("/testReadAccessCheckPositive");
    Path testPath = new Path(parentDir, "test.dat");

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
      allowRecursiveDelete(fs, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test to verify read access check.
   * @throws Throwable
   */

  @Test //(expected=WasbAuthorizationException.class)
  public void testReadAccessCheckNegative() throws Throwable {

    Path parentDir = new Path("/testReadAccessCheckNegative");
    Path testPath = new Path(parentDir, "test.dat");

    setExpectedFailureMessage("read", testPath);

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
      allowRecursiveDelete(fs, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test to verify file delete access check
   * @throws Throwable
   */
  @Test
  public void testFileDeleteAccessCheckPositive() throws Throwable {

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
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

    Path parentDir = new Path("/");
    Path testPath = new Path(parentDir, "test.dat");

    setExpectedFailureMessage("delete", testPath);

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);
    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);


      /* Remove permissions for delete to force failure */
      authorizer.deleteAllAuthRules();
      authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), false);
      fs.updateWasbAuthorizer(authorizer);

      fs.delete(testPath, false);
    }
    finally {
      /* Restore permissions to force a successful delete */
      authorizer.deleteAllAuthRules();
      authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
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

    Path parentDir = new Path("/testDeleteIntermediateFolder");
    Path testPath = new Path(parentDir, "1/2/test.dat");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true); // for create and delete
    authorizer.addAuthRule("/testDeleteIntermediateFolder*",
        WasbAuthorizationOperations.WRITE.toString(), true); // for recursive delete
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
      fs.delete(parentDir, true);
      ContractTestUtils.assertPathDoesNotExist(fs, "testPath exists after deletion!", parentDir);
    }
    finally {
      allowRecursiveDelete(fs, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Positive test for getFileStatus. No permissions are required for getting filestatus.
   * @throws Throwable
   */
  @Test
  public void testGetFileStatusPositive() throws Throwable {

    Path testPath = new Path("/");
    ContractTestUtils.assertIsDirectory(fs, testPath);
  }

  /**
   * Positive test for mkdirs access check
   * @throws Throwable
   */
  @Test
  public void testMkdirsCheckPositive() throws Throwable {

    Path testPath = new Path("/testMkdirsAccessCheckPositive/1/2/3");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.mkdirs(testPath);
      ContractTestUtils.assertIsDirectory(fs, testPath);
    }
    finally {
      allowRecursiveDelete(fs, "/testMkdirsAccessCheckPositive");
      fs.delete(new Path("/testMkdirsAccessCheckPositive"), true);
    }
  }

  /**
   * Positive test for mkdirs -p with existing hierarchy
   * @throws Throwable
   */
  @Test
  public void testMkdirsWithExistingHierarchyCheckPositive1() throws Throwable {

    Path testPath = new Path("/testMkdirsWithExistingHierarchyCheckPositive1");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.mkdirs(testPath);
      ContractTestUtils.assertIsDirectory(fs, testPath);

      /* Don't need permissions to create a directory that already exists */
      authorizer.deleteAllAuthRules();

      fs.mkdirs(testPath);
      ContractTestUtils.assertIsDirectory(fs, testPath);
    }
    finally {
      allowRecursiveDelete(fs, testPath.toString());
      fs.delete(testPath, true);
    }
  }

  @Test
  public void testMkdirsWithExistingHierarchyCheckPositive2() throws Throwable {

    Path testPath = new Path("/testMkdirsWithExistingHierarchyCheckPositive2");
    Path childPath1 = new Path(testPath, "1");
    Path childPath2 = new Path(childPath1, "2");
    Path childPath3 = new Path(childPath2, "3");

    authorizer.addAuthRule("/",
        WasbAuthorizationOperations.WRITE.toString(), true);

    authorizer.addAuthRule(childPath1.toString(),
        WasbAuthorizationOperations.WRITE.toString(), true);

    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.mkdirs(childPath1);
      ContractTestUtils.assertIsDirectory(fs, childPath1);

      // Path already exists => no-op.
      fs.mkdirs(testPath);
      ContractTestUtils.assertIsDirectory(fs, testPath);

      // Path already exists => no-op.
      fs.mkdirs(childPath1);
      ContractTestUtils.assertIsDirectory(fs, childPath1);

      // Check permissions against existing ancestor childPath1
      fs.mkdirs(childPath3);
      ContractTestUtils.assertIsDirectory(fs, childPath3);
    } finally {
      allowRecursiveDelete(fs, testPath.toString());
      fs.delete(testPath, true);
    }
  }
  /**
   * Negative test for mkdirs access check
   * @throws Throwable
   */
  @Test //(expected=WasbAuthorizationException.class)
  public void testMkdirsCheckNegative() throws Throwable {

    Path testPath = new Path("/testMkdirsAccessCheckNegative/1/2/3");

    setExpectedFailureMessage("mkdirs", testPath);

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.mkdirs(testPath);
      ContractTestUtils.assertPathDoesNotExist(fs, "testPath was not created", testPath);
    }
    finally {
      allowRecursiveDelete(fs, "/testMkdirsAccessCheckNegative");
      fs.delete(new Path("/testMkdirsAccessCheckNegative"), true);
    }
  }

  /**
   * Positive test triple slash format (wasb:///) access check
   * @throws Throwable
   */
  @Test
  public void testListStatusWithTripleSlashCheckPositive() throws Throwable {

    Path testPath = new Path("/");

    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    Path testPathWithTripleSlash = new Path("wasb:///" + testPath);
    fs.listStatus(testPathWithTripleSlash);
  }

  /**
   * Negative test for setOwner when Authorization is enabled
   */
  @Test
  public void testSetOwnerThrowsForUnauthorisedUsers() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);

    Path testPath = new Path("/testSetOwnerNegative");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    String owner = null;
    UserGroupInformation unauthorisedUser = UserGroupInformation.createUserForTesting(
          "unauthoriseduser", new String[] {"group1"});
    try {
      fs.mkdirs(testPath);
      ContractTestUtils.assertPathExists(fs, "test path does not exist", testPath);
      owner = fs.getFileStatus(testPath).getOwner();

      unauthorisedUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
          fs.setOwner(testPath, "newowner", null);
          return null;
        }
      });
    } finally {
      // check that the owner is not modified
      assertEquals(owner, fs.getFileStatus(testPath).getOwner());
      fs.delete(testPath, false);
    }
  }

  /**
   * Test for setOwner when Authorization is enabled and
   * the user is specified in chown allowed user list
   * */
  @Test
  public void testSetOwnerSucceedsForAuthorisedUsers() throws Throwable {

    Path testPath = new Path("/testSetOwnerPositive");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    String newOwner = "newowner";
    String newGroup = "newgroup";

    UserGroupInformation authorisedUser = UserGroupInformation.createUserForTesting(
          "user2", new String[]{"group1"});
    try {

      fs.mkdirs(testPath);
      ContractTestUtils.assertPathExists(fs, "test path does not exist", testPath);

      String owner = fs.getFileStatus(testPath).getOwner();
      Assume.assumeTrue("changing owner requires original and new owner to be different",
        !StringUtils.equalsIgnoreCase(owner, newOwner));

      authorisedUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
          fs.setOwner(testPath, newOwner, newGroup);
          assertEquals(newOwner, fs.getFileStatus(testPath).getOwner());
          assertEquals(newGroup, fs.getFileStatus(testPath).getGroup());
          return null;
        }
      });

    } finally {
      fs.delete(testPath, false);
    }
  }

  /**
   * Test for setOwner when Authorization is enabled and
   * the userlist is specified as '*'
   * */
  @Test
  public void testSetOwnerSucceedsForAnyUserWhenWildCardIsSpecified() throws Throwable {

    Configuration conf = fs.getConf();
    conf.set(NativeAzureFileSystem.AZURE_CHOWN_USERLIST_PROPERTY_NAME, "*");
    fs.setConf(conf);
    Path testPath = new Path("/testSetOwnerPositiveWildcard");

    authorizer.init(conf);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    String newOwner = "newowner";
    String newGroup = "newgroup";

    UserGroupInformation user = UserGroupInformation.createUserForTesting(
          "anyuser", new String[]{"group1"});
    try {

      fs.mkdirs(testPath);
      ContractTestUtils.assertPathExists(fs, "test path does not exist", testPath);

      String owner = fs.getFileStatus(testPath).getOwner();
      Assume.assumeTrue("changing owner requires original and new owner to be different",
        !StringUtils.equalsIgnoreCase(owner, newOwner));

      user.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
          fs.setOwner(testPath, newOwner, newGroup);
          assertEquals(newOwner, fs.getFileStatus(testPath).getOwner());
          assertEquals(newGroup, fs.getFileStatus(testPath).getGroup());
          return null;
        }
      });

    } finally {
      fs.delete(testPath, false);
    }
  }

  /** Test for setOwner  throws for illegal setup of chown
   * allowed testSetOwnerSucceedsForAuthorisedUsers
   */
  @Test
  public void testSetOwnerFailsForIllegalSetup() throws Throwable {

    expectedEx.expect(IllegalArgumentException.class);

    Configuration conf = fs.getConf();
    conf.set(NativeAzureFileSystem.AZURE_CHOWN_USERLIST_PROPERTY_NAME, "user1, *");
    fs.setConf(conf);
    Path testPath = new Path("/testSetOwnerFailsForIllegalSetup");

    authorizer.init(conf);
    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    String owner = null;
    UserGroupInformation user = UserGroupInformation.createUserForTesting(
          "anyuser", new String[]{"group1"});
    try {

      fs.mkdirs(testPath);
      ContractTestUtils.assertPathExists(fs, "test path does not exist", testPath);

      owner = fs.getFileStatus(testPath).getOwner();

      user.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
          fs.setOwner(testPath, "newowner", null);
          return null;
        }
      });
    } finally {
      // check that the owner is not modified
      assertEquals(owner, fs.getFileStatus(testPath).getOwner());
      fs.delete(testPath, false);
    }
  }
}
