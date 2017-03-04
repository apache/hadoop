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
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.sun.tools.javac.util.Assert;

/**
 * Test class to hold all WASB authorization tests.
 */
public class TestNativeAzureFileSystemAuthorization
  extends AbstractWasbTestBase {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    Configuration conf = new Configuration();
    conf.set(NativeAzureFileSystem.KEY_AZURE_AUTHORIZATION, "true");
    conf.set(RemoteWasbAuthorizerImpl.KEY_REMOTE_AUTH_SERVICE_URL, "test_url");
    return AzureBlobStorageTestAccount.create(conf);
  }

  /**
   * Positive test to verify Create and delete access check
   * @throws Throwable
   */
  @Test
  public void testCreateAccessCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    String testFile = "test.dat";
    Path testPath = new Path(fs.getWorkingDirectory(), testFile);

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);
    authorizer.addAuthRule(fs.getWorkingDirectory().toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), true);

    fs.create(testPath);
    Assert.check(fs.exists(testPath));
    fs.delete(testPath, false);
  }

  /**
   * Negative test to verify Create access check
   * @throws Throwable
   */

  @Test(expected=WasbAuthorizationException.class)
  public void testCreateAccessCheckNegative() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    String testFile = "test.dat";
    Path testPath = new Path(fs.getWorkingDirectory(), testFile);

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.WRITE.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    fs.create(new Path(testFile));
  }

  /**
   * Positive test to verify Create and delete access check
   * @throws Throwable
   */
  @Test
  public void testListAccessCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    String testFolder = "\\";
    Path testPath = new Path(fs.getWorkingDirectory(), testFolder);

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    fs.listStatus(testPath);
  }

  /**
   * Negative test to verify Create access check
   * @throws Throwable
   */

  @Test(expected=WasbAuthorizationException.class)
  public void testListAccessCheckNegative() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    String testFolder = "\\";
    Path testPath = new Path(fs.getWorkingDirectory(), testFolder);

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    fs.listStatus(testPath);
  }

  /**
   * Positive test to verify rename access check.
   * @throws Throwable
   */
  @Test
  public void testRenameAccessCheckPositive() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    String testFile = "test.dat";
    Path testPath = new Path(fs.getWorkingDirectory(), testFile);
    String renameFile = "test2.dat";
    Path renamePath = new Path(fs.getWorkingDirectory(), renameFile);

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(renamePath.toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(fs.getWorkingDirectory().toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);
    fs.create(testPath);

    Assert.check(fs.exists(testPath));
    fs.rename(testPath, renamePath);
    Assert.check(fs.exists(renamePath));
    fs.delete(renamePath, false);
  }

  /**
   * Negative test to verify rename access check.
   * @throws Throwable
   */
  @Test(expected=WasbAuthorizationException.class)
  public void testRenameAccessCheckNegative() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    String testFile = "test.dat";
    Path testPath = new Path(fs.getWorkingDirectory(), testFile);
    Path renamePath = new Path("test2.dat");

    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    try {
      fs.create(testPath);

      Assert.check(fs.exists(testPath));
      fs.rename(testPath, renamePath);
      Assert.check(fs.exists(renamePath));
      fs.delete(renamePath, false);
    } catch (WasbAuthorizationException ex) {
      throw ex;
    } finally {
      authorizer = new MockWasbAuthorizerImpl();
      authorizer.init(null);
      authorizer.addAuthRule(testPath.toString(),
          WasbAuthorizationOperations.EXECUTE.toString(), false);
      fs.updateWasbAuthorizer(authorizer);
      Assert.check(fs.exists(testPath));
      fs.delete(testPath, false);
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
    String testFile = "test.dat";
    Path testPath = new Path(fs.getWorkingDirectory(), testFile);
    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.READ.toString(), true);
    authorizer.addAuthRule(fs.getWorkingDirectory().toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), true);
    fs.updateWasbAuthorizer(authorizer);
    fs.create(testPath);
    Assert.check(fs.exists(testPath));
    FSDataInputStream inputStream = fs.open(testPath);
    inputStream.close();
    fs.delete(testPath, false);
  }

  /**
   * Negative test to verify read access check.
   * @throws Throwable
   */
  @Test(expected=WasbAuthorizationException.class)
  public void testReadAccessCheckNegative() throws Throwable {

    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();
    String testFile = "test.dat";
    Path testPath = new Path(fs.getWorkingDirectory(), testFile);
    MockWasbAuthorizerImpl authorizer = new MockWasbAuthorizerImpl();
    authorizer.init(null);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.EXECUTE.toString(), true);
    authorizer.addAuthRule(testPath.toString(),
        WasbAuthorizationOperations.READ.toString(), false);
    fs.updateWasbAuthorizer(authorizer);

    fs.create(new Path(testFile));
    Assert.check(fs.exists(testPath));
    FSDataInputStream inputStream = null;
    try {
      inputStream = fs.open(new Path(testFile));
    } catch (WasbAuthorizationException ex) {
      throw ex;
    } finally {
      fs.delete(new Path(testFile), false);
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }
}