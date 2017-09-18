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

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class that runs wasb authorization tests with owner check enabled.
 */
public class ITestNativeAzureFileSystemAuthorizationWithOwner
  extends TestNativeAzureFileSystemAuthorization {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    authorizer.init(fs.getConf(), true);
  }

  /**
   * Test case when owner matches current user.
   */
  @Test
  public void testOwnerPermissionPositive() throws Throwable {

    Path parentDir = new Path("/testOwnerPermissionPositive");
    Path testPath = new Path(parentDir, "test.data");

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(testPath.toString(), WasbAuthorizationOperations.READ.toString(), true);
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true);
    // additional rule used for assertPathExists
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.READ.toString(), true);
    fs.updateWasbAuthorizer(authorizer);

    try {
      // creates parentDir with owner as current user
      fs.mkdirs(parentDir);
      ContractTestUtils.assertPathExists(fs, "parentDir does not exist", parentDir);

      fs.create(testPath);
      fs.getFileStatus(testPath);
      ContractTestUtils.assertPathExists(fs, "testPath does not exist", testPath);

    } finally {
      allowRecursiveDelete(fs, parentDir.toString());
      fs.delete(parentDir, true);
    }
  }

  /**
   * Negative test case for owner does not match current user.
   */
  @Test
  public void testOwnerPermissionNegative() throws Throwable {
    expectedEx.expect(WasbAuthorizationException.class);

    Path parentDir = new Path("/testOwnerPermissionNegative");
    Path childDir = new Path(parentDir, "childDir");

    setExpectedFailureMessage("mkdirs", childDir);

    authorizer.addAuthRule("/", WasbAuthorizationOperations.WRITE.toString(), true);
    authorizer.addAuthRule(parentDir.toString(), WasbAuthorizationOperations.WRITE.toString(), true);

    fs.updateWasbAuthorizer(authorizer);

    try{
      fs.mkdirs(parentDir);
      UserGroupInformation ugiSuperUser = UserGroupInformation.createUserForTesting(
          "testuser", new String[] {});

      ugiSuperUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
          fs.mkdirs(childDir);
          return null;
        }
      });

    } finally {
       allowRecursiveDelete(fs, parentDir.toString());
       fs.delete(parentDir, true);
    }
  }

  /**
   * Test to verify that retrieving owner information does not
   * throw when file/folder does not exist.
   */
  @Test
  public void testRetrievingOwnerDoesNotFailWhenFileDoesNotExist() throws Throwable {

    Path testdirectory = new Path("/testDirectory123454565");

    String owner = fs.getOwnerForPath(testdirectory);
    assertEquals("", owner);
  }
}

