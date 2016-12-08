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
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.common.Parallelized;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Test createNonRecursive API.
 */
@RunWith(Parallelized.class)
public class TestAdlInternalCreateNonRecursive {
  private Path inputFileName;
  private FsPermission inputPermission;
  private boolean inputOverride;
  private boolean inputFileAlreadyExist;
  private boolean inputParentAlreadyExist;
  private Class<IOException> expectedExceptionType;
  private FileSystem adlStore;

  public TestAdlInternalCreateNonRecursive(String testScenario, String fileName,
      FsPermission permission, boolean override, boolean fileAlreadyExist,
      boolean parentAlreadyExist, Class<IOException> exceptionType) {

    // Random parent path for each test so that parallel execution does not fail
    // other running test.
    inputFileName = new Path(
        "/test/createNonRecursive/" + UUID.randomUUID().toString(), fileName);
    inputPermission = permission;
    inputFileAlreadyExist = fileAlreadyExist;
    inputOverride = override;
    inputParentAlreadyExist = parentAlreadyExist;
    expectedExceptionType = exceptionType;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection adlCreateNonRecursiveTestData()
      throws UnsupportedEncodingException {
    /*
      Test Data
      File name, Permission, Override flag, File already exist, Parent
      already exist
      shouldCreateSucceed, expectedExceptionIfFileCreateFails

      File already exist and Parent already exist are mutually exclusive.
    */
    return Arrays.asList(new Object[][] {
        {"CNR - When file do not exist.", UUID.randomUUID().toString(),
            FsPermission.getFileDefault(), false, false, true, null},
        {"CNR - When file exist. Override false", UUID.randomUUID().toString(),
            FsPermission.getFileDefault(), false, true, true,
            FileAlreadyExistsException.class},
        {"CNR - When file exist. Override true", UUID.randomUUID().toString(),
            FsPermission.getFileDefault(), true, true, true, null},

        //TODO: This test is skipped till the fixes are not made it to prod.
        /*{ "CNR - When parent do no exist.", UUID.randomUUID().toString(),
            FsPermission.getFileDefault(), false, false, true, false,
            IOException.class }*/});
  }

  @Before
  public void setUp() throws Exception {
    Assume.assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
    adlStore = AdlStorageConfiguration.createStorageConnector();
  }

  @Test
  public void testCreateNonRecursiveFunctionality() throws IOException {
    if (inputFileAlreadyExist) {
      FileSystem.create(adlStore, inputFileName, inputPermission);
    }

    // Mutually exclusive to inputFileAlreadyExist
    if (inputParentAlreadyExist) {
      adlStore.mkdirs(inputFileName.getParent());
    } else {
      adlStore.delete(inputFileName.getParent(), true);
    }

    try {
      adlStore.createNonRecursive(inputFileName, inputPermission, inputOverride,
          CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT,
          adlStore.getDefaultReplication(inputFileName),
          adlStore.getDefaultBlockSize(inputFileName), null);
    } catch (IOException e) {

      if (expectedExceptionType == null) {
        throw e;
      }

      Assert.assertEquals(expectedExceptionType, e.getClass());
      return;
    }

    if (expectedExceptionType != null) {
      Assert.fail("CreateNonRecursive should have failed with exception "
          + expectedExceptionType.getName());
    }
  }
}
