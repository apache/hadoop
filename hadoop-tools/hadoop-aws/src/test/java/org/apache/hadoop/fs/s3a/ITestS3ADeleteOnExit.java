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
package org.apache.hadoop.fs.s3a;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;

/**
 * Test deleteOnExit for S3A.
 * The following cases for deleteOnExit are tested:
 *  1. A nonexist file, which is added to deleteOnExit set.
 *  2. An existing file
 *  3. A file is added to deleteOnExist set first, then created.
 *  4. A directory with some files under it.
 */
public class ITestS3ADeleteOnExit extends AbstractS3ATestBase {

  private static final String PARENT_DIR_PATH_STR = "testDeleteOnExitDir";
  private static final String NON_EXIST_FILE_PATH_STR =
          PARENT_DIR_PATH_STR + "/nonExistFile";
  private static final String INORDER_FILE_PATH_STR =
          PARENT_DIR_PATH_STR + "/inOrderFile";
  private static final String OUT_OF_ORDER_FILE_PATH_STR =
          PARENT_DIR_PATH_STR + "/outOfOrderFile";
  private static final String SUBDIR_PATH_STR =
          PARENT_DIR_PATH_STR + "/subDir";
  private static final String FILE_UNDER_SUBDIR_PATH_STR =
          SUBDIR_PATH_STR + "/subDirFile";

  @Test
  public void testDeleteOnExit() throws Exception {
    FileSystem fs = getFileSystem();

    // Get a new filesystem object which is same as fs.
    FileSystem s3aFs = new S3AFileSystem();
    s3aFs.initialize(fs.getUri(), fs.getConf());
    Path nonExistFilePath = path(NON_EXIST_FILE_PATH_STR);
    Path inOrderFilePath = path(INORDER_FILE_PATH_STR);
    Path outOfOrderFilePath = path(OUT_OF_ORDER_FILE_PATH_STR);
    Path subDirPath = path(SUBDIR_PATH_STR);
    Path fileUnderSubDirPath = path(FILE_UNDER_SUBDIR_PATH_STR);
    // 1. set up the test directory.
    Path dir = path("testDeleteOnExitDir");
    s3aFs.mkdirs(dir);

    // 2. Add a nonexisting file to DeleteOnExit set.
    s3aFs.deleteOnExit(nonExistFilePath);
    ContractTestUtils.assertPathDoesNotExist(s3aFs,
            "File " + NON_EXIST_FILE_PATH_STR + " should not exist", nonExistFilePath);

    // 3. create a file and then add it to DeleteOnExit set.
    FSDataOutputStream stream = s3aFs.create(inOrderFilePath, true);
    byte[] data = ContractTestUtils.dataset(16, 'a', 26);
    try {
      stream.write(data);
    } finally {
      IOUtils.closeStream(stream);
    }

    ContractTestUtils.assertPathExists(s3aFs,
            "File " + INORDER_FILE_PATH_STR + " should exist", inOrderFilePath);

    s3aFs.deleteOnExit(inOrderFilePath);

    // 4. add a path to DeleteOnExit set first, then create it.
    s3aFs.deleteOnExit(outOfOrderFilePath);
    stream = s3aFs.create(outOfOrderFilePath, true);
    try {
      stream.write(data);
    } finally {
      IOUtils.closeStream(stream);
    }

    ContractTestUtils.assertPathExists(s3aFs,
            "File " + OUT_OF_ORDER_FILE_PATH_STR + " should exist", outOfOrderFilePath);

    // 5. create a subdirectory, a file under it,  and add subdirectory DeleteOnExit set.
    s3aFs.mkdirs(subDirPath);
    s3aFs.deleteOnExit(subDirPath);

    stream = s3aFs.create(fileUnderSubDirPath, true);
    try {
      stream.write(data);
    } finally {
      IOUtils.closeStream(stream);
    }

    ContractTestUtils.assertPathExists(s3aFs,
            "Directory " + SUBDIR_PATH_STR + " should exist", subDirPath);
    ContractTestUtils.assertPathExists(s3aFs,
            "File " + FILE_UNDER_SUBDIR_PATH_STR + " should exist", fileUnderSubDirPath);

    s3aFs.close();

    // After s3aFs is closed, make sure that all files/directories in deleteOnExit
    // set are deleted.
    ContractTestUtils.assertPathDoesNotExist(fs,
            "File " + NON_EXIST_FILE_PATH_STR + " should not exist", nonExistFilePath);
    ContractTestUtils.assertPathDoesNotExist(fs,
            "File " + INORDER_FILE_PATH_STR + " should not exist", inOrderFilePath);
    ContractTestUtils.assertPathDoesNotExist(fs,
            "File " + OUT_OF_ORDER_FILE_PATH_STR + " should not exist", outOfOrderFilePath);
    ContractTestUtils.assertPathDoesNotExist(fs,
            "Directory " + SUBDIR_PATH_STR + " should not exist", subDirPath);
  }
}
