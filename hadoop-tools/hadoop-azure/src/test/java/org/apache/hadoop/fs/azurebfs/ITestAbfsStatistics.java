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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Tests AzureBlobFileSystem Statistics.
 */
public class ITestAbfsStatistics extends AbstractAbfsIntegrationTest {

  private static final int NUMBER_OF_OPS = 10;

  public ITestAbfsStatistics() throws Exception {
  }

  /**
   * Testing statistics by creating files and directories.
   */
  @Test
  public void testCreateStatistics() throws IOException {
    describe("Testing counter values got by creating directories and files in"
        + " Abfs");

    AzureBlobFileSystem fs = getFileSystem();
    Path createFilePath = path(getMethodName());
    Path createDirectoryPath = path(getMethodName() + "Dir");

    Map<String, Long> metricMap = fs.getInstrumentation().toMap();

    /*
     Test for initial values of create statistics ; getFileStatus is called
     1 time after Abfs initialisation.
     */
    assertEquals("Mismatch in op_create", 0,
        (long) metricMap.get("op_create"));
    assertEquals("Mismatch in op_create_non_recursive", 0,
        (long) metricMap.get("op_create_non_recursive"));
    assertEquals("Mismatch in files_created", 0,
        (long) metricMap.get("files_created"));
    assertEquals("Mismatch in directories_created", 0,
        (long) metricMap.get("directories_created"));
    assertEquals("Mismatch in op_mkdirs", 0,
        (long) metricMap.get("op_mkdirs"));
    assertEquals("Mismatch in op_get_file_status", 1,
        (long) metricMap.get("op_get_file_status"));

    fs.mkdirs(createDirectoryPath);
    fs.createNonRecursive(createFilePath, FsPermission
        .getDefault(), false, 1024, (short) 1, 1024, null);

    metricMap = fs.getInstrumentation().toMap();
      /*
       Test of statistic values after creating a directory and a file ;
       getFileStatus is called 1 time after creating file and 1 time at
       time of initialising.
       */
    assertEquals("Mismatch in op_create", 1,
        (long) metricMap.get("op_create"));
    assertEquals("Mismatch in op_create_non_recursive", 1,
        (long) metricMap.get("op_create_non_recursive"));
    assertEquals("Mismatch in files_created", 1,
        (long) metricMap.get("files_created"));
    assertEquals("Mismatch in directories_created", 1,
        (long) metricMap.get("directories_created"));
    assertEquals("Mismatch in op_mkdirs", 1,
        (long) metricMap.get("op_mkdirs"));
    assertEquals("Mismatch in op_get_file_status", 2,
        (long) metricMap.get("op_get_file_status"));

    //re-initialising Abfs to reset statistic values.
    fs.initialize(fs.getUri(), fs.getConf());

      /*Creating 10 directories and files; Directories and files can't
       be created with same name, hence <Name> + i to give unique names.
       */
    for (int i = 0; i < NUMBER_OF_OPS; i++) {
      fs.mkdirs(path(getMethodName() + "Dir" + i));
      fs.createNonRecursive(path(getMethodName() + i),
          FsPermission.getDefault(), false, 1024, (short) 1,
          1024, null);
    }

    metricMap = fs.getInstrumentation().toMap();
      /*
       Test of statistics values after creating 10 directories and files;
       getFileStatus is called 1 time at initialise() plus number of
       times file is created.
       */
    assertEquals("Mismatch in op_create", NUMBER_OF_OPS,
        (long) metricMap.get("op_create"));
    assertEquals("Mismatch in op_create_non_recursive", NUMBER_OF_OPS,
        (long) metricMap.get("op_create_non_recursive"));
    assertEquals("Mismatch in files_created", NUMBER_OF_OPS,
        (long) metricMap.get("files_created"));
    assertEquals("Mismatch in directories_created", NUMBER_OF_OPS,
        (long) metricMap.get("directories_created"));
    assertEquals("Mismatch in op_mkdirs", NUMBER_OF_OPS,
        (long) metricMap.get("op_mkdirs"));
    assertEquals("Mismatch in op_get_file_status", 1 + NUMBER_OF_OPS,
        (long) metricMap.get("op_get_file_status"));

  }

  /**
   * Testing statistics by deleting files and directories.
   */
  @Test
  public void testDeleteStatistics() throws IOException {
    describe("Testing counter values got by deleting directory and files "
        + "in Abfs");
    AzureBlobFileSystem fs = getFileSystem();
    /*
     This directory path needs to be root for triggering the
     directories_deleted counter.
     */
    Path createDirectoryPath = path("/");
    Path createFilePath = path(getMethodName());

    Map<String, Long> metricMap = fs.getInstrumentation().toMap();
    //Test for initial statistic values before deleting any directory or files
    assertEquals("Mismatch in op_delete", 0,
        (long) metricMap.get("op_delete"));
    assertEquals("Mismatch in files_deleted", 0,
        (long) metricMap.get("files_deleted"));
    assertEquals("Mismatch in directories_deleted", 0,
        (long) metricMap.get("directories_deleted"));
    assertEquals("Mismatch in op_list_status", 0,
        (long) metricMap.get("op_list_status"));

      /*
      creating a directory and a file inside that directory.
      The directory is root. Hence, no parent. This allows us to invoke
      deleteRoot() method to see the population of directories_deleted and
      files_deleted counters.
       */
    fs.mkdirs(createDirectoryPath);
    fs.create(path(createDirectoryPath + getMethodName()));
    fs.delete(createDirectoryPath, true);

    metricMap = fs.getInstrumentation().toMap();

      /*
      Test for op_delete, files_deleted, op_list_status.
      since directory is delete recursively op_delete is called 2 times.
      1 file is deleted, 1 listStatus() call is made.
       */
    assertEquals("Mismatch in op_delete", 2,
        (long) metricMap.get("op_delete"));
    assertEquals("Mismatch in files_deleted", 1,
        (long) metricMap.get("files_deleted"));
    assertEquals("Mismatch in op_list_status", 1,
        (long) metricMap.get("op_list_status"));

      /*
      creating a root directory and deleting it recursively to see if
      directories_deleted is called or not.
       */
    fs.mkdirs(createDirectoryPath);
    fs.create(createFilePath);
    fs.delete(createDirectoryPath, true);
    metricMap = fs.getInstrumentation().toMap();

    // Test for directories_deleted.
    assertEquals("Mismatch in directories_deleted", 1,
        (long) metricMap.get("directories_deleted"));
  }

  /**
   * Testing statistics of open, append, rename and exists method calls.
   */
  @Test
  public void testOpenAppendRenameExists() throws IOException {
    describe("Testing counter values on calling open, append and rename and "
        + "exists methods on Abfs");

    AzureBlobFileSystem fs = getFileSystem();
    Path createFilePath = path(getMethodName());
    Path destCreateFilePath = path(getMethodName() + "New");

    Map<String, Long> metricMap = fs.getInstrumentation().toMap();

    //Tests for initial values of counters before any method is called.
    assertEquals("Mismatch in op_open", 0,
        (long) metricMap.get("op_open"));
    assertEquals("Mismatch in op_append", 0,
        (long) metricMap.get("op_append"));
    assertEquals("Mismatch in op_rename", 0,
        (long) metricMap.get("op_rename"));
    assertEquals("Mismatch in op_exists", 0,
        (long) metricMap.get("op_exists"));

    fs.create(createFilePath);
    fs.open(createFilePath);
    fs.append(createFilePath);
    fs.rename(createFilePath, destCreateFilePath);

    metricMap = fs.getInstrumentation().toMap();
    //Testing single method calls to open, append and rename.
    assertEquals("Mismatch in op_open", 1,
        (long) metricMap.get("op_open"));
    assertEquals("Mismatch in op_append", 1,
        (long) metricMap.get("op_append"));
    assertEquals("Mismatch in op_rename", 1,
        (long) metricMap.get("op_rename"));

    //Testing if file exists at path.
    assertTrue("Mismatch in file Path existing",
        fs.exists(destCreateFilePath));
    assertFalse("Mismatch in file Path existing", fs.exists(createFilePath));

    metricMap = fs.getInstrumentation().toMap();
    //Testing exists() calls.
    assertEquals("Mismatch in op_exists", 2,
        (long) metricMap.get("op_exists"));

    //re-initialising Abfs to reset statistic values.
    fs.initialize(fs.getUri(), fs.getConf());

    fs.create(destCreateFilePath);

    for (int i = 0; i < NUMBER_OF_OPS; i++) {
      fs.open(destCreateFilePath);
      fs.append(destCreateFilePath);
    }

    metricMap = fs.getInstrumentation().toMap();

    //Testing large number of method calls to open, append.
    assertEquals("Mismatch in op_open", NUMBER_OF_OPS,
        (long) metricMap.get("op_open"));
    assertEquals("Mismatch in op_append", NUMBER_OF_OPS,
        (long) metricMap.get("op_append"));

    for (int i = 0; i < NUMBER_OF_OPS; i++) {
      // rename and then back to earlier name for no error while looping.
      fs.rename(destCreateFilePath, createFilePath);
      fs.rename(createFilePath, destCreateFilePath);

      //check if first name is existing and 2nd is not existing.
      assertTrue("Mismatch in file Path existing",
          fs.exists(destCreateFilePath));
      assertFalse("Mismatch in file Path existing",
          fs.exists(createFilePath));

    }

    metricMap = fs.getInstrumentation().toMap();

      /*Testing exists() calls and rename calls. Since both were called 2
       times in 1 loop. 2*numberOfOps is expectedValue. */
    assertEquals("Mismatch in op_rename", 2 * NUMBER_OF_OPS,
        (long) metricMap.get("op_rename"));
    assertEquals("Mismatch in op_exists", 2 * NUMBER_OF_OPS,
        (long) metricMap.get("op_exists"));

  }
}
