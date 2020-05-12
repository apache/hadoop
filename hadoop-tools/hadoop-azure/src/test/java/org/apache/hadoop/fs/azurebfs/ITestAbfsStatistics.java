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
  private String createOp =
      AbfsStatistic.CALL_CREATE.getStatName();
  private String createNonRecursiveOp =
      AbfsStatistic.CALL_CREATE_NON_RECURSIVE.getStatName();
  private String fileCreated =
      AbfsStatistic.FILES_CREATED.getStatName();
  private String directoriesCreated =
      AbfsStatistic.DIRECTORIES_CREATED.getStatName();
  private String mkdirOp = AbfsStatistic.CALL_MKDIRS.getStatName();
  private String getFileStatusOp =
      AbfsStatistic.CALL_GET_FILE_STATUS.getStatName();
  private String deleteOp =
      AbfsStatistic.CALL_DELETE.getStatName();
  private String fileDeleted =
      AbfsStatistic.FILES_DELETED.getStatName();
  private String directoriesDeleted =
      AbfsStatistic.DIRECTORIES_DELETED.getStatName();
  private String listStatusOp =
      AbfsStatistic.CALL_LIST_STATUS.getStatName();
  private String openOp = AbfsStatistic.CALL_OPEN.getStatName();
  private String appendOp =
      AbfsStatistic.CALL_APPEND.getStatName();
  private String renameOp =
      AbfsStatistic.CALL_RENAME.getStatName();
  private String existsOp = AbfsStatistic.CALL_EXIST.getStatName();

  public ITestAbfsStatistics() throws Exception {
  }

  /**
   * Testing the initial value of statistics.
   */
  @Test
  public void testInitialStatsValues() throws IOException {
    describe("Testing the initial values of Abfs counters");
    AbfsInstrumentation abfsInstrumentation =
        new AbfsInstrumentation(getFileSystem().getUri());
    Map<String, Long> metricMap = abfsInstrumentation.toMap();

    for (Map.Entry<String, Long> entry : metricMap.entrySet()) {
      String key = entry.getKey();
      Long value = entry.getValue();

      //Verify if initial value of statistic is 0.
      checkInitialValue(key, value);
    }
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

    fs.mkdirs(createDirectoryPath);
    fs.createNonRecursive(createFilePath, FsPermission
        .getDefault(), false, 1024, (short) 1, 1024, null);

    Map<String, Long> metricMap = fs.getInstrumentation().toMap();
    /*
    Test of statistic values after creating a directory and a file ;
    getFileStatus is called 1 time after creating file and 1 time at time of
    initialising.
     */
    assertEquals("Mismatch in " + createOp, 1,
        (long) metricMap.get(createOp));
    assertEquals("Mismatch in " + createNonRecursiveOp, 1,
        (long) metricMap.get(createNonRecursiveOp));
    assertEquals("Mismatch in " + fileCreated, 1,
        (long) metricMap.get(fileCreated));
    assertEquals("Mismatch in " + directoriesCreated, 1,
        (long) metricMap.get(directoriesCreated));
    assertEquals("Mismatch in " + mkdirOp, 1,
        (long) metricMap.get(mkdirOp));
    assertEquals("Mismatch in " + getFileStatusOp, 2,
        (long) metricMap.get(getFileStatusOp));

    //re-initialising Abfs to reset statistic values.
    fs.initialize(fs.getUri(), fs.getConf());

    /*
    Creating 10 directories and files; Directories and files can't be created
    with same name, hence <Name> + i to give unique names.
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
    getFileStatus is called 1 time at initialise() plus number of times file
    is created.
     */
    assertEquals("Mismatch in " + createOp, NUMBER_OF_OPS,
        (long) metricMap.get(createOp));
    assertEquals("Mismatch in " + createNonRecursiveOp, NUMBER_OF_OPS,
        (long) metricMap.get(createNonRecursiveOp));
    assertEquals("Mismatch in " + fileCreated, NUMBER_OF_OPS,
        (long) metricMap.get(fileCreated));
    assertEquals("Mismatch in " + directoriesCreated, NUMBER_OF_OPS,
        (long) metricMap.get(directoriesCreated));
    assertEquals("Mismatch in " + mkdirOp, NUMBER_OF_OPS,
        (long) metricMap.get(mkdirOp));
    assertEquals("Mismatch in " + getFileStatusOp, 1 + NUMBER_OF_OPS,
        (long) metricMap.get(getFileStatusOp));

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

    /*
    creating a directory and a file inside that directory.
    The directory is root. Hence, no parent. This allows us to invoke
    deleteRoot() method to see the population of directories_deleted and
    files_deleted counters.
     */
    fs.mkdirs(createDirectoryPath);
    fs.create(path(createDirectoryPath + getMethodName()));
    fs.delete(createDirectoryPath, true);

    Map<String, Long> metricMap = fs.getInstrumentation().toMap();

    /*
    Test for op_delete, files_deleted, op_list_status.
    since directory is delete recursively op_delete is called 2 times.
    1 file is deleted, 1 listStatus() call is made.
     */
    assertEquals("Mismatch in " + deleteOp, 2,
        (long) metricMap.get(deleteOp));
    assertEquals("Mismatch in " + fileDeleted, 1,
        (long) metricMap.get(fileDeleted));
    assertEquals("Mismatch in " + listStatusOp, 1,
        (long) metricMap.get(listStatusOp));

    /*
    creating a root directory and deleting it recursively to see if
    directories_deleted is called or not.
     */
    fs.mkdirs(createDirectoryPath);
    fs.create(createFilePath);
    fs.delete(createDirectoryPath, true);
    metricMap = fs.getInstrumentation().toMap();

    //Test for directories_deleted.
    assertEquals("Mismatch in " + directoriesDeleted, 1,
        (long) metricMap.get(directoriesDeleted));
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

    fs.create(createFilePath);
    fs.open(createFilePath);
    fs.append(createFilePath);
    fs.rename(createFilePath, destCreateFilePath);

    Map<String, Long> metricMap = fs.getInstrumentation().toMap();
    //Testing single method calls to open, append and rename.
    assertEquals("Mismatch in " + openOp, 1,
        (long) metricMap.get(openOp));
    assertEquals("Mismatch in " + appendOp, 1,
        (long) metricMap.get(appendOp));
    assertEquals("Mismatch in " + renameOp, 1,
        (long) metricMap.get(renameOp));

    //Testing if file exists at path.
    assertTrue("File should exist",
        fs.exists(destCreateFilePath));
    assertFalse("File should not exist", fs.exists(createFilePath));

    metricMap = fs.getInstrumentation().toMap();
    //Testing exists() calls.
    assertEquals("Mismatch in " + existsOp, 2,
        (long) metricMap.get(existsOp));

    //re-initialising Abfs to reset statistic values.
    fs.initialize(fs.getUri(), fs.getConf());

    fs.create(destCreateFilePath);

    for (int i = 0; i < NUMBER_OF_OPS; i++) {
      fs.open(destCreateFilePath);
      fs.append(destCreateFilePath);
    }

    metricMap = fs.getInstrumentation().toMap();

    //Testing large number of method calls to open, append.
    assertEquals("Mismatch in " + openOp, NUMBER_OF_OPS,
        (long) metricMap.get(openOp));
    assertEquals("Mismatch in " + appendOp, NUMBER_OF_OPS,
        (long) metricMap.get(appendOp));

    for (int i = 0; i < NUMBER_OF_OPS; i++) {
      // rename and then back to earlier name for no error while looping.
      fs.rename(destCreateFilePath, createFilePath);
      fs.rename(createFilePath, destCreateFilePath);

      //check if first name is existing and 2nd is not existing.
      assertTrue("File should exist",
          fs.exists(destCreateFilePath));
      assertFalse("File should not exist",
          fs.exists(createFilePath));

    }

    metricMap = fs.getInstrumentation().toMap();

    /*
    Testing exists() calls and rename calls. Since both were called 2
    times in 1 loop. 2*numberOfOps is expectedValue.
    */
    assertEquals("Mismatch in " + renameOp, 2 * NUMBER_OF_OPS,
        (long) metricMap.get(renameOp));
    assertEquals("Mismatch in " + existsOp, 2 * NUMBER_OF_OPS,
        (long) metricMap.get(existsOp));

  }

  /**
   * Method to check initial value of the statistics which should be 0.
   *
   * @param statName  name of the statistic to be checked.
   * @param statValue value of the statistic.
   */
  private void checkInitialValue(String statName, long statValue) {
    assertEquals("Mismatch in " + statName, 0, statValue);
  }
}
