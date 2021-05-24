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
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * Tests AzureBlobFileSystem Statistics.
 */
public class ITestAbfsStatistics extends AbstractAbfsIntegrationTest {

  private static final int NUMBER_OF_OPS = 10;

  public ITestAbfsStatistics() throws Exception {
  }

  /**
   * Testing the initial value of statistics.
   */
  @Test
  public void testInitialStatsValues() throws IOException {
    describe("Testing the initial values of Abfs counters");

    AbfsCounters abfsCounters =
        new AbfsCountersImpl(getFileSystem().getUri());
    IOStatistics ioStatistics = abfsCounters.getIOStatistics();

    //Initial value verification for counters
    for (Map.Entry<String, Long> entry : ioStatistics.counters().entrySet()) {
      checkInitialValue(entry.getKey(), entry.getValue(), 0);
    }

    //Initial value verification for gauges
    for (Map.Entry<String, Long> entry : ioStatistics.gauges().entrySet()) {
      checkInitialValue(entry.getKey(), entry.getValue(), 0);
    }

    //Initial value verifications for DurationTrackers
    for (Map.Entry<String, Long> entry : ioStatistics.maximums().entrySet()) {
      checkInitialValue(entry.getKey(), entry.getValue(), -1);
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

    Map<String, Long> metricMap = fs.getInstrumentationMap();
    /*
    Test of statistic values after creating a directory and a file ;
    getFileStatus is called 1 time after creating file and 1 time at time of
    initialising.
     */
    assertAbfsStatistics(AbfsStatistic.CALL_CREATE, 1, metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_CREATE_NON_RECURSIVE, 1, metricMap);
    assertAbfsStatistics(AbfsStatistic.FILES_CREATED, 1, metricMap);
    assertAbfsStatistics(AbfsStatistic.DIRECTORIES_CREATED, 1, metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_MKDIRS, 1, metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_GET_FILE_STATUS, 2, metricMap);

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

    metricMap = fs.getInstrumentationMap();
    /*
    Test of statistics values after creating 10 directories and files;
    getFileStatus is called 1 time at initialise() plus number of times file
    is created.
     */
    assertAbfsStatistics(AbfsStatistic.CALL_CREATE, NUMBER_OF_OPS, metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_CREATE_NON_RECURSIVE, NUMBER_OF_OPS,
        metricMap);
    assertAbfsStatistics(AbfsStatistic.FILES_CREATED, NUMBER_OF_OPS, metricMap);
    assertAbfsStatistics(AbfsStatistic.DIRECTORIES_CREATED, NUMBER_OF_OPS,
        metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_MKDIRS, NUMBER_OF_OPS, metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_GET_FILE_STATUS,
        1 + NUMBER_OF_OPS, metricMap);
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

    Map<String, Long> metricMap = fs.getInstrumentationMap();

    /*
    Test for op_delete, files_deleted, op_list_status.
    since directory is delete recursively op_delete is called 2 times.
    1 file is deleted, 1 listStatus() call is made.
     */
    assertAbfsStatistics(AbfsStatistic.CALL_DELETE, 2, metricMap);
    assertAbfsStatistics(AbfsStatistic.FILES_DELETED, 1, metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_LIST_STATUS, 1, metricMap);

    /*
    creating a root directory and deleting it recursively to see if
    directories_deleted is called or not.
     */
    fs.mkdirs(createDirectoryPath);
    fs.create(createFilePath);
    fs.delete(createDirectoryPath, true);
    metricMap = fs.getInstrumentationMap();

    //Test for directories_deleted.
    assertAbfsStatistics(AbfsStatistic.DIRECTORIES_DELETED, 1, metricMap);
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
    assertTrue(fs.rename(createFilePath, destCreateFilePath));

    Map<String, Long> metricMap = fs.getInstrumentationMap();
    //Testing single method calls to open, append and rename.
    assertAbfsStatistics(AbfsStatistic.CALL_OPEN, 1, metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_APPEND, 1, metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_RENAME, 1, metricMap);

    //Testing if file exists at path.
    assertTrue(String.format("File with name %s should exist",
        destCreateFilePath),
        fs.exists(destCreateFilePath));
    assertFalse(String.format("File with name %s should not exist",
        createFilePath),
        fs.exists(createFilePath));

    metricMap = fs.getInstrumentationMap();
    //Testing exists() calls.
    assertAbfsStatistics(AbfsStatistic.CALL_EXIST, 2, metricMap);

    //re-initialising Abfs to reset statistic values.
    fs.initialize(fs.getUri(), fs.getConf());

    fs.create(destCreateFilePath);

    for (int i = 0; i < NUMBER_OF_OPS; i++) {
      fs.open(destCreateFilePath);
      fs.append(destCreateFilePath);
    }

    metricMap = fs.getInstrumentationMap();

    //Testing large number of method calls to open, append.
    assertAbfsStatistics(AbfsStatistic.CALL_OPEN, NUMBER_OF_OPS, metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_APPEND, NUMBER_OF_OPS, metricMap);

    for (int i = 0; i < NUMBER_OF_OPS; i++) {
      // rename and then back to earlier name for no error while looping.
      assertTrue(fs.rename(destCreateFilePath, createFilePath));
      assertTrue(fs.rename(createFilePath, destCreateFilePath));

      //check if first name is existing and 2nd is not existing.
      assertTrue(String.format("File with name %s should exist",
          destCreateFilePath),
          fs.exists(destCreateFilePath));
      assertFalse(String.format("File with name %s should not exist",
          createFilePath),
          fs.exists(createFilePath));

    }

    metricMap = fs.getInstrumentationMap();

    /*
    Testing exists() calls and rename calls. Since both were called 2
    times in 1 loop. 2*numberOfOps is expectedValue.
    */
    assertAbfsStatistics(AbfsStatistic.CALL_RENAME, 2 * NUMBER_OF_OPS,
        metricMap);
    assertAbfsStatistics(AbfsStatistic.CALL_EXIST, 2 * NUMBER_OF_OPS,
        metricMap);

  }

  /**
   * Method to check initial value of the statistics which should be 0.
   *
   * @param statName  name of the statistic to be checked.
   * @param statValue value of the statistic.
   * @param expectedInitialValue initial value expected from this statistic.
   */
  private void checkInitialValue(String statName, long statValue,
      long expectedInitialValue) {
    assertEquals("Mismatch in " + statName, expectedInitialValue, statValue);
  }
}
