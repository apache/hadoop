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
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test AzureBlobFileSystemStore listStatus with startFrom.
 * */
@ParameterizedClass(name="Testing path {0}, startFrom: {1}, Expecting result : {3}")
@MethodSource("params")
public class ITestAzureBlobFileSystemStoreListStatusWithRange extends
        AbstractAbfsIntegrationTest {
  private static final boolean SUCCEED = true;
  private static final boolean FAIL = false;
  private static final String[] SORTED_ENTRY_NAMES = {"1_folder", "A0", "D01", "a+", "c0", "name5"};

  private AzureBlobFileSystemStore store;
  private AzureBlobFileSystem fs;

  public String path;

  /**
   * A valid startFrom for listFileStatus with range is a non-fully qualified dir/file name
   * */
  public String startFrom;

  public int expectedStartIndexInArray;

  public boolean expectedResult;

  public static Iterable<Object[]> params() {
    return Arrays.asList(
            new Object[][]{
                    // case 0: list in root,  without range
                    {"/", null, 0, SUCCEED},

                    // case 1: list in the root, start from the second file
                    {"/", SORTED_ENTRY_NAMES[1], 1, SUCCEED},

                    // case 2: list in the root, invalid startFrom
                    {"/", "/", -1, FAIL},

                    // case 3: list in non-root level, valid startFrom : dir name
                    {"/" + SORTED_ENTRY_NAMES[2], SORTED_ENTRY_NAMES[1], 1, SUCCEED},

                    // case 4: list in non-root level, valid startFrom : file name
                    {"/" + SORTED_ENTRY_NAMES[2], SORTED_ENTRY_NAMES[2], 2, SUCCEED},

                    // case 5: list in non root level, invalid startFrom
                    {"/" + SORTED_ENTRY_NAMES[2], "/" + SORTED_ENTRY_NAMES[3], -1, FAIL},

                    // case 6: list using non existent startFrom, startFrom is smaller than the entries in lexical order
                    //          expecting return all entries
                    {"/" + SORTED_ENTRY_NAMES[2], "0-non-existent", 0, SUCCEED},

                    // case 7: list using non existent startFrom, startFrom is larger than the entries in lexical order
                    //         expecting return 0 entries
                    {"/" + SORTED_ENTRY_NAMES[2], "z-non-existent", -1, SUCCEED},

                    // case 8: list using non existent startFrom, startFrom is in the range
                    {"/" + SORTED_ENTRY_NAMES[2], "A1", 2, SUCCEED}
            });
  }

  public ITestAzureBlobFileSystemStoreListStatusWithRange(String pPath, String pStartFrom,
      int pExpectedStartIndexInArray, boolean pExpectedResult) throws Exception {
    super();

    this.path = pPath;
    this.startFrom = pStartFrom;
    this.expectedStartIndexInArray = pExpectedStartIndexInArray;
    this.expectedResult = pExpectedResult;

    if (this.getFileSystem() == null) {
      super.createFileSystem();
    }
    fs = this.getFileSystem();
    store = fs.getAbfsStore();
    prepareTestFiles();
    // Sort the names for verification, ABFS service should return the results in order.
    Arrays.sort(SORTED_ENTRY_NAMES);
  }

  @Test
  public void testListWithRange() throws IOException {
    try {
      FileStatus[] listResult = store.listStatus(new Path(path), startFrom,
          getTestTracingContext(fs, true));
      if (!expectedResult) {
        Assertions.fail("Excepting failure with IllegalArgumentException");
      }
      verifyFileStatus(listResult, new Path(path), expectedStartIndexInArray);
    } catch (IllegalArgumentException ex) {
      if (expectedResult) {
        Assertions.fail("Excepting success");
      }
    }
  }

  // compare the file status
  private void verifyFileStatus(FileStatus[] listResult, Path parentPath, int startIndexInSortedName) throws IOException {
    if (startIndexInSortedName == -1) {
      Assertions.assertEquals(0, listResult.length, "Expected empty FileStatus array");
      return;
    }

    FileStatus[] allFileStatuses = fs.listStatus(parentPath);
    Assertions.assertEquals(SORTED_ENTRY_NAMES.length, allFileStatuses.length,
        "number of dir/file doesn't match");
    int indexInResult = 0;
    for (int index = startIndexInSortedName; index < SORTED_ENTRY_NAMES.length; index++) {
      Assertions.assertEquals(allFileStatuses[index], listResult[indexInResult++],
          "fileStatus doesn't match");
    }
  }

  private void prepareTestFiles() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    // created 2 level file structures
    for (String levelOneFolder : SORTED_ENTRY_NAMES) {
      Path levelOnePath = new Path("/" + levelOneFolder);
      Assertions.assertTrue(fs.mkdirs(levelOnePath));
      for (String fileName : SORTED_ENTRY_NAMES) {
        Path filePath = new Path(levelOnePath, fileName);
        ContractTestUtils.touch(fs, filePath);
        ContractTestUtils.assertIsFile(fs, filePath);
      }
    }
  }
}
