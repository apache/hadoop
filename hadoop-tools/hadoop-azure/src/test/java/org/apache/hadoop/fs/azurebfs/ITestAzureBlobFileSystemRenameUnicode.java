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

package org.apache.hadoop.fs.azurebfs;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;

/**
 * Parameterized test of rename operations of unicode paths.
 */
@RunWith(Parameterized.class)
public class ITestAzureBlobFileSystemRenameUnicode extends
    AbstractAbfsIntegrationTest {

  @Parameterized.Parameter
  public String srcDir;

  @Parameterized.Parameter(1)
  public String destDir;

  @Parameterized.Parameter(2)
  public String filename;

  @Parameterized.Parameters
  public static Iterable<Object[]> params() {
    return Arrays.asList(
        new Object[][]{
            {"/src", "/dest", "filename"},
            {"/%2c%26", "/abcÖ⇒123", "%2c%27"},
            {"/ÖáΠ⇒", "/abcÖáΠ⇒123", "中文"},
            {"/A +B", "/B+ C", "C +D"},
            {
                "/A~`!@#$%^&*()-_+={};:'>,<B",
                "/B~`!@#$%^&*()-_+={};:'>,<C",
                "C~`!@#$%^&*()-_+={};:'>,<D"
            }
        });
  }

  public ITestAzureBlobFileSystemRenameUnicode() throws Exception {
  }

  /**
   * Known issue: ListStatus operation to folders/files whose name contains '?' will fail.
   * This is because Auto rest client didn't encode '?' in the uri query parameters
   */
  @Test
  public void testRenameFileUsingUnicode() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path folderPath1 = new Path(srcDir);
    assertMkdirs(fs, folderPath1);
    assertIsDirectory(fs, folderPath1);
    Path filePath = new Path(folderPath1 + "/" + filename);
    touch(filePath);
    assertIsFile(fs, filePath);

    Path folderPath2 = new Path(destDir);
    assertRenameOutcome(fs, folderPath1, folderPath2, true);
    assertPathDoesNotExist(fs, "renamed", folderPath1);
    assertIsDirectory(fs, folderPath2);
    assertPathExists(fs, "renamed file", new Path(folderPath2 + "/" + filename));

    FileStatus[] fileStatus = fs.listStatus(folderPath2);
    assertNotNull(fileStatus);
    assertTrue("Empty listing returned from listStatus(\"" + folderPath2 + "\")",
        fileStatus.length > 0);
    assertEquals(fileStatus[0].getPath().getName(), filename);
  }
}
