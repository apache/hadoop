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

import java.util.UUID;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;

/**
 * Test mkdir operation.
 */
public class ITestAzureBlobFileSystemMkDir extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemMkDir() throws Exception {
    super();
  }

  @Test
  public void testCreateDirWithExistingDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("testFolder");
    assertMkdirs(fs, path);
    assertMkdirs(fs, path);
  }

  @Test
  public void testCreateRoot() throws Exception {
    assertMkdirs(getFileSystem(), new Path("/"));
  }

  /**
   * Test mkdir for possible values of fs.azure.disable.default.create.overwrite
   * @throws Exception
   */
  @Test
  public void testDefaultCreateOverwriteDirTest() throws Throwable {
    // the config fs.azure.disable.default.create.overwrite should have no
    // effect on mkdirs
    testCreateDirOverwrite(true);
    testCreateDirOverwrite(false);
  }

  public void testCreateDirOverwrite(boolean enableConditionalCreateOverwrite)
      throws Throwable {
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(enableConditionalCreateOverwrite));

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    long totalConnectionMadeBeforeTest = fs.getInstrumentationMap()
        .get(CONNECTIONS_MADE.getStatName());

    int mkdirRequestCount = 0;
    final Path dirPath = new Path("/DirPath_"
        + UUID.randomUUID().toString());

    // Case 1: Dir does not pre-exist
    fs.mkdirs(dirPath);

    // One request to server
    mkdirRequestCount++;

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + mkdirRequestCount,
        fs.getInstrumentationMap());

    // Case 2: Dir pre-exists
    // Mkdir on existing Dir path will not lead to failure
    fs.mkdirs(dirPath);

    // One request to server
    mkdirRequestCount++;

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + mkdirRequestCount,
        fs.getInstrumentationMap());
  }
}
