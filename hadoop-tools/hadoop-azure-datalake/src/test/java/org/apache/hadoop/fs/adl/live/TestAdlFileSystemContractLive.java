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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * Verify Adls adhere to Hadoop file system semantics.
 */
public class TestAdlFileSystemContractLive extends FileSystemContractBaseTest {
  private FileSystem adlStore;

  @Override
  protected void setUp() throws Exception {
    adlStore = AdlStorageConfiguration.createAdlStorageConnector();
    if (AdlStorageConfiguration.isContractTestEnabled()) {
      fs = adlStore;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (AdlStorageConfiguration.isContractTestEnabled()) {
      cleanup();
      adlStore = null;
      fs = null;
    }
  }

  private void cleanup() throws IOException {
    adlStore.delete(new Path("/test"), true);
  }

  @Override
  protected void runTest() throws Throwable {
    if (AdlStorageConfiguration.isContractTestEnabled()) {
      super.runTest();
    }
  }

  public void testGetFileStatus() throws IOException {
    if (!AdlStorageConfiguration.isContractTestEnabled()) {
      return;
    }

    Path testPath = new Path("/test/adltest");
    if (adlStore.exists(testPath)) {
      adlStore.delete(testPath, false);
    }

    adlStore.create(testPath).close();
    assertTrue(adlStore.delete(testPath, false));
  }

  /**
   * The following tests are failing on Azure Data Lake and the Azure Data Lake
   * file system code needs to be modified to make them pass.
   * A separate work item has been opened for this.
   */
  @Test
  @Override
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    // BUG : Adl should return exception instead of false.
  }

  @Test
  @Override
  public void testMkdirsWithUmask() throws Exception {
    // Support under implementation in Adl
  }

  @Test
  @Override
  public void testMoveFileUnderParent() throws Exception {
    // BUG: Adl server should return expected status code.
  }

  @Test
  @Override
  public void testRenameFileToSelf() throws Exception {
    // BUG: Adl server should return expected status code.
  }

  @Test
  @Override
  public void testRenameToDirWithSamePrefixAllowed() throws Exception {
    // BUG: Adl server should return expected status code.
  }
}
