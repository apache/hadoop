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
import org.apache.hadoop.fs.FsShell;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;

/**
 * Tests for Azure Blob FileSystem CLI.
 */
public class ITestAzureBlobFileSystemCLI extends AbstractAbfsIntegrationTest {

  public  ITestAzureBlobFileSystemCLI() throws Exception {
    super();
  }

  /**
   * Test for HADOOP-16138: hadoop fs mkdir / of nonexistent abfs
   * container raises NPE.
   *
   * The command should return with 1 exit status, but there should be no NPE.
   *
   * @throws Exception
   */
  @Test
  public void testMkdirRootNonExistentContainer() throws Exception {
    final Configuration rawConf = getRawConfiguration();
    final String account = rawConf.get(FS_AZURE_ABFS_ACCOUNT_NAME, null);
    rawConf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
        false);
    String nonExistentContainer = "nonexistent-" + UUID.randomUUID();
    FsShell fsShell = new FsShell(rawConf);

    int result = fsShell.run(new String[] {"-mkdir",
        ABFS_SCHEME + "://" + nonExistentContainer + "@" + account + "/"});

    assertEquals(1, result);
  }
}
