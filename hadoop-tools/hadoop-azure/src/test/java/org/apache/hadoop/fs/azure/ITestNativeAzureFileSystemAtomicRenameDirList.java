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

package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import org.junit.Test;

/**
 * Test atomic renaming.
 */
public class ITestNativeAzureFileSystemAtomicRenameDirList
    extends AbstractWasbTestBase {

  // HBase-site config controlling HBase root dir
  private static final String HBASE_ROOT_DIR_CONF_STRING = "hbase.rootdir";
  private static final String HBASE_ROOT_DIR_VALUE_ON_DIFFERENT_FS =
      "wasb://somedifferentfilesystem.blob.core.windows.net/hbase";

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  @Test
  public void testAtomicRenameKeyDoesntNPEOnInitializingWithNonDefaultURI()
      throws IOException {
    NativeAzureFileSystem azureFs = fs;
    AzureNativeFileSystemStore azureStore = azureFs.getStore();
    Configuration conf = fs.getConf();
    conf.set(HBASE_ROOT_DIR_CONF_STRING, HBASE_ROOT_DIR_VALUE_ON_DIFFERENT_FS);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);
    azureStore.isAtomicRenameKey("anyrandomkey");
  }
}
