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
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

/**
 * Test config property KEY_PAGE_BLOB_DIRECTORIES.
 */
public class TestKeyPageBlobDirectories extends AbstractWasbTestBase{

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  public void expectPageBlobKey(boolean expectedOutcome, AzureNativeFileSystemStore store, String path) {
    assertEquals("Unexpected result for isPageBlobKey(" + path + ")",
            expectedOutcome, store.isPageBlobKey(path));

  }

  @Test
  public void testKeySetWithoutAsterisk() throws Exception {
    NativeAzureFileSystem azureFs = fs;
    AzureNativeFileSystemStore store = azureFs.getStore();
    Configuration conf = fs.getConf();
    String dirList = "/service/WALs,/data/mypageblobfiles";
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, dirList);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    expectPageBlobKey(false, store, "/");
    expectPageBlobKey(false, store, "service");

    expectPageBlobKey(false, store, "service/dir/recovered.edits");
    expectPageBlobKey(true, store, "service/WALs/recovered.edits");

    expectPageBlobKey(false, store, "data/dir/recovered.txt");
    expectPageBlobKey(true, store, "data/mypageblobfiles/recovered.txt");
  }

  @Test
  public void testKeySetWithAsterisk() throws Exception {
    NativeAzureFileSystem azureFs = fs;
    AzureNativeFileSystemStore store = azureFs.getStore();
    Configuration conf = fs.getConf();
    String dirList = "/service/*/*/*/recovered.edits,/*/recovered.edits,/*/*/*/WALs, /*/*/oldWALs/*/*";
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, dirList);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    expectPageBlobKey(false, store, "/");
    expectPageBlobKey(false, store, "service");

    expectPageBlobKey(false, store, "service/dir/recovered.edits");
    expectPageBlobKey(true, store, "service/dir1/dir2/dir3/recovered.edits");

    expectPageBlobKey(false, store, "data/dir/recovered.edits");
    expectPageBlobKey(true, store, "data/recovered.edits");

    expectPageBlobKey(false, store, "dir1/dir2/WALs/data");
    expectPageBlobKey(true, store, "dir1/dir2/dir3/WALs/data1");
    expectPageBlobKey(true, store, "dir1/dir2/dir3/WALs/data2");

    expectPageBlobKey(false, store, "dir1/oldWALs/data");
    expectPageBlobKey(false, store, "dir1/dir2/oldWALs/data");
    expectPageBlobKey(true, store, "dir1/dir2/oldWALs/dir3/dir4/data");
  }



  @Test
  public void testKeySetUsingFullName() throws Exception {
    NativeAzureFileSystem azureFs = fs;
    AzureNativeFileSystemStore store = azureFs.getStore();
    Configuration conf = fs.getConf();
    String dirList = "/service/WALs,/data/mypageblobfiles,/*/*/WALs,/*/*/recover.edits";
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, dirList);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    final String defaultFS = FileSystem.getDefaultUri(conf).toString();

    expectPageBlobKey(false, store, defaultFS + "service/recover.edits");
    expectPageBlobKey(true, store, defaultFS + "service/WALs/recover.edits");

    expectPageBlobKey(false, store, defaultFS + "data/mismatch/mypageblobfiles/data");
    expectPageBlobKey(true, store, defaultFS + "data/mypageblobfiles/data");

    expectPageBlobKey(false, store, defaultFS + "dir1/dir2/dir3/WALs/data");
    expectPageBlobKey(true, store, defaultFS + "dir1/dir2/WALs/data");

    expectPageBlobKey(false, store, defaultFS + "dir1/dir2/dir3/recover.edits");
    expectPageBlobKey(true, store, defaultFS + "dir1/dir2/recover.edits");

  }

  @Test
  public void testKeyContainsAsterisk() throws IOException {
    NativeAzureFileSystem azureFs = fs;
    AzureNativeFileSystemStore store = azureFs.getStore();
    Configuration conf = fs.getConf();
    // Test dir name which contains *
    String dirList = "/service/*/*/*/d*ir,/*/fi**le.data,/*/*/*/WALs*, /*/*/oldWALs";
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, dirList);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    expectPageBlobKey(false, store, "/");
    expectPageBlobKey(false, store, "service");

    expectPageBlobKey(false, store, "service/d*ir/data");
    expectPageBlobKey(true, store, "service/dir1/dir2/dir3/d*ir/data");

    expectPageBlobKey(false, store, "dir/fi*le.data");
    expectPageBlobKey(true, store, "dir/fi**le.data");

    expectPageBlobKey(false, store, "dir1/dir2/WALs/data");
    expectPageBlobKey(false, store, "dir1/dir2/dir3/WALs/data");
    expectPageBlobKey(true, store, "dir1/dir2/dir3/WALs*/data1");
    expectPageBlobKey(true, store, "dir1/dir2/dir3/WALs*/data2");

    expectPageBlobKey(false, store, "dir1/oldWALs/data");
    expectPageBlobKey(true, store, "dir1/dir2/oldWALs/data1");
    expectPageBlobKey(true, store, "dir1/dir2/oldWALs/data2");
  }

  @Test
  public void testKeyWithCommonPrefix() throws IOException {
    NativeAzureFileSystem azureFs = fs;
    AzureNativeFileSystemStore store = azureFs.getStore();
    Configuration conf = fs.getConf();
    // Test dir name which contains *
    String dirList = "/service/WALs,/*/*/WALs";
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, dirList);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);

    expectPageBlobKey(false, store, "/");
    expectPageBlobKey(false, store, "service");

    expectPageBlobKey(false, store, "service/WALsssssss/dir");
    expectPageBlobKey(true, store, "service/WALs/dir");

    expectPageBlobKey(false, store, "service/dir/WALsss/data");
    expectPageBlobKey(true, store, "service/dir/WALs/data");
  }
}
