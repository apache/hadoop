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

package org.apache.hadoop.fs.tosfs.object;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.tosfs.util.TestUtility.scheme;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestDirectoryStorage {
  private final ObjectStorage storage;

  public TestDirectoryStorage() {
    Configuration conf = new Configuration();
    storage =
        ObjectStorageFactory.createWithPrefix(String.format("%s-%s/", scheme(), UUIDUtils.random()),
            scheme(), TestUtility.bucket(), conf);
  }

  @BeforeAll
  public static void before() {
    assumeTrue(TestEnv.checkTestEnabled());
  }

  @AfterEach
  public void tearDown() {
    CommonUtils.runQuietly(() -> storage.deleteAll(""));
    for (MultipartUpload upload : storage.listUploads("")) {
      storage.abortMultipartUpload(upload.key(), upload.uploadId());
    }
  }

  @Test
  public void testListEmptyDir() {
    String key = "testListEmptyDir/";
    mkdir(key);
    assertNotNull(directoryStorage().head(key));

    assertFalse(directoryStorage().listDir(key, false).iterator().hasNext());
    assertFalse(directoryStorage().listDir(key, false).iterator().hasNext());
    assertTrue(directoryStorage().isEmptyDir(key));
  }

  @Test
  public void testListNonExistDir() {
    String key = "testListNonExistDir/";
    assertNull(directoryStorage().head(key));

    assertFalse(directoryStorage().listDir(key, false).iterator().hasNext());
    assertFalse(directoryStorage().listDir(key, false).iterator().hasNext());
    assertTrue(directoryStorage().isEmptyDir(key));
  }

  @Test
  public void testRecursiveList() {
    String root = "root/";
    String file1 = "root/file1";
    String file2 = "root/afile2";
    String dir1 = "root/dir1/";
    String file3 = "root/dir1/file3";

    mkdir(root);
    mkdir(dir1);
    touchFile(file1, TestUtility.rand(8));
    touchFile(file2, TestUtility.rand(8));
    touchFile(file3, TestUtility.rand(8));

    assertThat(directoryStorage().listDir(root, false))
        .hasSize(3)
        .extracting(ObjectInfo::key)
        .contains(dir1, file1, file2);

    assertThat(directoryStorage().listDir(root, true))
        .hasSize(4)
        .extracting(ObjectInfo::key)
        .contains(dir1, file1, file2, file3);
  }

  @Test
  public void testRecursiveListWithSmallBatch() {
    Configuration conf = new Configuration(directoryStorage().conf());
    conf.setInt(TosKeys.FS_TOS_LIST_OBJECTS_COUNT, 5);
    directoryStorage().initialize(conf, directoryStorage().bucket().name());

    String root = "root/";
    mkdir(root);

    // Create 2 files start with 'a', 2 sub dirs start with 'b', 2 files start with 'c'
    for (int i = 1; i <= 2; i++) {
      touchFile("root/a-file-" + i, TestUtility.rand(8));
      mkdir("root/b-dir-" + i + "/");
      touchFile("root/c-file-" + i, TestUtility.rand(8));
    }

    // Create two files under each sub dirs.
    for (int j = 1; j <= 2; j++) {
      touchFile(String.format("root/b-dir-%d/file1", j), TestUtility.rand(8));
      touchFile(String.format("root/b-dir-%d/file2", j), TestUtility.rand(8));
    }

    assertThat(directoryStorage().listDir(root, false))
        .hasSize(6)
        .extracting(ObjectInfo::key)
        .contains(
            "root/a-file-1", "root/a-file-2",
            "root/b-dir-1/", "root/b-dir-2/",
            "root/c-file-1", "root/c-file-2");

    assertThat(directoryStorage().listDir(root, true))
        .hasSize(10)
        .extracting(ObjectInfo::key)
        .contains(
            "root/a-file-1", "root/a-file-2",
            "root/b-dir-1/", "root/b-dir-1/file1", "root/b-dir-1/file2",
            "root/b-dir-2/", "root/b-dir-2/file1", "root/b-dir-2/file2",
            "root/c-file-1", "root/c-file-2");
  }

  @Test
  public void testRecursiveListRoot() {
    String root = "root/";
    String dir1 = "root/dir1/";
    mkdir(root);
    mkdir(dir1);

    assertThat(directoryStorage().listDir("", true))
        .hasSize(2)
        .extracting(ObjectInfo::key)
        .contains("root/", "root/dir1/");
  }

  @Test
  public void testDeleteEmptyDir() {
    String dir = "a/b/";
    mkdir(dir);

    directoryStorage().deleteDir(dir, false);
    assertNull(directoryStorage().head(dir));
  }

  @Test
  public void testDeleteNonEmptyDir() {
    String dir = "a/b/";
    String subDir = "a/b/c/";
    String file = "a/b/file.txt";
    mkdir(dir);
    mkdir(subDir);
    touchFile(file, new byte[10]);

    assertThrows(RuntimeException.class, () -> directoryStorage().deleteDir(dir, false));
    assertNotNull(directoryStorage().head(dir));
    assertNotNull(directoryStorage().head(subDir));
    assertNotNull(directoryStorage().head(file));

    directoryStorage().deleteDir(dir, true);
    assertNull(directoryStorage().head(dir));
    assertNull(directoryStorage().head(subDir));
    assertNull(directoryStorage().head(file));
  }

  @Test
  public void testRecursiveDeleteDirViaTosSDK() {
    Configuration conf = new Configuration(directoryStorage().conf());
    conf.setBoolean(TosKeys.FS_TOS_RMR_CLIENT_ENABLE, true);
    directoryStorage().initialize(conf, directoryStorage().bucket().name());

    testDeleteNonEmptyDir();
  }

  // TOS doesn't enable recursive delete in server side currently.
  @Disabled
  @Test
  public void testAtomicDeleteDir() {
    Configuration conf = new Configuration(directoryStorage().conf());
    conf.setBoolean(TosKeys.FS_TOS_RMR_SERVER_ENABLED, true);
    directoryStorage().initialize(conf, directoryStorage().bucket().name());

    testDeleteNonEmptyDir();
  }

  private void touchFile(String key, byte[] data) {
    directoryStorage().put(key, data);
  }

  private void mkdir(String key) {
    directoryStorage().put(key, new byte[0]);
  }

  private DirectoryStorage directoryStorage() {
    assumeTrue(storage.bucket().isDirectory());
    return (DirectoryStorage) storage;
  }
}
