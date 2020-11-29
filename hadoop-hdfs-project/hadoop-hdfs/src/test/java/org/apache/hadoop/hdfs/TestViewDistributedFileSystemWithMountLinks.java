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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.TestViewFileSystemOverloadSchemeWithHdfsScheme;
import org.apache.hadoop.fs.viewfs.ViewFsTestSetup;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME_DEFAULT;

public class TestViewDistributedFileSystemWithMountLinks extends
    TestViewFileSystemOverloadSchemeWithHdfsScheme {
  @Override
  public void setUp() throws IOException {
    super.setUp();
    Configuration conf = getConf();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    conf.set("fs.hdfs.impl",
        ViewDistributedFileSystem.class.getName());
    conf.setBoolean(CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME,
        CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME_DEFAULT);
    URI defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    ConfigUtil.addLinkFallback(conf, defaultFSURI.getAuthority(),
        new Path(defaultFSURI.toString() + "/").toUri());
    setConf(conf);
  }

  @Test(timeout = 30000)
  public void testCreateOnRoot() throws Exception {
    testCreateOnRoot(true);
  }

  @Test(timeout = 30000)
  public void testMountLinkWithNonExistentLink() throws Exception {
    testMountLinkWithNonExistentLink(false);
  }

  @Test
  public void testRenameOnInternalDirWithFallback() throws Exception {
    Configuration conf = getConf();
    URI defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    final Path hdfsTargetPath1 = new Path(defaultFSURI + "/HDFSUser");
    final Path hdfsTargetPath2 = new Path(defaultFSURI + "/NewHDFSUser/next");
    ViewFsTestSetup.addMountLinksToConf(defaultFSURI.getAuthority(),
        new String[] {"/HDFSUser", "/NewHDFSUser/next"},
        new String[] {hdfsTargetPath1.toUri().toString(),
            hdfsTargetPath2.toUri().toString()}, conf);
    //Making sure parent dir structure as mount points available in fallback.
    try (DistributedFileSystem dfs = new DistributedFileSystem()) {
      dfs.initialize(defaultFSURI, conf);
      dfs.mkdirs(hdfsTargetPath1);
      dfs.mkdirs(hdfsTargetPath2);
    }

    try (FileSystem fs = FileSystem.get(conf)) {
      Path src = new Path("/newFileOnRoot");
      Path dst = new Path("/newFileOnRoot1");
      fs.create(src).close();
      verifyRename(fs, src, dst);

      src = new Path("/newFileOnRoot1");
      dst = new Path("/NewHDFSUser/newFileOnRoot");
      fs.mkdirs(dst.getParent());
      verifyRename(fs, src, dst);

      src = new Path("/NewHDFSUser/newFileOnRoot");
      dst = new Path("/NewHDFSUser/newFileOnRoot1");
      verifyRename(fs, src, dst);

      src = new Path("/NewHDFSUser/newFileOnRoot1");
      dst = new Path("/newFileOnRoot");
      verifyRename(fs, src, dst);

      src = new Path("/HDFSUser/newFileOnRoot1");
      dst = new Path("/HDFSUser/newFileOnRoot");
      fs.create(src).close();
      verifyRename(fs, src, dst);
    }
  }

  @Test
  public void testRenameWhenDstOnInternalDirWithFallback() throws Exception {
    Configuration conf = getConf();
    URI defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    final Path hdfsTargetPath1 = new Path(defaultFSURI + "/HDFSUser");
    final Path hdfsTargetPath2 =
        new Path(defaultFSURI + "/dstNewHDFSUser" + "/next");
    ViewFsTestSetup.addMountLinksToConf(defaultFSURI.getAuthority(),
        new String[] {"/InternalDirDoesNotExistInFallback/test",
            "/NewHDFSUser/next/next1"},
        new String[] {hdfsTargetPath1.toUri().toString(),
            hdfsTargetPath2.toUri().toString()}, conf);
    try (DistributedFileSystem dfs = new DistributedFileSystem()) {
      dfs.initialize(defaultFSURI, conf);
      dfs.mkdirs(hdfsTargetPath1);
      dfs.mkdirs(hdfsTargetPath2);
      dfs.mkdirs(new Path("/NewHDFSUser/next/next1"));
    }

    try (FileSystem fs = FileSystem.get(conf)) {
      Path src = new Path("/newFileOnRoot");
      Path dst = new Path("/NewHDFSUser/next");
      fs.create(src).close();
      verifyRename(fs, src, dst);

      src = new Path("/newFileOnRoot");
      dst = new Path("/NewHDFSUser/next/file");
      fs.create(src).close();
      verifyRename(fs, src, dst);

      src = new Path("/newFileOnRoot");
      dst = new Path("/InternalDirDoesNotExistInFallback/file");
      fs.create(src).close();
      // If fallback does not have same structure as internal, rename will fail.
      Assert.assertFalse(fs.rename(src, dst));
    }
  }

  private void verifyRename(FileSystem fs, Path src, Path dst)
      throws IOException {
    fs.rename(src, dst);
    Assert.assertFalse(fs.exists(src));
    Assert.assertTrue(fs.exists(dst));
  }
}
