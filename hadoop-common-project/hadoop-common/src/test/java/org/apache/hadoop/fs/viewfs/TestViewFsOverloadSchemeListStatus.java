/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * ViewFsOverloadScheme ListStatus.
 */
public class TestViewFsOverloadSchemeListStatus {

  private static final File TEST_DIR =
      GenericTestUtils.getTestDir(TestViewfsFileStatus.class.getSimpleName());
  private Configuration conf;
  private static final String FILE_NAME = "file";

  @Before
  public void setUp() {
    conf = new Configuration();
    conf.set(String.format("fs.%s.impl", FILE_NAME),
        ViewFileSystemOverloadScheme.class.getName());
    conf.set(String
        .format(FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN,
            FILE_NAME), LocalFileSystem.class.getName());
    FileUtil.fullyDelete(TEST_DIR);
    assertTrue(TEST_DIR.mkdirs());
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
  }

  /**
   * Tests the ACL and isDirectory returned from listStatus for directories and
   * files.
   */
  @Test
  public void testListStatusACL() throws IOException, URISyntaxException {
    String testfilename = "testFileACL";
    String childDirectoryName = "testDirectoryACL";
    TEST_DIR.mkdirs();
    File infile = new File(TEST_DIR, testfilename);
    final byte[] content = "dingos".getBytes();

    try (FileOutputStream fos = new FileOutputStream(infile)) {
      fos.write(content);
    }
    assertEquals(content.length, infile.length());
    File childDir = new File(TEST_DIR, childDirectoryName);
    childDir.mkdirs();

    ConfigUtil.addLink(conf, "/file", infile.toURI());
    ConfigUtil.addLink(conf, "/dir", childDir.toURI());

    String fileUriStr = "file:///";
    try (FileSystem vfs = FileSystem.get(new URI(fileUriStr), conf)) {
      assertEquals(ViewFileSystemOverloadScheme.class, vfs.getClass());
      FileStatus[] statuses = vfs.listStatus(new Path("/"));

      FileSystem localFs = ((ViewFileSystemOverloadScheme) vfs)
          .getRawFileSystem(new Path(fileUriStr), conf);
      FileStatus fileStat = localFs.getFileStatus(new Path(infile.getPath()));
      FileStatus dirStat = localFs.getFileStatus(new Path(childDir.getPath()));
      for (FileStatus status : statuses) {
        if (status.getPath().getName().equals(FILE_NAME)) {
          assertEquals(fileStat.getPermission(), status.getPermission());
        } else {
          assertEquals(dirStat.getPermission(), status.getPermission());
        }
      }

      localFs.setPermission(new Path(infile.getPath()),
          FsPermission.valueOf("-rwxr--r--"));
      localFs.setPermission(new Path(childDir.getPath()),
          FsPermission.valueOf("-r--rwxr--"));

      statuses = vfs.listStatus(new Path("/"));
      for (FileStatus status : statuses) {
        if (status.getPath().getName().equals(FILE_NAME)) {
          assertEquals(FsPermission.valueOf("-rwxr--r--"),
              status.getPermission());
          assertFalse(status.isDirectory());
        } else {
          assertEquals(FsPermission.valueOf("-r--rwxr--"),
              status.getPermission());
          assertTrue(status.isDirectory());
        }
      }
    }
  }

  /**
   * Tests that ViewFSOverloadScheme should consider initialized fs as fallback
   * if there are no mount links configured. It should add fallback with the
   * chrootedFS at it's uri's root.
   */
  @Test(timeout = 30000)
  public void testViewFSOverloadSchemeWithoutAnyMountLinks() throws Exception {
    Path initUri = new Path(TEST_DIR.toURI().toString(), "init");
    try (FileSystem fs = FileSystem.get(initUri.toUri(), conf)) {
      ViewFileSystemOverloadScheme vfs = (ViewFileSystemOverloadScheme) fs;
      assertEquals(0, vfs.getMountPoints().length);
      Path testOnFallbackPath = new Path(TEST_DIR.toURI().toString(), "test");
      assertTrue(vfs.mkdirs(testOnFallbackPath));
      FileStatus[] status = vfs.listStatus(testOnFallbackPath.getParent());
      assertEquals(Path.getPathWithoutSchemeAndAuthority(testOnFallbackPath),
          Path.getPathWithoutSchemeAndAuthority(status[0].getPath()));
      //Check directly on localFS. The fallBackFs(localFS) should be chrooted
      //at it's root. So, after
      FileSystem lfs = vfs.getRawFileSystem(testOnFallbackPath, conf);
      FileStatus[] statusOnLocalFS =
          lfs.listStatus(testOnFallbackPath.getParent());
      assertEquals(testOnFallbackPath.getName(),
          statusOnLocalFS[0].getPath().getName());
      //initUri should not have exist in lfs, as it would have chrooted on it's
      // root only.
      assertFalse(lfs.exists(initUri));
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
  }

}
