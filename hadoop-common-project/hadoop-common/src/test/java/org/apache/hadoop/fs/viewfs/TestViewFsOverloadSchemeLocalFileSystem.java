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
package org.apache.hadoop.fs.viewfs;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * Test the TestViewFsOverloadSchemeLocalFS using a file with authority:
 * file://mountTableName/ i.e, the authority is used to load a mount table.
 */
public class TestViewFsOverloadSchemeLocalFileSystem {
  private static final String FILE = "file";
  private static final Log LOG =
      LogFactory.getLog(TestViewFsOverloadSchemeLocalFileSystem.class);
  private FileSystem fsTarget;
  private Configuration conf;
  private Path targetTestRoot;
  private FileSystemTestHelper fileSystemTestHelper;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(String.format(FsConstants.FS_IMPL_PATTERN_KEY, FILE),
        ViewFsOverloadScheme.class.getName());
    conf.set(String.format(
        FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN_KEY,
        FILE),
        LocalFileSystem.class.getName());
    conf.set(FsConstants.VIEWFS_OVERLOAD_SCHEME_KEY, FILE);
    fsTarget = new LocalFileSystem();
    fsTarget.initialize(new URI("file:///"), conf);
    fileSystemTestHelper = new FileSystemTestHelper();
    // create the test root on local_fs
    targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
    fsTarget.delete(targetTestRoot, true);
    fsTarget.mkdirs(targetTestRoot);
  }

  /**
   * Tests write file and read file with ViewFSOverloadScheme.
   */
  @Test
  public void testLocalTargetLinkWriteSimple() throws IOException {
    LOG.info("Starting testLocalTargetLinkWriteSimple");
    final String testString = "Hello Local!...";
    final Path lfsRoot = new Path("/lfsRoot");
    ConfigUtil.addLink(conf, lfsRoot.toString(),
        URI.create(targetTestRoot + "/local"));
    final FileSystem lViewFs = FileSystem.get(URI.create("file:///"), conf);

    final Path testPath = new Path(lfsRoot, "test.txt");
    final FSDataOutputStream fsDos = lViewFs.create(testPath);
    try {
      fsDos.writeUTF(testString);
    } finally {
      fsDos.close();
    }

    FSDataInputStream lViewIs = lViewFs.open(testPath);
    try {
      Assert.assertEquals(testString, lViewIs.readUTF());
    } finally {
      lViewIs.close();
    }
  }
  
  /**
   * Tests create file and delete file with ViewFSOverloadScheme.
   */
  @Test
  public void testLocalFsCreateAndDelete() throws Exception {
    LOG.info("Starting testLocalFsCreateAndDelete");
    ConfigUtil.addLink(conf, "mt", "/lfsroot",
        URI.create(targetTestRoot + "/wd2"));
    final URI mountURI = URI.create("file://mt/");
    final FileSystem lViewFS = FileSystem.get(mountURI, conf);
    try {
      Path testPath = new Path(mountURI.toString() + "/lfsroot/test");
      lViewFS.create(testPath);
      Assert.assertTrue(lViewFS.exists(testPath));
      lViewFS.delete(testPath, true);
      Assert.assertFalse(lViewFS.exists(testPath));
    } finally {
      lViewFS.close();
    }
  }
  
  /**
   * Tests root level file with linkMergeSlash with ViewFSOverloadScheme.
   */
  @Test
  public void testLocalFsLinkSlashMerge() throws Exception {
    LOG.info("Starting testLocalFSCreateAndDelete");
    ConfigUtil.addLinkMergeSlash(conf, "mt",
        URI.create(targetTestRoot + "/wd2"));
    final URI mountURI = URI.create("file://mt/");
    final FileSystem lViewFS = FileSystem.get(mountURI, conf);
    try {
      Path fileOnRoot = new Path(mountURI.toString() + "/NewFile");
      lViewFS.create(fileOnRoot);
      Assert.assertTrue(lViewFS.exists(fileOnRoot));
    } finally {
      lViewFS.close();
    }
  }
  
  /**
   * Tests with linkMergeSlash and other mounts in ViewFSOverloadScheme.
   */
  @Test(expected = IOException.class)
  public void testLocalFsLinkSlashMergeWithOtherMountLinks() throws Exception {
    LOG.info("Starting testLocalFsCreateAndDelete");
    ConfigUtil.addLink(conf, "mt", "/lfsroot",
        URI.create(targetTestRoot + "/wd2"));
    ConfigUtil.addLinkMergeSlash(conf, "mt",
        URI.create(targetTestRoot + "/wd2"));
    final URI mountURI = URI.create("file://mt/");
    FileSystem.get(mountURI, conf);
    Assert.fail("A merge slash cannot be configured with other mount links.");
  }

  @After
  public void tearDown() throws Exception {
    fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
  }
}
