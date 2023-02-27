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
import java.net.URISyntaxException;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Test the TestViewFileSystemOverloadSchemeLF using a file with authority:
 * file://mountTableName/ i.e, the authority is used to load a mount table.
 */
public class TestViewFileSystemOverloadSchemeLocalFileSystem {
  private static final String FILE = "file";
  private static final Logger LOG =
      LoggerFactory.getLogger(TestViewFileSystemOverloadSchemeLocalFileSystem.class);
  private FileSystem fsTarget;
  private Configuration conf;
  private Path targetTestRoot;
  private FileSystemTestHelper fileSystemTestHelper =
      new FileSystemTestHelper();

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(String.format("fs.%s.impl", FILE),
        ViewFileSystemOverloadScheme.class.getName());
    conf.set(String.format(
        FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN, FILE),
        LocalFileSystem.class.getName());
    fsTarget = new LocalFileSystem();
    fsTarget.initialize(new URI("file:///"), conf);
    // create the test root on local_fs
    targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
    fsTarget.delete(targetTestRoot, true);
    fsTarget.mkdirs(targetTestRoot);
  }

  /**
   * Adds the given mount links to config. sources contains mount link src and
   * the respective index location in targets contains the target uri.
   */
  void addMountLinks(String mountTable, String[] sources, String[] targets,
      Configuration config) throws IOException, URISyntaxException {
    ViewFsTestSetup.addMountLinksToConf(mountTable, sources, targets, config);
  }

  /**
   * Tests write file and read file with ViewFileSystemOverloadScheme.
   */
  @Test
  public void testLocalTargetLinkWriteSimple()
      throws IOException, URISyntaxException {
    LOG.info("Starting testLocalTargetLinkWriteSimple");
    final String testString = "Hello Local!...";
    final Path lfsRoot = new Path("/lfsRoot");
    addMountLinks(null, new String[] {lfsRoot.toString() },
        new String[] {targetTestRoot + "/local" }, conf);
    try (FileSystem lViewFs = FileSystem.get(URI.create("file:///"), conf)) {
      final Path testPath = new Path(lfsRoot, "test.txt");
      try (FSDataOutputStream fsDos = lViewFs.create(testPath)) {
        fsDos.writeUTF(testString);
      }

      try (FSDataInputStream lViewIs = lViewFs.open(testPath)) {
        Assert.assertEquals(testString, lViewIs.readUTF());
      }
    }
  }

  /**
   * Tests create file and delete file with ViewFileSystemOverloadScheme.
   */
  @Test
  public void testLocalFsCreateAndDelete() throws Exception {
    LOG.info("Starting testLocalFsCreateAndDelete");
    addMountLinks("mt", new String[] {"/lfsroot" },
        new String[] {targetTestRoot + "/wd2" }, conf);
    final URI mountURI = URI.create("file://mt/");
    try (FileSystem lViewFS = FileSystem.get(mountURI, conf)) {
      Path testPath = new Path(mountURI.toString() + "/lfsroot/test");
      lViewFS.createNewFile(testPath);
      Assert.assertTrue(lViewFS.exists(testPath));
      lViewFS.delete(testPath, true);
      Assert.assertFalse(lViewFS.exists(testPath));
    }
  }

  /**
   * Tests root level file with linkMergeSlash with
   * ViewFileSystemOverloadScheme.
   */
  @Test
  public void testLocalFsLinkSlashMerge() throws Exception {
    LOG.info("Starting testLocalFsLinkSlashMerge");
    addMountLinks("mt",
        new String[] {Constants.CONFIG_VIEWFS_LINK_MERGE_SLASH },
        new String[] {targetTestRoot + "/wd2" }, conf);
    final URI mountURI = URI.create("file://mt/");
    try (FileSystem lViewFS = FileSystem.get(mountURI, conf)) {
      Path fileOnRoot = new Path(mountURI.toString() + "/NewFile");
      lViewFS.createNewFile(fileOnRoot);
      Assert.assertTrue(lViewFS.exists(fileOnRoot));
    }
  }

  /**
   * Tests with linkMergeSlash and other mounts in
   * ViewFileSystemOverloadScheme.
   */
  @Test(expected = IOException.class)
  public void testLocalFsLinkSlashMergeWithOtherMountLinks() throws Exception {
    LOG.info("Starting testLocalFsLinkSlashMergeWithOtherMountLinks");
    addMountLinks("mt",
        new String[] {"/lfsroot", Constants.CONFIG_VIEWFS_LINK_MERGE_SLASH },
        new String[] {targetTestRoot + "/wd2", targetTestRoot + "/wd2" }, conf);
    final URI mountURI = URI.create("file://mt/");
    FileSystem.get(mountURI, conf);
    Assert.fail("A merge slash cannot be configured with other mount links.");
  }

  @After
  public void tearDown() throws Exception {
    if (null != fsTarget) {
      fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
      fsTarget.close();
    }
  }

  /**
   * Returns the test root dir.
   */
  public Path getTestRoot() {
    return this.targetTestRoot;
  }

  /**
   * Returns the conf.
   */
  public Configuration getConf() {
    return this.conf;
  }
}
