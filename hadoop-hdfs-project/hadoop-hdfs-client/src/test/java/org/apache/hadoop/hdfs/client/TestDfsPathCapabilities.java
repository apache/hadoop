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
package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonPathCapabilities.FS_SYMLINKS;

public class TestDfsPathCapabilities {
  public static final Logger LOG = LoggerFactory.getLogger(TestDfsPathCapabilities.class);

  static FileSystem getFileSystem(String url) throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, url);
    final FileSystem fs = FileSystem.get(conf);
    Assert.assertTrue(fs.supportsSymlinks());
    return fs;
  }

  /**
   * Test if {@link DistributedFileSystem#hasPathCapability(Path, String)}
   * returns correct result for FS_SYMLINKS.
   */
  @Test
  public void testSymlinks() throws Exception {
    final DistributedFileSystem dfs = Mockito.spy(
        (DistributedFileSystem) getFileSystem("hdfs://localhost/"));
    final WebHdfsFileSystem webhdfs = Mockito.spy(
        (WebHdfsFileSystem) getFileSystem("webhdfs://localhost/"));

    final FileSystem[] fileSystems = {dfs, webhdfs};
    final Path path = new Path("/");

    for (FileSystem fs : fileSystems) {
      LOG.info("fs is {}", fs.getClass().getSimpleName());
      // Symlinks support disabled
      Mockito.doReturn(false).when(fs).supportsSymlinks();
      Assert.assertFalse(fs.supportsSymlinks());
      Assert.assertFalse(FileSystem.areSymlinksEnabled());
      Assert.assertFalse(fs.hasPathCapability(path, FS_SYMLINKS));

      // Symlinks support enabled
      Mockito.doReturn(true).when(fs).supportsSymlinks();
      Assert.assertTrue(fs.supportsSymlinks());
      Assert.assertFalse(FileSystem.areSymlinksEnabled());
      Assert.assertFalse(fs.hasPathCapability(path, FS_SYMLINKS));
    }

    // Once it is enabled, it cannot be disabled.
    FileSystem.enableSymlinks();

    for (FileSystem fs : fileSystems) {
      LOG.info("fs is {}", fs.getClass().getSimpleName());
      // Symlinks enabled
      Mockito.doReturn(true).when(fs).supportsSymlinks();
      Assert.assertTrue(fs.supportsSymlinks());
      Assert.assertTrue(FileSystem.areSymlinksEnabled());
      Assert.assertTrue(fs.hasPathCapability(path, FS_SYMLINKS));

      // Symlinks enabled but symlink-support is disabled
      Mockito.doReturn(false).when(fs).supportsSymlinks();
      Assert.assertFalse(fs.supportsSymlinks());
      Assert.assertTrue(FileSystem.areSymlinksEnabled());
      Assert.assertFalse(fs.hasPathCapability(path, FS_SYMLINKS));
    }
  }
}
