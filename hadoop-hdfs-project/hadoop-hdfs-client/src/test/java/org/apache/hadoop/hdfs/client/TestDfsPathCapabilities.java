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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.fs.CommonPathCapabilities.FS_SYMLINKS;

public class TestDfsPathCapabilities {
  /**
   * Test if {@link DistributedFileSystem#hasPathCapability(Path, String)}
   * returns correct result for FS_SYMLINKS.
   */
  @Test
  public void testFS_SYMLINKS() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost/");
    final FileSystem fs = FileSystem.get(conf);
    Assert.assertTrue(fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = Mockito.spy((DistributedFileSystem) fs);
    final Path path = new Path("/");

    // Symlinks support disabled
    Mockito.doReturn(false).when(dfs).supportsSymlinks();
    Assert.assertFalse(FileSystem.areSymlinksEnabled());
    Assert.assertFalse(dfs.hasPathCapability(path, FS_SYMLINKS));

    // Symlinks support enabled
    Mockito.doReturn(true).when(dfs).supportsSymlinks();
    Assert.assertFalse(FileSystem.areSymlinksEnabled());
    Assert.assertFalse(dfs.hasPathCapability(path, FS_SYMLINKS));

    // Symlinks enabled
    FileSystem.enableSymlinks();
    Mockito.doReturn(true).when(dfs).supportsSymlinks();
    Assert.assertTrue(FileSystem.areSymlinksEnabled());
    Assert.assertTrue(dfs.hasPathCapability(path, FS_SYMLINKS));

    // Symlinks enabled but symlink-support is disabled
    FileSystem.enableSymlinks();
    Mockito.doReturn(false).when(dfs).supportsSymlinks();
    Assert.assertTrue(FileSystem.areSymlinksEnabled());
    Assert.assertTrue(dfs.hasPathCapability(path, FS_SYMLINKS));
  }
}
