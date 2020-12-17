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
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;

/**
 * Tests ViewFileSystemOverloadScheme with configured mount links.
 */
public class TestViewFSOverloadSchemeWithMountTableConfigInHDFS
    extends TestViewFileSystemOverloadSchemeWithHdfsScheme {
  private Path oldVersionMountTablePath;
  private Path newVersionMountTablePath;

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
    String mountTableDir =
        URI.create(getConf().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY))
            .toString() + "/MountTable/";
    getConf().set(Constants.CONFIG_VIEWFS_MOUNTTABLE_PATH, mountTableDir);
    FileSystem fs = new ViewFileSystemOverloadScheme.ChildFsGetter("hdfs")
        .getNewInstance(new Path(mountTableDir).toUri(), getConf());
    fs.mkdirs(new Path(mountTableDir));
    String oldVersionMountTable = "mount-table.30.xml";
    String newVersionMountTable = "mount-table.31.xml";
    oldVersionMountTablePath = new Path(mountTableDir, oldVersionMountTable);
    newVersionMountTablePath = new Path(mountTableDir, newVersionMountTable);
    fs.createNewFile(oldVersionMountTablePath);
    fs.createNewFile(newVersionMountTablePath);
  }

  /**
   * This method saves the mount links in a hdfs file newVersionMountTable.
   * Since this file has highest version, this should be loaded by
   * ViewFSOverloadScheme.
   */
  @Override
  void addMountLinks(String mountTable, String[] sources, String[] targets,
      Configuration config) throws IOException, URISyntaxException {
    ViewFsTestSetup.addMountLinksToFile(mountTable, sources, targets,
        newVersionMountTablePath, getConf());
  }
}