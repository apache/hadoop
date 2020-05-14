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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;

/**
 * Test the TestViewFSOverloadSchemeCentralMountTableConfig with mount-table
 * configuration files in configured fs location.
 */
public class TestViewFSOverloadSchemeCentralMountTableConfig
    extends TestViewFileSystemOverloadSchemeLocalFileSystem {
  private Path oldMountTablePath;
  private Path latestMountTablepath;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    // Mount table name format: mount-table.<versionNumber>.xml
    String mountTableFileName1 = "mount-table.1.xml";
    String mountTableFileName2 = "mount-table.2.xml";
    oldMountTablePath =
        new Path(getTestRoot() + File.separator + mountTableFileName1);
    latestMountTablepath =
        new Path(getTestRoot() + File.separator + mountTableFileName2);
    getConf().set(Constants.CONFIG_VIEWFS_MOUNTTABLE_PATH,
        getTestRoot().toString());
    File f = new File(oldMountTablePath.toUri());
    f.createNewFile(); // Just creating empty mount-table file.
    File f2 = new File(latestMountTablepath.toUri());
    latestMountTablepath = new Path(f2.toURI());
    f2.createNewFile();
  }

  /**
   * This method saves the mount links in a local files.
   */
  @Override
  void addMountLinks(String mountTable, String[] sources, String[] targets,
      Configuration conf) throws IOException, URISyntaxException {
    // we don't use conf here, instead we use config paths to store links.
    // Mount-table old version file mount-table-<version>.xml
    try (BufferedWriter out = new BufferedWriter(
        new FileWriter(new File(oldMountTablePath.toUri())))) {
      out.write("<configuration>\n");
      // Invalid tag. This file should not be read.
      out.write("</\\\name//\\>");
      out.write("</configuration>\n");
      out.flush();
    }
    ViewFsTestSetup.addMountLinksToFile(mountTable, sources, targets,
        latestMountTablepath, conf);
  }
}
