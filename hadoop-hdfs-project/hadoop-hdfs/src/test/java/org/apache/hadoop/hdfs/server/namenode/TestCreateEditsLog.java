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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.fail;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;

import java.io.File;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Util;

/**
 * Tests the CreateEditsLog utility.
 */
public class TestCreateEditsLog {

  private static final File HDFS_DIR = new File(
    MiniDFSCluster.getBaseDirectory()).getAbsoluteFile();
  private static final File TEST_DIR =
      GenericTestUtils.getTestDir("TestCreateEditsLog");

  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    deleteIfExists(HDFS_DIR);
    deleteIfExists(TEST_DIR);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    deleteIfExists(HDFS_DIR);
    deleteIfExists(TEST_DIR);
  }

  /**
   * Tests that an edits log created using CreateEditsLog is valid and can be
   * loaded successfully by a namenode.
   */
  @Test(timeout=60000)
  public void testCanLoadCreatedEditsLog() throws Exception {
    // Format namenode.
    HdfsConfiguration conf = new HdfsConfiguration();
    File nameDir = new File(HDFS_DIR, "name");
    conf.set(DFS_NAMENODE_NAME_DIR_KEY, Util.fileAsURI(nameDir).toString());
    DFSTestUtil.formatNameNode(conf);

    // Call CreateEditsLog and move the resulting edits to the name dir.
    CreateEditsLog.main(new String[] { "-f", "1000", "0", "1", "-d",
      TEST_DIR.getAbsolutePath() });
    Path editsWildcard = new Path(TEST_DIR.getAbsolutePath(), "*");
    FileContext localFc = FileContext.getLocalFSFileContext();
    for (FileStatus edits: localFc.util().globStatus(editsWildcard)) {
      Path src = edits.getPath();
      Path dst = new Path(new File(nameDir, "current").getAbsolutePath(),
        src.getName());
      localFc.rename(src, dst);
    }

    // Start a namenode to try to load the edits.
    cluster = new MiniDFSCluster.Builder(conf)
      .format(false)
      .manageNameDfsDirs(false)
      .waitSafeMode(false)
      .build();
    cluster.waitClusterUp();

    // Test successful, because no exception thrown.
  }

  /**
   * Fully delete the given directory if it exists.
   * 
   * @param file File to delete
   */
  private static void deleteIfExists(File file) {
    if (file.exists() && !FileUtil.fullyDelete(file)) {
      fail("Could not delete  '" + file + "'");
    }
  }
}
