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

import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getFinalizedEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getImageFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getInProgressEditsFileName;
import static org.apache.hadoop.test.GenericTestUtils.assertGlobEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.junit.Test;

import com.google.common.base.Joiner;


/**
 * Functional tests for NNStorageRetentionManager. This differs from
 * {@link TestNNStorageRetentionManager} in that the other test suite
 * is only unit/mock-based tests whereas this suite starts miniclusters,
 * etc.
 */
public class TestNNStorageRetentionFunctional {

  private static final File TEST_ROOT_DIR =
    new File(MiniDFSCluster.getBaseDirectory());
  private static final Log LOG = LogFactory.getLog(
      TestNNStorageRetentionFunctional.class);

 /**
  * Test case where two directories are configured as NAME_AND_EDITS
  * and one of them fails to save storage. Since the edits and image
  * failure states are decoupled, the failure of image saving should
  * not prevent the purging of logs from that dir.
  */
  @Test
  public void testPurgingWithNameEditsDirAfterFailure()
      throws Exception {
    MiniDFSCluster cluster = null;    
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY, 0);

    File sd0 = new File(TEST_ROOT_DIR, "nn0");
    File sd1 = new File(TEST_ROOT_DIR, "nn1");
    File cd0 = new File(sd0, "current");
    File cd1 = new File(sd1, "current");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        Joiner.on(",").join(sd0, sd1));

    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .manageNameDfsDirs(false)
        .format(true).build();
  
      NameNode nn = cluster.getNameNode();

      doSaveNamespace(nn);
      LOG.info("After first save, images 0 and 2 should exist in both dirs");
      assertGlobEquals(cd0, "fsimage_\\d*", 
          getImageFileName(0), getImageFileName(2));
      assertGlobEquals(cd1, "fsimage_\\d*",
          getImageFileName(0), getImageFileName(2));
      assertGlobEquals(cd0, "edits_.*",
          getFinalizedEditsFileName(1, 2),
          getInProgressEditsFileName(3));
      assertGlobEquals(cd1, "edits_.*",
          getFinalizedEditsFileName(1, 2),
          getInProgressEditsFileName(3));
      
      doSaveNamespace(nn);
      LOG.info("After second save, image 0 should be purged, " +
          "and image 4 should exist in both.");
      assertGlobEquals(cd0, "fsimage_\\d*",
          getImageFileName(2), getImageFileName(4));
      assertGlobEquals(cd1, "fsimage_\\d*",
          getImageFileName(2), getImageFileName(4));
      assertGlobEquals(cd0, "edits_.*",
          getFinalizedEditsFileName(3, 4),
          getInProgressEditsFileName(5));
      assertGlobEquals(cd1, "edits_.*",
          getFinalizedEditsFileName(3, 4),
          getInProgressEditsFileName(5));
      
      LOG.info("Failing first storage dir by chmodding it");
      assertEquals(0, FileUtil.chmod(cd0.getAbsolutePath(), "000"));
      doSaveNamespace(nn);      
      LOG.info("Restoring accessibility of first storage dir");      
      assertEquals(0, FileUtil.chmod(cd0.getAbsolutePath(), "755"));

      LOG.info("nothing should have been purged in first storage dir");
      assertGlobEquals(cd0, "fsimage_\\d*",
          getImageFileName(2), getImageFileName(4));
      assertGlobEquals(cd0, "edits_.*",
          getFinalizedEditsFileName(3, 4),
          getInProgressEditsFileName(5));

      LOG.info("fsimage_2 should be purged in second storage dir");
      assertGlobEquals(cd1, "fsimage_\\d*",
          getImageFileName(4), getImageFileName(6));
      assertGlobEquals(cd1, "edits_.*",
          getFinalizedEditsFileName(5, 6),
          getInProgressEditsFileName(7));

      LOG.info("On next save, we should purge logs from the failed dir," +
          " but not images, since the image directory is in failed state.");
      doSaveNamespace(nn);
      assertGlobEquals(cd1, "fsimage_\\d*",
          getImageFileName(6), getImageFileName(8));
      assertGlobEquals(cd1, "edits_.*",
          getFinalizedEditsFileName(7, 8),
          getInProgressEditsFileName(9));
      assertGlobEquals(cd0, "fsimage_\\d*",
          getImageFileName(2), getImageFileName(4));
      assertGlobEquals(cd0, "edits_.*",
          getInProgressEditsFileName(9));
    } finally {
      FileUtil.chmod(cd0.getAbsolutePath(), "755");

      LOG.info("Shutting down...");
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static void doSaveNamespace(NameNode nn) throws IOException {
    LOG.info("Saving namespace...");
    nn.getRpcServer().setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
    nn.getRpcServer().saveNamespace(0, 0);
    nn.getRpcServer().setSafeMode(SafeModeAction.SAFEMODE_LEAVE, false);
  }
}
