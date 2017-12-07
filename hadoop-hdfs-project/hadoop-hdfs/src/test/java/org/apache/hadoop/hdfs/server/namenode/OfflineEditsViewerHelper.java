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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.util.Time;

/**
 * OfflineEditsViewerHelper is a helper class for TestOfflineEditsViewer,
 * it performs NN operations that generate all op codes
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OfflineEditsViewerHelper {

  private static final Log LOG = 
    LogFactory.getLog(OfflineEditsViewerHelper.class);

    final long           blockSize = 512;
    MiniDFSCluster cluster   = null;
    final Configuration  config    = new Configuration();

  /**
   * Generates edits with all op codes and returns the edits filename
   */
  public String generateEdits() throws IOException {
    CheckpointSignature signature = runOperations();
    return getEditsFilename(signature);
  }

  /**
   * Get edits filename
   *
   * @return edits file name for cluster
   */
  private String getEditsFilename(CheckpointSignature sig) throws IOException {
    FSImage image = cluster.getNameNode().getFSImage();
    // it was set up to only have ONE StorageDirectory
    Iterator<StorageDirectory> it
      = image.getStorage().dirIterator(NameNodeDirType.EDITS);
    StorageDirectory sd = it.next();
    File ret = NNStorage.getFinalizedEditsFile(
        sd, 1, sig.curSegmentTxId - 1);
    assert ret.exists() : "expected " + ret + " exists";
    return ret.getAbsolutePath();
  }

  /**
   * Sets up a MiniDFSCluster, configures it to create one edits file,
   * starts DelegationTokenSecretManager (to get security op codes)
   *
   * @param dfsDir DFS directory (where to setup MiniDFS cluster)
   */
  public void startCluster(String dfsDir) throws IOException {

    // same as manageDfsDirs but only one edits file instead of two
    config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
      Util.fileAsURI(new File(dfsDir, "name")).toString());
    config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY,
      Util.fileAsURI(new File(dfsDir, "namesecondary1")).toString());
    // blocksize for concat (file size must be multiple of blocksize)
    config.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    // for security to work (fake JobTracker user)
    config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL,
      "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//" + "DEFAULT");
    config.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    config.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    final int numDataNodes = 9;
    cluster =
      new MiniDFSCluster.Builder(config).manageNameDfsDirs(false)
          .numDataNodes(numDataNodes).build();
    cluster.waitClusterUp();
  }

  /**
   * Shutdown the cluster
   */
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Run file operations to create edits for all op codes
   * to be tested.
   *
   * the following op codes are deprecated and therefore not tested:
   *
   * OP_DATANODE_ADD    ( 5)
   * OP_DATANODE_REMOVE ( 6)
   * OP_SET_NS_QUOTA    (11)
   * OP_CLEAR_NS_QUOTA  (12)
   */
  private CheckpointSignature runOperations() throws IOException {
    LOG.info("Creating edits by performing fs operations");
    // no check, if it's not it throws an exception which is what we want
    DistributedFileSystem dfs = cluster.getFileSystem();
    DFSTestUtil.runOperations(cluster, dfs, cluster.getConfiguration(0),
        dfs.getDefaultBlockSize(), 0);

    // OP_ROLLING_UPGRADE_START
    cluster.getNamesystem().getEditLog().logStartRollingUpgrade(Time.now());
    // OP_ROLLING_UPGRADE_FINALIZE
    cluster.getNamesystem().getEditLog().logFinalizeRollingUpgrade(Time.now());

    // Force a roll so we get an OP_END_LOG_SEGMENT txn
    return cluster.getNameNodeRpc().rollEditLog();
  }
}
