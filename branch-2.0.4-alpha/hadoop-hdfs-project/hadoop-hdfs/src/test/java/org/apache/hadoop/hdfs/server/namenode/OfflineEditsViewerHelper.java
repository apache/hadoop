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
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClientAdapter;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * OfflineEditsViewerHelper is a helper class for TestOfflineEditsViewer,
 * it performs NN operations that generate all op codes
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OfflineEditsViewerHelper {

  private static final Log LOG = 
    LogFactory.getLog(OfflineEditsViewerHelper.class);

    long           blockSize = 512;
    MiniDFSCluster cluster   = null;
    Configuration  config    = new Configuration();

  /**
   * Generates edits with all op codes and returns the edits filename
   *
   * @param dfsDir DFS directory (where to setup MiniDFS cluster)
   * @param editsFilename where to copy the edits
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
    cluster =
      new MiniDFSCluster.Builder(config).manageNameDfsDirs(false).build();
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
    DistributedFileSystem dfs =
      (DistributedFileSystem)cluster.getFileSystem();
    FileContext fc = FileContext.getFileContext(cluster.getURI(0), config);
    // OP_ADD 0, OP_SET_GENSTAMP 10
    Path pathFileCreate = new Path("/file_create_u\1F431");
    FSDataOutputStream s = dfs.create(pathFileCreate);
    // OP_CLOSE 9
    s.close();
    // OP_RENAME_OLD 1
    Path pathFileMoved = new Path("/file_moved");
    dfs.rename(pathFileCreate, pathFileMoved);
    // OP_DELETE 2
    dfs.delete(pathFileMoved, false);
    // OP_MKDIR 3
    Path pathDirectoryMkdir = new Path("/directory_mkdir");
    dfs.mkdirs(pathDirectoryMkdir);
    // OP_SET_REPLICATION 4
    s = dfs.create(pathFileCreate);
    s.close();
    dfs.setReplication(pathFileCreate, (short)1);
    // OP_SET_PERMISSIONS 7
    Short permission = 0777;
    dfs.setPermission(pathFileCreate, new FsPermission(permission));
    // OP_SET_OWNER 8
    dfs.setOwner(pathFileCreate, new String("newOwner"), null);
    // OP_CLOSE 9 see above
    // OP_SET_GENSTAMP 10 see above
    // OP_SET_NS_QUOTA 11 obsolete
    // OP_CLEAR_NS_QUOTA 12 obsolete
    // OP_TIMES 13
    long mtime = 1285195527000L; // Wed, 22 Sep 2010 22:45:27 GMT
    long atime = mtime;
    dfs.setTimes(pathFileCreate, mtime, atime);
    // OP_SET_QUOTA 14
    dfs.setQuota(pathDirectoryMkdir, 1000L, HdfsConstants.QUOTA_DONT_SET);
    // OP_RENAME 15
    fc.rename(pathFileCreate, pathFileMoved, Rename.NONE);
    // OP_CONCAT_DELETE 16
    Path   pathConcatTarget = new Path("/file_concat_target");
    Path[] pathConcatFiles  = new Path[2];
    pathConcatFiles[0]      = new Path("/file_concat_0");
    pathConcatFiles[1]      = new Path("/file_concat_1");

    long  length      = blockSize * 3; // multiple of blocksize for concat
    short replication = 1;
    long  seed        = 1;

    DFSTestUtil.createFile(dfs, pathConcatTarget, length, replication, seed);
    DFSTestUtil.createFile(dfs, pathConcatFiles[0], length, replication, seed);
    DFSTestUtil.createFile(dfs, pathConcatFiles[1], length, replication, seed);
    dfs.concat(pathConcatTarget, pathConcatFiles);
    // OP_SYMLINK 17
    Path pathSymlink = new Path("/file_symlink");
    fc.createSymlink(pathConcatTarget, pathSymlink, false);
    // OP_GET_DELEGATION_TOKEN 18
    // OP_RENEW_DELEGATION_TOKEN 19
    // OP_CANCEL_DELEGATION_TOKEN 20
    // see TestDelegationToken.java
    // fake the user to renew token for
    final Token<?>[] tokens = dfs.addDelegationTokens("JobTracker", null);
    UserGroupInformation longUgi = UserGroupInformation.createRemoteUser(
      "JobTracker/foo.com@FOO.COM");
    try {
      longUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException, InterruptedException {
          for (Token<?> token : tokens) {
            token.renew(config);
            token.cancel(config);
          }
          return null;
        }
      });
    } catch(InterruptedException e) {
      throw new IOException(
        "renewDelegationToken threw InterruptedException", e);
    }
    // OP_UPDATE_MASTER_KEY 21
    //   done by getDelegationTokenSecretManager().startThreads();

    // sync to disk, otherwise we parse partial edits
    cluster.getNameNode().getFSImage().getEditLog().logSync();
    
    // OP_REASSIGN_LEASE 22
    String filePath = "/hard-lease-recovery-test";
    byte[] bytes = "foo-bar-baz".getBytes();
    DFSClientAdapter.stopLeaseRenewer(dfs);
    FSDataOutputStream leaseRecoveryPath = dfs.create(new Path(filePath));
    leaseRecoveryPath.write(bytes);
    leaseRecoveryPath.hflush();
    // Set the hard lease timeout to 1 second.
    cluster.setLeasePeriod(60 * 1000, 1000);
    // wait for lease recovery to complete
    LocatedBlocks locatedBlocks;
    do {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.info("Innocuous exception", e);
      }
      locatedBlocks = DFSClientAdapter.callGetBlockLocations(
          cluster.getNameNodeRpc(), filePath, 0L, bytes.length);
    } while (locatedBlocks.isUnderConstruction());

    // Force a roll so we get an OP_END_LOG_SEGMENT txn
    return cluster.getNameNodeRpc().rollEditLog();
  }
}
