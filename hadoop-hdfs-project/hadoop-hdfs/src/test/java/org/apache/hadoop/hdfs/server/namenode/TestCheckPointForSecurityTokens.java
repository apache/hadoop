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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Test;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestCheckPointForSecurityTokens {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  static final int numDatanodes = 3;
  short replication = 3;
  MiniDFSCluster cluster = null;

  private void cancelToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    cluster.getNamesystem().cancelDelegationToken(token);
  }
  
  private void renewToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
      cluster.getNamesystem().renewDelegationToken(token);
  }
  
  /**
   * Tests save namespace.
   */
  @Test
  public void testSaveNamespace() throws IOException {
    DistributedFileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      FSNamesystem namesystem = cluster.getNamesystem();
      String renewer = UserGroupInformation.getLoginUser().getUserName();
      Token<DelegationTokenIdentifier> token1 = namesystem
          .getDelegationToken(new Text(renewer)); 
      Token<DelegationTokenIdentifier> token2 = namesystem
          .getDelegationToken(new Text(renewer));
      
      // Saving image without safe mode should fail
      DFSAdmin admin = new DFSAdmin(conf);
      String[] args = new String[]{"-saveNamespace"};

      // verify that the edits file is NOT empty
      NameNode nn = cluster.getNameNode();
      for (StorageDirectory sd : nn.getFSImage().getStorage().dirIterable(null)) {
        EditLogFile log = FSImageTestUtil.findLatestEditsLog(sd);
        assertTrue(log.isInProgress());
        log.scanLog(Long.MAX_VALUE, true);
        long numTransactions = (log.getLastTxId() - log.getFirstTxId()) + 1;
        assertEquals("In-progress log " + log + " should have 5 transactions",
                     5, numTransactions);
      }

      // Saving image in safe mode should succeed
      fs.setSafeMode(SafeModeAction.ENTER);
      try {
        admin.run(args);
      } catch(Exception e) {
        throw new IOException(e.getMessage());
      }
      // verify that the edits file is empty except for the START txn
      for (StorageDirectory sd : nn.getFSImage().getStorage().dirIterable(null)) {
        EditLogFile log = FSImageTestUtil.findLatestEditsLog(sd);
        assertTrue(log.isInProgress());
        log.scanLog(Long.MAX_VALUE, true);
        long numTransactions = (log.getLastTxId() - log.getFirstTxId()) + 1;
        assertEquals("In-progress log " + log + " should only have START txn",
            1, numTransactions);
      }

      // restart cluster
      cluster.shutdown();
      cluster = null;

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(false).build();
      cluster.waitActive();
      //Should be able to renew & cancel the delegation token after cluster restart
      try {
        renewToken(token1);
        renewToken(token2);
      } catch (IOException e) {
        fail("Could not renew or cancel the token");
      }

      namesystem = cluster.getNamesystem();
      Token<DelegationTokenIdentifier> token3 = namesystem
          .getDelegationToken(new Text(renewer));
      Token<DelegationTokenIdentifier> token4 = namesystem
          .getDelegationToken(new Text(renewer));

      // restart cluster again
      cluster.shutdown();
      cluster = null;

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(false).build();
      cluster.waitActive();

      namesystem = cluster.getNamesystem();
      Token<DelegationTokenIdentifier> token5 = namesystem
          .getDelegationToken(new Text(renewer));

      try {
        renewToken(token1);
        renewToken(token2);
        renewToken(token3);
        renewToken(token4);
        renewToken(token5);

      } catch (IOException e) {
        fail("Could not renew or cancel the token");
      }

      // restart cluster again
      cluster.shutdown();
      cluster = null;

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(false).build();
      cluster.waitActive();

      namesystem = cluster.getNamesystem();
      try {
        renewToken(token1);
        cancelToken(token1);
        renewToken(token2);
        cancelToken(token2);
        renewToken(token3);
        cancelToken(token3);
        renewToken(token4);
        cancelToken(token4);
        renewToken(token5);
        cancelToken(token5);
      } catch (IOException e) {
        fail("Could not renew or cancel the token");
      }

    } finally {
      if(fs != null) fs.close();
      if(cluster!= null) cluster.shutdown();
    }
  }
}
