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

import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.SafeModeInfo;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * This is a utility class to expose NameNode functionality for unit tests.
 */
public class NameNodeAdapter {
  /**
   * Get the namesystem from the namenode
   */
  public static FSNamesystem getNamesystem(NameNode namenode) {
    return namenode.getNamesystem();
  }

  /**
   * Get block locations within the specified range.
   */
  public static LocatedBlocks getBlockLocations(NameNode namenode,
      String src, long offset, long length) throws IOException {
    return namenode.getNamesystem().getBlockLocations("foo",
        src, offset, length);
  }
  
  public static HdfsFileStatus getFileInfo(NameNode namenode, String src,
      boolean resolveLink) throws AccessControlException, UnresolvedLinkException,
        StandbyException, IOException {
    return FSDirStatAndListingOp.getFileInfo(namenode.getNamesystem()
            .getFSDirectory(), src, resolveLink);
  }
  
  public static boolean mkdirs(NameNode namenode, String src,
      PermissionStatus permissions, boolean createParent)
      throws UnresolvedLinkException, IOException {
    return namenode.getNamesystem().mkdirs(src, permissions, createParent);
  }
  
  public static void saveNamespace(NameNode namenode)
      throws AccessControlException, IOException {
    namenode.getNamesystem().saveNamespace();
  }
  
  public static void enterSafeMode(NameNode namenode, boolean resourcesLow)
      throws IOException {
    namenode.getNamesystem().enterSafeMode(resourcesLow);
  }
  
  public static void leaveSafeMode(NameNode namenode) {
    namenode.getNamesystem().leaveSafeMode();
  }
  
  public static void abortEditLogs(NameNode nn) {
    FSEditLog el = nn.getFSImage().getEditLog();
    el.abortCurrentLogSegment();
  }
  
  /**
   * Get the internal RPC server instance.
   * @return rpc server
   */
  public static Server getRpcServer(NameNode namenode) {
    return ((NameNodeRpcServer)namenode.getRpcServer()).clientRpcServer;
  }

  public static DelegationTokenSecretManager getDtSecretManager(
      final FSNamesystem ns) {
    return ns.getDelegationTokenSecretManager();
  }

  public static HeartbeatResponse sendHeartBeat(DatanodeRegistration nodeReg,
      DatanodeDescriptor dd, FSNamesystem namesystem) throws IOException {
    return namesystem.handleHeartbeat(nodeReg,
        BlockManagerTestUtil.getStorageReportsForDatanode(dd),
        dd.getCacheCapacity(), dd.getCacheRemaining(), 0, 0, 0, null);
  }

  public static boolean setReplication(final FSNamesystem ns,
      final String src, final short replication) throws IOException {
    return ns.setReplication(src, replication);
  }
  
  public static LeaseManager getLeaseManager(final FSNamesystem ns) {
    return ns.leaseManager;
  }

  /** Set the softLimit and hardLimit of client lease periods. */
  public static void setLeasePeriod(final FSNamesystem namesystem, long soft, long hard) {
    getLeaseManager(namesystem).setLeasePeriod(soft, hard);
    namesystem.leaseManager.triggerMonitorCheckNow();
  }

  public static String getLeaseHolderForPath(NameNode namenode, String path) {
    Lease l = namenode.getNamesystem().leaseManager.getLeaseByPath(path);
    return l == null? null: l.getHolder();
  }

  /**
   * @return the timestamp of the last renewal of the given lease,
   *   or -1 in the case that the lease doesn't exist.
   */
  public static long getLeaseRenewalTime(NameNode nn, String path) {
    LeaseManager lm = nn.getNamesystem().leaseManager;
    Lease l = lm.getLeaseByPath(path);
    if (l == null) {
      return -1;
    }
    return l.getLastUpdate();
  }

  /**
   * Return the datanode descriptor for the given datanode.
   */
  public static DatanodeDescriptor getDatanode(final FSNamesystem ns,
      DatanodeID id) throws IOException {
    ns.readLock();
    try {
      return ns.getBlockManager().getDatanodeManager().getDatanode(id);
    } finally {
      ns.readUnlock();
    }
  }
  
  /**
   * Return the FSNamesystem stats
   */
  public static long[] getStats(final FSNamesystem fsn) {
    return fsn.getStats();
  }
  
  public static ReentrantReadWriteLock spyOnFsLock(FSNamesystem fsn) {
    ReentrantReadWriteLock spy = Mockito.spy(fsn.getFsLockForTests());
    fsn.setFsLockForTests(spy);
    return spy;
  }

  public static FSImage spyOnFsImage(NameNode nn1) {
    FSNamesystem fsn = nn1.getNamesystem();
    FSImage spy = Mockito.spy(fsn.getFSImage());
    Whitebox.setInternalState(fsn, "fsImage", spy);
    return spy;
  }
  
  public static FSEditLog spyOnEditLog(NameNode nn) {
    FSEditLog spyEditLog = spy(nn.getNamesystem().getFSImage().getEditLog());
    DFSTestUtil.setEditLogForTesting(nn.getNamesystem(), spyEditLog);
    EditLogTailer tailer = nn.getNamesystem().getEditLogTailer();
    if (tailer != null) {
      tailer.setEditLog(spyEditLog);
    }
    return spyEditLog;
  }
  
  public static JournalSet spyOnJournalSet(NameNode nn) {
    FSEditLog editLog = nn.getFSImage().getEditLog();
    JournalSet js = Mockito.spy(editLog.getJournalSet());
    editLog.setJournalSetForTesting(js);
    return js;
  }
  
  public static String getMkdirOpPath(FSEditLogOp op) {
    if (op.opCode == FSEditLogOpCodes.OP_MKDIR) {
      return ((MkdirOp) op).path;
    } else {
      return null;
    }
  }
  
  public static FSEditLogOp createMkdirOp(String path) {
    MkdirOp op = MkdirOp.getInstance(new FSEditLogOp.OpInstanceCache())
      .setPath(path)
      .setTimestamp(0)
      .setPermissionStatus(new PermissionStatus(
              "testuser", "testgroup", FsPermission.getDefault()));
    return op;
  }
  
  /**
   * @return the number of blocks marked safe by safemode, or -1
   * if safemode is not running.
   */
  public static int getSafeModeSafeBlocks(NameNode nn) {
    SafeModeInfo smi = nn.getNamesystem().getSafeModeInfoForTests();
    if (smi == null) {
      return -1;
    }
    return smi.blockSafe;
  }
  
  /**
   * @return Replication queue initialization status
   */
  public static boolean safeModeInitializedReplQueues(NameNode nn) {
    return nn.getNamesystem().isPopulatingReplQueues();
  }
  
  public static File getInProgressEditsFile(StorageDirectory sd, long startTxId) {
    return NNStorage.getInProgressEditsFile(sd, startTxId);
  }

  public static NamenodeCommand startCheckpoint(NameNode nn,
      NamenodeRegistration backupNode, NamenodeRegistration activeNamenode)
          throws IOException {
    return nn.getNamesystem().startCheckpoint(backupNode, activeNamenode);
  }
}

