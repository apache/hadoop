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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCloseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CancelDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ClearNSQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ConcatDeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.GetDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ReassignLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOldOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenewDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetNSQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetOwnerOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetPermissionsOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetReplicationOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SymlinkOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TimesOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateMasterKeyOp;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;

public class FSEditLogLoader {
  private final FSNamesystem fsNamesys;

  public FSEditLogLoader(FSNamesystem fsNamesys) {
    this.fsNamesys = fsNamesys;
  }
  
  /**
   * Load an edit log, and apply the changes to the in-memory structure
   * This is where we apply edits that we've been writing to disk all
   * along.
   */
  int loadFSEdits(EditLogInputStream edits) throws IOException {
    long startTime = now();
    int numEdits = loadFSEdits(edits, true);
    FSImage.LOG.info("Edits file " + edits.getName() 
        + " of size " + edits.length() + " edits # " + numEdits 
        + " loaded in " + (now()-startTime)/1000 + " seconds.");
    return numEdits;
  }

  /**
   * Read the header of fsedit log
   * @param in fsedit stream
   * @return the edit log version number
   * @throws IOException if error occurs
   */
  int readLogVersion(DataInputStream in) throws IOException {
    int logVersion = 0;
    // Read log file version. Could be missing.
    in.mark(4);
    // If edits log is greater than 2G, available method will return negative
    // numbers, so we avoid having to call available
    boolean available = true;
    try {
      logVersion = in.readByte();
    } catch (EOFException e) {
      available = false;
    }
    if (available) {
      in.reset();
      logVersion = in.readInt();
      if (logVersion < FSConstants.LAYOUT_VERSION) // future version
        throw new IOException(
            "Unexpected version of the file system log file: "
            + logVersion + ". Current version = "
            + FSConstants.LAYOUT_VERSION + ".");
    }
    assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
      "Unsupported version " + logVersion;
    return logVersion;
  }
  
  int loadFSEdits(EditLogInputStream edits, boolean closeOnExit) throws IOException {
    BufferedInputStream bin = new BufferedInputStream(edits);
    DataInputStream in = new DataInputStream(bin);

    int numEdits = 0;
    int logVersion = 0;

    try {
      logVersion = readLogVersion(in);
      Checksum checksum = null;
      if (LayoutVersion.supports(Feature.EDITS_CHESKUM, logVersion)) {
        checksum = FSEditLog.getChecksum();
        in = new DataInputStream(new CheckedInputStream(bin, checksum));
      }

      numEdits = loadEditRecords(logVersion, in, checksum, false);
    } finally {
      if(closeOnExit)
        in.close();
    }
    if (logVersion != FSConstants.LAYOUT_VERSION) // other version
      numEdits++; // save this image asap
    return numEdits;
  }

  @SuppressWarnings("deprecation")
  int loadEditRecords(int logVersion, DataInputStream in,
      Checksum checksum, boolean closeOnExit) throws IOException {
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;

    int numOpAdd = 0, numOpClose = 0, numOpDelete = 0,
        numOpRenameOld = 0, numOpSetRepl = 0, numOpMkDir = 0,
        numOpSetPerm = 0, numOpSetOwner = 0, numOpSetGenStamp = 0,
        numOpTimes = 0, numOpRename = 0, numOpConcatDelete = 0, 
        numOpSymlink = 0, numOpGetDelegationToken = 0,
        numOpRenewDelegationToken = 0, numOpCancelDelegationToken = 0, 
        numOpUpdateMasterKey = 0, numOpReassignLease = 0, numOpOther = 0;

    fsNamesys.writeLock();
    fsDir.writeLock();

    // Keep track of the file offsets of the last several opcodes.
    // This is handy when manually recovering corrupted edits files.
    PositionTrackingInputStream tracker = new PositionTrackingInputStream(in);
    in = new DataInputStream(tracker);
    long recentOpcodeOffsets[] = new long[4];
    Arrays.fill(recentOpcodeOffsets, -1);

    try {
      try {
        FSEditLogOp.Reader reader = new FSEditLogOp.Reader(in, logVersion,
                                                           checksum);
        FSEditLogOp op;
        while ((op = reader.readOp()) != null) {
          recentOpcodeOffsets[numEdits % recentOpcodeOffsets.length] =
              tracker.getPos();
          numEdits++;
          switch (op.opCode) {
          case OP_ADD:
          case OP_CLOSE: {
            AddCloseOp addCloseOp = (AddCloseOp)op;

            // versions > 0 support per file replication
            // get name and replication
            short replication
              = fsNamesys.adjustReplication(addCloseOp.replication);

            long blockSize = addCloseOp.blockSize;
            BlockInfo blocks[] = new BlockInfo[addCloseOp.blocks.length];
            for (int i = 0; i < addCloseOp.blocks.length; i++) {
              if(addCloseOp.opCode == FSEditLogOpCodes.OP_ADD
                 && i == addCloseOp.blocks.length-1) {
                blocks[i] = new BlockInfoUnderConstruction(addCloseOp.blocks[i],
                                                           replication);
              } else {
                blocks[i] = new BlockInfo(addCloseOp.blocks[i], replication);
              }
            }

            PermissionStatus permissions = fsNamesys.getUpgradePermission();
            if (addCloseOp.permissions != null) {
              permissions = addCloseOp.permissions;
            }


            // Older versions of HDFS does not store the block size in inode.
            // If the file has more than one block, use the size of the
            // first block as the blocksize. Otherwise use the default
            // block size.
            if (-8 <= logVersion && blockSize == 0) {
              if (blocks.length > 1) {
                blockSize = blocks[0].getNumBytes();
              } else {
                long first = ((blocks.length == 1)? blocks[0].getNumBytes(): 0);
                blockSize = Math.max(fsNamesys.getDefaultBlockSize(), first);
              }
            }


            // The open lease transaction re-creates a file if necessary.
            // Delete the file if it already exists.
            if (FSNamesystem.LOG.isDebugEnabled()) {
              FSNamesystem.LOG.debug(op.opCode + ": " + addCloseOp.path +
                  " numblocks : " + blocks.length +
                  " clientHolder " + addCloseOp.clientName +
                  " clientMachine " + addCloseOp.clientMachine);
            }

            fsDir.unprotectedDelete(addCloseOp.path, addCloseOp.mtime);

            // add to the file tree
            INodeFile node = (INodeFile)fsDir.unprotectedAddFile(
                addCloseOp.path, permissions,
                blocks, replication,
                addCloseOp.mtime, addCloseOp.atime, blockSize);
            if (addCloseOp.opCode == FSEditLogOpCodes.OP_ADD) {
              numOpAdd++;
              //
              // Replace current node with a INodeUnderConstruction.
              // Recreate in-memory lease record.
              //
              INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
                                        node.getLocalNameBytes(),
                                        node.getReplication(),
                                        node.getModificationTime(),
                                        node.getPreferredBlockSize(),
                                        node.getBlocks(),
                                        node.getPermissionStatus(),
                                        addCloseOp.clientName,
                                        addCloseOp.clientMachine,
                                        null);
              fsDir.replaceNode(addCloseOp.path, node, cons);
              fsNamesys.leaseManager.addLease(cons.getClientName(),
                                              addCloseOp.path);
            }
            break;
          }
          case OP_SET_REPLICATION: {
            numOpSetRepl++;
            SetReplicationOp setReplicationOp = (SetReplicationOp)op;
            short replication
              = fsNamesys.adjustReplication(setReplicationOp.replication);
            fsDir.unprotectedSetReplication(setReplicationOp.path,
                                            replication, null);
            break;
          }
          case OP_CONCAT_DELETE: {
            numOpConcatDelete++;

            ConcatDeleteOp concatDeleteOp = (ConcatDeleteOp)op;
            fsDir.unprotectedConcat(concatDeleteOp.trg, concatDeleteOp.srcs,
                concatDeleteOp.timestamp);
            break;
          }
          case OP_RENAME_OLD: {
            numOpRenameOld++;
            RenameOldOp renameOp = (RenameOldOp)op;
            HdfsFileStatus dinfo = fsDir.getFileInfo(renameOp.dst, false);
            fsDir.unprotectedRenameTo(renameOp.src, renameOp.dst,
                                      renameOp.timestamp);
            fsNamesys.unprotectedChangeLease(renameOp.src, renameOp.dst, dinfo);
            break;
          }
          case OP_DELETE: {
            numOpDelete++;

            DeleteOp deleteOp = (DeleteOp)op;
            fsDir.unprotectedDelete(deleteOp.path, deleteOp.timestamp);
            break;
          }
          case OP_MKDIR: {
            numOpMkDir++;
            MkdirOp mkdirOp = (MkdirOp)op;
            PermissionStatus permissions = fsNamesys.getUpgradePermission();
            if (mkdirOp.permissions != null) {
              permissions = mkdirOp.permissions;
            }

            fsDir.unprotectedMkdir(mkdirOp.path, permissions,
                                   mkdirOp.timestamp);
            break;
          }
          case OP_SET_GENSTAMP: {
            numOpSetGenStamp++;
            SetGenstampOp setGenstampOp = (SetGenstampOp)op;
            fsNamesys.setGenerationStamp(setGenstampOp.genStamp);
            break;
          }
          case OP_SET_PERMISSIONS: {
            numOpSetPerm++;

            SetPermissionsOp setPermissionsOp = (SetPermissionsOp)op;
            fsDir.unprotectedSetPermission(setPermissionsOp.src,
                                           setPermissionsOp.permissions);
            break;
          }
          case OP_SET_OWNER: {
            numOpSetOwner++;

            SetOwnerOp setOwnerOp = (SetOwnerOp)op;
            fsDir.unprotectedSetOwner(setOwnerOp.src, setOwnerOp.username,
                                      setOwnerOp.groupname);
            break;
          }
          case OP_SET_NS_QUOTA: {
            SetNSQuotaOp setNSQuotaOp = (SetNSQuotaOp)op;
            fsDir.unprotectedSetQuota(setNSQuotaOp.src,
                                      setNSQuotaOp.nsQuota,
                                      FSConstants.QUOTA_DONT_SET);
            break;
          }
          case OP_CLEAR_NS_QUOTA: {
            ClearNSQuotaOp clearNSQuotaOp = (ClearNSQuotaOp)op;
            fsDir.unprotectedSetQuota(clearNSQuotaOp.src,
                                      FSConstants.QUOTA_RESET,
                                      FSConstants.QUOTA_DONT_SET);
            break;
          }

          case OP_SET_QUOTA:
            SetQuotaOp setQuotaOp = (SetQuotaOp)op;
            fsDir.unprotectedSetQuota(setQuotaOp.src,
                                      setQuotaOp.nsQuota,
                                      setQuotaOp.dsQuota);
            break;

          case OP_TIMES: {
            numOpTimes++;
            TimesOp timesOp = (TimesOp)op;

            fsDir.unprotectedSetTimes(timesOp.path,
                                      timesOp.mtime,
                                      timesOp.atime, true);
            break;
          }
          case OP_SYMLINK: {
            numOpSymlink++;

            SymlinkOp symlinkOp = (SymlinkOp)op;
            fsDir.unprotectedSymlink(symlinkOp.path, symlinkOp.value,
                                     symlinkOp.mtime, symlinkOp.atime,
                                     symlinkOp.permissionStatus);
            break;
          }
          case OP_RENAME: {
            numOpRename++;
            RenameOp renameOp = (RenameOp)op;

            HdfsFileStatus dinfo = fsDir.getFileInfo(renameOp.dst, false);
            fsDir.unprotectedRenameTo(renameOp.src, renameOp.dst,
                                      renameOp.timestamp, renameOp.options);
            fsNamesys.unprotectedChangeLease(renameOp.src, renameOp.dst, dinfo);
            break;
          }
          case OP_GET_DELEGATION_TOKEN: {
            numOpGetDelegationToken++;
            GetDelegationTokenOp getDelegationTokenOp
              = (GetDelegationTokenOp)op;

            fsNamesys.getDelegationTokenSecretManager()
              .addPersistedDelegationToken(getDelegationTokenOp.token,
                                           getDelegationTokenOp.expiryTime);
            break;
          }
          case OP_RENEW_DELEGATION_TOKEN: {
            numOpRenewDelegationToken++;

            RenewDelegationTokenOp renewDelegationTokenOp
              = (RenewDelegationTokenOp)op;
            fsNamesys.getDelegationTokenSecretManager()
              .updatePersistedTokenRenewal(renewDelegationTokenOp.token,
                                           renewDelegationTokenOp.expiryTime);
            break;
          }
          case OP_CANCEL_DELEGATION_TOKEN: {
            numOpCancelDelegationToken++;

            CancelDelegationTokenOp cancelDelegationTokenOp
              = (CancelDelegationTokenOp)op;
            fsNamesys.getDelegationTokenSecretManager()
                .updatePersistedTokenCancellation(
                    cancelDelegationTokenOp.token);
            break;
          }
          case OP_UPDATE_MASTER_KEY: {
            numOpUpdateMasterKey++;
            UpdateMasterKeyOp updateMasterKeyOp = (UpdateMasterKeyOp)op;
            fsNamesys.getDelegationTokenSecretManager()
              .updatePersistedMasterKey(updateMasterKeyOp.key);
            break;
          }
          case OP_REASSIGN_LEASE: {
            numOpReassignLease++;
            ReassignLeaseOp reassignLeaseOp = (ReassignLeaseOp)op;

            Lease lease = fsNamesys.leaseManager.getLease(
                reassignLeaseOp.leaseHolder);
            INodeFileUnderConstruction pendingFile =
                (INodeFileUnderConstruction) fsDir.getFileINode(
                    reassignLeaseOp.path);
            fsNamesys.reassignLeaseInternal(lease,
                reassignLeaseOp.path, reassignLeaseOp.newHolder, pendingFile);
            break;
          }
          case OP_DATANODE_ADD:
          case OP_DATANODE_REMOVE:
            numOpOther++;
            break;
          default:
            throw new IOException("Invalid operation read " + op.opCode);
          }
        }
      } catch (IOException ex) {
        check203UpgradeFailure(logVersion, ex);
      } finally {
        if(closeOnExit)
          in.close();
      }
    } catch (Throwable t) {
      // Catch Throwable because in the case of a truly corrupt edits log, any
      // sort of error might be thrown (NumberFormat, NullPointer, EOF, etc.)
      StringBuilder sb = new StringBuilder();
      sb.append("Error replaying edit log at offset " + tracker.getPos());
      if (recentOpcodeOffsets[0] != -1) {
        Arrays.sort(recentOpcodeOffsets);
        sb.append("\nRecent opcode offsets:");
        for (long offset : recentOpcodeOffsets) {
          if (offset != -1) {
            sb.append(' ').append(offset);
          }
        }
      }
      String errorMessage = sb.toString();
      FSImage.LOG.error(errorMessage);
      throw new IOException(errorMessage, t);
    } finally {
      fsDir.writeUnlock();
      fsNamesys.writeUnlock();
    }
    if (FSImage.LOG.isDebugEnabled()) {
      FSImage.LOG.debug("numOpAdd = " + numOpAdd + " numOpClose = " + numOpClose 
          + " numOpDelete = " + numOpDelete 
          + " numOpRenameOld = " + numOpRenameOld 
          + " numOpSetRepl = " + numOpSetRepl + " numOpMkDir = " + numOpMkDir
          + " numOpSetPerm = " + numOpSetPerm 
          + " numOpSetOwner = " + numOpSetOwner
          + " numOpSetGenStamp = " + numOpSetGenStamp 
          + " numOpTimes = " + numOpTimes
          + " numOpConcatDelete  = " + numOpConcatDelete
          + " numOpRename = " + numOpRename
          + " numOpGetDelegationToken = " + numOpGetDelegationToken
          + " numOpRenewDelegationToken = " + numOpRenewDelegationToken
          + " numOpCancelDelegationToken = " + numOpCancelDelegationToken
          + " numOpUpdateMasterKey = " + numOpUpdateMasterKey
          + " numOpReassignLease = " + numOpReassignLease
          + " numOpOther = " + numOpOther);
    }
    return numEdits;
  }

  /**
   * Throw appropriate exception during upgrade from 203, when editlog loading
   * could fail due to opcode conflicts.
   */
  private void check203UpgradeFailure(int logVersion, IOException ex)
      throws IOException {
    // 0.20.203 version version has conflicting opcodes with the later releases.
    // The editlog must be emptied by restarting the namenode, before proceeding
    // with the upgrade.
    if (Storage.is203LayoutVersion(logVersion)
        && logVersion != FSConstants.LAYOUT_VERSION) {
      String msg = "During upgrade failed to load the editlog version "
          + logVersion + " from release 0.20.203. Please go back to the old "
          + " release and restart the namenode. This empties the editlog "
          + " and saves the namespace. Resume the upgrade after this step.";
      throw new IOException(msg, ex);
    } else {
      throw ex;
    }
  }
  
  /**
   * Stream wrapper that keeps track of the current file position.
   */
  private static class PositionTrackingInputStream extends FilterInputStream {
    private long curPos = 0;
    private long markPos = -1;

    public PositionTrackingInputStream(InputStream is) {
      super(is);
    }

    public int read() throws IOException {
      int ret = super.read();
      if (ret != -1) curPos++;
      return ret;
    }

    public int read(byte[] data) throws IOException {
      int ret = super.read(data);
      if (ret > 0) curPos += ret;
      return ret;
    }

    public int read(byte[] data, int offset, int length) throws IOException {
      int ret = super.read(data, offset, length);
      if (ret > 0) curPos += ret;
      return ret;
    }

    public void mark(int limit) {
      super.mark(limit);
      markPos = curPos;
    }

    public void reset() throws IOException {
      if (markPos == -1) {
        throw new IOException("Not marked!");
      }
      super.reset();
      curPos = markPos;
      markPos = -1;
    }

    public long getPos() {
      return curPos;
    }
  }
}
