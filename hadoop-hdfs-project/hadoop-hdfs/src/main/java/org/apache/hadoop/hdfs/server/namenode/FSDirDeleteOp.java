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

import com.google.protobuf.ByteString;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.util.Time.now;

class FSDirDeleteOp {
  /**
   * Delete the target directory and collect the blocks under it
   *
   * @param tx the Transaction
   * @param paths the path to be deleted
   * @param collectedBlocks Blocks under the deleted directory
   * @return the number of files that have been removed
   */
  static long delete(
      RWTransaction tx, Resolver.Result paths,
      BlocksMapUpdateInfo collectedBlocks, List<Long> removedUCFiles, long mtime) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + paths.src);
    }
    final long filesRemoved;
    if (!deleteAllowed(paths)) {
      filesRemoved = -1;
    } else {
      filesRemoved = unprotectedDelete(tx, paths, collectedBlocks,
                                       removedUCFiles, mtime);
    }
    return filesRemoved;
  }

  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time holding
   * the {@link FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   *
   * @param fsn namespace
   * @param src path name to be deleted
   * @param recursive boolean true to apply to all sub-directories recursively
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *          rebuilding
   * @return blocks collected from the deleted path
   * @throws IOException
   */
  static BlocksMapUpdateInfo delete(
      FSNamesystem fsn, String src, boolean recursive, boolean logRetryCache)
      throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    FSPermissionChecker pc = fsd.getPermissionChecker();
    try (RWTransaction tx = fsd.newRWTransaction().begin()) {
      Resolver.Result paths = Resolver.resolve(tx, src);
      if (paths.notFound()) {
        // The caller expects no exception when the file does not exist
        return null;
      } else if (paths.invalidPath()) {
        throw new InvalidPathException(src);
      }

      FlatINodesInPath iip = paths.inodesInPath();
      if (!recursive && FSDirectory.isNonEmptyDirectory(tx, iip)) {
        throw new PathIsNotEmptyDirectoryException(src + " is non empty");
      }

      if (fsd.isPermissionEnabled()) {
        fsd.checkPermission(pc, iip, false, null, FsAction.WRITE, null,
                            FsAction.ALL, true);
      }
      BlocksMapUpdateInfo ret = deleteInternal(tx, fsn, paths, logRetryCache);
      tx.commit();
      return ret;
    }

  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * <br>
   * Note: This is to be used by
   * {@link org.apache.hadoop.hdfs.server.namenode.FSEditLog} only.
   * <br>
   *
   * @param fsd the FSDirectory instance
   * @param src a string representation of a path to an inode
   * @param mtime the time the inode is removed
   */
  static void deleteForEditLog(FSDirectory fsd, String src, long mtime)
      throws IOException {
    FSNamesystem fsn = fsd.getFSNamesystem();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<Long> removedINodes = new ChunkedArrayList<>();
    List<Long> removedUCFiles = new ChunkedArrayList<>();
    try (ReplayTransaction tx = fsd.newReplayTransaction().begin()) {
      Resolver.Result paths = Resolver.resolve(tx, src);
      if (!deleteAllowed(paths)) {
        return;
      }
      long filesRemoved = unprotectedDelete(tx, paths, collectedBlocks,
                                            removedUCFiles, mtime);
      if (filesRemoved >= 0) {
        fsn.removeLeases(removedUCFiles);
        fsn.removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
      }
      tx.commit();
    }
  }

  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time holding
   * the {@link org.apache.hadoop.hdfs.server.namenode.FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   * @param fsn namespace
   * @param paths the path to be deleted
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *          rebuilding
   * @return blocks collected from the deleted path
   * @throws IOException
   */
  static BlocksMapUpdateInfo deleteInternal(
      RWTransaction tx, FSNamesystem fsn, Resolver.Result paths,
      boolean logRetryCache)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + paths.src);
    }

    FSDirectory fsd = fsn.getFSDirectory();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<Long> removedINodes = new ChunkedArrayList<>();
    List<Long> removedUCFiles = new ChunkedArrayList<>();

    long mtime = now();
    // Unlink the target directory from directory tree
    long filesRemoved = delete(tx, paths, collectedBlocks, removedUCFiles, mtime);
    if (filesRemoved < 0) {
      return null;
    }
    fsd.getEditLog().logDelete(paths.src, mtime, logRetryCache);
    incrDeletedFileCount(filesRemoved);

    fsn.removeLeases(removedUCFiles);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* Namesystem.delete: " + paths.src + " is removed");
    }
    return collectedBlocks;
  }

  static void incrDeletedFileCount(long count) {
    NameNode.getNameNodeMetrics().incrFilesDeleted(count);
  }

  private static boolean deleteAllowed(Resolver.Result paths) {
    String src = paths.src;
    if (paths.notFound()) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedDelete: failed to remove "
                + src + " because it does not exist");
      }
      return false;
    } else if (paths.inodesInPath().length() == 1) { // src is the root
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedDelete: failed to remove " + src +
              " because the root is not allowed to be deleted");
      return false;
    }
    return true;
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   *
   * @param tx Transaction
   * @param paths the path to be deleted
   * @param collectedBlocks blocks collected from the deleted path
   * @param removedUCFiles inodes whose leases need to be released
   * @param mtime the time the inode is removed
   * @return true if there are inodes deleted
   */
  private static long unprotectedDelete(
      RWTransaction tx, Resolver.Result paths,
      BlocksMapUpdateInfo collectedBlocks, List<Long> removedUCFiles, long mtime) {
    // TODO: Update quota
    FlatINode parent = paths.inodesInPath().getLastINode(-2);
    FlatINode inode = paths.inodesInPath().getLastINode();
    long deleted = deleteSubtree(tx, inode.id(), collectedBlocks,
                                 removedUCFiles);
    ByteString newParent = new FlatINode.Builder()
        .mergeFrom(parent).mtime(mtime).build();
    tx.putINode(parent.id(), newParent);
    tx.deleteChild(parent.id(),
                   paths.inodesInPath().getLastPathComponent().asReadOnlyByteBuffer());
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          + paths.src + " is removed");
    }
    return deleted + 1;
  }

  private static long deleteSubtree(
      RWTransaction tx, long parentId, BlocksMapUpdateInfo collectedBlocks,
      List<Long> removedUCFiles) {
    long deleted = 0;
    for (long child : tx.childrenView(parentId).values()) {
      FlatINode node = tx.getINode(child);
      if (node.isFile()) {
        FlatINodeFileFeature f = node.feature(FlatINodeFileFeature.class);
        assert f != null;
        if (f.inConstruction()) {
          removedUCFiles.add(child);
        }
        for (Block b : f.blocks()) {
          collectedBlocks.addDeleteBlock(b);
        }
      } else if (node.isDirectory()) {
        deleted += deleteSubtree(tx, child, collectedBlocks, removedUCFiles);
      }
    }
    return deleted;
  }
}
