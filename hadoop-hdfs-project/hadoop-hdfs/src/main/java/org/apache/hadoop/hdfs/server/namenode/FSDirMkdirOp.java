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

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.util.Time.now;

class FSDirMkdirOp {
  static HdfsFileStatus mkdirs(
      FSNamesystem fsn, String src, PermissionStatus permissions,
      boolean createParent) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }
    FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);
    INodesInPath iip = fsd.getINodesInPath4Write(src);
    if (fsd.isPermissionEnabled()) {
      fsd.checkTraverse(pc, iip);
    }

    if (!isDirMutable(fsd, iip)) {
      if (fsd.isPermissionEnabled()) {
        fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
      }

      if (!createParent) {
        fsd.verifyParentDir(iip, src);
      }

      // validate that we have enough inodes. This is, at best, a
      // heuristic because the mkdirs() operation might need to
      // create multiple inodes.
      fsn.checkFsObjectLimit();
      iip = mkdirsRecursively(fsd, iip, permissions, false, now());
      if (iip == null) {
        throw new IOException("Failed to create directory: " + src);
      }
    }
    return fsd.getAuditFileInfo(iip);
  }

  static INode unprotectedMkdir(
      FSDirectory fsd, long inodeId, String src,
      PermissionStatus permissions, List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, UnresolvedLinkException, AclException {
    assert fsd.hasWriteLock();
    byte[][] components = INode.getPathComponents(src);
    final INodesInPath iip = fsd.getExistingPathINodes(components);
    final int pos = iip.length() - 1;
    final INodesInPath newiip = unprotectedMkdir(fsd, inodeId, iip, pos,
        components[pos], permissions, aclEntries, timestamp);
    return newiip.getINode(pos);
  }

  /**
   * Create a directory
   * If ancestor directories do not exist, automatically create them.

   * @param fsd FSDirectory
   * @param iip the INodesInPath instance containing all the existing INodes
   *            and null elements for non-existing components in the path
   * @param permissions the permission of the directory
   * @param inheritPermission
   *   if the permission of the directory should inherit from its parent or not.
   *   u+wx is implicitly added to the automatically created directories,
   *   and to the given directory if inheritPermission is true
   * @param now creation time
   * @return non-null INodesInPath instance if operation succeeds
   * @throws QuotaExceededException if directory creation violates
   *                                any quota limit
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  static INodesInPath mkdirsRecursively(FSDirectory fsd, INodesInPath iip,
      PermissionStatus permissions, boolean inheritPermission, long now)
      throws FileAlreadyExistsException, QuotaExceededException,
             UnresolvedLinkException, SnapshotAccessControlException,
             AclException {
    final int lastInodeIndex = iip.length() - 1;
    final byte[][] components = iip.getPathComponents();
    final String[] names = new String[components.length];
    for (int i = 0; i < components.length; i++) {
      names[i] = DFSUtil.bytes2String(components[i]);
    }

    fsd.writeLock();
    try {
      if (iip.isSnapshot()) {
        throw new SnapshotAccessControlException(
                "Modification on RO snapshot is disallowed");
      }
      final int length = iip.length();
      // find the index of the first null in inodes[]
      StringBuilder pathbuilder = new StringBuilder();
      int i = 1;
      INode curNode;
      for(; i < length && (curNode = iip.getINode(i)) != null; i++) {
        pathbuilder.append(Path.SEPARATOR).append(names[i]);
        if (!curNode.isDirectory()) {
          throw new FileAlreadyExistsException("Parent path is not a directory: "
                  + pathbuilder + " " + curNode.getLocalName());
        }
      }

      // default to creating parent dirs with the given perms
      PermissionStatus parentPermissions = permissions;

      // if not inheriting and it's the last inode, there's no use in
      // computing perms that won't be used
      if (inheritPermission || (i < lastInodeIndex)) {
        // if inheriting (ie. creating a file or symlink), use the parent dir,
        // else the supplied permissions
        // NOTE: the permissions of the auto-created directories violate posix
        FsPermission parentFsPerm = inheritPermission ?
            iip.getINode(i-1).getFsPermission() : permissions.getPermission();

        // ensure that the permissions allow user write+execute
        if (!parentFsPerm.getUserAction().implies(FsAction.WRITE_EXECUTE)) {
          parentFsPerm = new FsPermission(
                  parentFsPerm.getUserAction().or(FsAction.WRITE_EXECUTE),
                  parentFsPerm.getGroupAction(),
                  parentFsPerm.getOtherAction()
          );
        }

        if (!parentPermissions.getPermission().equals(parentFsPerm)) {
          parentPermissions = new PermissionStatus(
                  parentPermissions.getUserName(),
                  parentPermissions.getGroupName(),
                  parentFsPerm
          );
          // when inheriting, use same perms for entire path
          if (inheritPermission) permissions = parentPermissions;
        }
      }

      // create directories beginning from the first null index
      for(; i < length; i++) {
        pathbuilder.append(Path.SEPARATOR).append(names[i]);
        iip = unprotectedMkdir(fsd, fsd.allocateNewInodeId(), iip, i,
            components[i], (i < lastInodeIndex) ? parentPermissions :
                permissions, null, now);
        if (iip.getINode(i) == null) {
          return null;
        }
        // Directory creation also count towards FilesCreated
        // to match count of FilesDeleted metric.
        NameNode.getNameNodeMetrics().incrFilesCreated();

        final String cur = pathbuilder.toString();
        fsd.getEditLog().logMkDir(cur, iip.getINode(i));
        if(NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
                  "mkdirs: created directory " + cur);
        }
      }
    } finally {
      fsd.writeUnlock();
    }
    return iip;
  }

  /**
   * Check whether the path specifies a directory
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  private static boolean isDirMutable(FSDirectory fsd, INodesInPath iip)
      throws SnapshotAccessControlException {
    fsd.readLock();
    try {
      INode node = iip.getLastINode();
      return node != null && node.isDirectory();
    } finally {
      fsd.readUnlock();
    }
  }

  /** create a directory at index pos.
   * The parent path to the directory is at [0, pos-1].
   * All ancestors exist. Newly created one stored at index pos.
   */
  private static INodesInPath unprotectedMkdir(
      FSDirectory fsd, long inodeId, INodesInPath inodesInPath, int pos,
      byte[] name, PermissionStatus permission, List<AclEntry> aclEntries,
      long timestamp)
      throws QuotaExceededException, AclException {
    assert fsd.hasWriteLock();
    final INodeDirectory dir = new INodeDirectory(inodeId, name, permission,
        timestamp);
    if (fsd.addChild(inodesInPath, pos, dir, true)) {
      if (aclEntries != null) {
        AclStorage.updateINodeAcl(dir, aclEntries, Snapshot.CURRENT_STATE_ID);
      }
      return INodesInPath.replace(inodesInPath, pos, dir);
    } else {
      return inodesInPath;
    }
  }
}
