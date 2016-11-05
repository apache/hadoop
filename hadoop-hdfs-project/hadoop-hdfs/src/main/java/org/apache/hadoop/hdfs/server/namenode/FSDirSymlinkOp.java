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

import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;

import java.io.IOException;

import static org.apache.hadoop.util.Time.now;

class FSDirSymlinkOp {

  static HdfsFileStatus createSymlinkInt(
      FSNamesystem fsn, String target, final String linkArg,
      PermissionStatus dirPerms, boolean createParent, boolean logRetryCache)
      throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    String link = linkArg;
    if (!DFSUtil.isValidName(link)) {
      throw new InvalidPathException("Invalid link name: " + link);
    }
    if (FSDirectory.isReservedName(target) || target.isEmpty()
        || FSDirectory.isExactReservedName(target)) {
      throw new InvalidPathException("Invalid target name: " + target);
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.createSymlink: target="
          + target + " link=" + link);
    }

    FSPermissionChecker pc = fsn.getPermissionChecker();
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, link, DirOp.WRITE_LINK);
      link = iip.getPath();
      if (!createParent) {
        fsd.verifyParentDir(iip);
      }
      if (!fsd.isValidToCreate(link, iip)) {
        throw new IOException(
            "failed to create link " + link +
                " either because the filename is invalid or the file exists");
      }
      if (fsd.isPermissionEnabled()) {
        fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
      }
      // validate that we have enough inodes.
      fsn.checkFsObjectLimit();

      // add symbolic link to namespace
      addSymlink(fsd, link, iip, target, dirPerms, createParent, logRetryCache);
    } finally {
      fsd.writeUnlock();
    }
    NameNode.getNameNodeMetrics().incrCreateSymlinkOps();
    return fsd.getAuditFileInfo(iip);
  }

  static INodeSymlink unprotectedAddSymlink(FSDirectory fsd, INodesInPath iip,
      byte[] localName, long id, String target, long mtime, long atime,
      PermissionStatus perm)
      throws UnresolvedLinkException, QuotaExceededException {
    assert fsd.hasWriteLock();
    final INodeSymlink symlink = new INodeSymlink(id, null, perm, mtime, atime,
        target);
    symlink.setLocalName(localName);
    return fsd.addINode(iip, symlink, perm.getPermission()) != null ?
        symlink : null;
  }

  /**
   * Add the given symbolic link to the fs. Record it in the edits log.
   */
  private static INodeSymlink addSymlink(FSDirectory fsd, String path,
      INodesInPath iip, String target, PermissionStatus dirPerms,
      boolean createParent, boolean logRetryCache) throws IOException {
    final long mtime = now();
    final INodesInPath parent;
    if (createParent) {
      parent = FSDirMkdirOp.createAncestorDirectories(fsd, iip, dirPerms);
      if (parent == null) {
        return null;
      }
    } else {
      parent = iip.getParentINodesInPath();
    }
    final String userName = dirPerms.getUserName();
    long id = fsd.allocateNewInodeId();
    PermissionStatus perm = new PermissionStatus(
        userName, null, FsPermission.getDefault());
    INodeSymlink newNode = unprotectedAddSymlink(fsd, parent,
        iip.getLastLocalName(), id, target, mtime, mtime, perm);
    if (newNode == null) {
      NameNode.stateChangeLog.info("addSymlink: failed to add " + path);
      return null;
    }
    fsd.getEditLog().logSymlink(path, target, mtime, mtime, newNode,
        logRetryCache);

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("addSymlink: " + path + " is added");
    }
    return newNode;
  }
}
