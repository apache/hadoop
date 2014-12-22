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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/** 
 * Class that helps in checking file system permission.
 * The state of this class need not be synchronized as it has data structures that
 * are read-only.
 * 
 * Some of the helper methods are gaurded by {@link FSNamesystem#readLock()}.
 */
class FSPermissionChecker {
  static final Log LOG = LogFactory.getLog(UserGroupInformation.class);

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INode inode, int snapshotId,
      FsAction access, FsPermission mode) {
    return toAccessControlString(inode, snapshotId, access, mode, false);
  }

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INode inode, int snapshotId, FsAction access,
      FsPermission mode, boolean deniedFromAcl) {
    StringBuilder sb = new StringBuilder("Permission denied: ")
      .append("user=").append(user).append(", ")
      .append("access=").append(access).append(", ")
      .append("inode=\"").append(inode.getFullPathName()).append("\":")
      .append(inode.getUserName(snapshotId)).append(':')
      .append(inode.getGroupName(snapshotId)).append(':')
      .append(inode.isDirectory() ? 'd' : '-')
      .append(mode);
    if (deniedFromAcl) {
      sb.append("+");
    }
    return sb.toString();
  }

  private final String user;
  /** A set with group namess. Not synchronized since it is unmodifiable */
  private final Set<String> groups;
  private final boolean isSuper;

  FSPermissionChecker(String fsOwner, String supergroup,
      UserGroupInformation callerUgi) {
    HashSet<String> s = new HashSet<String>(Arrays.asList(callerUgi.getGroupNames()));
    groups = Collections.unmodifiableSet(s);
    user = callerUgi.getShortUserName();
    isSuper = user.equals(fsOwner) || groups.contains(supergroup);
  }

  /**
   * Check if the callers group contains the required values.
   * @param group group to check
   */
  public boolean containsGroup(String group) {return groups.contains(group);}

  public String getUser() {
    return user;
  }
  
  public boolean isSuperUser() {
    return isSuper;
  }
  
  /**
   * Verify if the caller has the required permission. This will result into 
   * an exception if the caller is not allowed to access the resource.
   */
  public void checkSuperuserPrivilege()
      throws AccessControlException {
    if (!isSuper) {
      throw new AccessControlException("Access denied for user " 
          + user + ". Superuser privilege is required");
    }
  }
  
  /**
   * Check whether current user have permissions to access the path.
   * Traverse is always checked.
   *
   * Parent path means the parent directory for the path.
   * Ancestor path means the last (the closest) existing ancestor directory
   * of the path.
   * Note that if the parent path exists,
   * then the parent path and the ancestor path are the same.
   *
   * For example, suppose the path is "/foo/bar/baz".
   * No matter baz is a file or a directory,
   * the parent path is "/foo/bar".
   * If bar exists, then the ancestor path is also "/foo/bar".
   * If bar does not exist and foo exists,
   * then the ancestor path is "/foo".
   * Further, if both foo and bar do not exist,
   * then the ancestor path is "/".
   *
   * @param doCheckOwner Require user to be the owner of the path?
   * @param ancestorAccess The access required by the ancestor of the path.
   * @param parentAccess The access required by the parent of the path.
   * @param access The access required by the path.
   * @param subAccess If path is a directory,
   * it is the access required of the path and all the sub-directories.
   * If path is not a directory, there is no effect.
   * @param ignoreEmptyDir Ignore permission checking for empty directory?
   * @throws AccessControlException
   * 
   * Guarded by {@link FSNamesystem#readLock()}
   * Caller of this method must hold that lock.
   */
  void checkPermission(INodesInPath inodesInPath, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess, boolean ignoreEmptyDir)
      throws AccessControlException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ACCESS CHECK: " + this
          + ", doCheckOwner=" + doCheckOwner
          + ", ancestorAccess=" + ancestorAccess
          + ", parentAccess=" + parentAccess
          + ", access=" + access
          + ", subAccess=" + subAccess
          + ", ignoreEmptyDir=" + ignoreEmptyDir);
    }
    // check if (parentAccess != null) && file exists, then check sb
    // If resolveLink, the check is performed on the link target.
    final int snapshotId = inodesInPath.getPathSnapshotId();
    final int length = inodesInPath.length();
    final INode last = length > 0 ? inodesInPath.getLastINode() : null;
    final INode parent = length > 1 ? inodesInPath.getINode(-2) : null;

    checkTraverse(inodesInPath, snapshotId);

    if (parentAccess != null && parentAccess.implies(FsAction.WRITE)
        && length > 1 && last != null) {
      checkStickyBit(parent, last, snapshotId);
    }
    if (ancestorAccess != null && length > 1) {
      List<INode> inodes = inodesInPath.getReadOnlyINodes();
      INode ancestor = null;
      for (int i = inodes.size() - 2; i >= 0 && (ancestor = inodes.get(i)) ==
          null; i--);
      check(ancestor, snapshotId, ancestorAccess);
    }
    if (parentAccess != null && length > 1 && parent != null) {
      check(parent, snapshotId, parentAccess);
    }
    if (access != null) {
      check(last, snapshotId, access);
    }
    if (subAccess != null) {
      checkSubAccess(last, snapshotId, subAccess, ignoreEmptyDir);
    }
    if (doCheckOwner) {
      checkOwner(last, snapshotId);
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkOwner(INode inode, int snapshotId
      ) throws AccessControlException {
    if (inode != null && user.equals(inode.getUserName(snapshotId))) {
      return;
    }
    throw new AccessControlException(
            "Permission denied. user="
            + user + " is not the owner of inode=" + inode);
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkTraverse(INodesInPath iip, int snapshotId)
      throws AccessControlException {
    List<INode> inodes = iip.getReadOnlyINodes();
    for (int i = 0; i < inodes.size() - 1; i++) {
      INode inode = inodes.get(i);
      if (inode == null) {
        break;
      }
      check(inode, snapshotId, FsAction.EXECUTE);
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkSubAccess(INode inode, int snapshotId, FsAction access,
      boolean ignoreEmptyDir) throws AccessControlException {
    if (inode == null || !inode.isDirectory()) {
      return;
    }

    Stack<INodeDirectory> directories = new Stack<INodeDirectory>();
    for(directories.push(inode.asDirectory()); !directories.isEmpty(); ) {
      INodeDirectory d = directories.pop();
      ReadOnlyList<INode> cList = d.getChildrenList(snapshotId);
      if (!(cList.isEmpty() && ignoreEmptyDir)) {
        check(d, snapshotId, access);
      }

      for(INode child : cList) {
        if (child.isDirectory()) {
          directories.push(child.asDirectory());
        }
      }
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void check(INode inode, int snapshotId, FsAction access)
      throws AccessControlException {
    if (inode == null) {
      return;
    }
    FsPermission mode = inode.getFsPermission(snapshotId);
    AclFeature aclFeature = inode.getAclFeature(snapshotId);
    if (aclFeature != null) {
      // It's possible that the inode has a default ACL but no access ACL.
      int firstEntry = aclFeature.getEntryAt(0);
      if (AclEntryStatusFormat.getScope(firstEntry) == AclEntryScope.ACCESS) {
        checkAccessAcl(inode, snapshotId, access, mode, aclFeature);
        return;
      }
    }
    checkFsPermission(inode, snapshotId, access, mode);
  }

  private void checkFsPermission(INode inode, int snapshotId, FsAction access,
      FsPermission mode) throws AccessControlException {
    if (user.equals(inode.getUserName(snapshotId))) { //user class
      if (mode.getUserAction().implies(access)) { return; }
    }
    else if (groups.contains(inode.getGroupName(snapshotId))) { //group class
      if (mode.getGroupAction().implies(access)) { return; }
    }
    else { //other class
      if (mode.getOtherAction().implies(access)) { return; }
    }
    throw new AccessControlException(
      toAccessControlString(inode, snapshotId, access, mode));
  }

  /**
   * Checks requested access against an Access Control List.  This method relies
   * on finding the ACL data in the relevant portions of {@link FsPermission} and
   * {@link AclFeature} as implemented in the logic of {@link AclStorage}.  This
   * method also relies on receiving the ACL entries in sorted order.  This is
   * assumed to be true, because the ACL modification methods in
   * {@link AclTransformation} sort the resulting entries.
   *
   * More specifically, this method depends on these invariants in an ACL:
   * - The list must be sorted.
   * - Each entry in the list must be unique by scope + type + name.
   * - There is exactly one each of the unnamed user/group/other entries.
   * - The mask entry must not have a name.
   * - The other entry must not have a name.
   * - Default entries may be present, but they are ignored during enforcement.
   *
   * @param inode INode accessed inode
   * @param snapshotId int snapshot ID
   * @param access FsAction requested permission
   * @param mode FsPermission mode from inode
   * @param aclFeature AclFeature of inode
   * @throws AccessControlException if the ACL denies permission
   */
  private void checkAccessAcl(INode inode, int snapshotId, FsAction access,
      FsPermission mode, AclFeature aclFeature)
      throws AccessControlException {
    boolean foundMatch = false;

    // Use owner entry from permission bits if user is owner.
    if (user.equals(inode.getUserName(snapshotId))) {
      if (mode.getUserAction().implies(access)) {
        return;
      }
      foundMatch = true;
    }

    // Check named user and group entries if user was not denied by owner entry.
    if (!foundMatch) {
      for (int pos = 0, entry; pos < aclFeature.getEntriesSize(); pos++) {
        entry = aclFeature.getEntryAt(pos);
        if (AclEntryStatusFormat.getScope(entry) == AclEntryScope.DEFAULT) {
          break;
        }
        AclEntryType type = AclEntryStatusFormat.getType(entry);
        String name = AclEntryStatusFormat.getName(entry);
        if (type == AclEntryType.USER) {
          // Use named user entry with mask from permission bits applied if user
          // matches name.
          if (user.equals(name)) {
            FsAction masked = AclEntryStatusFormat.getPermission(entry).and(
                mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
            break;
          }
        } else if (type == AclEntryType.GROUP) {
          // Use group entry (unnamed or named) with mask from permission bits
          // applied if user is a member and entry grants access.  If user is a
          // member of multiple groups that have entries that grant access, then
          // it doesn't matter which is chosen, so exit early after first match.
          String group = name == null ? inode.getGroupName(snapshotId) : name;
          if (groups.contains(group)) {
            FsAction masked = AclEntryStatusFormat.getPermission(entry).and(
                mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
          }
        }
      }
    }

    // Use other entry if user was not denied by an earlier match.
    if (!foundMatch && mode.getOtherAction().implies(access)) {
      return;
    }

    throw new AccessControlException(
      toAccessControlString(inode, snapshotId, access, mode, true));
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkStickyBit(INode parent, INode inode, int snapshotId
      ) throws AccessControlException {
    if(!parent.getFsPermission(snapshotId).getStickyBit()) {
      return;
    }

    // If this user is the directory owner, return
    if(parent.getUserName(snapshotId).equals(user)) {
      return;
    }

    // if this user is the file owner, return
    if(inode.getUserName(snapshotId).equals(user)) {
      return;
    }

    throw new AccessControlException("Permission denied by sticky bit setting:" +
      " user=" + user + ", inode=" + inode);
  }

  /**
   * Whether a cache pool can be accessed by the current context
   *
   * @param pool CachePool being accessed
   * @param access type of action being performed on the cache pool
   * @throws AccessControlException if pool cannot be accessed
   */
  public void checkPermission(CachePool pool, FsAction access)
      throws AccessControlException {
    FsPermission mode = pool.getMode();
    if (isSuperUser()) {
      return;
    }
    if (user.equals(pool.getOwnerName())
        && mode.getUserAction().implies(access)) {
      return;
    }
    if (groups.contains(pool.getGroupName())
        && mode.getGroupAction().implies(access)) {
      return;
    }
    if (mode.getOtherAction().implies(access)) {
      return;
    }
    throw new AccessControlException("Permission denied while accessing pool "
        + pool.getPoolName() + ": user " + user + " does not have "
        + access.toString() + " permissions.");
  }
}
