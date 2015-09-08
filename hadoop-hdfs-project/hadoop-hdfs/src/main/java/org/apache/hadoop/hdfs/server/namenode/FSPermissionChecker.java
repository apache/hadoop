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
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider.AccessControlEnforcer;
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
class FSPermissionChecker implements AccessControlEnforcer {
  static final Log LOG = LogFactory.getLog(UserGroupInformation.class);

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INodeAttributes inodeAttrib, String path,
      FsAction access, FsPermission mode) {
    return toAccessControlString(inodeAttrib, path, access, mode, false);
  }

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INodeAttributes inodeAttrib,
      String path, FsAction access, FsPermission mode, boolean deniedFromAcl) {
    StringBuilder sb = new StringBuilder("Permission denied: ")
      .append("user=").append(getUser()).append(", ")
      .append("access=").append(access).append(", ")
      .append("inode=\"").append(path).append("\":")
      .append(inodeAttrib.getUserName()).append(':')
      .append(inodeAttrib.getGroupName()).append(':')
      .append(inodeAttrib.isDirectory() ? 'd' : '-')
      .append(mode);
    if (deniedFromAcl) {
      sb.append("+");
    }
    return sb.toString();
  }

  private final String fsOwner;
  private final String supergroup;
  private final UserGroupInformation callerUgi;

  private final String user;
  private final Set<String> groups;
  private final boolean isSuper;
  private final INodeAttributeProvider attributeProvider;


  FSPermissionChecker(String fsOwner, String supergroup,
      UserGroupInformation callerUgi,
      INodeAttributeProvider attributeProvider) {
    this.fsOwner = fsOwner;
    this.supergroup = supergroup;
    this.callerUgi = callerUgi;
    HashSet<String> s =
        new HashSet<String>(Arrays.asList(callerUgi.getGroupNames()));
    groups = Collections.unmodifiableSet(s);
    user = callerUgi.getShortUserName();
    isSuper = user.equals(fsOwner) || groups.contains(supergroup);
    this.attributeProvider = attributeProvider;
  }

  public boolean containsGroup(String group) {
    return groups.contains(group);
  }

  public String getUser() {
    return user;
  }

  public Set<String> getGroups() {
    return groups;
  }

  public boolean isSuperUser() {
    return isSuper;
  }

  public INodeAttributeProvider getAttributesProvider() {
    return attributeProvider;
  }

  /**
   * Verify if the caller has the required permission. This will result into 
   * an exception if the caller is not allowed to access the resource.
   */
  public void checkSuperuserPrivilege()
      throws AccessControlException {
    if (!isSuperUser()) {
      throw new AccessControlException("Access denied for user " 
          + getUser() + ". Superuser privilege is required");
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
    final INode[] inodes = inodesInPath.getINodesArray();
    final INodeAttributes[] inodeAttrs = new INodeAttributes[inodes.length];
    final byte[][] pathByNameArr = new byte[inodes.length][];
    for (int i = 0; i < inodes.length && inodes[i] != null; i++) {
      if (inodes[i] != null) {
        pathByNameArr[i] = inodes[i].getLocalNameBytes();
        inodeAttrs[i] = getINodeAttrs(pathByNameArr, i, inodes[i], snapshotId);
      }
    }

    String path = inodesInPath.getPath();
    int ancestorIndex = inodes.length - 2;

    AccessControlEnforcer enforcer =
        getAttributesProvider().getExternalAccessControlEnforcer(this);
    enforcer.checkPermission(fsOwner, supergroup, callerUgi, inodeAttrs, inodes,
        pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
        ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir);
  }

  /**
   * Check whether exception e is due to an ancestor inode's not being
   * directory.
   */
  private void checkAncestorType(INode[] inodes, int checkedAncestorIndex,
      AccessControlException e) throws AccessControlException {
    for (int i = 0; i <= checkedAncestorIndex; i++) {
      if (inodes[i] == null) {
        break;
      }
      if (!inodes[i].isDirectory()) {
        throw new AccessControlException(
            e.getMessage() + " (Ancestor " + inodes[i].getFullPathName()
                + " is not a directory).");
      }
    }
    throw e;
  }

  @Override
  public void checkPermission(String fsOwner, String supergroup,
      UserGroupInformation callerUgi, INodeAttributes[] inodeAttrs,
      INode[] inodes, byte[][] pathByNameArr, int snapshotId, String path,
      int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
      FsAction parentAccess, FsAction access, FsAction subAccess,
      boolean ignoreEmptyDir)
      throws AccessControlException {
    for(; ancestorIndex >= 0 && inodes[ancestorIndex] == null;
        ancestorIndex--);

    checkTraverse(inodeAttrs, inodes, path, ancestorIndex);

    final INodeAttributes last = inodeAttrs[inodeAttrs.length - 1];
    if (parentAccess != null && parentAccess.implies(FsAction.WRITE)
        && inodeAttrs.length > 1 && last != null) {
      checkStickyBit(inodeAttrs[inodeAttrs.length - 2], last, path);
    }
    if (ancestorAccess != null && inodeAttrs.length > 1) {
      check(inodeAttrs, path, ancestorIndex, ancestorAccess);
    }
    if (parentAccess != null && inodeAttrs.length > 1) {
      check(inodeAttrs, path, inodeAttrs.length - 2, parentAccess);
    }
    if (access != null) {
      check(last, path, access);
    }
    if (subAccess != null) {
      INode rawLast = inodes[inodeAttrs.length - 1];
      checkSubAccess(pathByNameArr, inodeAttrs.length - 1, rawLast,
          snapshotId, subAccess, ignoreEmptyDir);
    }
    if (doCheckOwner) {
      checkOwner(last);
    }
  }

  private INodeAttributes getINodeAttrs(byte[][] pathByNameArr, int pathIdx,
      INode inode, int snapshotId) {
    INodeAttributes inodeAttrs = inode.getSnapshotINode(snapshotId);
    if (getAttributesProvider() != null) {
      String[] elements = new String[pathIdx + 1];
      for (int i = 0; i < elements.length; i++) {
        elements[i] = DFSUtil.bytes2String(pathByNameArr[i]);
      }
      inodeAttrs = getAttributesProvider().getAttributes(elements, inodeAttrs);
    }
    return inodeAttrs;
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkOwner(INodeAttributes inode
      ) throws AccessControlException {
    if (getUser().equals(inode.getUserName())) {
      return;
    }
    throw new AccessControlException(
            "Permission denied. user="
            + getUser() + " is not the owner of inode=" + inode);
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkTraverse(INodeAttributes[] inodeAttrs, INode[] inodes,
      String path, int last) throws AccessControlException {
    int j = 0;
    try {
      for (; j <= last; j++) {
        check(inodeAttrs[j], path, FsAction.EXECUTE);
      }
    } catch (AccessControlException e) {
      checkAncestorType(inodes, j, e);
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkSubAccess(byte[][] pathByNameArr, int pathIdx, INode inode,
      int snapshotId, FsAction access, boolean ignoreEmptyDir)
      throws AccessControlException {
    if (inode == null || !inode.isDirectory()) {
      return;
    }

    Stack<INodeDirectory> directories = new Stack<INodeDirectory>();
    for(directories.push(inode.asDirectory()); !directories.isEmpty(); ) {
      INodeDirectory d = directories.pop();
      ReadOnlyList<INode> cList = d.getChildrenList(snapshotId);
      if (!(cList.isEmpty() && ignoreEmptyDir)) {
        //TODO have to figure this out with inodeattribute provider
        check(getINodeAttrs(pathByNameArr, pathIdx, d, snapshotId),
            inode.getFullPathName(), access);
      }

      for(INode child : cList) {
        if (child.isDirectory()) {
          directories.push(child.asDirectory());
        }
      }
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void check(INodeAttributes[] inodes, String path, int i, FsAction access
      ) throws AccessControlException {
    check(i >= 0 ? inodes[i] : null, path, access);
  }

  private void check(INodeAttributes inode, String path, FsAction access
      ) throws AccessControlException {
    if (inode == null) {
      return;
    }
    final FsPermission mode = inode.getFsPermission();
    final AclFeature aclFeature = inode.getAclFeature();
    if (aclFeature != null) {
      // It's possible that the inode has a default ACL but no access ACL.
      int firstEntry = aclFeature.getEntryAt(0);
      if (AclEntryStatusFormat.getScope(firstEntry) == AclEntryScope.ACCESS) {
        checkAccessAcl(inode, path, access, mode, aclFeature);
        return;
      }
    }
    if (getUser().equals(inode.getUserName())) { //user class
      if (mode.getUserAction().implies(access)) { return; }
    }
    else if (getGroups().contains(inode.getGroupName())) { //group class
      if (mode.getGroupAction().implies(access)) { return; }
    }
    else { //other class
      if (mode.getOtherAction().implies(access)) { return; }
    }
    throw new AccessControlException(
        toAccessControlString(inode, path, access, mode));
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
   * @param inode INodeAttributes accessed inode
   * @param snapshotId int snapshot ID
   * @param access FsAction requested permission
   * @param mode FsPermission mode from inode
   * @param aclFeature AclFeature of inode
   * @throws AccessControlException if the ACL denies permission
   */
  private void checkAccessAcl(INodeAttributes inode, String path,
      FsAction access, FsPermission mode, AclFeature aclFeature)
      throws AccessControlException {
    boolean foundMatch = false;

    // Use owner entry from permission bits if user is owner.
    if (getUser().equals(inode.getUserName())) {
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
          if (getUser().equals(name)) {
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
          String group = name == null ? inode.getGroupName() : name;
          if (getGroups().contains(group)) {
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
        toAccessControlString(inode, path, access, mode));
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkStickyBit(INodeAttributes parent, INodeAttributes inode,
      String path) throws AccessControlException {
    if (!parent.getFsPermission().getStickyBit()) {
      return;
    }

    // If this user is the directory owner, return
    if (parent.getUserName().equals(getUser())) {
      return;
    }

    // if this user is the file owner, return
    if (inode.getUserName().equals(getUser())) {
      return;
    }

    throw new AccessControlException(String.format(
        "Permission denied by sticky bit: user=%s, path=\"%s\":%s:%s:%s%s, " +
        "parent=\"%s\":%s:%s:%s%s", user,
        path, inode.getUserName(), inode.getGroupName(),
        inode.isDirectory() ? "d" : "-", inode.getFsPermission().toString(),
        path.substring(0, path.length() - inode.toString().length() - 1 ),
        parent.getUserName(), parent.getGroupName(),
        parent.isDirectory() ? "d" : "-", parent.getFsPermission().toString()));
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
    if (getUser().equals(pool.getOwnerName())
        && mode.getUserAction().implies(access)) {
      return;
    }
    if (getGroups().contains(pool.getGroupName())
        && mode.getGroupAction().implies(access)) {
      return;
    }
    if (mode.getOtherAction().implies(access)) {
      return;
    }
    throw new AccessControlException("Permission denied while accessing pool "
        + pool.getPoolName() + ": user " + getUser() + " does not have "
        + access.toString() + " permissions.");
  }
}
