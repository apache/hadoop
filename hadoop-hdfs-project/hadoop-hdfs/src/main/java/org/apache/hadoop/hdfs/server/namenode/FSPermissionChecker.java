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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Stack;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
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

  private static String getPath(byte[][] components, int start, int end) {
    return DFSUtil.byteArray2PathString(components, start, end - start + 1);
  }

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INodeAttributes inodeAttrib, String path,
      FsAction access) {
    return toAccessControlString(inodeAttrib, path, access, false);
  }

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INodeAttributes inodeAttrib,
      String path, FsAction access, boolean deniedFromAcl) {
    StringBuilder sb = new StringBuilder("Permission denied: ")
      .append("user=").append(getUser()).append(", ")
      .append("access=").append(access).append(", ")
      .append("inode=\"").append(path).append("\":")
      .append(inodeAttrib.getUserName()).append(':')
      .append(inodeAttrib.getGroupName()).append(':')
      .append(inodeAttrib.isDirectory() ? 'd' : '-')
      .append(inodeAttrib.getFsPermission());
    if (deniedFromAcl) {
      sb.append("+");
    }
    return sb.toString();
  }

  private final String fsOwner;
  private final String supergroup;
  private final UserGroupInformation callerUgi;

  private final String user;
  private final Collection<String> groups;
  private final boolean isSuper;
  private final INodeAttributeProvider attributeProvider;


  FSPermissionChecker(String fsOwner, String supergroup,
      UserGroupInformation callerUgi,
      INodeAttributeProvider attributeProvider) {
    this.fsOwner = fsOwner;
    this.supergroup = supergroup;
    this.callerUgi = callerUgi;
    this.groups = callerUgi.getGroups();
    user = callerUgi.getShortUserName();
    isSuper = user.equals(fsOwner) || groups.contains(supergroup);
    this.attributeProvider = attributeProvider;
  }

  public boolean isMemberOfGroup(String group) {
    return groups.contains(group);
  }

  public String getUser() {
    return user;
  }

  public boolean isSuperUser() {
    return isSuper;
  }

  public INodeAttributeProvider getAttributesProvider() {
    return attributeProvider;
  }

  private AccessControlEnforcer getAccessControlEnforcer() {
    return (attributeProvider != null)
        ? attributeProvider.getExternalAccessControlEnforcer(this) : this;
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
    final byte[][] components = inodesInPath.getPathComponents();
    for (int i = 0; i < inodes.length && inodes[i] != null; i++) {
      inodeAttrs[i] = getINodeAttrs(components, i, inodes[i], snapshotId);
    }

    String path = inodesInPath.getPath();
    int ancestorIndex = inodes.length - 2;

    AccessControlEnforcer enforcer = getAccessControlEnforcer();
    enforcer.checkPermission(fsOwner, supergroup, callerUgi, inodeAttrs, inodes,
        components, snapshotId, path, ancestorIndex, doCheckOwner,
        ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir);
  }

  /**
   * Check permission only for the given inode (not checking the children's
   * access).
   *
   * @param inode the inode to check.
   * @param snapshotId the snapshot id.
   * @param access the target access.
   * @throws AccessControlException
   */
  void checkPermission(INode inode, int snapshotId, FsAction access)
      throws AccessControlException {
    try {
      byte[][] localComponents = {inode.getLocalNameBytes()};
      INodeAttributes[] iNodeAttr = {inode.getSnapshotINode(snapshotId)};
      AccessControlEnforcer enforcer = getAccessControlEnforcer();
      enforcer.checkPermission(
          fsOwner, supergroup, callerUgi,
          iNodeAttr, // single inode attr in the array
          new INode[]{inode}, // single inode in the array
          localComponents, snapshotId,
          null, -1, // this will skip checkTraverse() because
          // not checking ancestor here
          false, null, null,
          access, // the target access to be checked against the inode
          null, // passing null sub access avoids checking children
          false);
    } catch (AccessControlException ace) {
      throw new AccessControlException(
          toAccessControlString(inode, inode.getFullPathName(), access));
    }
  }

  @Override
  public void checkPermission(String fsOwner, String supergroup,
      UserGroupInformation callerUgi, INodeAttributes[] inodeAttrs,
      INode[] inodes, byte[][] components, int snapshotId, String path,
      int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
      FsAction parentAccess, FsAction access, FsAction subAccess,
      boolean ignoreEmptyDir)
      throws AccessControlException {
    for(; ancestorIndex >= 0 && inodes[ancestorIndex] == null;
        ancestorIndex--);

    try {
      checkTraverse(inodeAttrs, inodes, components, ancestorIndex);
    } catch (UnresolvedPathException | ParentNotDirectoryException ex) {
      // must tunnel these exceptions out to avoid breaking interface for
      // external enforcer
      throw new TraverseAccessControlException(ex);
    }

    final INodeAttributes last = inodeAttrs[inodeAttrs.length - 1];
    if (parentAccess != null && parentAccess.implies(FsAction.WRITE)
        && inodeAttrs.length > 1 && last != null) {
      checkStickyBit(inodeAttrs, components, inodeAttrs.length - 2);
    }
    if (ancestorAccess != null && inodeAttrs.length > 1) {
      check(inodeAttrs, components, ancestorIndex, ancestorAccess);
    }
    if (parentAccess != null && inodeAttrs.length > 1) {
      check(inodeAttrs, components, inodeAttrs.length - 2, parentAccess);
    }
    if (access != null) {
      check(inodeAttrs, components, inodeAttrs.length - 1, access);
    }
    if (subAccess != null) {
      INode rawLast = inodes[inodeAttrs.length - 1];
      checkSubAccess(components, inodeAttrs.length - 1, rawLast,
          snapshotId, subAccess, ignoreEmptyDir);
    }
    if (doCheckOwner) {
      checkOwner(inodeAttrs, components, inodeAttrs.length - 1);
    }
  }

  private INodeAttributes getINodeAttrs(byte[][] pathByNameArr, int pathIdx,
      INode inode, int snapshotId) {
    INodeAttributes inodeAttrs = inode.getSnapshotINode(snapshotId);
    if (getAttributesProvider() != null) {
      String[] elements = new String[pathIdx + 1];
      /**
       * {@link INode#getPathComponents(String)} returns a null component
       * for the root only path "/". Assign an empty string if so.
       */
      if (pathByNameArr.length == 1 && pathByNameArr[0] == null) {
        elements[0] = "";
      } else {
        for (int i = 0; i < elements.length; i++) {
          elements[i] = DFSUtil.bytes2String(pathByNameArr[i]);
        }
      }
      inodeAttrs = getAttributesProvider().getAttributes(elements, inodeAttrs);
    }
    return inodeAttrs;
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkOwner(INodeAttributes[] inodes, byte[][] components, int i)
      throws AccessControlException {
    if (getUser().equals(inodes[i].getUserName())) {
      return;
    }
    throw new AccessControlException(
        "Permission denied. user=" + getUser() +
        " is not the owner of inode=" + getPath(components, 0, i));
  }

  /** Guarded by {@link FSNamesystem#readLock()}
   * @throws AccessControlException
   * @throws ParentNotDirectoryException
   * @throws UnresolvedPathException
   */
  private void checkTraverse(INodeAttributes[] inodeAttrs, INode[] inodes,
      byte[][] components, int last) throws AccessControlException,
          UnresolvedPathException, ParentNotDirectoryException {
    for (int i=0; i <= last; i++) {
      checkIsDirectory(inodes[i], components, i);
      check(inodeAttrs, components, i, FsAction.EXECUTE);
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkSubAccess(byte[][] components, int pathIdx,
      INode inode, int snapshotId, FsAction access, boolean ignoreEmptyDir)
      throws AccessControlException {
    if (inode == null || !inode.isDirectory()) {
      return;
    }

    // Each inode in the subtree has a level. The root inode has level 0.
    // List subINodePath tracks the inode path in the subtree during
    // traversal. The root inode is not stored because it is already in array
    // components. The list index is (level - 1).
    ArrayList<INodeDirectory> subINodePath = new ArrayList<>();

    // The stack of levels matches the stack of directory inodes.
    Stack<Integer> levels = new Stack<>();
    levels.push(0);    // Level 0 is the root

    Stack<INodeDirectory> directories = new Stack<INodeDirectory>();
    for(directories.push(inode.asDirectory()); !directories.isEmpty(); ) {
      INodeDirectory d = directories.pop();
      int level = levels.pop();
      ReadOnlyList<INode> cList = d.getChildrenList(snapshotId);
      if (!(cList.isEmpty() && ignoreEmptyDir)) {
        //TODO have to figure this out with inodeattribute provider
        INodeAttributes inodeAttr =
            getINodeAttrs(components, pathIdx, d, snapshotId);
        if (!hasPermission(inodeAttr, access)) {
          throw new AccessControlException(
              toAccessControlString(inodeAttr, d.getFullPathName(), access));
        }

        if (level > 0) {
          if (level - 1 < subINodePath.size()) {
            subINodePath.set(level - 1, d);
          } else {
            Preconditions.checkState(level - 1 == subINodePath.size());
            subINodePath.add(d);
          }
        }

        if (inodeAttr.getFsPermission().getStickyBit()) {
          for (INode child : cList) {
            INodeAttributes childInodeAttr =
                getINodeAttrs(components, pathIdx, child, snapshotId);
            if (isStickyBitViolated(inodeAttr, childInodeAttr)) {
              List<byte[]> allComponentList = new ArrayList<>();
              for (int i = 0; i <= pathIdx; ++i) {
                allComponentList.add(components[i]);
              }
              for (int i = 0; i < level; ++i) {
                allComponentList.add(subINodePath.get(i).getLocalNameBytes());
              }
              allComponentList.add(child.getLocalNameBytes());
              int index = pathIdx + level;
              byte[][] allComponents =
                  allComponentList.toArray(new byte[][]{});
              throwStickyBitException(
                  getPath(allComponents, 0, index + 1), child,
                  getPath(allComponents, 0, index), inode);
            }
          }
        }
      }

      for(INode child : cList) {
        if (child.isDirectory()) {
          directories.push(child.asDirectory());
          levels.push(level + 1);
        }
      }
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void check(INodeAttributes[] inodes, byte[][] components, int i,
      FsAction access) throws AccessControlException {
    INodeAttributes inode = (i >= 0) ? inodes[i] : null;
    if (inode != null && !hasPermission(inode, access)) {
      throw new AccessControlException(
          toAccessControlString(inode, getPath(components, 0, i), access));
    }
  }

  // return whether access is permitted.  note it neither requires a path or
  // throws so the caller can build the path only if required for an exception.
  // very beneficial for subaccess checks!
  private boolean hasPermission(INodeAttributes inode, FsAction access) {
    if (inode == null) {
      return true;
    }
    final FsPermission mode = inode.getFsPermission();
    final AclFeature aclFeature = inode.getAclFeature();
    if (aclFeature != null) {
      // It's possible that the inode has a default ACL but no access ACL.
      int firstEntry = aclFeature.getEntryAt(0);
      if (AclEntryStatusFormat.getScope(firstEntry) == AclEntryScope.ACCESS) {
        return hasAclPermission(inode, access, mode, aclFeature);
      }
    }
    final FsAction checkAction;
    if (getUser().equals(inode.getUserName())) { //user class
      checkAction = mode.getUserAction();
    } else if (isMemberOfGroup(inode.getGroupName())) { //group class
      checkAction = mode.getGroupAction();
    } else { //other class
      checkAction = mode.getOtherAction();
    }
    return checkAction.implies(access);
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
  private boolean hasAclPermission(INodeAttributes inode,
      FsAction access, FsPermission mode, AclFeature aclFeature) {
    boolean foundMatch = false;

    // Use owner entry from permission bits if user is owner.
    if (getUser().equals(inode.getUserName())) {
      if (mode.getUserAction().implies(access)) {
        return true;
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
              return true;
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
          if (isMemberOfGroup(group)) {
            FsAction masked = AclEntryStatusFormat.getPermission(entry).and(
                mode.getGroupAction());
            if (masked.implies(access)) {
              return true;
            }
            foundMatch = true;
          }
        }
      }
    }

    // Use other entry if user was not denied by an earlier match.
    return !foundMatch && mode.getOtherAction().implies(access);
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkStickyBit(INodeAttributes[] inodes, byte[][] components,
      int index) throws AccessControlException {
    INodeAttributes parent = inodes[index];
    if (!parent.getFsPermission().getStickyBit()) {
      return;
    }

    INodeAttributes inode = inodes[index + 1];
    if (!isStickyBitViolated(parent, inode)) {
      return;
    }

    throwStickyBitException(getPath(components, 0, index + 1), inode,
        getPath(components, 0, index), parent);
  }

  /** Return true when sticky bit is violated. */
  private boolean isStickyBitViolated(INodeAttributes parent,
                                      INodeAttributes inode) {
    // If this user is the directory owner, return
    if (parent.getUserName().equals(getUser())) {
      return false;
    }

    // if this user is the file owner, return
    if (inode.getUserName().equals(getUser())) {
      return false;
    }

    return true;
  }

  private void throwStickyBitException(
      String inodePath, INodeAttributes inode,
      String parentPath, INodeAttributes parent)
      throws AccessControlException {
    throw new AccessControlException(String.format(
        FSExceptionMessages.PERMISSION_DENIED_BY_STICKY_BIT +
            ": user=%s, path=\"%s\":%s:%s:%s%s, " +
            "parent=\"%s\":%s:%s:%s%s", user, inodePath, inode.getUserName(),
        inode.getGroupName(), inode.isDirectory() ? "d" : "-",
        inode.getFsPermission().toString(), parentPath, parent.getUserName(),
        parent.getGroupName(), parent.isDirectory() ? "d" : "-",
        parent.getFsPermission().toString()));
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
    if (isMemberOfGroup(pool.getGroupName())
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

  /**
   * Verifies that all existing ancestors are directories.  If a permission
   * checker is provided then the user must have exec access.  Ancestor
   * symlinks will throw an unresolved exception, and resolveLink determines
   * if the last inode will throw an unresolved exception.  This method
   * should always be called after a path is resolved into an IIP.
   * @param pc for permission checker, null for no checking
   * @param iip path to verify
   * @param resolveLink whether last inode may be a symlink
   * @throws AccessControlException
   * @throws UnresolvedPathException
   * @throws ParentNotDirectoryException
   */
  static void checkTraverse(FSPermissionChecker pc, INodesInPath iip,
      boolean resolveLink) throws AccessControlException,
          UnresolvedPathException, ParentNotDirectoryException {
    try {
      if (pc == null || pc.isSuperUser()) {
        checkSimpleTraverse(iip);
      } else {
        pc.checkPermission(iip, false, null, null, null, null, false);
      }
    } catch (TraverseAccessControlException tace) {
      // unwrap the non-ACE (unresolved, parent not dir) exception
      // tunneled out of checker.
      tace.throwCause();
    }
    // maybe check that the last inode is a symlink
    if (resolveLink) {
      int last = iip.length() - 1;
      checkNotSymlink(iip.getINode(last), iip.getPathComponents(), last);
    }
  }

  // rudimentary permission-less directory check
  private static void checkSimpleTraverse(INodesInPath iip)
      throws UnresolvedPathException, ParentNotDirectoryException {
    byte[][] components = iip.getPathComponents();
    for (int i=0; i < iip.length() - 1; i++) {
      INode inode = iip.getINode(i);
      if (inode == null) {
        break;
      }
      checkIsDirectory(inode, components, i);
    }
  }

  private static void checkIsDirectory(INode inode, byte[][] components, int i)
      throws UnresolvedPathException, ParentNotDirectoryException {
    if (inode != null && !inode.isDirectory()) {
      checkNotSymlink(inode, components, i);
      throw new ParentNotDirectoryException(
          getPath(components, 0, i) + " (is not a directory)");
    }
  }

  private static void checkNotSymlink(INode inode, byte[][] components, int i)
      throws UnresolvedPathException {
    if (inode != null && inode.isSymlink()) {
      final int last = components.length - 1;
      final String path = getPath(components, 0, last);
      final String preceding = getPath(components, 0, i - 1);
      final String remainder = getPath(components, i + 1, last);
      final String target = inode.asSymlink().getSymlinkString();
      if (LOG.isDebugEnabled()) {
        final String link = inode.getLocalName();
        LOG.debug("UnresolvedPathException " +
            " path: " + path + " preceding: " + preceding +
            " count: " + i + " link: " + link + " target: " + target +
            " remainder: " + remainder);
      }
      throw new UnresolvedPathException(path, preceding, remainder, target);
    }
  }

  //used to tunnel non-ACE exceptions encountered during path traversal.
  //ops that create inodes are expected to throw ParentNotDirectoryExceptions.
  //the signature of other methods requires the PNDE to be thrown as an ACE.
  @SuppressWarnings("serial")
  static class TraverseAccessControlException extends AccessControlException {
    TraverseAccessControlException(IOException ioe) {
      super(ioe);
    }
    public void throwCause() throws UnresolvedPathException,
        ParentNotDirectoryException, AccessControlException {
      Throwable ioe = getCause();
      if (ioe instanceof UnresolvedPathException) {
        throw (UnresolvedPathException)ioe;
      }
      if (ioe instanceof ParentNotDirectoryException) {
        throw (ParentNotDirectoryException)ioe;
      }
      throw this;
    }
  }
}
