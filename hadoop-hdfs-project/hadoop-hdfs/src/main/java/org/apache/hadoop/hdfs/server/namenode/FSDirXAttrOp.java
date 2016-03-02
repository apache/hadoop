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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;

class FSDirXAttrOp {
  private static final XAttr KEYID_XATTR =
      XAttrHelper.buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, null);
  private static final XAttr UNREADABLE_BY_SUPERUSER_XATTR =
      XAttrHelper.buildXAttr(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER, null);

  /**
   * Set xattr for a file or directory.
   *
   * @param src
   *          - path on which it sets the xattr
   * @param xAttr
   *          - xAttr details to set
   * @param flag
   *          - xAttrs flags
   * @throws IOException
   */
  static HdfsFileStatus setXAttr(
      FSDirectory fsd, String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag,
      boolean logRetryCache)
      throws IOException {
    checkXAttrsConfigFlag(fsd);
    checkXAttrSize(fsd, xAttr);
    FSPermissionChecker pc = fsd.getPermissionChecker();
    XAttrPermissionFilter.checkPermissionForApi(
        pc, xAttr, FSDirectory.isReservedRawName(src));
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(xAttr);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.getINodesInPath4Write(src);
      checkXAttrChangeAccess(fsd, iip, xAttr, pc);
      unprotectedSetXAttrs(fsd, src, xAttrs, flag);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);
    return fsd.getAuditFileInfo(iip);
  }

  static List<XAttr> getXAttrs(FSDirectory fsd, final String srcArg,
                               List<XAttr> xAttrs)
      throws IOException {
    String src = srcArg;
    checkXAttrsConfigFlag(fsd);
    FSPermissionChecker pc = fsd.getPermissionChecker();
    final boolean isRawPath = FSDirectory.isReservedRawName(src);
    boolean getAll = xAttrs == null || xAttrs.isEmpty();
    if (!getAll) {
      XAttrPermissionFilter.checkPermissionForApi(pc, xAttrs, isRawPath);
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);
    final INodesInPath iip = fsd.getINodesInPath(src, true);
    if (fsd.isPermissionEnabled()) {
      fsd.checkPathAccess(pc, iip, FsAction.READ);
    }
    List<XAttr> all = FSDirXAttrOp.getXAttrs(fsd, src);
    List<XAttr> filteredAll = XAttrPermissionFilter.
        filterXAttrsForApi(pc, all, isRawPath);

    if (getAll) {
      return filteredAll;
    }
    if (filteredAll == null || filteredAll.isEmpty()) {
      return null;
    }
    List<XAttr> toGet = Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr xAttr : xAttrs) {
      boolean foundIt = false;
      for (XAttr a : filteredAll) {
        if (xAttr.getNameSpace() == a.getNameSpace() && xAttr.getName().equals(
            a.getName())) {
          toGet.add(a);
          foundIt = true;
          break;
        }
      }
      if (!foundIt) {
        throw new IOException(
            "At least one of the attributes provided was not found.");
      }
    }
    return toGet;
  }

  static List<XAttr> listXAttrs(
      FSDirectory fsd, String src) throws IOException {
    FSDirXAttrOp.checkXAttrsConfigFlag(fsd);
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    final boolean isRawPath = FSDirectory.isReservedRawName(src);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);
    final INodesInPath iip = fsd.getINodesInPath(src, true);
    if (fsd.isPermissionEnabled()) {
      /* To access xattr names, you need EXECUTE in the owning directory. */
      fsd.checkParentAccess(pc, iip, FsAction.EXECUTE);
    }
    final List<XAttr> all = FSDirXAttrOp.getXAttrs(fsd, src);
    return XAttrPermissionFilter.
        filterXAttrsForApi(pc, all, isRawPath);
  }

  /**
   * Remove an xattr for a file or directory.
   *
   * @param src
   *          - path to remove the xattr from
   * @param xAttr
   *          - xAttr to remove
   * @throws IOException
   */
  static HdfsFileStatus removeXAttr(
      FSDirectory fsd, String src, XAttr xAttr, boolean logRetryCache)
      throws IOException {
    FSDirXAttrOp.checkXAttrsConfigFlag(fsd);
    FSPermissionChecker pc = fsd.getPermissionChecker();
    XAttrPermissionFilter.checkPermissionForApi(
        pc, xAttr, FSDirectory.isReservedRawName(src));
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(
        src);

    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(xAttr);
    INodesInPath iip;
    fsd.writeLock();
    try {
      src = fsd.resolvePath(pc, src, pathComponents);
      iip = fsd.getINodesInPath4Write(src);
      checkXAttrChangeAccess(fsd, iip, xAttr, pc);

      List<XAttr> removedXAttrs = unprotectedRemoveXAttrs(fsd, src, xAttrs);
      if (removedXAttrs != null && !removedXAttrs.isEmpty()) {
        fsd.getEditLog().logRemoveXAttrs(src, removedXAttrs, logRetryCache);
      } else {
        throw new IOException(
            "No matching attributes found for remove operation");
      }
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static List<XAttr> unprotectedRemoveXAttrs(
      FSDirectory fsd, final String src, final List<XAttr> toRemove)
      throws IOException {
    assert fsd.hasWriteLock();
    INodesInPath iip = fsd.getINodesInPath4Write(
        FSDirectory.normalizePath(src), true);
    INode inode = FSDirectory.resolveLastINode(iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
    List<XAttr> removedXAttrs = Lists.newArrayListWithCapacity(toRemove.size());
    List<XAttr> newXAttrs = filterINodeXAttrs(existingXAttrs, toRemove,
                                              removedXAttrs);
    if (existingXAttrs.size() != newXAttrs.size()) {
      XAttrStorage.updateINodeXAttrs(inode, newXAttrs, snapshotId);
      return removedXAttrs;
    }
    return null;
  }

  /**
   * Filter XAttrs from a list of existing XAttrs. Removes matched XAttrs from
   * toFilter and puts them into filtered. Upon completion,
   * toFilter contains the filter XAttrs that were not found, while
   * fitleredXAttrs contains the XAttrs that were found.
   *
   * @param existingXAttrs Existing XAttrs to be filtered
   * @param toFilter XAttrs to filter from the existing XAttrs
   * @param filtered Return parameter, XAttrs that were filtered
   * @return List of XAttrs that does not contain filtered XAttrs
   */
  @VisibleForTesting
  static List<XAttr> filterINodeXAttrs(
      final List<XAttr> existingXAttrs, final List<XAttr> toFilter,
      final List<XAttr> filtered)
    throws AccessControlException {
    if (existingXAttrs == null || existingXAttrs.isEmpty() ||
        toFilter == null || toFilter.isEmpty()) {
      return existingXAttrs;
    }

    // Populate a new list with XAttrs that pass the filter
    List<XAttr> newXAttrs =
        Lists.newArrayListWithCapacity(existingXAttrs.size());
    for (XAttr a : existingXAttrs) {
      boolean add = true;
      for (ListIterator<XAttr> it = toFilter.listIterator(); it.hasNext()
          ;) {
        XAttr filter = it.next();
        Preconditions.checkArgument(
            !KEYID_XATTR.equalsIgnoreValue(filter),
            "The encryption zone xattr should never be deleted.");
        if (UNREADABLE_BY_SUPERUSER_XATTR.equalsIgnoreValue(filter)) {
          throw new AccessControlException("The xattr '" +
              SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' can not be deleted.");
        }
        if (a.equalsIgnoreValue(filter)) {
          add = false;
          it.remove();
          filtered.add(filter);
          break;
        }
      }
      if (add) {
        newXAttrs.add(a);
      }
    }

    return newXAttrs;
  }

  static INode unprotectedSetXAttrs(
      FSDirectory fsd, final String src, final List<XAttr> xAttrs,
      final EnumSet<XAttrSetFlag> flag)
      throws IOException {
    assert fsd.hasWriteLock();
    INodesInPath iip = fsd.getINodesInPath4Write(FSDirectory.normalizePath(src),
        true);
    INode inode = FSDirectory.resolveLastINode(iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
    List<XAttr> newXAttrs = setINodeXAttrs(fsd, existingXAttrs, xAttrs, flag);
    final boolean isFile = inode.isFile();

    for (XAttr xattr : newXAttrs) {
      final String xaName = XAttrHelper.getPrefixName(xattr);

      /*
       * If we're adding the encryption zone xattr, then add src to the list
       * of encryption zones.
       */
      if (CRYPTO_XATTR_ENCRYPTION_ZONE.equals(xaName)) {
        final HdfsProtos.ZoneEncryptionInfoProto ezProto =
            HdfsProtos.ZoneEncryptionInfoProto.parseFrom(xattr.getValue());
        fsd.ezManager.addEncryptionZone(inode.getId(),
                                        PBHelper.convert(ezProto.getSuite()),
                                        PBHelper.convert(
                                            ezProto.getCryptoProtocolVersion()),
                                        ezProto.getKeyName());
      }

      if (!isFile && SECURITY_XATTR_UNREADABLE_BY_SUPERUSER.equals(xaName)) {
        throw new IOException("Can only set '" +
            SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' on a file.");
      }
    }

    XAttrStorage.updateINodeXAttrs(inode, newXAttrs, snapshotId);
    return inode;
  }

  static List<XAttr> setINodeXAttrs(
      FSDirectory fsd, final List<XAttr> existingXAttrs,
      final List<XAttr> toSet, final EnumSet<XAttrSetFlag> flag)
      throws IOException {
    // Check for duplicate XAttrs in toSet
    // We need to use a custom comparator, so using a HashSet is not suitable
    for (int i = 0; i < toSet.size(); i++) {
      for (int j = i + 1; j < toSet.size(); j++) {
        if (toSet.get(i).equalsIgnoreValue(toSet.get(j))) {
          throw new IOException("Cannot specify the same XAttr to be set " +
              "more than once");
        }
      }
    }

    // Count the current number of user-visible XAttrs for limit checking
    int userVisibleXAttrsNum = 0; // Number of user visible xAttrs

    // The XAttr list is copied to an exactly-sized array when it's stored,
    // so there's no need to size it precisely here.
    int newSize = (existingXAttrs != null) ? existingXAttrs.size() : 0;
    newSize += toSet.size();
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(newSize);

    // Check if the XAttr already exists to validate with the provided flag
    for (XAttr xAttr: toSet) {
      boolean exist = false;
      if (existingXAttrs != null) {
        for (XAttr a : existingXAttrs) {
          if (a.equalsIgnoreValue(xAttr)) {
            exist = true;
            break;
          }
        }
      }
      XAttrSetFlag.validate(xAttr.getName(), exist, flag);
      // add the new XAttr since it passed validation
      xAttrs.add(xAttr);
      if (isUserVisible(xAttr)) {
        userVisibleXAttrsNum++;
      }
    }

    // Add the existing xattrs back in, if they weren't already set
    if (existingXAttrs != null) {
      for (XAttr existing : existingXAttrs) {
        boolean alreadySet = false;
        for (XAttr set : toSet) {
          if (set.equalsIgnoreValue(existing)) {
            alreadySet = true;
            break;
          }
        }
        if (!alreadySet) {
          xAttrs.add(existing);
          if (isUserVisible(existing)) {
            userVisibleXAttrsNum++;
          }
        }
      }
    }

    if (userVisibleXAttrsNum > fsd.getInodeXAttrsLimit()) {
      throw new IOException("Cannot add additional XAttr to inode, "
          + "would exceed limit of " + fsd.getInodeXAttrsLimit());
    }

    return xAttrs;
  }

  static List<XAttr> getXAttrs(FSDirectory fsd, INode inode, int snapshotId)
      throws IOException {
    fsd.readLock();
    try {
      return XAttrStorage.readINodeXAttrs(inode, snapshotId);
    } finally {
      fsd.readUnlock();
    }
  }

  static XAttr unprotectedGetXAttrByName(
      INode inode, int snapshotId, String xAttrName)
      throws IOException {
    List<XAttr> xAttrs = XAttrStorage.readINodeXAttrs(inode, snapshotId);
    if (xAttrs == null) {
      return null;
    }
    for (XAttr x : xAttrs) {
      if (XAttrHelper.getPrefixName(x)
          .equals(xAttrName)) {
        return x;
      }
    }
    return null;
  }

  private static void checkXAttrChangeAccess(
      FSDirectory fsd, INodesInPath iip, XAttr xAttr,
      FSPermissionChecker pc)
      throws AccessControlException, FileNotFoundException {
    if (fsd.isPermissionEnabled() && xAttr.getNameSpace() == XAttr.NameSpace
        .USER) {
      final INode inode = iip.getLastINode();
      if (inode != null &&
          inode.isDirectory() &&
          inode.getFsPermission().getStickyBit()) {
        if (!pc.isSuperUser()) {
          fsd.checkOwner(pc, iip);
        }
      } else {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
    }
  }

  /**
   * Verifies that the combined size of the name and value of an xattr is within
   * the configured limit. Setting a limit of zero disables this check.
   */
  private static void checkXAttrSize(FSDirectory fsd, XAttr xAttr) {
    if (fsd.getXattrMaxSize() == 0) {
      return;
    }
    int size = xAttr.getName().getBytes(Charsets.UTF_8).length;
    if (xAttr.getValue() != null) {
      size += xAttr.getValue().length;
    }
    if (size > fsd.getXattrMaxSize()) {
      throw new HadoopIllegalArgumentException(
          "The XAttr is too big. The maximum combined size of the"
          + " name and value is " + fsd.getXattrMaxSize()
          + ", but the total size is " + size);
    }
  }

  private static void checkXAttrsConfigFlag(FSDirectory fsd) throws
                                                             IOException {
    if (!fsd.isXattrsEnabled()) {
      throw new IOException(String.format(
          "The XAttr operation has been rejected.  "
              + "Support for XAttrs has been disabled by setting %s to false.",
          DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }
  }

  private static List<XAttr> getXAttrs(FSDirectory fsd,
                                String src) throws IOException {
    String srcs = FSDirectory.normalizePath(src);
    fsd.readLock();
    try {
      INodesInPath iip = fsd.getINodesInPath(srcs, true);
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getPathSnapshotId();
      return XAttrStorage.readINodeXAttrs(fsd.getAttributes(src,
              inode.getLocalNameBytes(), inode, snapshotId));
    } finally {
      fsd.readUnlock();
    }
  }

  private static boolean isUserVisible(XAttr xAttr) {
    XAttr.NameSpace ns = xAttr.getNameSpace();
    return ns == XAttr.NameSpace.USER || ns == XAttr.NameSpace.TRUSTED;
  }
}
