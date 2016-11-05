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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_ERASURECODING_POLICY;

/**
 * Helper class to perform erasure coding related operations.
 */
final class FSDirErasureCodingOp {

  /**
   * Private constructor for preventing FSDirErasureCodingOp object
   * creation. Static-only class.
   */
  private FSDirErasureCodingOp() {}

  /**
   * Set an erasure coding policy on the given path.
   *
   * @param fsn The namespace
   * @param srcArg The path of the target directory.
   * @param ecPolicy The erasure coding policy to set on the target directory.
   * @param logRetryCache whether to record RPC ids in editlog for retry
   *          cache rebuilding
   * @return {@link HdfsFileStatus}
   * @throws IOException
   */
  static HdfsFileStatus setErasureCodingPolicy(final FSNamesystem fsn,
      final String srcArg, final ErasureCodingPolicy ecPolicy,
      final boolean logRetryCache) throws IOException {
    assert fsn.hasWriteLock();

    String src = srcArg;
    FSPermissionChecker pc = null;
    pc = fsn.getPermissionChecker();
    FSDirectory fsd = fsn.getFSDirectory();
    final INodesInPath iip;
    List<XAttr> xAttrs;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE_LINK);
      src = iip.getPath();
      xAttrs = createErasureCodingPolicyXAttr(fsn, iip, ecPolicy);
    } finally {
      fsd.writeUnlock();
    }
    fsn.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);
    return fsd.getAuditFileInfo(iip);
  }

  static List<XAttr> createErasureCodingPolicyXAttr(final FSNamesystem fsn,
      final INodesInPath srcIIP, ErasureCodingPolicy ecPolicy) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    assert fsd.hasWriteLock();
    Preconditions.checkNotNull(srcIIP, "INodes cannot be null");
    String src = srcIIP.getPath();
    if (srcIIP.getLastINode() != null &&
        !srcIIP.getLastINode().isDirectory()) {
      throw new IOException("Attempt to set an erasure coding policy " +
          "for a file " + src);
    }
    if (getErasureCodingPolicyForPath(fsn, srcIIP) != null) {
      throw new IOException("Directory " + src + " already has an " +
          "erasure coding policy.");
    }

    // System default erasure coding policy will be used since no specified.
    if (ecPolicy == null) {
      ecPolicy = ErasureCodingPolicyManager.getSystemDefaultPolicy();
    } else {
      // If ecPolicy is specified check if it is one among active policies.
      boolean validPolicy = false;
      ErasureCodingPolicy[] activePolicies =
          FSDirErasureCodingOp.getErasureCodingPolicies(fsd.getFSNamesystem());
      for (ErasureCodingPolicy activePolicy : activePolicies) {
        if (activePolicy.equals(ecPolicy)) {
          validPolicy = true;
          break;
        }
      }
      if (!validPolicy) {
        List<String> ecPolicyNames = new ArrayList<String>();
        for (ErasureCodingPolicy activePolicy : activePolicies) {
          ecPolicyNames.add(activePolicy.getName());
        }
        throw new HadoopIllegalArgumentException("Policy [ " +
            ecPolicy.getName()+ " ] does not match any of the " +
            "supported policies. Please select any one of " + ecPolicyNames);
      }
    }

    final XAttr ecXAttr;
    DataOutputStream dOut = null;
    try {
      ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      dOut = new DataOutputStream(bOut);
      WritableUtils.writeString(dOut, ecPolicy.getName());
      ecXAttr = XAttrHelper.buildXAttr(XATTR_ERASURECODING_POLICY,
          bOut.toByteArray());
    } finally {
      IOUtils.closeStream(dOut);
    }
    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ecXAttr);
    FSDirXAttrOp.unprotectedSetXAttrs(fsd, srcIIP, xattrs,
        EnumSet.of(XAttrSetFlag.CREATE));
    return xattrs;
  }

  /**
   * Get the erasure coding policy information for specified path.
   *
   * @param fsn namespace
   * @param src path
   * @return {@link ErasureCodingPolicy}
   * @throws IOException
   * @throws FileNotFoundException if the path does not exist.
   */
  static ErasureCodingPolicy getErasureCodingPolicy(final FSNamesystem fsn,
      final String src) throws IOException {
    assert fsn.hasReadLock();

    final INodesInPath iip = getINodesInPath(fsn, src);
    if (iip.getLastINode() == null) {
      throw new FileNotFoundException("Path not found: " + iip.getPath());
    }
    return getErasureCodingPolicyForPath(fsn, iip);
  }

  /**
   * Check if the file or directory has an erasure coding policy.
   *
   * @param fsn namespace
   * @param srcArg path
   * @return Whether the file or directory has an erasure coding policy.
   * @throws IOException
   */
  static boolean hasErasureCodingPolicy(final FSNamesystem fsn,
      final String srcArg) throws IOException {
    return hasErasureCodingPolicy(fsn, getINodesInPath(fsn, srcArg));
  }

  /**
   * Check if the file or directory has an erasure coding policy.
   *
   * @param fsn namespace
   * @param iip inodes in the path containing the file
   * @return Whether the file or directory has an erasure coding policy.
   * @throws IOException
   */
  static boolean hasErasureCodingPolicy(final FSNamesystem fsn,
      final INodesInPath iip) throws IOException {
    return getErasureCodingPolicy(fsn, iip) != null;
  }

  /**
   * Get the erasure coding policy.
   *
   * @param fsn namespace
   * @param iip inodes in the path containing the file
   * @return {@link ErasureCodingPolicy}
   * @throws IOException
   */
  static ErasureCodingPolicy getErasureCodingPolicy(final FSNamesystem fsn,
      final INodesInPath iip) throws IOException {
    assert fsn.hasReadLock();

    return getErasureCodingPolicyForPath(fsn, iip);
  }

  /**
   * Get available erasure coding polices.
   *
   * @param fsn namespace
   * @return {@link ErasureCodingPolicy} array
   */
  static ErasureCodingPolicy[] getErasureCodingPolicies(final FSNamesystem fsn)
      throws IOException {
    assert fsn.hasReadLock();
    return fsn.getErasureCodingPolicyManager().getPolicies();
  }

  private static INodesInPath getINodesInPath(final FSNamesystem fsn,
      final String srcArg) throws IOException {
    final FSDirectory fsd = fsn.getFSDirectory();
    final FSPermissionChecker pc = fsn.getPermissionChecker();
    INodesInPath iip = fsd.resolvePath(pc, srcArg, DirOp.READ);
    if (fsn.isPermissionEnabled()) {
      fsn.getFSDirectory().checkPathAccess(pc, iip, FsAction.READ);
    }
    return iip;
  }

  private static ErasureCodingPolicy getErasureCodingPolicyForPath(FSNamesystem fsn,
      INodesInPath iip) throws IOException {
    Preconditions.checkNotNull(iip, "INodes cannot be null");
    FSDirectory fsd = fsn.getFSDirectory();
    fsd.readLock();
    try {
      List<INode> inodes = iip.getReadOnlyINodes();
      for (int i = inodes.size() - 1; i >= 0; i--) {
        final INode inode = inodes.get(i);
        if (inode == null) {
          continue;
        }
        if (inode.isFile()) {
          byte id = inode.asFile().getErasureCodingPolicyID();
          return id < 0 ? null : fsd.getFSNamesystem().
              getErasureCodingPolicyManager().getPolicyByID(id);
        }
        // We don't allow setting EC policies on paths with a symlink. Thus
        // if a symlink is encountered, the dir shouldn't have EC policy.
        // TODO: properly support symlinks
        if (inode.isSymlink()) {
          return null;
        }
        final XAttrFeature xaf = inode.getXAttrFeature();
        if (xaf != null) {
          XAttr xattr = xaf.getXAttr(XATTR_ERASURECODING_POLICY);
          if (xattr != null) {
            ByteArrayInputStream bIn = new ByteArrayInputStream(xattr.getValue());
            DataInputStream dIn = new DataInputStream(bIn);
            String ecPolicyName = WritableUtils.readString(dIn);
            return fsd.getFSNamesystem().getErasureCodingPolicyManager().
                getPolicyByName(ecPolicyName);
          }
        }
      }
    } finally {
      fsd.readUnlock();
    }
    return null;
  }
}
