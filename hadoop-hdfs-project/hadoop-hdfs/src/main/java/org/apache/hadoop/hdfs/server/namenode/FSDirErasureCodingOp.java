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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.NoECPolicySetException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.erasurecode.CodecRegistry;
import org.apache.hadoop.security.AccessControlException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
   * Check if the ecPolicyName is valid and enabled, return the corresponding
   * EC policy if is, including the REPLICATION EC policy.
   * @param fsn namespace
   * @param ecPolicyName name of EC policy to be checked
   * @return an erasure coding policy if ecPolicyName is valid and enabled
   * @throws IOException
   */
  static ErasureCodingPolicy getErasureCodingPolicyByName(
      final FSNamesystem fsn, final String ecPolicyName) throws IOException {
    assert fsn.hasReadLock();
    ErasureCodingPolicy ecPolicy = fsn.getErasureCodingPolicyManager()
        .getEnabledPolicyByName(ecPolicyName);
    if (ecPolicy == null) {
      final String sysPolicies =
          Arrays.asList(
              fsn.getErasureCodingPolicyManager().getEnabledPolicies())
              .stream()
              .map(ErasureCodingPolicy::getName)
              .collect(Collectors.joining(", "));
      final String message = String.format("Policy '%s' does not match any " +
              "enabled erasure" +
              " coding policies: [%s]. An erasure coding policy can be" +
              " enabled by enableErasureCodingPolicy API.",
          ecPolicyName,
          sysPolicies
      );
      throw new HadoopIllegalArgumentException(message);
    }
    return ecPolicy;
  }

  /**
   * Set an erasure coding policy on the given path.
   *
   * @param fsn The namespace
   * @param srcArg The path of the target directory.
   * @param ecPolicyName The erasure coding policy name to set on the target
   *                    directory.
   * @param logRetryCache whether to record RPC ids in editlog for retry
   *          cache rebuilding
   * @return {@link FileStatus}
   * @throws IOException
   * @throws HadoopIllegalArgumentException if the policy is not enabled
   * @throws AccessControlException if the user does not have write access
   */
  static FileStatus setErasureCodingPolicy(final FSNamesystem fsn,
      final String srcArg, final String ecPolicyName,
      final FSPermissionChecker pc, final boolean logRetryCache)
      throws IOException, AccessControlException {
    assert fsn.hasWriteLock();

    String src = srcArg;
    FSDirectory fsd = fsn.getFSDirectory();
    final INodesInPath iip;
    List<XAttr> xAttrs;
    fsd.writeLock();
    try {
      ErasureCodingPolicy ecPolicy = getErasureCodingPolicyByName(fsn,
          ecPolicyName);
      iip = fsd.resolvePath(pc, src, DirOp.WRITE_LINK);
      // Write access is required to set erasure coding policy
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      src = iip.getPath();
      xAttrs = setErasureCodingPolicyXAttr(fsn, iip, ecPolicy);
    } finally {
      fsd.writeUnlock();
    }
    fsn.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);
    return fsd.getAuditFileInfo(iip);
  }

  private static List<XAttr> setErasureCodingPolicyXAttr(final FSNamesystem fsn,
      final INodesInPath srcIIP, ErasureCodingPolicy ecPolicy) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    assert fsd.hasWriteLock();
    Preconditions.checkNotNull(srcIIP, "INodes cannot be null");
    Preconditions.checkNotNull(ecPolicy, "EC policy cannot be null");
    String src = srcIIP.getPath();
    final INode inode = srcIIP.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("Path not found: " + srcIIP.getPath());
    }
    if (!inode.isDirectory()) {
      throw new IOException("Attempt to set an erasure coding policy " +
          "for a file " + src);
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
    // check whether the directory already has an erasure coding policy
    // directly on itself.
    final Boolean hasEcXAttr =
        getErasureCodingPolicyXAttrForINode(fsn, inode) == null ? false : true;
    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ecXAttr);
    final EnumSet<XAttrSetFlag> flag = hasEcXAttr ?
        EnumSet.of(XAttrSetFlag.REPLACE) : EnumSet.of(XAttrSetFlag.CREATE);
    FSDirXAttrOp.unprotectedSetXAttrs(fsd, srcIIP, xattrs, flag);
    return xattrs;
  }

  /**
   * Unset erasure coding policy from the given directory.
   *
   * @param fsn The namespace
   * @param srcArg The path of the target directory.
   * @param logRetryCache whether to record RPC ids in editlog for retry
   *          cache rebuilding
   * @return {@link FileStatus}
   * @throws IOException
   * @throws AccessControlException if the user does not have write access
   */
  static FileStatus unsetErasureCodingPolicy(final FSNamesystem fsn,
      final String srcArg, final FSPermissionChecker pc,
      final boolean logRetryCache) throws IOException {
    assert fsn.hasWriteLock();

    String src = srcArg;
    FSDirectory fsd = fsn.getFSDirectory();
    final INodesInPath iip;
    List<XAttr> xAttrs;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE_LINK);
      // Write access is required to unset erasure coding policy
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      src = iip.getPath();
      xAttrs = removeErasureCodingPolicyXAttr(fsn, iip);
    } finally {
      fsd.writeUnlock();
    }
    if (xAttrs != null) {
      fsn.getEditLog().logRemoveXAttrs(src, xAttrs, logRetryCache);
    } else {
      throw new NoECPolicySetException(
          "No erasure coding policy explicitly set on " + src);
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * Add an erasure coding policy.
   *
   * @param fsn namespace
   * @param policy the new policy to be added into system
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @throws IOException
   */
  static ErasureCodingPolicy addErasureCodingPolicy(final FSNamesystem fsn,
      ErasureCodingPolicy policy, final boolean logRetryCache) {
    Preconditions.checkNotNull(policy);
    ErasureCodingPolicy retPolicy =
        fsn.getErasureCodingPolicyManager().addPolicy(policy);
    fsn.getEditLog().logAddErasureCodingPolicy(policy, logRetryCache);
    return retPolicy;
  }

  /**
   * Remove an erasure coding policy.
   *
   * @param fsn namespace
   * @param ecPolicyName the name of the policy to be removed
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @throws IOException
   */
  static void removeErasureCodingPolicy(final FSNamesystem fsn,
      String ecPolicyName, final boolean logRetryCache) throws IOException {
    Preconditions.checkNotNull(ecPolicyName);
    fsn.getErasureCodingPolicyManager().removePolicy(ecPolicyName);
    fsn.getEditLog().logRemoveErasureCodingPolicy(ecPolicyName, logRetryCache);
  }

  /**
   * Enable an erasure coding policy.
   *
   * @param fsn namespace
   * @param ecPolicyName the name of the policy to be enabled
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @throws IOException
   */
  static boolean enableErasureCodingPolicy(final FSNamesystem fsn,
      String ecPolicyName, final boolean logRetryCache) throws IOException {
    Preconditions.checkNotNull(ecPolicyName);
    boolean success =
        fsn.getErasureCodingPolicyManager().enablePolicy(ecPolicyName);
    if (success) {
      fsn.getEditLog().logEnableErasureCodingPolicy(ecPolicyName,
          logRetryCache);
    }
    return success;
  }

  /**
   * Disable an erasure coding policy.
   *
   * @param fsn namespace
   * @param ecPolicyName the name of the policy to be disabled
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @throws IOException
   */
  static boolean disableErasureCodingPolicy(final FSNamesystem fsn,
      String ecPolicyName, final boolean logRetryCache) throws IOException {
    Preconditions.checkNotNull(ecPolicyName);
    boolean success =
        fsn.getErasureCodingPolicyManager().disablePolicy(ecPolicyName);
    if (success) {
      fsn.getEditLog().logDisableErasureCodingPolicy(ecPolicyName,
          logRetryCache);
    }
    return success;
  }

  private static List<XAttr> removeErasureCodingPolicyXAttr(
      final FSNamesystem fsn, final INodesInPath srcIIP) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    assert fsd.hasWriteLock();
    Preconditions.checkNotNull(srcIIP, "INodes cannot be null");
    String src = srcIIP.getPath();
    final INode inode = srcIIP.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("Path not found: " + srcIIP.getPath());
    }
    if (!inode.isDirectory()) {
      throw new IOException("Cannot unset an erasure coding policy " +
          "on a file " + src);
    }

    // Check whether the directory has a specific erasure coding policy
    // directly on itself.
    final XAttr ecXAttr = getErasureCodingPolicyXAttrForINode(fsn, inode);
    if (ecXAttr == null) {
      return null;
    }

    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ecXAttr);
    return FSDirXAttrOp.unprotectedRemoveXAttrs(fsd, srcIIP, xattrs);
  }

  /**
   * Get the erasure coding policy information for specified path.
   *
   * @param fsn namespace
   * @param src path
   * @return {@link ErasureCodingPolicy}, or null if no policy has
   * been set or the policy is REPLICATION
   * @throws IOException
   * @throws FileNotFoundException if the path does not exist.
   * @throws AccessControlException if no read access
   */
  static ErasureCodingPolicy getErasureCodingPolicy(final FSNamesystem fsn,
      final String src, FSPermissionChecker pc)
      throws IOException, AccessControlException {
    assert fsn.hasReadLock();

    if (FSDirectory.isExactReservedName(src)) {
      return null;
    }

    FSDirectory fsd = fsn.getFSDirectory();
    final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ);
    if (fsn.isPermissionEnabled()) {
      fsn.getFSDirectory().checkPathAccess(pc, iip, FsAction.READ);
    }

    ErasureCodingPolicy ecPolicy;
    if (iip.isDotSnapshotDir()) {
      ecPolicy = null;
    } else if (iip.getLastINode() == null) {
      throw new FileNotFoundException("Path not found: " + src);
    } else {
      ecPolicy = getErasureCodingPolicyForPath(fsd, iip);
    }

    if (ecPolicy != null && ecPolicy.isReplicationPolicy()) {
      ecPolicy = null;
    }
    return ecPolicy;
  }

  /**
   * Get the erasure coding policy information for specified path and policy
   * name. If ec policy name is given, it will be parsed and the corresponding
   * policy will be returned. Otherwise, get the policy from the parents of the
   * iip.
   *
   * @param fsn namespace
   * @param ecPolicyName the ec policy name
   * @param iip inodes in the path containing the file
   * @return {@link ErasureCodingPolicy}, or null if no policy is found
   * @throws IOException
   */
  static ErasureCodingPolicy getErasureCodingPolicy(FSNamesystem fsn,
      String ecPolicyName, INodesInPath iip) throws IOException {
    ErasureCodingPolicy ecPolicy;
    if (!StringUtils.isEmpty(ecPolicyName)) {
      ecPolicy = FSDirErasureCodingOp.getErasureCodingPolicyByName(
          fsn, ecPolicyName);
    } else {
      ecPolicy = FSDirErasureCodingOp.unprotectedGetErasureCodingPolicy(
          fsn, iip);
    }
    return ecPolicy;
  }

  /**
   * Get the erasure coding policy, including the REPLICATION policy. This does
   * not do any permission checking.
   *
   * @param fsn namespace
   * @param iip inodes in the path containing the file
   * @return {@link ErasureCodingPolicy}
   * @throws IOException
   */
  static ErasureCodingPolicy unprotectedGetErasureCodingPolicy(
      final FSNamesystem fsn, final INodesInPath iip) throws IOException {
    assert fsn.hasReadLock();

    return getErasureCodingPolicyForPath(fsn.getFSDirectory(), iip);
  }

  /**
   * Get available erasure coding polices.
   *
   * @param fsn namespace
   * @return {@link ErasureCodingPolicyInfo} array
   */
  static ErasureCodingPolicyInfo[] getErasureCodingPolicies(
      final FSNamesystem fsn) throws IOException {
    assert fsn.hasReadLock();
    return fsn.getErasureCodingPolicyManager().getPolicies();
  }

  /**
   * Get available erasure coding codecs and coders.
   *
   * @param fsn namespace
   * @return {@link java.util.HashMap} array
   */
  static Map<String, String> getErasureCodingCodecs(final FSNamesystem fsn)
      throws IOException {
    assert fsn.hasReadLock();
    return CodecRegistry.getInstance().getCodec2CoderCompactMap();
  }

  //return erasure coding policy for path, including REPLICATION policy
  private static ErasureCodingPolicy getErasureCodingPolicyForPath(
      FSDirectory fsd, INodesInPath iip) throws IOException {
    Preconditions.checkNotNull(iip, "INodes cannot be null");
    fsd.readLock();
    try {
      for (int i = iip.length() - 1; i >= 0; i--) {
        final INode inode = iip.getINode(i);
        if (inode == null) {
          continue;
        }
        if (inode.isFile()) {
          byte id = inode.asFile().getErasureCodingPolicyID();
          return id < 0 ? null :
              fsd.getFSNamesystem().getErasureCodingPolicyManager().getByID(id);
        }
        // We don't allow setting EC policies on paths with a symlink. Thus
        // if a symlink is encountered, the dir shouldn't have EC policy.
        // TODO: properly support symlinks
        if (inode.isSymlink()) {
          return null;
        }
        final XAttrFeature xaf = inode.getXAttrFeature(iip.getPathSnapshotId());
        if (xaf != null) {
          XAttr xattr = xaf.getXAttr(XATTR_ERASURECODING_POLICY);
          if (xattr != null) {
            ByteArrayInputStream bIn = new ByteArrayInputStream(xattr.getValue());
            DataInputStream dIn = new DataInputStream(bIn);
            String ecPolicyName = WritableUtils.readString(dIn);
            return fsd.getFSNamesystem().getErasureCodingPolicyManager()
              .getByName(ecPolicyName);
          }
        }
      }
    } finally {
      fsd.readUnlock();
    }
    return null;
  }

  private static XAttr getErasureCodingPolicyXAttrForINode(
      FSNamesystem fsn, INode inode) throws IOException {
    // INode can be null
    if (inode == null) {
      return null;
    }
    FSDirectory fsd = fsn.getFSDirectory();
    fsd.readLock();
    try {
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
          return xattr;
        }
      }
    } finally {
      fsd.readUnlock();
    }
    return null;
  }
}
