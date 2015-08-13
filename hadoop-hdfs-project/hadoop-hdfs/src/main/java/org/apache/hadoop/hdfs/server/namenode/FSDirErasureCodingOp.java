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
import java.util.List;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingZone;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

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
   * Create an erasure coding zone on directory src.
   *
   * @param fsn namespace
   * @param srcArg the path of a directory which will be the root of the
   *          erasure coding zone. The directory must be empty.
   * @param ecPolicy erasure coding policy for the erasure coding zone
   * @param logRetryCache whether to record RPC ids in editlog for retry
   *          cache rebuilding
   * @return {@link HdfsFileStatus}
   * @throws IOException
   */
  static HdfsFileStatus createErasureCodingZone(final FSNamesystem fsn,
      final String srcArg, final ErasureCodingPolicy ecPolicy,
      final boolean logRetryCache) throws IOException {
    assert fsn.hasWriteLock();

    String src = srcArg;
    FSPermissionChecker pc = null;
    byte[][] pathComponents = null;
    pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    pc = fsn.getPermissionChecker();
    FSDirectory fsd = fsn.getFSDirectory();
    src = fsd.resolvePath(pc, src, pathComponents);
    final INodesInPath iip;
    List<XAttr> xAttrs;
    fsd.writeLock();
    try {
      iip = fsd.getINodesInPath4Write(src, false);
      xAttrs = fsn.getErasureCodingZoneManager().createErasureCodingZone(
          iip, ecPolicy);
    } finally {
      fsd.writeUnlock();
    }
    fsn.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * Get the erasure coding zone information for specified path.
   *
   * @param fsn namespace
   * @param src path
   * @return {@link ErasureCodingZone}
   * @throws IOException
   */
  static ErasureCodingZone getErasureCodingZone(final FSNamesystem fsn,
      final String src) throws IOException {
    assert fsn.hasReadLock();

    final INodesInPath iip = getINodesInPath(fsn, src);
    return getErasureCodingZoneForPath(fsn, iip);
  }

  /**
   * Get erasure coding zone information for specified path.
   *
   * @param fsn namespace
   * @param iip inodes in the path containing the file
   * @return {@link ErasureCodingZone}
   * @throws IOException
   */
  static ErasureCodingZone getErasureCodingZone(final FSNamesystem fsn,
      final INodesInPath iip) throws IOException {
    assert fsn.hasReadLock();

    return getErasureCodingZoneForPath(fsn, iip);
  }

  /**
   * Check if the file is in erasure coding zone.
   *
   * @param fsn namespace
   * @param srcArg path
   * @return true represents the file is in erasure coding zone, false otw
   * @throws IOException
   */
  static boolean isInErasureCodingZone(final FSNamesystem fsn,
      final String srcArg) throws IOException {
    assert fsn.hasReadLock();

    final INodesInPath iip = getINodesInPath(fsn, srcArg);
    return getErasureCodingPolicyForPath(fsn, iip) != null;
  }

  /**
   * Check if the file is in erasure coding zone.
   *
   * @param fsn namespace
   * @param iip inodes in the path containing the file
   * @return true represents the file is in erasure coding zone, false otw
   * @throws IOException
   */
  static boolean isInErasureCodingZone(final FSNamesystem fsn,
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
    String src = srcArg;
    final byte[][] pathComponents = FSDirectory
        .getPathComponentsForReservedPath(src);
    final FSDirectory fsd = fsn.getFSDirectory();
    final FSPermissionChecker pc = fsn.getPermissionChecker();
    src = fsd.resolvePath(pc, src, pathComponents);
    INodesInPath iip = fsd.getINodesInPath(src, true);
    if (fsn.isPermissionEnabled()) {
      fsn.getFSDirectory().checkPathAccess(pc, iip, FsAction.READ);
    }
    return iip;
  }

  private static ErasureCodingZone getErasureCodingZoneForPath(
      final FSNamesystem fsn, final INodesInPath iip) throws IOException {
    final FSDirectory fsd = fsn.getFSDirectory();
    fsd.readLock();
    try {
      return fsn.getErasureCodingZoneManager().getErasureCodingZone(iip);
    } finally {
      fsd.readUnlock();
    }
  }

  private static ErasureCodingPolicy getErasureCodingPolicyForPath(final FSNamesystem fsn,
      final INodesInPath iip) throws IOException {
    final FSDirectory fsd = fsn.getFSDirectory();
    fsd.readLock();
    try {
      return fsn.getErasureCodingZoneManager().getErasureCodingPolicy(iip);
    } finally {
      fsd.readUnlock();
    }
  }
}
