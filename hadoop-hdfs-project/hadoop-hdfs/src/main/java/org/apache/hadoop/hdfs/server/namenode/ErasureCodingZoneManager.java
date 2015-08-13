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

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingZone;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_ERASURECODING_ZONE;

/**
 * Manages the list of erasure coding zones in the filesystem.
 * <p/>
 * The ErasureCodingZoneManager has its own lock, but relies on the FSDirectory
 * lock being held for many operations. The FSDirectory lock should not be
 * taken if the manager lock is already held.
 * TODO: consolidate zone logic w/ encrypt. zones {@link EncryptionZoneManager}
 */
public class ErasureCodingZoneManager {
  private final FSDirectory dir;

  /**
   * Construct a new ErasureCodingZoneManager.
   *
   * @param dir Enclosing FSDirectory
   */
  public ErasureCodingZoneManager(FSDirectory dir) {
    this.dir = dir;
  }

  ErasureCodingPolicy getErasureCodingPolicy(INodesInPath iip) throws IOException {
    ErasureCodingZone ecZone = getErasureCodingZone(iip);
    return ecZone == null ? null : ecZone.getErasureCodingPolicy();
  }

  ErasureCodingZone getErasureCodingZone(INodesInPath iip) throws IOException {
    assert dir.hasReadLock();
    Preconditions.checkNotNull(iip, "INodes cannot be null");
    List<INode> inodes = iip.getReadOnlyINodes();
    for (int i = inodes.size() - 1; i >= 0; i--) {
      final INode inode = inodes.get(i);
      if (inode == null) {
        continue;
      }
      // We don't allow symlinks in an EC zone, or pointing to a file/dir in
      // an EC. Therefore if a symlink is encountered, the dir shouldn't have
      // EC
      // TODO: properly support symlinks in EC zones
      if (inode.isSymlink()) {
        return null;
      }
      final List<XAttr> xAttrs = inode.getXAttrFeature() == null ?
          new ArrayList<XAttr>(0)
          : inode.getXAttrFeature().getXAttrs();
      for (XAttr xAttr : xAttrs) {
        if (XATTR_ERASURECODING_ZONE.equals(XAttrHelper.getPrefixName(xAttr))) {
          ByteArrayInputStream bIn=new ByteArrayInputStream(xAttr.getValue());
          DataInputStream dIn=new DataInputStream(bIn);
          String ecPolicyName = WritableUtils.readString(dIn);
          ErasureCodingPolicy ecPolicy = dir.getFSNamesystem()
              .getErasureCodingPolicyManager().getPolicy(ecPolicyName);
          return new ErasureCodingZone(dir.getInode(inode.getId())
              .getFullPathName(), ecPolicy);
        }
      }
    }
    return null;
  }

  List<XAttr> createErasureCodingZone(final INodesInPath srcIIP,
      ErasureCodingPolicy ecPolicy) throws IOException {
    assert dir.hasWriteLock();
    Preconditions.checkNotNull(srcIIP, "INodes cannot be null");
    String src = srcIIP.getPath();
    if (dir.isNonEmptyDirectory(srcIIP)) {
      throw new IOException(
          "Attempt to create an erasure coding zone for a " +
              "non-empty directory " + src);
    }
    if (srcIIP.getLastINode() != null &&
        !srcIIP.getLastINode().isDirectory()) {
      throw new IOException("Attempt to create an erasure coding zone " +
          "for a file " + src);
    }
    if (getErasureCodingPolicy(srcIIP) != null) {
      throw new IOException("Directory " + src + " is already in an " +
          "erasure coding zone.");
    }

    // System default erasure coding policy will be used since no specified.
    if (ecPolicy == null) {
      ecPolicy = ErasureCodingPolicyManager.getSystemDefaultPolicy();
    }

    final XAttr ecXAttr;
    DataOutputStream dOut = null;
    try {
      ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      dOut = new DataOutputStream(bOut);
      WritableUtils.writeString(dOut, ecPolicy.getName());
      ecXAttr = XAttrHelper.buildXAttr(XATTR_ERASURECODING_ZONE,
          bOut.toByteArray());
    } finally {
      IOUtils.closeStream(dOut);
    }
    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ecXAttr);
    FSDirXAttrOp.unprotectedSetXAttrs(dir, src, xattrs,
        EnumSet.of(XAttrSetFlag.CREATE));
    return xattrs;
  }

  void checkMoveValidity(INodesInPath srcIIP, INodesInPath dstIIP, String src)
      throws IOException {
    assert dir.hasReadLock();
    final ErasureCodingZone srcZone = getErasureCodingZone(srcIIP);
    final ErasureCodingZone dstZone = getErasureCodingZone(dstIIP);
    if (srcZone != null && srcZone.getDir().equals(src) && dstZone == null) {
      return;
    }
    final ErasureCodingPolicy srcECPolicy =
        srcZone != null ? srcZone.getErasureCodingPolicy() : null;
    final ErasureCodingPolicy dstECPolicy =
        dstZone != null ? dstZone.getErasureCodingPolicy() : null;
    if (srcECPolicy != null && !srcECPolicy.equals(dstECPolicy) ||
        dstECPolicy != null && !dstECPolicy.equals(srcECPolicy)) {
      throw new IOException(
          src + " can't be moved because the source and destination have " +
              "different erasure coding policies.");
    }
  }
}
