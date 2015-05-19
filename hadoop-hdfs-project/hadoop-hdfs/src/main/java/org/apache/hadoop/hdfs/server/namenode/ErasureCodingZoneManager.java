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
import org.apache.hadoop.hdfs.protocol.ErasureCodingZoneInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;

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

  ECSchema getECSchema(INodesInPath iip) throws IOException {
    ErasureCodingZoneInfo ecZoneInfo = getECZoneInfo(iip);
    return ecZoneInfo == null ? null : ecZoneInfo.getSchema();
  }

  ErasureCodingZoneInfo getECZoneInfo(INodesInPath iip) throws IOException {
    assert dir.hasReadLock();
    Preconditions.checkNotNull(iip);
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
          int cellSize = WritableUtils.readVInt(dIn);
          String schemaName = WritableUtils.readString(dIn);
          ECSchema schema = dir.getFSNamesystem().getECSchemaManager()
              .getSchema(schemaName);
          return new ErasureCodingZoneInfo(inode.getFullPathName(), schema,
              cellSize);
        }
      }
    }
    return null;
  }

  XAttr createErasureCodingZone(String src, ECSchema schema, int cellSize)
      throws IOException {
    assert dir.hasWriteLock();
    final INodesInPath srcIIP = dir.getINodesInPath4Write(src, false);
    if (dir.isNonEmptyDirectory(srcIIP)) {
      throw new IOException(
          "Attempt to create an erasure coding zone for a " +
              "non-empty directory.");
    }
    if (srcIIP != null &&
        srcIIP.getLastINode() != null &&
        !srcIIP.getLastINode().isDirectory()) {
      throw new IOException("Attempt to create an erasure coding zone " +
          "for a file.");
    }
    if (getECSchema(srcIIP) != null) {
      throw new IOException("Directory " + src + " is already in an " +
          "erasure coding zone.");
    }

    // System default schema will be used since no specified.
    if (schema == null) {
      schema = ErasureCodingSchemaManager.getSystemDefaultSchema();
    }

    if (cellSize <= 0) {
      cellSize = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
    }

    // Write the cellsize first and then schema name
    final XAttr ecXAttr;
    DataOutputStream dOut = null;
    try {
      ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      dOut = new DataOutputStream(bOut);
      WritableUtils.writeVInt(dOut, cellSize);
      // Now persist the schema name in xattr
      WritableUtils.writeString(dOut, schema.getSchemaName());
      ecXAttr = XAttrHelper.buildXAttr(XATTR_ERASURECODING_ZONE,
          bOut.toByteArray());
    } finally {
      IOUtils.closeStream(dOut);
    }
    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ecXAttr);
    FSDirXAttrOp.unprotectedSetXAttrs(dir, src, xattrs,
        EnumSet.of(XAttrSetFlag.CREATE));
    return ecXAttr;
  }

  void checkMoveValidity(INodesInPath srcIIP, INodesInPath dstIIP, String src)
      throws IOException {
    assert dir.hasReadLock();
    final ECSchema srcSchema = getECSchema(srcIIP);
    final ECSchema dstSchema = getECSchema(dstIIP);
    if ((srcSchema != null && !srcSchema.equals(dstSchema)) ||
        (dstSchema != null && !dstSchema.equals(srcSchema))) {
      throw new IOException(
          src + " can't be moved because the source and destination have " +
              "different erasure coding policies.");
    }
  }
}
