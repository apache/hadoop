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
import java.util.EnumSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.EncryptionZoneWithId;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants
    .CRYPTO_XATTR_ENCRYPTION_ZONE;

/**
 * Manages the list of encryption zones in the filesystem.
 * <p/>
 * The EncryptionZoneManager has its own lock, but relies on the FSDirectory
 * lock being held for many operations. The FSDirectory lock should not be
 * taken if the manager lock is already held.
 */
public class EncryptionZoneManager {

  public static Logger LOG = LoggerFactory.getLogger(EncryptionZoneManager
      .class);

  private static final EncryptionZoneWithId NULL_EZ =
      new EncryptionZoneWithId("", "", -1);

  /**
   * EncryptionZoneInt is the internal representation of an encryption zone. The
   * external representation of an EZ is embodied in an EncryptionZone and
   * contains the EZ's pathname.
   */
  private static class EncryptionZoneInt {
    private final String keyName;
    private final long inodeId;

    EncryptionZoneInt(long inodeId, String keyName) {
      this.keyName = keyName;
      this.inodeId = inodeId;
    }

    String getKeyName() {
      return keyName;
    }

    long getINodeId() {
      return inodeId;
    }
  }

  private final TreeMap<Long, EncryptionZoneInt> encryptionZones;
  private final FSDirectory dir;
  private final int maxListEncryptionZonesResponses;

  /**
   * Construct a new EncryptionZoneManager.
   *
   * @param dir Enclosing FSDirectory
   */
  public EncryptionZoneManager(FSDirectory dir, Configuration conf) {
    this.dir = dir;
    encryptionZones = new TreeMap<Long, EncryptionZoneInt>();
    maxListEncryptionZonesResponses = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES_DEFAULT
    );
    Preconditions.checkArgument(maxListEncryptionZonesResponses >= 0,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES + " " +
            "must be a positive integer."
    );
  }

  /**
   * Add a new encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   *
   * @param inodeId of the encryption zone
   * @param keyName encryption zone key name
   */
  void addEncryptionZone(Long inodeId, String keyName) {
    assert dir.hasWriteLock();
    final EncryptionZoneInt ez = new EncryptionZoneInt(inodeId, keyName);
    encryptionZones.put(inodeId, ez);
  }

  /**
   * Remove an encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  void removeEncryptionZone(Long inodeId) {
    assert dir.hasWriteLock();
    encryptionZones.remove(inodeId);
  }

  /**
   * Returns true if an IIP is within an encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  boolean isInAnEZ(INodesInPath iip)
      throws UnresolvedLinkException, SnapshotAccessControlException {
    assert dir.hasReadLock();
    return (getEncryptionZoneForPath(iip) != null);
  }

  /**
   * Returns the path of the EncryptionZoneInt.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  private String getFullPathName(EncryptionZoneInt ezi) {
    assert dir.hasReadLock();
    return dir.getInode(ezi.getINodeId()).getFullPathName();
  }

  /**
   * Get the key name for an encryption zone. Returns null if <tt>iip</tt> is
   * not within an encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  String getKeyName(final INodesInPath iip) {
    assert dir.hasReadLock();
    EncryptionZoneInt ezi = getEncryptionZoneForPath(iip);
    if (ezi == null) {
      return null;
    }
    return ezi.getKeyName();
  }

  /**
   * Looks up the EncryptionZoneInt for a path within an encryption zone.
   * Returns null if path is not within an EZ.
   * <p/>
   * Must be called while holding the manager lock.
   */
  private EncryptionZoneInt getEncryptionZoneForPath(INodesInPath iip) {
    assert dir.hasReadLock();
    Preconditions.checkNotNull(iip);
    final INode[] inodes = iip.getINodes();
    for (int i = inodes.length - 1; i >= 0; i--) {
      final INode inode = inodes[i];
      if (inode != null) {
        final EncryptionZoneInt ezi = encryptionZones.get(inode.getId());
        if (ezi != null) {
          return ezi;
        }
      }
    }
    return null;
  }

  /**
   * Returns an EncryptionZoneWithId representing the ez for a given path.
   * Returns an empty marker EncryptionZoneWithId if path is not in an ez.
   *
   * @param iip The INodesInPath of the path to check
   * @return the EncryptionZoneWithId representing the ez for the path.
   */
  EncryptionZoneWithId getEZINodeForPath(INodesInPath iip) {
    final EncryptionZoneInt ezi = getEncryptionZoneForPath(iip);
    if (ezi == null) {
      return NULL_EZ;
    } else {
      return new EncryptionZoneWithId(getFullPathName(ezi), ezi.getKeyName(),
          ezi.getINodeId());
    }
  }

  /**
   * Throws an exception if the provided path cannot be renamed into the
   * destination because of differing encryption zones.
   * <p/>
   * Called while holding the FSDirectory lock.
   *
   * @param srcIIP source IIP
   * @param dstIIP destination IIP
   * @param src    source path, used for debugging
   * @throws IOException if the src cannot be renamed to the dst
   */
  void checkMoveValidity(INodesInPath srcIIP, INodesInPath dstIIP, String src)
      throws IOException {
    assert dir.hasReadLock();
    final EncryptionZoneInt srcEZI = getEncryptionZoneForPath(srcIIP);
    final EncryptionZoneInt dstEZI = getEncryptionZoneForPath(dstIIP);
    final boolean srcInEZ = (srcEZI != null);
    final boolean dstInEZ = (dstEZI != null);
    if (srcInEZ) {
      if (!dstInEZ) {
        throw new IOException(
            src + " can't be moved from an encryption zone.");
      }
    } else {
      if (dstInEZ) {
        throw new IOException(
            src + " can't be moved into an encryption zone.");
      }
    }

    if (srcInEZ || dstInEZ) {
      Preconditions.checkState(srcEZI != null, "couldn't find src EZ?");
      Preconditions.checkState(dstEZI != null, "couldn't find dst EZ?");
      if (srcEZI != dstEZI) {
        final String srcEZPath = getFullPathName(srcEZI);
        final String dstEZPath = getFullPathName(dstEZI);
        final StringBuilder sb = new StringBuilder(src);
        sb.append(" can't be moved from encryption zone ");
        sb.append(srcEZPath);
        sb.append(" to encryption zone ");
        sb.append(dstEZPath);
        sb.append(".");
        throw new IOException(sb.toString());
      }
    }
  }

  /**
   * Create a new encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  XAttr createEncryptionZone(String src, String keyName)
      throws IOException {
    assert dir.hasWriteLock();
    if (dir.isNonEmptyDirectory(src)) {
      throw new IOException(
          "Attempt to create an encryption zone for a non-empty directory.");
    }

    final INodesInPath srcIIP = dir.getINodesInPath4Write(src, false);
    if (srcIIP != null &&
        srcIIP.getLastINode() != null &&
        !srcIIP.getLastINode().isDirectory()) {
      throw new IOException("Attempt to create an encryption zone for a file.");
    }
    EncryptionZoneInt ezi = getEncryptionZoneForPath(srcIIP);
    if (ezi != null) {
      throw new IOException("Directory " + src + " is already in an " +
          "encryption zone. (" + getFullPathName(ezi) + ")");
    }

    final XAttr ezXAttr = XAttrHelper
        .buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, keyName.getBytes());

    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ezXAttr);
    // updating the xattr will call addEncryptionZone,
    // done this way to handle edit log loading
    dir.unprotectedSetXAttrs(src, xattrs, EnumSet.of(XAttrSetFlag.CREATE));
    return ezXAttr;
  }

  /**
   * Cursor-based listing of encryption zones.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  BatchedListEntries<EncryptionZoneWithId> listEncryptionZones(long prevId)
      throws IOException {
    assert dir.hasReadLock();
    NavigableMap<Long, EncryptionZoneInt> tailMap = encryptionZones.tailMap
        (prevId, false);
    final int numResponses = Math.min(maxListEncryptionZonesResponses,
        tailMap.size());
    final List<EncryptionZoneWithId> zones =
        Lists.newArrayListWithExpectedSize(numResponses);

    int count = 0;
    for (EncryptionZoneInt ezi : tailMap.values()) {
      zones.add(new EncryptionZoneWithId(getFullPathName(ezi),
          ezi.getKeyName(), ezi.getINodeId()));
      count++;
      if (count >= numResponses) {
        break;
      }
    }
    final boolean hasMore = (numResponses < tailMap.size());
    return new BatchedListEntries<EncryptionZoneWithId>(zones, hasMore);
  }
}
