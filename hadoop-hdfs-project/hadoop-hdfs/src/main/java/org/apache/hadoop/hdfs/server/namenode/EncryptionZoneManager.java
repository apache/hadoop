package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;


import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants
    .CRYPTO_XATTR_ENCRYPTION_ZONE;

/**
 * Manages the list of encryption zones in the filesystem. Relies on the
 * FSDirectory lock for synchronization.
 */
public class EncryptionZoneManager {

  /**
   * EncryptionZoneInt is the internal representation of an encryption zone. The
   * external representation of an EZ is embodied in an EncryptionZone and
   * contains the EZ's pathname.
   */
  private class EncryptionZoneInt {
    private final String keyId;
    private final long inodeId;

    EncryptionZoneInt(long inodeId, String keyId) {
      this.keyId = keyId;
      this.inodeId = inodeId;
    }

    String getKeyId() {
      return keyId;
    }

    long getINodeId() {
      return inodeId;
    }

    String getFullPathName() {
      return dir.getInode(inodeId).getFullPathName();
    }
  }

  private final Map<Long, EncryptionZoneInt> encryptionZones;

  private final FSDirectory dir;

  /**
   * Construct a new EncryptionZoneManager.
   *
   * @param dir Enclosing FSDirectory
   */
  public EncryptionZoneManager(FSDirectory dir) {
    this.dir = dir;
    encryptionZones = new HashMap<Long, EncryptionZoneInt>();
  }

  /**
   * Add a new encryption zone.
   *
   * @param inodeId of the encryption zone
   * @param keyId   encryption zone key id
   */
  void addEncryptionZone(Long inodeId, String keyId) {
    final EncryptionZoneInt ez = new EncryptionZoneInt(inodeId, keyId);
    encryptionZones.put(inodeId, ez);
  }

  void removeEncryptionZone(Long inodeId) {
    encryptionZones.remove(inodeId);
  }

  /**
   * Returns true if an IIP is within an encryption zone.
   */
  boolean isInAnEZ(INodesInPath iip)
      throws UnresolvedLinkException, SnapshotAccessControlException {
    return (getEncryptionZoneForPath(iip) != null);
  }

  private EncryptionZoneInt getEncryptionZoneForPath(INodesInPath iip) {
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
   * Throws an exception if the provided inode cannot be renamed into the
   * destination because of differing encryption zones.
   *
   * @param srcIIP source IIP
   * @param dstIIP destination IIP
   * @param src    source path, used for debugging
   * @throws IOException if the src cannot be renamed to the dst
   */
  void checkMoveValidity(INodesInPath srcIIP, INodesInPath dstIIP, String src)
      throws IOException {
    final boolean srcInEZ = (getEncryptionZoneForPath(srcIIP) != null);
    final boolean dstInEZ = (getEncryptionZoneForPath(dstIIP) != null);
    if (srcInEZ) {
      if (!dstInEZ) {
        throw new IOException(src + " can't be moved from an encryption zone.");
      }
    } else {
      if (dstInEZ) {
        throw new IOException(src + " can't be moved into an encryption zone.");
      }
    }

    if (srcInEZ || dstInEZ) {
      final EncryptionZoneInt srcEZI = getEncryptionZoneForPath(srcIIP);
      final EncryptionZoneInt dstEZI = getEncryptionZoneForPath(dstIIP);
      Preconditions.checkArgument(srcEZI != null, "couldn't find src EZ?");
      Preconditions.checkArgument(dstEZI != null, "couldn't find dst EZ?");
      if (srcEZI != dstEZI) {
        final String srcEZPath = srcEZI.getFullPathName();
        final String dstEZPath = dstEZI.getFullPathName();
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

  XAttr createEncryptionZone(String src, String keyId) throws IOException {
    if (dir.isNonEmptyDirectory(src)) {
      throw new IOException(
          "Attempt to create an encryption zone for a non-empty directory.");
    }

    final INodesInPath srcIIP = dir.getINodesInPath4Write(src, false);
    final EncryptionZoneInt ezi = getEncryptionZoneForPath(srcIIP);
    if (ezi != null) {
      throw new IOException("Directory " + src +
          " is already in an encryption zone. (" + ezi.getFullPathName() + ")");
    }

    final XAttr keyIdXAttr =
        XAttrHelper.buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, keyId.getBytes());
    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(keyIdXAttr);
    final INode inode =
        dir.unprotectedSetXAttrs(src, xattrs, EnumSet.of(XAttrSetFlag.CREATE));
    addEncryptionZone(inode.getId(), keyId);
    return keyIdXAttr;
  }

  List<EncryptionZone> listEncryptionZones() throws IOException {
    final List<EncryptionZone> ret =
        Lists.newArrayListWithExpectedSize(encryptionZones.size());
    for (EncryptionZoneInt ezi : encryptionZones.values()) {
      ret.add(new EncryptionZone(ezi.getFullPathName(), ezi.getKeyId()));
    }
    return ret;
  }
}
