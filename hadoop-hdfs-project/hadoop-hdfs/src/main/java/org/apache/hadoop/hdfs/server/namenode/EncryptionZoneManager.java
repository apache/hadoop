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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_KEY;
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

  public static final Logger LOG = LoggerFactory.getLogger(EncryptionZoneManager
      .class);

  /**
   * EncryptionZoneInt is the internal representation of an encryption zone. The
   * external representation of an EZ is embodied in an EncryptionZone and
   * contains the EZ's pathname.
   */
  private static class EncryptionZoneInt {
    private final long inodeId;
    private final CipherSuite suite;
    private final CryptoProtocolVersion version;
    private final String keyName;

    EncryptionZoneInt(long inodeId, CipherSuite suite,
        CryptoProtocolVersion version, String keyName) {
      Preconditions.checkArgument(suite != CipherSuite.UNKNOWN);
      Preconditions.checkArgument(version != CryptoProtocolVersion.UNKNOWN);
      this.inodeId = inodeId;
      this.suite = suite;
      this.version = version;
      this.keyName = keyName;
    }

    long getINodeId() {
      return inodeId;
    }

    CipherSuite getSuite() {
      return suite;
    }

    CryptoProtocolVersion getVersion() { return version; }

    String getKeyName() {
      return keyName;
    }
  }

  private TreeMap<Long, EncryptionZoneInt> encryptionZones = null;
  private final FSDirectory dir;
  private final int maxListEncryptionZonesResponses;
  private final int maxListRecncryptionStatusResponses;

  private ThreadFactory reencryptionThreadFactory;
  private ExecutorService reencryptHandlerExecutor;
  private ReencryptionHandler reencryptionHandler;
  // Reencryption status is kept here to decouple status listing (which should
  // work as long as NN is up), with the actual handler (which only exists if
  // keyprovider exists)
  private final ReencryptionStatus reencryptionStatus;

  public static final BatchedListEntries<ZoneReencryptionStatus> EMPTY_LIST =
      new BatchedListEntries<>(new ArrayList<ZoneReencryptionStatus>(), false);

  @VisibleForTesting
  public void pauseReencryptForTesting() {
    reencryptionHandler.pauseForTesting();
  }

  @VisibleForTesting
  public void resumeReencryptForTesting() {
    reencryptionHandler.resumeForTesting();
  }

  @VisibleForTesting
  public void pauseForTestingAfterNthSubmission(final int count) {
    reencryptionHandler.pauseForTestingAfterNthSubmission(count);
  }

  @VisibleForTesting
  public void pauseReencryptUpdaterForTesting() {
    reencryptionHandler.pauseUpdaterForTesting();
  }

  @VisibleForTesting
  public void resumeReencryptUpdaterForTesting() {
    reencryptionHandler.resumeUpdaterForTesting();
  }

  @VisibleForTesting
  public void pauseForTestingAfterNthCheckpoint(final String zone,
      final int count) throws IOException {
    INodesInPath iip;
    final FSPermissionChecker pc = dir.getPermissionChecker();
    dir.readLock();
    try {
      iip = dir.resolvePath(pc, zone, DirOp.READ);
    } finally {
      dir.readUnlock();
    }
    reencryptionHandler
        .pauseForTestingAfterNthCheckpoint(iip.getLastINode().getId(), count);
  }

  @VisibleForTesting
  public void resetMetricsForTesting() {
    reencryptionStatus.resetMetrics();
  }

  @VisibleForTesting
  public ReencryptionStatus getReencryptionStatus() {
    return reencryptionStatus;
  }

  @VisibleForTesting
  public ZoneReencryptionStatus getZoneStatus(final String zone)
      throws IOException {
    final FSPermissionChecker pc = dir.getPermissionChecker();
    final INode inode;
    dir.getFSNamesystem().readLock();
    dir.readLock();
    try {
      final INodesInPath iip = dir.resolvePath(pc, zone, DirOp.READ);
      inode = iip.getLastINode();
      if (inode == null) {
        return null;
      }
      return getReencryptionStatus().getZoneStatus(inode.getId());
    } finally {
      dir.readUnlock();
      dir.getFSNamesystem().readUnlock();
    }
  }

  FSDirectory getFSDirectory() {
    return dir;
  }

  /**
   * Construct a new EncryptionZoneManager.
   *
   * @param dir Enclosing FSDirectory
   */
  public EncryptionZoneManager(FSDirectory dir, Configuration conf) {
    this.dir = dir;
    maxListEncryptionZonesResponses = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES_DEFAULT
    );
    Preconditions.checkArgument(maxListEncryptionZonesResponses >= 0,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES + " " +
            "must be a positive integer."
    );
    if (getProvider() != null) {
      reencryptionHandler = new ReencryptionHandler(this, conf);
      reencryptionThreadFactory = new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("reencryptionHandlerThread #%d").build();
    }
    maxListRecncryptionStatusResponses =
        conf.getInt(DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_KEY,
            DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_DEFAULT);
    Preconditions.checkArgument(maxListRecncryptionStatusResponses >= 0,
        DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_KEY +
            " must be a positive integer."
    );
    reencryptionStatus = new ReencryptionStatus();
  }

  KeyProviderCryptoExtension getProvider() {
    return dir.getProvider();
  }

  void startReencryptThreads() {
    if (getProvider() == null) {
      return;
    }
    Preconditions.checkNotNull(reencryptionHandler);
    reencryptHandlerExecutor =
        Executors.newSingleThreadExecutor(reencryptionThreadFactory);
    reencryptHandlerExecutor.execute(reencryptionHandler);
    reencryptionHandler.startUpdaterThread();
  }

  void stopReencryptThread() {
    if (getProvider() == null || reencryptionHandler == null) {
      return;
    }
    dir.writeLock();
    try {
      reencryptionHandler.stopThreads();
    } finally {
      dir.writeUnlock();
    }
    if (reencryptHandlerExecutor != null) {
      reencryptHandlerExecutor.shutdownNow();
      reencryptHandlerExecutor = null;
    }
  }

  /**
   * Add a new encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   *
   * @param inodeId of the encryption zone
   * @param keyName encryption zone key name
   */
  void addEncryptionZone(Long inodeId, CipherSuite suite,
      CryptoProtocolVersion version, String keyName) {
    assert dir.hasWriteLock();
    unprotectedAddEncryptionZone(inodeId, suite, version, keyName);
  }

  /**
   * Add a new encryption zone.
   * <p/>
   * Does not assume that the FSDirectory lock is held.
   *
   * @param inodeId of the encryption zone
   * @param keyName encryption zone key name
   */
  void unprotectedAddEncryptionZone(Long inodeId,
      CipherSuite suite, CryptoProtocolVersion version, String keyName) {
    final EncryptionZoneInt ez = new EncryptionZoneInt(
        inodeId, suite, version, keyName);
    if (encryptionZones == null) {
      encryptionZones = new TreeMap<>();
    }
    encryptionZones.put(inodeId, ez);
  }

  /**
   * Remove an encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  void removeEncryptionZone(Long inodeId) {
    assert dir.hasWriteLock();
    if (hasCreatedEncryptionZone()) {
      if (encryptionZones.remove(inodeId) == null
          || !getReencryptionStatus().hasRunningZone(inodeId)) {
        return;
      }
      if (reencryptionHandler != null) {
        reencryptionHandler.removeZone(inodeId);
      }
    }
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
   * Returns the full path from an INode id.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  String getFullPathName(Long nodeId) {
    assert dir.hasReadLock();
    INode inode = dir.getInode(nodeId);
    if (inode == null) {
      return null;
    }
    return inode.getFullPathName();
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
   * Called while holding the FSDirectory lock.
   */
  private EncryptionZoneInt getEncryptionZoneForPath(INodesInPath iip) {
    assert dir.hasReadLock();
    Preconditions.checkNotNull(iip);
    if (!hasCreatedEncryptionZone()) {
      return null;
    }
    for (int i = iip.length() - 1; i >= 0; i--) {
      final INode inode = iip.getINode(i);
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
   * Looks up the nearest ancestor EncryptionZoneInt that contains the given
   * path (excluding itself).
   * Returns null if path is not within an EZ, or the path is the root dir '/'
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  private EncryptionZoneInt getParentEncryptionZoneForPath(INodesInPath iip) {
    assert dir.hasReadLock();
    Preconditions.checkNotNull(iip);
    INodesInPath parentIIP = iip.getParentINodesInPath();
    return parentIIP == null ? null : getEncryptionZoneForPath(parentIIP);
  }

  /**
   * Returns an EncryptionZone representing the ez for a given path.
   * Returns an empty marker EncryptionZone if path is not in an ez.
   *
   * @param iip The INodesInPath of the path to check
   * @return the EncryptionZone representing the ez for the path.
   */
  EncryptionZone getEZINodeForPath(INodesInPath iip) {
    final EncryptionZoneInt ezi = getEncryptionZoneForPath(iip);
    if (ezi == null) {
      return null;
    } else {
      return new EncryptionZone(ezi.getINodeId(),
          getFullPathName(ezi.getINodeId()),
          ezi.getSuite(), ezi.getVersion(), ezi.getKeyName());
    }
  }

  /**
   * Throws an exception if the provided path cannot be renamed into the
   * destination because of differing parent encryption zones.
   * <p/>
   * Called while holding the FSDirectory lock.
   *
   * @param srcIIP source IIP
   * @param dstIIP destination IIP
   * @throws IOException if the src cannot be renamed to the dst
   */
  void checkMoveValidity(INodesInPath srcIIP, INodesInPath dstIIP)
      throws IOException {
    assert dir.hasReadLock();
    if (!hasCreatedEncryptionZone()) {
      return;
    }
    final EncryptionZoneInt srcParentEZI =
        getParentEncryptionZoneForPath(srcIIP);
    final EncryptionZoneInt dstParentEZI =
        getParentEncryptionZoneForPath(dstIIP);
    final boolean srcInEZ = (srcParentEZI != null);
    final boolean dstInEZ = (dstParentEZI != null);
    if (srcInEZ && !dstInEZ) {
      throw new IOException(
          srcIIP.getPath() + " can't be moved from an encryption zone.");
    } else if (dstInEZ && !srcInEZ) {
      throw new IOException(
          srcIIP.getPath() + " can't be moved into an encryption zone.");
    }

    if (srcInEZ) {
      if (srcParentEZI != dstParentEZI) {
        final String srcEZPath = getFullPathName(srcParentEZI.getINodeId());
        final String dstEZPath = getFullPathName(dstParentEZI.getINodeId());
        final StringBuilder sb = new StringBuilder(srcIIP.getPath());
        sb.append(" can't be moved from encryption zone ");
        sb.append(srcEZPath);
        sb.append(" to encryption zone ");
        sb.append(dstEZPath);
        sb.append(".");
        throw new IOException(sb.toString());
      }
      checkMoveValidityForReencryption(srcIIP.getPath(),
          srcParentEZI.getINodeId());
    } else if (dstInEZ) {
      checkMoveValidityForReencryption(dstIIP.getPath(),
          dstParentEZI.getINodeId());
    }
  }

  private void checkMoveValidityForReencryption(final String pathName,
      final long zoneId) throws IOException {
    assert dir.hasReadLock();
    final ZoneReencryptionStatus zs = reencryptionStatus.getZoneStatus(zoneId);
    if (zs != null && zs.getState() != ZoneReencryptionStatus.State.Completed) {
      final StringBuilder sb = new StringBuilder(pathName);
      sb.append(" can't be moved because encryption zone ");
      sb.append(getFullPathName(zoneId));
      sb.append(" is currently under re-encryption");
      throw new IOException(sb.toString());
    }
  }

  /**
   * Create a new encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  XAttr createEncryptionZone(INodesInPath srcIIP, CipherSuite suite,
      CryptoProtocolVersion version, String keyName)
      throws IOException {
    assert dir.hasWriteLock();

    // Check if src is a valid path for new EZ creation
    if (srcIIP.getLastINode() == null) {
      throw new FileNotFoundException("cannot find " + srcIIP.getPath());
    }
    if (dir.isNonEmptyDirectory(srcIIP)) {
      throw new IOException(
          "Attempt to create an encryption zone for a non-empty directory.");
    }

    INode srcINode = srcIIP.getLastINode();
    if (!srcINode.isDirectory()) {
      throw new IOException("Attempt to create an encryption zone for a file.");
    }

    if (hasCreatedEncryptionZone() && encryptionZones.
        get(srcINode.getId()) != null) {
      throw new IOException(
          "Directory " + srcIIP.getPath() + " is already an encryption zone.");
    }

    final HdfsProtos.ZoneEncryptionInfoProto proto =
        PBHelperClient.convert(suite, version, keyName);
    final XAttr ezXAttr = XAttrHelper
        .buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, proto.toByteArray());

    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ezXAttr);
    // updating the xattr will call addEncryptionZone,
    // done this way to handle edit log loading
    FSDirXAttrOp.unprotectedSetXAttrs(dir, srcIIP, xattrs,
                                      EnumSet.of(XAttrSetFlag.CREATE));
    return ezXAttr;
  }

  /**
   * Cursor-based listing of encryption zones.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  BatchedListEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    assert dir.hasReadLock();
    if (!hasCreatedEncryptionZone()) {
      return new BatchedListEntries<EncryptionZone>(Lists.newArrayList(), false);
    }
    NavigableMap<Long, EncryptionZoneInt> tailMap = encryptionZones.tailMap
        (prevId, false);
    final int numResponses = Math.min(maxListEncryptionZonesResponses,
        tailMap.size());
    final List<EncryptionZone> zones =
        Lists.newArrayListWithExpectedSize(numResponses);

    int count = 0;
    for (EncryptionZoneInt ezi : tailMap.values()) {
      /*
       Skip EZs that are only present in snapshots. Re-resolve the path to 
       see if the path's current inode ID matches EZ map's INode ID.

       INode#getFullPathName simply calls getParent recursively, so will return
       the INode's parents at the time it was snapshotted. It will not
       contain a reference INode.
      */
      final String pathName = getFullPathName(ezi.getINodeId());
      if (!pathResolvesToId(ezi.getINodeId(), pathName)) {
        continue;
      }
      // Add the EZ to the result list
      zones.add(new EncryptionZone(ezi.getINodeId(), pathName,
          ezi.getSuite(), ezi.getVersion(), ezi.getKeyName()));
      count++;
      if (count >= numResponses) {
        break;
      }
    }
    final boolean hasMore = (numResponses < tailMap.size());
    return new BatchedListEntries<EncryptionZone>(zones, hasMore);
  }

  /**
   * Resolves the path to inode id, then check if it's the same as the inode id
   * passed in. This is necessary to filter out zones in snapshots.
   * @param zoneId
   * @param zonePath
   * @return true if path resolve to the id, false if not.
   * @throws UnresolvedLinkException
   */
  private boolean pathResolvesToId(final long zoneId, final String zonePath)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    assert dir.hasReadLock();
    INode inode = dir.getInode(zoneId);
    if (inode == null) {
      return false;
    }
    INode lastINode = null;
    if (INode.isValidAbsolutePath(zonePath)) {
      INodesInPath iip = dir.getINodesInPath(zonePath, DirOp.READ_LINK);
      lastINode = iip.getLastINode();
    }
    if (lastINode == null || lastINode.getId() != zoneId) {
      return false;
    }
    return true;
  }

  /**
   * Re-encrypts the given encryption zone path. If the given path is not the
   * root of an encryption zone, an exception is thrown.
   */
  List<XAttr> reencryptEncryptionZone(final INodesInPath zoneIIP,
      final String keyVersionName) throws IOException {
    assert dir.hasWriteLock();
    if (reencryptionHandler == null) {
      throw new IOException("No key provider configured, re-encryption "
          + "operation is rejected");
    }
    final List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    final INode inode = zoneIIP.getLastINode();
    final String zoneName = zoneIIP.getPath();
    checkEncryptionZoneRoot(inode, zoneName);
    if (getReencryptionStatus().hasRunningZone(inode.getId())) {
      throw new IOException("Zone " + zoneName
          + " is already submitted for re-encryption.");
    }
    LOG.info("Zone {}({}) is submitted for re-encryption.", zoneName,
        inode.getId());
    final XAttr xattr = FSDirEncryptionZoneOp
        .updateReencryptionSubmitted(dir, zoneIIP, keyVersionName);
    xAttrs.add(xattr);
    reencryptionHandler.notifyNewSubmission();
    return xAttrs;
  }

  /**
   * Cancels the currently-running re-encryption of the given encryption zone.
   * If the given path is not the root of an encryption zone,
   * * an exception is thrown.
   */
  List<XAttr> cancelReencryptEncryptionZone(final INodesInPath zoneIIP)
      throws IOException {
    assert dir.hasWriteLock();
    if (reencryptionHandler == null) {
      throw new IOException("No key provider configured, re-encryption "
          + "operation is rejected");
    }
    final long zoneId = zoneIIP.getLastINode().getId();
    final String zoneName = zoneIIP.getPath();
    checkEncryptionZoneRoot(zoneIIP.getLastINode(), zoneName);
    reencryptionHandler.cancelZone(zoneId, zoneName);
    LOG.info("Cancelled zone {}({}) for re-encryption.", zoneName, zoneId);
    return FSDirEncryptionZoneOp.updateReencryptionFinish(dir, zoneIIP,
        reencryptionStatus.getZoneStatus(zoneId));
  }

  /**
   * Cursor-based listing of zone re-encryption status.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  BatchedListEntries<ZoneReencryptionStatus> listReencryptionStatus(
      final long prevId) throws IOException {
    assert dir.hasReadLock();
    if (!hasCreatedEncryptionZone()) {
      return ReencryptionStatus.EMPTY_LIST;
    }

    NavigableMap<Long, ZoneReencryptionStatus> stats =
        reencryptionStatus.getZoneStatuses();

    if (stats.isEmpty()) {
      return EMPTY_LIST;
    }

    NavigableMap<Long, ZoneReencryptionStatus> tailMap =
        stats.tailMap(prevId, false);
    final int numResp =
        Math.min(maxListRecncryptionStatusResponses, tailMap.size());
    final List<ZoneReencryptionStatus> ret =
        Lists.newArrayListWithExpectedSize(numResp);
    int count = 0;
    for (ZoneReencryptionStatus zs : tailMap.values()) {
      final String name = getFullPathName(zs.getId());
      if (name == null || !pathResolvesToId(zs.getId(), name)) {
        continue;
      }
      zs.setZoneName(name);
      ret.add(zs);
      ++count;
      if (count >= numResp) {
        break;
      }
    }
    final boolean hasMore = (numResp < tailMap.size());
    return new BatchedListEntries<>(ret, hasMore);
  }

  /**
   * Return whether an INode is an encryption zone root.
   */
  boolean isEncryptionZoneRoot(final INode inode, final String name)
      throws FileNotFoundException {
    assert dir.hasReadLock();
    if (inode == null) {
      throw new FileNotFoundException("INode does not exist for " + name);
    }
    if (!inode.isDirectory()) {
      return false;
    }
    if (!hasCreatedEncryptionZone()
        || !encryptionZones.containsKey(inode.getId())) {
      return false;
    }
    return true;
  }

  /**
   * Return whether an INode is an encryption zone root.
   *
   * @param inode the zone inode
   * @throws IOException if the inode is not a directory,
   *                     or is a directory but not the root of an EZ.
   */
  void checkEncryptionZoneRoot(final INode inode, final String name)
      throws IOException {
    if (!isEncryptionZoneRoot(inode, name)) {
      throw new IOException("Path " + name + " is not the root of an"
          + " encryption zone.");
    }
  }

  /**
   * @return number of encryption zones.
   */
  public int getNumEncryptionZones() {
    return hasCreatedEncryptionZone() ?
        encryptionZones.size() : 0;
  }

  /**
   * @return Whether there has been any attempt to create an encryption zone in
   * the cluster at all. If not, it is safe to quickly return null when
   * checking the encryption information of any file or directory in the
   * cluster.
   */
  public boolean hasCreatedEncryptionZone() {
    return encryptionZones != null;
  }

  /**
   * @return a list of all key names.
   */
  String[] getKeyNames() {
    assert dir.hasReadLock();
    if (!hasCreatedEncryptionZone()) {
      return new String[0];
    }
    String[] ret = new String[encryptionZones.size()];
    int index = 0;
    for (Map.Entry<Long, EncryptionZoneInt> entry : encryptionZones
        .entrySet()) {
      ret[index++] = entry.getValue().getKeyName();
    }
    return ret;
  }
}
