package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
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

  /**
   * EncryptionZoneInt is the internal representation of an encryption zone. The
   * external representation of an EZ is embodied in an EncryptionZone and
   * contains the EZ's pathname.
   */
  private class EncryptionZoneInt {
    private final String keyId;
    private final long inodeId;

    private final HashSet<KeyVersion> keyVersions;
    private KeyVersion latestVersion;

    EncryptionZoneInt(long inodeId, String keyId) {
      this.keyId = keyId;
      this.inodeId = inodeId;
      keyVersions = Sets.newHashSet();
      latestVersion = null;
    }

    KeyVersion getLatestKeyVersion() {
      return latestVersion;
    }

    void addKeyVersion(KeyVersion version) {
      Preconditions.checkNotNull(version);
      if (!keyVersions.contains(version)) {
        LOG.debug("Key {} has new key version {}", keyId, version);
        keyVersions.add(version);
      }
      // Always set the latestVersion to not get stuck on an old version in
      // racy situations. Should eventually converge thanks to the
      // monitor.
      latestVersion = version;
    }

    String getKeyId() {
      return keyId;
    }

    long getINodeId() {
      return inodeId;
    }

  }

  /**
   * Protects the <tt>encryptionZones</tt> map and its contents.
   */
  private final ReentrantReadWriteLock lock;

  private void readLock() {
    lock.readLock().lock();
  }

  private void readUnlock() {
    lock.readLock().unlock();
  }

  private void writeLock() {
    lock.writeLock().lock();
  }

  private void writeUnlock() {
    lock.writeLock().unlock();
  }

  public boolean hasWriteLock() {
    return lock.isWriteLockedByCurrentThread();
  }

  public boolean hasReadLock() {
    return lock.getReadHoldCount() > 0 || hasWriteLock();
  }

  private final Map<Long, EncryptionZoneInt> encryptionZones;
  private final FSDirectory dir;
  private final ScheduledExecutorService monitor;
  private final KeyProvider provider;

  /**
   * Construct a new EncryptionZoneManager.
   *
   * @param dir Enclosing FSDirectory
   */
  public EncryptionZoneManager(FSDirectory dir, Configuration conf,
      KeyProvider provider) {

    this.dir = dir;
    this.provider = provider;
    lock = new ReentrantReadWriteLock();
    encryptionZones = new HashMap<Long, EncryptionZoneInt>();

    monitor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(EncryptionZoneMonitor.class.getSimpleName() + "-%d")
            .build());
    final int refreshMs = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_KEY_VERSION_REFRESH_INTERVAL_MS_KEY,
        DFSConfigKeys.DFS_NAMENODE_KEY_VERSION_REFRESH_INTERVAL_MS_DEFAULT
    );
    Preconditions.checkArgument(refreshMs >= 0, "%s cannot be negative",
        DFSConfigKeys.DFS_NAMENODE_KEY_VERSION_REFRESH_INTERVAL_MS_KEY);
    monitor.scheduleAtFixedRate(new EncryptionZoneMonitor(), 0, refreshMs,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Periodically wakes up to fetch the latest version of each encryption
   * zone key.
   */
  private class EncryptionZoneMonitor implements Runnable {
    @Override
    public void run() {
      LOG.debug("Monitor waking up to refresh encryption zone key versions");
      HashMap<Long, String> toFetch = Maps.newHashMap();
      HashMap<Long, KeyVersion> toUpdate =
          Maps.newHashMap();
      // Determine the keyIds to fetch
      readLock();
      try {
        for (EncryptionZoneInt ezi : encryptionZones.values()) {
          toFetch.put(ezi.getINodeId(), ezi.getKeyId());
        }
      } finally {
        readUnlock();
      }
      LOG.trace("Found {} keys to check", toFetch.size());
      // Fetch the key versions while not holding the lock
      for (Map.Entry<Long, String> entry : toFetch.entrySet()) {
        try {
          KeyVersion version = provider.getCurrentKey(entry.getValue());
          toUpdate.put(entry.getKey(), version);
        } catch (IOException e) {
          LOG.warn("Error while getting the current key for {} {}",
              entry.getValue(), e);
        }
      }
      LOG.trace("Fetched {} key versions from KeyProvider", toUpdate.size());
      // Update the key versions for each encryption zone
      writeLock();
      try {
        for (Map.Entry<Long, KeyVersion> entry : toUpdate.entrySet()) {
          EncryptionZoneInt ezi = encryptionZones.get(entry.getKey());
          // zone might have been removed in the intervening time
          if (ezi == null) {
            continue;
          }
          ezi.addKeyVersion(entry.getValue());
        }
      } finally {
        writeUnlock();
      }
    }
  }

  /**
   * Forces the EncryptionZoneMonitor to run, waiting until completion.
   */
  @VisibleForTesting
  public void kickMonitor() throws Exception {
    Future future = monitor.submit(new EncryptionZoneMonitor());
    future.get();
  }

  /**
   * Immediately fetches the latest KeyVersion for an encryption zone,
   * also updating the encryption zone.
   *
   * @param iip of the encryption zone
   * @return latest KeyVersion
   * @throws IOException on KeyProvider error
   */
  KeyVersion updateLatestKeyVersion(INodesInPath iip) throws IOException {
    EncryptionZoneInt ezi;
    readLock();
    try {
      ezi = getEncryptionZoneForPath(iip);
    } finally {
      readUnlock();
    }
    if (ezi == null) {
      throw new IOException("Cannot update KeyVersion since iip is not within" +
          " an encryption zone");
    }

    // Do not hold the lock while doing KeyProvider operations
    KeyVersion version = provider.getCurrentKey(ezi.getKeyId());

    writeLock();
    try {
      ezi.addKeyVersion(version);
      return version;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Add a new encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   *
   * @param inodeId of the encryption zone
   * @param keyId   encryption zone key id
   */
  void addEncryptionZone(Long inodeId, String keyId) {
    assert dir.hasWriteLock();
    final EncryptionZoneInt ez = new EncryptionZoneInt(inodeId, keyId);
    writeLock();
    try {
      encryptionZones.put(inodeId, ez);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Remove an encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  void removeEncryptionZone(Long inodeId) {
    assert dir.hasWriteLock();
    writeLock();
    try {
      encryptionZones.remove(inodeId);
    } finally {
      writeUnlock();
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
    readLock();
    try {
      return (getEncryptionZoneForPath(iip) != null);
    } finally {
      readUnlock();
    }
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

  KeyVersion getLatestKeyVersion(final INodesInPath iip) {
    readLock();
    try {
      EncryptionZoneInt ezi = getEncryptionZoneForPath(iip);
      if (ezi == null) {
        return null;
      }
      return ezi.getLatestKeyVersion();
    } finally {
      readUnlock();
    }
  }

  /**
   * @return true if the provided <tt>keyVersionName</tt> is the name of a
   * valid KeyVersion for the encryption zone of <tt>iip</tt>,
   * and <tt>iip</tt> is within an encryption zone.
   */
  boolean isValidKeyVersion(final INodesInPath iip, String keyVersionName) {
    readLock();
    try {
      EncryptionZoneInt ezi = getEncryptionZoneForPath(iip);
      if (ezi == null) {
        return false;
      }
      for (KeyVersion ezVersion : ezi.keyVersions) {
        if (keyVersionName.equals(ezVersion.getVersionName())) {
          return true;
        }
      }
      return false;
    } finally {
      readUnlock();
    }
  }

  /**
   * Looks up the EncryptionZoneInt for a path within an encryption zone.
   * Returns null if path is not within an EZ.
   * <p/>
   * Must be called while holding the manager lock.
   */
  private EncryptionZoneInt getEncryptionZoneForPath(INodesInPath iip) {
    assert hasReadLock();
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
    readLock();
    try {
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
    } finally {
      readUnlock();
    }
  }

  /**
   * Create a new encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  XAttr createEncryptionZone(String src, String keyId, KeyVersion keyVersion)
      throws IOException {
    assert dir.hasWriteLock();
    writeLock();
    try {
      if (dir.isNonEmptyDirectory(src)) {
        throw new IOException(
            "Attempt to create an encryption zone for a non-empty directory.");
      }

      final INodesInPath srcIIP = dir.getINodesInPath4Write(src, false);
      EncryptionZoneInt ezi = getEncryptionZoneForPath(srcIIP);
      if (ezi != null) {
        throw new IOException("Directory " + src + " is already in an " +
            "encryption zone. (" + getFullPathName(ezi) + ")");
      }

      final XAttr keyIdXAttr = XAttrHelper
          .buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, keyId.getBytes());

      final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
      xattrs.add(keyIdXAttr);
      // updating the xattr will call addEncryptionZone,
      // done this way to handle edit log loading
      dir.unprotectedSetXAttrs(src, xattrs, EnumSet.of(XAttrSetFlag.CREATE));
      // Re-get the new encryption zone add the latest key version
      ezi = getEncryptionZoneForPath(srcIIP);
      ezi.addKeyVersion(keyVersion);
      return keyIdXAttr;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Return the current list of encryption zones.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  List<EncryptionZone> listEncryptionZones() throws IOException {
    assert dir.hasReadLock();
    readLock();
    try {
      final List<EncryptionZone> ret =
          Lists.newArrayListWithExpectedSize(encryptionZones.size());
      for (EncryptionZoneInt ezi : encryptionZones.values()) {
        ret.add(new EncryptionZone(getFullPathName(ezi), ezi.getKeyId()));
      }
      return ret;
    } finally {
      readUnlock();
    }
  }
}
