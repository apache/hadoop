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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_FILE_ENCRYPTION_INFO;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.concurrent.ExecutorService;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.security.SecurityUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Helper class to perform encryption zone operation.
 */
final class FSDirEncryptionZoneOp {

  /**
   * Private constructor for preventing FSDirEncryptionZoneOp object creation.
   * Static-only class.
   */
  private FSDirEncryptionZoneOp() {}

  /**
   * Invoke KeyProvider APIs to generate an encrypted data encryption key for
   * an encryption zone. Should not be called with any locks held.
   *
   * @param fsd fsdirectory
   * @param ezKeyName key name of an encryption zone
   * @return New EDEK, or null if ezKeyName is null
   * @throws IOException
   */
  private static EncryptedKeyVersion generateEncryptedDataEncryptionKey(
      final FSDirectory fsd, final String ezKeyName) throws IOException {
    // must not be holding lock during this operation
    assert !fsd.getFSNamesystem().hasReadLock();
    assert !fsd.getFSNamesystem().hasWriteLock();
    if (ezKeyName == null) {
      return null;
    }
    long generateEDEKStartTime = monotonicNow();
    // Generate EDEK with login user (hdfs) so that KMS does not need
    // an extra proxy configuration allowing hdfs to proxy its clients and
    // KMS does not need configuration to allow non-hdfs user GENERATE_EEK
    // operation.
    EncryptedKeyVersion edek = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<EncryptedKeyVersion>() {
          @Override
          public EncryptedKeyVersion run() throws IOException {
            try {
              return fsd.getProvider().generateEncryptedKey(ezKeyName);
            } catch (GeneralSecurityException e) {
              throw new IOException(e);
            }
          }
        });
    long generateEDEKTime = monotonicNow() - generateEDEKStartTime;
    NameNode.getNameNodeMetrics().addGenerateEDEKTime(generateEDEKTime);
    Preconditions.checkNotNull(edek);
    return edek;
  }

  static KeyProvider.Metadata ensureKeyIsInitialized(final FSDirectory fsd,
      final String keyName, final String src) throws IOException {
    KeyProviderCryptoExtension provider = fsd.getProvider();
    if (provider == null) {
      throw new IOException("Can't create an encryption zone for " + src
          + " since no key provider is available.");
    }
    if (keyName == null || keyName.isEmpty()) {
      throw new IOException("Must specify a key name when creating an "
          + "encryption zone");
    }
    KeyProvider.Metadata metadata = provider.getMetadata(keyName);
    if (metadata == null) {
      /*
       * It would be nice if we threw something more specific than
       * IOException when the key is not found, but the KeyProvider API
       * doesn't provide for that. If that API is ever changed to throw
       * something more specific (e.g. UnknownKeyException) then we can
       * update this to match it, or better yet, just rethrow the
       * KeyProvider's exception.
       */
      throw new IOException("Key " + keyName + " doesn't exist.");
    }
    // If the provider supports pool for EDEKs, this will fill in the pool
    provider.warmUpEncryptedKeys(keyName);
    return metadata;
  }

  /**
   * Create an encryption zone on directory path using the specified key.
   *
   * @param fsd fsdirectory
   * @param srcArg the path of a directory which will be the root of the
   *               encryption zone. The directory must be empty
   * @param pc permission checker to check fs permission
   * @param cipher cipher
   * @param keyName name of a key which must be present in the configured
   *                KeyProvider
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @return HdfsFileStatus
   * @throws IOException
   */
  static HdfsFileStatus createEncryptionZone(final FSDirectory fsd,
      final String srcArg, final FSPermissionChecker pc, final String cipher,
      final String keyName, final boolean logRetryCache) throws IOException {
    final CipherSuite suite = CipherSuite.convert(cipher);
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    // For now this is hard coded, as we only support one method.
    final CryptoProtocolVersion version =
        CryptoProtocolVersion.ENCRYPTION_ZONES;

    final INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, srcArg, DirOp.WRITE);
      final XAttr ezXAttr = fsd.ezManager.createEncryptionZone(iip, suite,
          version, keyName);
      xAttrs.add(ezXAttr);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetXAttrs(iip.getPath(), xAttrs, logRetryCache);
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * Get the encryption zone for the specified path.
   *
   * @param fsd fsdirectory
   * @param srcArg the path of a file or directory to get the EZ for
   * @param pc permission checker to check fs permission
   * @return the EZ with file status.
   */
  static Map.Entry<EncryptionZone, HdfsFileStatus> getEZForPath(
      final FSDirectory fsd, final String srcArg, final FSPermissionChecker pc)
      throws IOException {
    final INodesInPath iip;
    final EncryptionZone ret;
    fsd.readLock();
    try {
      iip = fsd.resolvePath(pc, srcArg, DirOp.READ);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      ret = fsd.ezManager.getEZINodeForPath(iip);
    } finally {
      fsd.readUnlock();
    }
    HdfsFileStatus auditStat = fsd.getAuditFileInfo(iip);
    return new AbstractMap.SimpleImmutableEntry<>(ret, auditStat);
  }

  static EncryptionZone getEZForPath(final FSDirectory fsd,
      final INodesInPath iip) {
    fsd.readLock();
    try {
      return fsd.ezManager.getEZINodeForPath(iip);
    } finally {
      fsd.readUnlock();
    }
  }

  static BatchedListEntries<EncryptionZone> listEncryptionZones(
      final FSDirectory fsd, final long prevId) throws IOException {
    fsd.readLock();
    try {
      return fsd.ezManager.listEncryptionZones(prevId);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * Set the FileEncryptionInfo for an INode.
   *
   * @param fsd fsdirectory
   * @param src the path of a directory which will be the root of the
   *            encryption zone.
   * @param info file encryption information
   * @throws IOException
   */
  static void setFileEncryptionInfo(final FSDirectory fsd,
      final INodesInPath iip, final FileEncryptionInfo info)
          throws IOException {
    // Make the PB for the xattr
    final HdfsProtos.PerFileEncryptionInfoProto proto =
        PBHelperClient.convertPerFileEncInfo(info);
    final byte[] protoBytes = proto.toByteArray();
    final XAttr fileEncryptionAttr =
        XAttrHelper.buildXAttr(CRYPTO_XATTR_FILE_ENCRYPTION_INFO, protoBytes);
    final List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(fileEncryptionAttr);
    fsd.writeLock();
    try {
      FSDirXAttrOp.unprotectedSetXAttrs(fsd, iip, xAttrs,
                                        EnumSet.of(XAttrSetFlag.CREATE));
    } finally {
      fsd.writeUnlock();
    }
  }

  /**
   * This function combines the per-file encryption info (obtained
   * from the inode's XAttrs), and the encryption info from its zone, and
   * returns a consolidated FileEncryptionInfo instance. Null is returned
   * for non-encrypted or raw files.
   *
   * @param fsd fsdirectory
   * @param iip inodes in the path containing the file, passed in to
   *            avoid obtaining the list of inodes again
   * @return consolidated file encryption info; null for non-encrypted files
   */
  static FileEncryptionInfo getFileEncryptionInfo(final FSDirectory fsd,
      final INodesInPath iip) throws IOException {
    if (iip.isRaw() ||
        !fsd.ezManager.hasCreatedEncryptionZone() ||
        !iip.getLastINode().isFile()) {
      return null;
    }
    fsd.readLock();
    try {
      EncryptionZone encryptionZone = getEZForPath(fsd, iip);
      if (encryptionZone == null) {
        // not an encrypted file
        return null;
      } else if(encryptionZone.getPath() == null
          || encryptionZone.getPath().isEmpty()) {
        if (NameNode.LOG.isDebugEnabled()) {
          NameNode.LOG.debug("Encryption zone " +
              encryptionZone.getPath() + " does not have a valid path.");
        }
      }

      final CryptoProtocolVersion version = encryptionZone.getVersion();
      final CipherSuite suite = encryptionZone.getSuite();
      final String keyName = encryptionZone.getKeyName();
      XAttr fileXAttr = FSDirXAttrOp.unprotectedGetXAttrByPrefixedName(
          iip, CRYPTO_XATTR_FILE_ENCRYPTION_INFO);

      if (fileXAttr == null) {
        NameNode.LOG.warn("Could not find encryption XAttr for file " +
            iip.getPath() + " in encryption zone " + encryptionZone.getPath());
        return null;
      }
      try {
        HdfsProtos.PerFileEncryptionInfoProto fileProto =
            HdfsProtos.PerFileEncryptionInfoProto.parseFrom(
                fileXAttr.getValue());
        return PBHelperClient.convert(fileProto, suite, version, keyName);
      } catch (InvalidProtocolBufferException e) {
        throw new IOException("Could not parse file encryption info for " +
            "inode " + iip.getPath(), e);
      }
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * If the file and encryption key are valid, return the encryption info,
   * else throw a retry exception.  The startFile method generates the EDEK
   * outside of the lock so the zone must be reverified.
   *
   * @param dir fsdirectory
   * @param iip inodes in the file path
   * @param ezInfo the encryption key
   * @return FileEncryptionInfo for the file
   * @throws RetryStartFileException if key is inconsistent with current zone
   */
  static FileEncryptionInfo getFileEncryptionInfo(FSDirectory dir,
      INodesInPath iip, EncryptionKeyInfo ezInfo)
          throws RetryStartFileException {
    FileEncryptionInfo feInfo = null;
    final EncryptionZone zone = getEZForPath(dir, iip);
    if (zone != null) {
      // The path is now within an EZ, but we're missing encryption parameters
      if (ezInfo == null) {
        throw new RetryStartFileException();
      }
      // Path is within an EZ and we have provided encryption parameters.
      // Make sure that the generated EDEK matches the settings of the EZ.
      final String ezKeyName = zone.getKeyName();
      if (!ezKeyName.equals(ezInfo.edek.getEncryptionKeyName())) {
        throw new RetryStartFileException();
      }
      feInfo = new FileEncryptionInfo(ezInfo.suite, ezInfo.protocolVersion,
          ezInfo.edek.getEncryptedKeyVersion().getMaterial(),
          ezInfo.edek.getEncryptedKeyIv(),
          ezKeyName, ezInfo.edek.getEncryptionKeyVersionName());
    }
    return feInfo;
  }

  static boolean isInAnEZ(final FSDirectory fsd, final INodesInPath iip)
      throws UnresolvedLinkException, SnapshotAccessControlException {
    if (!fsd.ezManager.hasCreatedEncryptionZone()) {
      return false;
    }
    fsd.readLock();
    try {
      return fsd.ezManager.isInAnEZ(iip);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * Proactively warm up the edek cache. We'll get all the edek key names,
   * then launch up a separate thread to warm them up.
   */
  static void warmUpEdekCache(final ExecutorService executor,
      final FSDirectory fsd, final int delay, final int interval) {
    fsd.readLock();
    try {
      String[] edeks  = fsd.ezManager.getKeyNames();
      executor.execute(
          new EDEKCacheLoader(edeks, fsd.getProvider(), delay, interval));
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * EDEKCacheLoader is being run in a separate thread to loop through all the
   * EDEKs and warm them up in the KMS cache.
   */
  static class EDEKCacheLoader implements Runnable {
    private final String[] keyNames;
    private final KeyProviderCryptoExtension kp;
    private int initialDelay;
    private int retryInterval;

    EDEKCacheLoader(final String[] names, final KeyProviderCryptoExtension kp,
        final int delay, final int interval) {
      this.keyNames = names;
      this.kp = kp;
      this.initialDelay = delay;
      this.retryInterval = interval;
    }

    @Override
    public void run() {
      NameNode.LOG.info("Warming up {} EDEKs... (initialDelay={}, "
          + "retryInterval={})", keyNames.length, initialDelay, retryInterval);
      try {
        Thread.sleep(initialDelay);
      } catch (InterruptedException ie) {
        NameNode.LOG.info("EDEKCacheLoader interrupted before warming up.");
        return;
      }

      final int logCoolDown = 10000; // periodically print error log (if any)
      int sinceLastLog = logCoolDown; // always print the first failure
      boolean success = false;
      IOException lastSeenIOE = null;
      long warmUpEDEKStartTime = monotonicNow();
      while (true) {
        try {
          kp.warmUpEncryptedKeys(keyNames);
          NameNode.LOG
              .info("Successfully warmed up {} EDEKs.", keyNames.length);
          success = true;
          break;
        } catch (IOException ioe) {
          lastSeenIOE = ioe;
          if (sinceLastLog >= logCoolDown) {
            NameNode.LOG.info("Failed to warm up EDEKs.", ioe);
            sinceLastLog = 0;
          } else {
            NameNode.LOG.debug("Failed to warm up EDEKs.", ioe);
          }
        } catch (Exception e) {
          NameNode.LOG.error("Cannot warm up EDEKs.", e);
          throw e;
        }
        try {
          Thread.sleep(retryInterval);
        } catch (InterruptedException ie) {
          NameNode.LOG.info("EDEKCacheLoader interrupted during retry.");
          break;
        }
        sinceLastLog += retryInterval;
      }
      long warmUpEDEKTime = monotonicNow() - warmUpEDEKStartTime;
      NameNode.getNameNodeMetrics().addWarmUpEDEKTime(warmUpEDEKTime);
      if (!success) {
        NameNode.LOG.warn("Unable to warm up EDEKs.");
        if (lastSeenIOE != null) {
          NameNode.LOG.warn("Last seen exception:", lastSeenIOE);
        }
      }
    }
  }

  /**
   * If the file is in an encryption zone, we optimistically create an
   * EDEK for the file by calling out to the configured KeyProvider.
   * Since this typically involves doing an RPC, the fsn lock is yielded.
   *
   * Since the path can flip-flop between being in an encryption zone and not
   * in the meantime, the call MUST re-resolve the IIP and re-check
   * preconditions if this method does not return null;
   *
   * @param fsn the namesystem.
   * @param iip the inodes for the path
   * @param supportedVersions client's supported versions
   * @return EncryptionKeyInfo if the path is in an EZ, else null
   */
  static EncryptionKeyInfo getEncryptionKeyInfo(FSNamesystem fsn,
      INodesInPath iip, CryptoProtocolVersion[] supportedVersions)
      throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    // Nothing to do if the path is not within an EZ
    final EncryptionZone zone = getEZForPath(fsd, iip);
    if (zone == null) {
      EncryptionFaultInjector.getInstance().startFileNoKey();
      return null;
    }
    CryptoProtocolVersion protocolVersion = fsn.chooseProtocolVersion(
        zone, supportedVersions);
    CipherSuite suite = zone.getSuite();
    String ezKeyName = zone.getKeyName();

    Preconditions.checkNotNull(protocolVersion);
    Preconditions.checkNotNull(suite);
    Preconditions.checkArgument(!suite.equals(CipherSuite.UNKNOWN),
                                "Chose an UNKNOWN CipherSuite!");
    Preconditions.checkNotNull(ezKeyName);

    // Generate EDEK while not holding the fsn lock.
    fsn.writeUnlock();
    try {
      EncryptionFaultInjector.getInstance().startFileBeforeGenerateKey();
      return new EncryptionKeyInfo(protocolVersion, suite, ezKeyName,
          generateEncryptedDataEncryptionKey(fsd, ezKeyName));
    } finally {
      fsn.writeLock();
      EncryptionFaultInjector.getInstance().startFileAfterGenerateKey();
    }
  }

  static class EncryptionKeyInfo {
    final CryptoProtocolVersion protocolVersion;
    final CipherSuite suite;
    final String ezKeyName;
    final KeyProviderCryptoExtension.EncryptedKeyVersion edek;

    EncryptionKeyInfo(
        CryptoProtocolVersion protocolVersion, CipherSuite suite,
        String ezKeyName, KeyProviderCryptoExtension.EncryptedKeyVersion edek) {
      this.protocolVersion = protocolVersion;
      this.suite = suite;
      this.ezKeyName = ezKeyName;
      this.edek = edek;
    }
  }
}
