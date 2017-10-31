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

import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReencryptionInfoProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo.UpdatedReplicationInfo;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.fs.CommonConfigurationKeys.FS_PROTECTED_DIRECTORIES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;

/**
 * Both FSDirectory and FSNamesystem manage the state of the namespace.
 * FSDirectory is a pure in-memory data structure, all of whose operations
 * happen entirely in memory. In contrast, FSNamesystem persists the operations
 * to the disk.
 * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem
 **/
@InterfaceAudience.Private
public class FSDirectory implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(FSDirectory.class);

  private static INodeDirectory createRoot(FSNamesystem namesystem) {
    final INodeDirectory r = new INodeDirectory(
        INodeId.ROOT_INODE_ID,
        INodeDirectory.ROOT_NAME,
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)),
        0L);
    r.addDirectoryWithQuotaFeature(
        new DirectoryWithQuotaFeature.Builder().
            nameSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA).
            storageSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_STORAGE_SPACE_QUOTA).
            build());
    r.addSnapshottableFeature();
    r.setSnapshotQuota(0);
    return r;
  }

  @VisibleForTesting
  static boolean CHECK_RESERVED_FILE_NAMES = true;
  public final static String DOT_RESERVED_STRING = ".reserved";
  public final static String DOT_RESERVED_PATH_PREFIX = Path.SEPARATOR
      + DOT_RESERVED_STRING;
  public final static byte[] DOT_RESERVED = 
      DFSUtil.string2Bytes(DOT_RESERVED_STRING);
  private final static String RAW_STRING = "raw";
  private final static byte[] RAW = DFSUtil.string2Bytes(RAW_STRING);
  public final static String DOT_INODES_STRING = ".inodes";
  public final static byte[] DOT_INODES = 
      DFSUtil.string2Bytes(DOT_INODES_STRING);
  private final static byte[] DOT_DOT =
      DFSUtil.string2Bytes("..");

  public final static HdfsFileStatus DOT_RESERVED_STATUS =
      new HdfsFileStatus.Builder()
        .isdir(true)
        .perm(new FsPermission((short) 01770))
        .build();

  public final static HdfsFileStatus DOT_SNAPSHOT_DIR_STATUS =
      new HdfsFileStatus.Builder()
        .isdir(true)
        .build();

  INodeDirectory rootDir;
  private final FSNamesystem namesystem;
  private volatile boolean skipQuotaCheck = false; //skip while consuming edits
  private final int maxComponentLength;
  private final int maxDirItems;
  private final int lsLimit;  // max list limit
  private final int contentCountLimit; // max content summary counts per run
  private final long contentSleepMicroSec;
  private final INodeMap inodeMap; // Synchronized by dirLock
  private long yieldCount = 0; // keep track of lock yield count.
  private int quotaInitThreads;

  private final int inodeXAttrsLimit; //inode xattrs max limit

  // A set of directories that have been protected using the
  // dfs.namenode.protected.directories setting. These directories cannot
  // be deleted unless they are empty.
  //
  // Each entry in this set must be a normalized path.
  private volatile SortedSet<String> protectedDirectories;

  // lock to protect the directory and BlockMap
  private final ReentrantReadWriteLock dirLock;

  private final boolean isPermissionEnabled;
  /**
   * Support for ACLs is controlled by a configuration flag. If the
   * configuration flag is false, then the NameNode will reject all
   * ACL-related operations.
   */
  private final boolean aclsEnabled;
  /**
   * Support for POSIX ACL inheritance. Not final for testing purpose.
   */
  private boolean posixAclInheritanceEnabled;
  private final boolean xattrsEnabled;
  private final int xattrMaxSize;

  // precision of access times.
  private final long accessTimePrecision;
  // whether setStoragePolicy is allowed.
  private final boolean storagePolicyEnabled;
  // whether quota by storage type is allowed
  private final boolean quotaByStorageTypeEnabled;

  private final String fsOwnerShortUserName;
  private final String supergroup;
  private final INodeId inodeId;

  private final FSEditLog editLog;

  private HdfsFileStatus[] reservedStatuses;

  private INodeAttributeProvider attributeProvider;

  // A HashSet of principals of users for whom the external attribute provider
  // will be bypassed
  private HashSet<String> usersToBypassExtAttrProvider = null;

  public void setINodeAttributeProvider(INodeAttributeProvider provider) {
    attributeProvider = provider;
  }

  // utility methods to acquire and release read lock and write lock
  void readLock() {
    this.dirLock.readLock().lock();
  }

  void readUnlock() {
    this.dirLock.readLock().unlock();
  }

  void writeLock() {
    this.dirLock.writeLock().lock();
  }

  void writeUnlock() {
    this.dirLock.writeLock().unlock();
  }

  boolean hasWriteLock() {
    return this.dirLock.isWriteLockedByCurrentThread();
  }

  boolean hasReadLock() {
    return this.dirLock.getReadHoldCount() > 0 || hasWriteLock();
  }

  public int getReadHoldCount() {
    return this.dirLock.getReadHoldCount();
  }

  public int getWriteHoldCount() {
    return this.dirLock.getWriteHoldCount();
  }

  @VisibleForTesting
  public final EncryptionZoneManager ezManager;

  /**
   * Caches frequently used file names used in {@link INode} to reuse 
   * byte[] objects and reduce heap usage.
   */
  private final NameCache<ByteArray> nameCache;

  // used to specify path resolution type. *_LINK will return symlinks instead
  // of throwing an unresolved exception
  public enum DirOp {
    READ,
    READ_LINK,
    WRITE,  // disallows snapshot paths.
    WRITE_LINK,
    CREATE, // like write, but also blocks invalid path names.
    CREATE_LINK;
  };

  FSDirectory(FSNamesystem ns, Configuration conf) throws IOException {
    this.dirLock = new ReentrantReadWriteLock(true); // fair
    this.inodeId = new INodeId();
    rootDir = createRoot(ns);
    inodeMap = INodeMap.newInstance(rootDir);
    this.isPermissionEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT);
    this.fsOwnerShortUserName =
      UserGroupInformation.getCurrentUser().getShortUserName();
    this.supergroup = conf.get(
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.aclsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT);
    LOG.info("ACLs enabled? " + aclsEnabled);
    this.posixAclInheritanceEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_DEFAULT);
    LOG.info("POSIX ACL inheritance enabled? " + posixAclInheritanceEnabled);
    this.xattrsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_DEFAULT);
    LOG.info("XAttrs enabled? " + xattrsEnabled);
    this.xattrMaxSize = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT);
    Preconditions.checkArgument(xattrMaxSize > 0,
        "The maximum size of an xattr should be > 0: (%s).",
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY);
    Preconditions.checkArgument(xattrMaxSize <=
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_HARD_LIMIT,
        "The maximum size of an xattr should be <= maximum size"
        + " hard limit " + DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_HARD_LIMIT
        + ": (%s).", DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY);

    this.accessTimePrecision = conf.getLong(
        DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
        DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);

    this.storagePolicyEnabled =
        conf.getBoolean(DFS_STORAGE_POLICY_ENABLED_KEY,
                        DFS_STORAGE_POLICY_ENABLED_DEFAULT);

    this.quotaByStorageTypeEnabled =
        conf.getBoolean(DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY,
                        DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT);

    int configuredLimit = conf.getInt(
        DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit>0 ?
        configuredLimit : DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;
    this.contentCountLimit = conf.getInt(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_DEFAULT);
    this.contentSleepMicroSec = conf.getLong(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_DEFAULT);
    
    // filesystem limits
    this.maxComponentLength = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
    this.maxDirItems = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);
    this.inodeXAttrsLimit = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);

    this.protectedDirectories = parseProtectedDirectories(conf);

    Preconditions.checkArgument(this.inodeXAttrsLimit >= 0,
        "Cannot set a negative limit on the number of xattrs per inode (%s).",
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY);
    // We need a maximum maximum because by default, PB limits message sizes
    // to 64MB. This means we can only store approximately 6.7 million entries
    // per directory, but let's use 6.4 million for some safety.
    final int MAX_DIR_ITEMS = 64 * 100 * 1000;
    Preconditions.checkArgument(
        maxDirItems > 0 && maxDirItems <= MAX_DIR_ITEMS, "Cannot set "
            + DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY
            + " to a value less than 1 or greater than " + MAX_DIR_ITEMS);

    int threshold = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY,
        DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT);
    NameNode.LOG.info("Caching file names occurring more than " + threshold
        + " times");
    nameCache = new NameCache<ByteArray>(threshold);
    namesystem = ns;
    this.editLog = ns.getEditLog();
    ezManager = new EncryptionZoneManager(this, conf);

    this.quotaInitThreads = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_QUOTA_INIT_THREADS_KEY,
        DFSConfigKeys.DFS_NAMENODE_QUOTA_INIT_THREADS_DEFAULT);

    initUsersToBypassExtProvider(conf);
  }

  private void initUsersToBypassExtProvider(Configuration conf) {
    String[] bypassUsers = conf.getTrimmedStrings(
        DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_BYPASS_USERS_KEY,
        DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_BYPASS_USERS_DEFAULT);
    for(int i = 0; i < bypassUsers.length; i++) {
      String tmp = bypassUsers[i].trim();
      if (!tmp.isEmpty()) {
        if (usersToBypassExtAttrProvider == null) {
          usersToBypassExtAttrProvider = new HashSet<String>();
        }
        LOG.info("Add user " + tmp + " to the list that will bypass external"
            + " attribute provider.");
        usersToBypassExtAttrProvider.add(tmp);
      }
    }
  }

  /**
   * Check if a given user is configured to bypass external attribute provider.
   * @param user user principal
   * @return true if the user is to bypass external attribute provider
   */
  private boolean isUserBypassingExtAttrProvider(final String user) {
    return (usersToBypassExtAttrProvider != null) &&
          usersToBypassExtAttrProvider.contains(user);
  }

  /**
   * Return attributeProvider or null if ugi is to bypass attributeProvider.
   * @param ugi
   * @return configured attributeProvider or null
   */
  private INodeAttributeProvider getUserFilteredAttributeProvider(
      UserGroupInformation ugi) {
    if (attributeProvider == null ||
        (ugi != null && isUserBypassingExtAttrProvider(ugi.getUserName()))) {
      return null;
    }
    return attributeProvider;
  }

  /**
   * Get HdfsFileStatuses of the reserved paths: .inodes and raw.
   *
   * @return Array of HdfsFileStatus
   */
  HdfsFileStatus[] getReservedStatuses() {
    Preconditions.checkNotNull(reservedStatuses, "reservedStatuses should "
        + " not be null. It is populated when FSNamesystem loads FS image."
        + " It has to be set at this time instead of initialization time"
        + " because CTime is loaded during FSNamesystem#loadFromDisk.");
    return reservedStatuses;
  }

  /**
   * Create HdfsFileStatuses of the reserved paths: .inodes and raw.
   * These statuses are solely for listing purpose. All other operations
   * on the reserved dirs are disallowed.
   * Operations on sub directories are resolved by
   * {@link FSDirectory#resolvePath(String, byte[][], FSDirectory)}
   * and conducted directly, without the need to check the reserved dirs.
   *
   * This method should only be invoked once during namenode initialization.
   *
   * @param cTime CTime of the file system
   * @return Array of HdfsFileStatus
   */
  void createReservedStatuses(long cTime) {
    HdfsFileStatus inodes = new HdfsFileStatus.Builder()
        .isdir(true)
        .mtime(cTime)
        .atime(cTime)
        .perm(new FsPermission((short) 0770))
        .group(supergroup)
        .path(DOT_INODES)
        .build();
    HdfsFileStatus raw = new HdfsFileStatus.Builder()
        .isdir(true)
        .mtime(cTime)
        .atime(cTime)
        .perm(new FsPermission((short) 0770))
        .group(supergroup)
        .path(RAW)
        .build();
    reservedStatuses = new HdfsFileStatus[] {inodes, raw};
  }

  FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  /**
   * Parse configuration setting dfs.namenode.protected.directories to
   * retrieve the set of protected directories.
   *
   * @param conf
   * @return a TreeSet
   */
  @VisibleForTesting
  static SortedSet<String> parseProtectedDirectories(Configuration conf) {
    return parseProtectedDirectories(conf
        .getTrimmedStringCollection(FS_PROTECTED_DIRECTORIES));
  }

  /**
   * Parse configuration setting dfs.namenode.protected.directories to retrieve
   * the set of protected directories.
   *
   * @param protectedDirsString
   *          a comma separated String representing a bunch of paths.
   * @return a TreeSet
   */
  @VisibleForTesting
  static SortedSet<String> parseProtectedDirectories(
      final String protectedDirsString) {
    return parseProtectedDirectories(StringUtils
        .getTrimmedStringCollection(protectedDirsString));
  }

  private static SortedSet<String> parseProtectedDirectories(
      final Collection<String> protectedDirs) {
    // Normalize each input path to guard against administrator error.
    return new TreeSet<>(
        normalizePaths(protectedDirs, FS_PROTECTED_DIRECTORIES));
  }

  SortedSet<String> getProtectedDirectories() {
    return protectedDirectories;
  }

  /**
   * Set directories that cannot be removed unless empty, even by an
   * administrator.
   *
   * @param protectedDirsString
   *          comma separated list of protected directories
   */
  String setProtectedDirectories(String protectedDirsString) {
    if (protectedDirsString == null) {
      protectedDirectories = new TreeSet<>();
    } else {
      protectedDirectories = parseProtectedDirectories(protectedDirsString);
    }

    return Joiner.on(",").skipNulls().join(protectedDirectories);
  }

  BlockManager getBlockManager() {
    return getFSNamesystem().getBlockManager();
  }

  KeyProviderCryptoExtension getProvider() {
    return getFSNamesystem().getProvider();
  }

  /** @return the root directory inode. */
  public INodeDirectory getRoot() {
    return rootDir;
  }

  public BlockStoragePolicySuite getBlockStoragePolicySuite() {
    return getBlockManager().getStoragePolicySuite();
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }
  boolean isAclsEnabled() {
    return aclsEnabled;
  }

  @VisibleForTesting
  public boolean isPosixAclInheritanceEnabled() {
    return posixAclInheritanceEnabled;
  }

  @VisibleForTesting
  public void setPosixAclInheritanceEnabled(
      boolean posixAclInheritanceEnabled) {
    this.posixAclInheritanceEnabled = posixAclInheritanceEnabled;
  }

  boolean isXattrsEnabled() {
    return xattrsEnabled;
  }
  int getXattrMaxSize() { return xattrMaxSize; }
  boolean isStoragePolicyEnabled() {
    return storagePolicyEnabled;
  }
  boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }
  long getAccessTimePrecision() {
    return accessTimePrecision;
  }
  boolean isQuotaByStorageTypeEnabled() {
    return quotaByStorageTypeEnabled;
  }


  int getLsLimit() {
    return lsLimit;
  }

  int getContentCountLimit() {
    return contentCountLimit;
  }

  long getContentSleepMicroSec() {
    return contentSleepMicroSec;
  }

  int getInodeXAttrsLimit() {
    return inodeXAttrsLimit;
  }

  FSEditLog getEditLog() {
    return editLog;
  }

  /**
   * Shutdown the filestore
   */
  @Override
  public void close() throws IOException {}

  void markNameCacheInitialized() {
    writeLock();
    try {
      nameCache.initialized();
    } finally {
      writeUnlock();
    }
  }

  boolean shouldSkipQuotaChecks() {
    return skipQuotaCheck;
  }

  /** Enable quota verification */
  void enableQuotaChecks() {
    skipQuotaCheck = false;
  }

  /** Disable quota verification */
  void disableQuotaChecks() {
    skipQuotaCheck = true;
  }

  /**
   * Resolves a given path into an INodesInPath.  All ancestor inodes that
   * exist are validated as traversable directories.  Symlinks in the ancestry
   * will generate an UnresolvedLinkException.  The returned IIP will be an
   * accessible path that also passed additional sanity checks based on how
   * the path will be used as specified by the DirOp.
   *   READ:   Expands reserved paths and performs permission checks
   *           during traversal.  Raw paths are only accessible by a superuser.
   *   WRITE:  In addition to READ checks, ensures the path is not a
   *           snapshot path.
   *   CREATE: In addition to WRITE checks, ensures path does not contain
   *           illegal character sequences.
   *
   * @param pc  A permission checker for traversal checks.  Pass null for
   *            no permission checks.
   * @param src The path to resolve.
   * @param dirOp The {@link DirOp} that controls additional checks.
   * @param resolveLink If false, only ancestor symlinks will be checked.  If
   *         true, the last inode will also be checked.
   * @return if the path indicates an inode, return path after replacing up to
   *         <inodeid> with the corresponding path of the inode, else the path
   *         in {@code src} as is. If the path refers to a path in the "raw"
   *         directory, return the non-raw pathname.
   * @throws FileNotFoundException
   * @throws AccessControlException
   * @throws ParentNotDirectoryException
   * @throws UnresolvedLinkException
   */
  @VisibleForTesting
  public INodesInPath resolvePath(FSPermissionChecker pc, String src,
      DirOp dirOp) throws UnresolvedLinkException, FileNotFoundException,
      AccessControlException, ParentNotDirectoryException {
    boolean isCreate = (dirOp == DirOp.CREATE || dirOp == DirOp.CREATE_LINK);
    // prevent creation of new invalid paths
    if (isCreate && !DFSUtil.isValidName(src)) {
      throw new InvalidPathException("Invalid file name: " + src);
    }

    byte[][] components = INode.getPathComponents(src);
    boolean isRaw = isReservedRawName(components);
    if (isPermissionEnabled && pc != null && isRaw) {
      pc.checkSuperuserPrivilege();
    }
    components = resolveComponents(components, this);
    INodesInPath iip = INodesInPath.resolve(rootDir, components, isRaw);
    // verify all ancestors are dirs and traversable.  note that only
    // methods that create new namespace items have the signature to throw
    // PNDE
    try {
      checkTraverse(pc, iip, dirOp);
    } catch (ParentNotDirectoryException pnde) {
      if (!isCreate) {
        throw new AccessControlException(pnde.getMessage());
      }
      throw pnde;
    }
    return iip;
  }

  INodesInPath resolvePath(FSPermissionChecker pc, String src, long fileId)
      throws UnresolvedLinkException, FileNotFoundException,
      AccessControlException, ParentNotDirectoryException {
    // Older clients may not have given us an inode ID to work with.
    // In this case, we have to try to resolve the path and hope it
    // hasn't changed or been deleted since the file was opened for write.
    INodesInPath iip;
    if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
      iip = resolvePath(pc, src, DirOp.WRITE);
    } else {
      INode inode = getInode(fileId);
      if (inode == null) {
        iip = INodesInPath.fromComponents(INode.getPathComponents(src));
      } else {
        iip = INodesInPath.fromINode(inode);
      }
    }
    return iip;
  }

  // this method can be removed after IIP is used more extensively
  static String resolvePath(String src,
      FSDirectory fsd) throws FileNotFoundException {
    byte[][] pathComponents = INode.getPathComponents(src);
    pathComponents = resolveComponents(pathComponents, fsd);
    return DFSUtil.byteArray2PathString(pathComponents);
  }

  /**
   * @return true if the path is a non-empty directory; otherwise, return false.
   */
  boolean isNonEmptyDirectory(INodesInPath inodesInPath) {
    readLock();
    try {
      final INode inode = inodesInPath.getLastINode();
      if (inode == null || !inode.isDirectory()) {
        //not found or not a directory
        return false;
      }
      final int s = inodesInPath.getPathSnapshotId();
      return !inode.asDirectory().getChildrenList(s).isEmpty();
    } finally {
      readUnlock();
    }
  }

  /**
   * Check whether the filepath could be created
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  boolean isValidToCreate(String src, INodesInPath iip)
      throws SnapshotAccessControlException {
    String srcs = normalizePath(src);
    return srcs.startsWith("/") && !srcs.endsWith("/") &&
        iip.getLastINode() == null;
  }

  /**
   * Tell the block manager to update the replication factors when delete
   * happens. Deleting a file or a snapshot might decrease the replication
   * factor of the blocks as the blocks are always replicated to the highest
   * replication factor among all snapshots.
   */
  void updateReplicationFactor(Collection<UpdatedReplicationInfo> blocks) {
    BlockManager bm = getBlockManager();
    for (UpdatedReplicationInfo e : blocks) {
      BlockInfo b = e.block();
      bm.setReplication(b.getReplication(), e.targetReplication(), b);
    }
  }

  /**
   * Update the count of each directory with quota in the namespace.
   * A directory's count is defined as the total number inodes in the tree
   * rooted at the directory.
   *
   * This is an update of existing state of the filesystem and does not
   * throw QuotaExceededException.
   */
  void updateCountForQuota(int initThreads) {
    writeLock();
    try {
      int threads = (initThreads < 1) ? 1 : initThreads;
      LOG.info("Initializing quota with " + threads + " thread(s)");
      long start = Time.monotonicNow();
      QuotaCounts counts = new QuotaCounts.Builder().build();
      ForkJoinPool p = new ForkJoinPool(threads);
      RecursiveAction task = new InitQuotaTask(getBlockStoragePolicySuite(),
          rootDir.getStoragePolicyID(), rootDir, counts);
      p.execute(task);
      task.join();
      p.shutdown();
      LOG.info("Quota initialization completed in " + (Time.monotonicNow() - start) +
          " milliseconds\n" + counts);
    } finally {
      writeUnlock();
    }
  }

  void updateCountForQuota() {
    updateCountForQuota(quotaInitThreads);
  }

  /**
   * parallel initialization using fork-join.
   */
  private static class InitQuotaTask extends RecursiveAction {
    private final INodeDirectory dir;
    private final QuotaCounts counts;
    private final BlockStoragePolicySuite bsps;
    private final byte blockStoragePolicyId;

    public InitQuotaTask(BlockStoragePolicySuite bsps,
        byte blockStoragePolicyId, INodeDirectory dir, QuotaCounts counts) {
      this.dir = dir;
      this.counts = counts;
      this.bsps = bsps;
      this.blockStoragePolicyId = blockStoragePolicyId;
    }

    public void compute() {
      QuotaCounts myCounts =  new QuotaCounts.Builder().build();
      dir.computeQuotaUsage4CurrentDirectory(bsps, blockStoragePolicyId,
          myCounts);

      ReadOnlyList<INode> children =
          dir.getChildrenList(CURRENT_STATE_ID);

      if (children.size() > 0) {
        List<InitQuotaTask> subtasks = new ArrayList<InitQuotaTask>();
        for (INode child : children) {
          final byte childPolicyId =
              child.getStoragePolicyIDForQuota(blockStoragePolicyId);
          if (child.isDirectory()) {
            subtasks.add(new InitQuotaTask(bsps, childPolicyId,
                child.asDirectory(), myCounts));
          } else {
            // file or symlink. count using the local counts variable
            myCounts.add(child.computeQuotaUsage(bsps, childPolicyId, false,
                CURRENT_STATE_ID));
          }
        }
        // invoke and wait for completion
        invokeAll(subtasks);
      }

      if (dir.isQuotaSet()) {
        // check if quota is violated. It indicates a software bug.
        final QuotaCounts q = dir.getQuotaCounts();

        final long nsConsumed = myCounts.getNameSpace();
        final long nsQuota = q.getNameSpace();
        if (Quota.isViolated(nsQuota, nsConsumed)) {
          LOG.warn("Namespace quota violation in image for "
              + dir.getFullPathName()
              + " quota = " + nsQuota + " < consumed = " + nsConsumed);
        }

        final long ssConsumed = myCounts.getStorageSpace();
        final long ssQuota = q.getStorageSpace();
        if (Quota.isViolated(ssQuota, ssConsumed)) {
          LOG.warn("Storagespace quota violation in image for "
              + dir.getFullPathName()
              + " quota = " + ssQuota + " < consumed = " + ssConsumed);
        }

        final EnumCounters<StorageType> tsConsumed = myCounts.getTypeSpaces();
        for (StorageType t : StorageType.getTypesSupportingQuota()) {
          final long typeSpace = tsConsumed.get(t);
          final long typeQuota = q.getTypeSpaces().get(t);
          if (Quota.isViolated(typeQuota, typeSpace)) {
            LOG.warn("Storage type quota violation in image for "
                + dir.getFullPathName()
                + " type = " + t.toString() + " quota = "
                + typeQuota + " < consumed " + typeSpace);
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting quota for " + dir + "\n" + myCounts);
        }
        dir.getDirectoryWithQuotaFeature().setSpaceConsumed(nsConsumed,
            ssConsumed, tsConsumed);
      }

      synchronized(counts) {
        counts.add(myCounts);
      }
    }
  }

  /** Updates namespace, storagespace and typespaces consumed for all
   * directories until the parent directory of file represented by path.
   *
   * @param iip the INodesInPath instance containing all the INodes for
   *            updating quota usage
   * @param nsDelta the delta change of namespace
   * @param ssDelta the delta change of storage space consumed without replication
   * @param replication the replication factor of the block consumption change
   * @throws QuotaExceededException if the new count violates any quota limit
   * @throws FileNotFoundException if path does not exist.
   */
  void updateSpaceConsumed(INodesInPath iip, long nsDelta, long ssDelta, short replication)
    throws QuotaExceededException, FileNotFoundException,
    UnresolvedLinkException, SnapshotAccessControlException {
    writeLock();
    try {
      if (iip.getLastINode() == null) {
        throw new FileNotFoundException("Path not found: " + iip.getPath());
      }
      updateCount(iip, nsDelta, ssDelta, replication, true);
    } finally {
      writeUnlock();
    }
  }

  public void updateCount(INodesInPath iip, INode.QuotaDelta quotaDelta,
      boolean check) throws QuotaExceededException {
    QuotaCounts counts = quotaDelta.getCountsCopy();
    updateCount(iip, iip.length() - 1, counts.negation(), check);
    Map<INode, QuotaCounts> deltaInOtherPaths = quotaDelta.getUpdateMap();
    for (Map.Entry<INode, QuotaCounts> entry : deltaInOtherPaths.entrySet()) {
      INodesInPath path = INodesInPath.fromINode(entry.getKey());
      updateCount(path, path.length() - 1, entry.getValue().negation(), check);
    }
    for (Map.Entry<INodeDirectory, QuotaCounts> entry :
        quotaDelta.getQuotaDirMap().entrySet()) {
      INodeDirectory quotaDir = entry.getKey();
      quotaDir.getDirectoryWithQuotaFeature().addSpaceConsumed2Cache(
          entry.getValue().negation());
    }
  }

  /**
   * Update the quota usage after deletion. The quota update is only necessary
   * when image/edits have been loaded and the file/dir to be deleted is not
   * contained in snapshots.
   */
  void updateCountForDelete(final INode inode, final INodesInPath iip) {
    if (getFSNamesystem().isImageLoaded() &&
        !inode.isInLatestSnapshot(iip.getLatestSnapshotId())) {
      QuotaCounts counts = inode.computeQuotaUsage(getBlockStoragePolicySuite());
      unprotectedUpdateCount(iip, iip.length() - 1, counts.negation());
    }
  }

  /**
   * Update usage count without replication factor change
   */
  void updateCount(INodesInPath iip, long nsDelta, long ssDelta, short replication,
      boolean checkQuota) throws QuotaExceededException {
    final INodeFile fileINode = iip.getLastINode().asFile();
    EnumCounters<StorageType> typeSpaceDeltas =
      getStorageTypeDeltas(fileINode.getStoragePolicyID(), ssDelta,
          replication, replication);
    updateCount(iip, iip.length() - 1,
      new QuotaCounts.Builder().nameSpace(nsDelta).storageSpace(ssDelta * replication).
          typeSpaces(typeSpaceDeltas).build(),
        checkQuota);
  }

  /**
   * Update usage count with replication factor change due to setReplication
   */
  void updateCount(INodesInPath iip, long nsDelta, long ssDelta, short oldRep,
      short newRep, boolean checkQuota) throws QuotaExceededException {
    final INodeFile fileINode = iip.getLastINode().asFile();
    EnumCounters<StorageType> typeSpaceDeltas =
        getStorageTypeDeltas(fileINode.getStoragePolicyID(), ssDelta, oldRep, newRep);
    updateCount(iip, iip.length() - 1,
        new QuotaCounts.Builder().nameSpace(nsDelta).
            storageSpace(ssDelta * (newRep - oldRep)).
            typeSpaces(typeSpaceDeltas).build(),
        checkQuota);
  }

  /** update count of each inode with quota
   * 
   * @param iip inodes in a path
   * @param numOfINodes the number of inodes to update starting from index 0
   * @param counts the count of space/namespace/type usage to be update
   * @param checkQuota if true then check if quota is exceeded
   * @throws QuotaExceededException if the new count violates any quota limit
   */
  void updateCount(INodesInPath iip, int numOfINodes,
                    QuotaCounts counts, boolean checkQuota)
                    throws QuotaExceededException {
    assert hasWriteLock();
    if (!namesystem.isImageLoaded()) {
      //still initializing. do not check or update quotas.
      return;
    }
    if (numOfINodes > iip.length()) {
      numOfINodes = iip.length();
    }
    if (checkQuota && !skipQuotaCheck) {
      verifyQuota(iip, numOfINodes, counts, null);
    }
    unprotectedUpdateCount(iip, numOfINodes, counts);
  }
  
  /** 
   * update quota of each inode and check to see if quota is exceeded. 
   * See {@link #updateCount(INodesInPath, int, QuotaCounts, boolean)}
   */ 
   void updateCountNoQuotaCheck(INodesInPath inodesInPath,
      int numOfINodes, QuotaCounts counts) {
    assert hasWriteLock();
    try {
      updateCount(inodesInPath, numOfINodes, counts, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.error("BUG: unexpected exception ", e);
    }
  }
  
  /**
   * updates quota without verification
   * callers responsibility is to make sure quota is not exceeded
   */
  static void unprotectedUpdateCount(INodesInPath inodesInPath,
      int numOfINodes, QuotaCounts counts) {
    for(int i=0; i < numOfINodes; i++) {
      if (inodesInPath.getINode(i).isQuotaSet()) { // a directory with quota
        inodesInPath.getINode(i).asDirectory().getDirectoryWithQuotaFeature()
            .addSpaceConsumed2Cache(counts);
      }
    }
  }

  /**
   * Update the cached quota space for a block that is being completed.
   * Must only be called once, as the block is being completed.
   * @param completeBlk - Completed block for which to update space
   * @param inodes - INodes in path to file containing completeBlk; if null
   *                 this will be resolved internally
   */
  public void updateSpaceForCompleteBlock(BlockInfo completeBlk,
      INodesInPath inodes) throws IOException {
    assert namesystem.hasWriteLock();
    INodesInPath iip = inodes != null ? inodes :
        INodesInPath.fromINode(namesystem.getBlockCollection(completeBlk));
    INodeFile fileINode = iip.getLastINode().asFile();
    // Adjust disk space consumption if required
    final long diff;
    final short replicationFactor;
    if (fileINode.isStriped()) {
      final ErasureCodingPolicy ecPolicy =
          FSDirErasureCodingOp
              .unprotectedGetErasureCodingPolicy(namesystem, iip);
      final short numDataUnits = (short) ecPolicy.getNumDataUnits();
      final short numParityUnits = (short) ecPolicy.getNumParityUnits();

      final long numBlocks = numDataUnits + numParityUnits;
      final long fullBlockGroupSize =
          fileINode.getPreferredBlockSize() * numBlocks;

      final BlockInfoStriped striped =
          new BlockInfoStriped(completeBlk, ecPolicy);
      final long actualBlockGroupSize = striped.spaceConsumed();

      diff = fullBlockGroupSize - actualBlockGroupSize;
      replicationFactor = (short) 1;
    } else {
      diff = fileINode.getPreferredBlockSize() - completeBlk.getNumBytes();
      replicationFactor = fileINode.getFileReplication();
    }
    if (diff > 0) {
      try {
        updateSpaceConsumed(iip, 0, -diff, replicationFactor);
      } catch (IOException e) {
        LOG.warn("Unexpected exception while updating disk space.", e);
      }
    }
  }

  public EnumCounters<StorageType> getStorageTypeDeltas(byte storagePolicyID,
      long dsDelta, short oldRep, short newRep) {
    EnumCounters<StorageType> typeSpaceDeltas =
        new EnumCounters<StorageType>(StorageType.class);
    // empty file
    if(dsDelta == 0){
      return typeSpaceDeltas;
    }
    // Storage type and its quota are only available when storage policy is set
    if (storagePolicyID != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      BlockStoragePolicy storagePolicy = getBlockManager().getStoragePolicy(storagePolicyID);

      if (oldRep != newRep) {
        List<StorageType> oldChosenStorageTypes =
            storagePolicy.chooseStorageTypes(oldRep);

        for (StorageType t : oldChosenStorageTypes) {
          if (!t.supportTypeQuota()) {
            continue;
          }
          Preconditions.checkArgument(dsDelta > 0);
          typeSpaceDeltas.add(t, -dsDelta);
        }
      }

      List<StorageType> newChosenStorageTypes =
          storagePolicy.chooseStorageTypes(newRep);

      for (StorageType t : newChosenStorageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        typeSpaceDeltas.add(t, dsDelta);
      }
    }
    return typeSpaceDeltas;
  }

  /**
   * Add the given child to the namespace.
   * @param existing the INodesInPath containing all the ancestral INodes
   * @param child the new INode to add
   * @param modes create modes
   * @return a new INodesInPath instance containing the new child INode. Null
   * if the adding fails.
   * @throws QuotaExceededException is thrown if it violates quota limit
   */
  INodesInPath addINode(INodesInPath existing, INode child,
                        FsPermission modes)
      throws QuotaExceededException, UnresolvedLinkException {
    cacheName(child);
    writeLock();
    try {
      return addLastINode(existing, child, modes, true);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Verify quota for adding or moving a new INode with required 
   * namespace and storagespace to a given position.
   *  
   * @param iip INodes corresponding to a path
   * @param pos position where a new INode will be added
   * @param deltas needed namespace, storagespace and storage types
   * @param commonAncestor Last node in inodes array that is a common ancestor
   *          for a INode that is being moved from one location to the other.
   *          Pass null if a node is not being moved.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  static void verifyQuota(INodesInPath iip, int pos, QuotaCounts deltas,
                          INode commonAncestor) throws QuotaExceededException {
    if (deltas.getNameSpace() <= 0 && deltas.getStorageSpace() <= 0
        && deltas.getTypeSpaces().allLessOrEqual(0L)) {
      // if quota is being freed or not being consumed
      return;
    }

    // check existing components in the path
    for(int i = (pos > iip.length() ? iip.length(): pos) - 1; i >= 0; i--) {
      if (commonAncestor == iip.getINode(i)) {
        // Stop checking for quota when common ancestor is reached
        return;
      }
      final DirectoryWithQuotaFeature q
          = iip.getINode(i).asDirectory().getDirectoryWithQuotaFeature();
      if (q != null) { // a directory with quota
        try {
          q.verifyQuota(deltas);
        } catch (QuotaExceededException e) {
          e.setPathName(iip.getPath(i));
          throw e;
        }
      }
    }
  }

  /** Verify if the inode name is legal. */
  void verifyINodeName(byte[] childName) throws HadoopIllegalArgumentException {
    if (Arrays.equals(HdfsServerConstants.DOT_SNAPSHOT_DIR_BYTES, childName)) {
      String s = "\"" + HdfsConstants.DOT_SNAPSHOT_DIR + "\" is a reserved name.";
      if (!namesystem.isImageLoaded()) {
        s += "  Please rename it before upgrade.";
      }
      throw new HadoopIllegalArgumentException(s);
    }
  }

  /**
   * Verify child's name for fs limit.
   *
   * @param childName byte[] containing new child name
   * @param parentPath String containing parent path
   * @throws PathComponentTooLongException child's name is too long.
   */
  void verifyMaxComponentLength(byte[] childName, String parentPath)
      throws PathComponentTooLongException {
    if (maxComponentLength == 0) {
      return;
    }

    final int length = childName.length;
    if (length > maxComponentLength) {
      final PathComponentTooLongException e = new PathComponentTooLongException(
          maxComponentLength, length, parentPath,
          DFSUtil.bytes2String(childName));
      if (namesystem.isImageLoaded()) {
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("ERROR in FSDirectory.verifyINodeName", e);
      }
    }
  }

  /**
   * Verify children size for fs limit.
   *
   * @throws MaxDirectoryItemsExceededException too many children.
   */
  void verifyMaxDirItems(INodeDirectory parent, String parentPath)
      throws MaxDirectoryItemsExceededException {
    final int count = parent.getChildrenList(CURRENT_STATE_ID).size();
    if (count >= maxDirItems) {
      final MaxDirectoryItemsExceededException e
          = new MaxDirectoryItemsExceededException(parentPath, maxDirItems,
          count);
      if (namesystem.isImageLoaded()) {
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("FSDirectory.verifyMaxDirItems: "
            + e.getLocalizedMessage());
      }
    }
  }

  /**
   * Turn on HDFS-6962 POSIX ACL inheritance when the property
   * {@link DFSConfigKeys#DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY} is
   * true and a compatible client has sent both masked and unmasked create
   * modes.
   *
   * @param child INode newly created child
   * @param modes create modes
   */
  private void copyINodeDefaultAcl(INode child, FsPermission modes) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("child: {}, posixAclInheritanceEnabled: {}, modes: {}",
          child, posixAclInheritanceEnabled, modes);
    }

    if (posixAclInheritanceEnabled && modes != null &&
        modes.getUnmasked() != null) {
      //
      // HDFS-6962: POSIX ACL inheritance
      //
      child.setPermission(modes.getUnmasked());
      if (!AclStorage.copyINodeDefaultAcl(child)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: no parent default ACL to inherit", child);
        }
        child.setPermission(modes.getMasked());
      }
    } else {
      //
      // Old behavior before HDFS-6962
      //
      AclStorage.copyINodeDefaultAcl(child);
    }
  }

  /**
   * Add a child to the end of the path specified by INodesInPath.
   * @param existing the INodesInPath containing all the ancestral INodes
   * @param inode the new INode to add
   * @param modes create modes
   * @param checkQuota whether to check quota
   * @return an INodesInPath instance containing the new INode
   */
  @VisibleForTesting
  public INodesInPath addLastINode(INodesInPath existing, INode inode,
      FsPermission modes, boolean checkQuota) throws QuotaExceededException {
    assert existing.getLastINode() != null &&
        existing.getLastINode().isDirectory();

    final int pos = existing.length();
    // Disallow creation of /.reserved. This may be created when loading
    // editlog/fsimage during upgrade since /.reserved was a valid name in older
    // release. This may also be called when a user tries to create a file
    // or directory /.reserved.
    if (pos == 1 && existing.getINode(0) == rootDir && isReservedName(inode)) {
      throw new HadoopIllegalArgumentException(
          "File name \"" + inode.getLocalName() + "\" is reserved and cannot "
              + "be created. If this is during upgrade change the name of the "
              + "existing file or directory to another name before upgrading "
              + "to the new release.");
    }
    final INodeDirectory parent = existing.getINode(pos - 1).asDirectory();
    // The filesystem limits are not really quotas, so this check may appear
    // odd. It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location because a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      final String parentPath = existing.getPath();
      verifyMaxComponentLength(inode.getLocalNameBytes(), parentPath);
      verifyMaxDirItems(parent, parentPath);
    }
    // always verify inode name
    verifyINodeName(inode.getLocalNameBytes());

    final QuotaCounts counts = inode.computeQuotaUsage(getBlockStoragePolicySuite());
    updateCount(existing, pos, counts, checkQuota);

    boolean isRename = (inode.getParent() != null);
    boolean added;
    try {
      added = parent.addChild(inode, true, existing.getLatestSnapshotId());
    } catch (QuotaExceededException e) {
      updateCountNoQuotaCheck(existing, pos, counts.negation());
      throw e;
    }
    if (!added) {
      updateCountNoQuotaCheck(existing, pos, counts.negation());
      return null;
    } else {
      if (!isRename) {
        copyINodeDefaultAcl(inode, modes);
      }
      addToInodeMap(inode);
    }
    return INodesInPath.append(existing, inode, inode.getLocalNameBytes());
  }

  INodesInPath addLastINodeNoQuotaCheck(INodesInPath existing, INode i) {
    try {
      // All callers do not have create modes to pass.
      return addLastINode(existing, i, null, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
    }
    return null;
  }

  /**
   * Remove the last inode in the path from the namespace.
   * Note: the caller needs to update the ancestors' quota count.
   *
   * @return -1 for failing to remove;
   *          0 for removing a reference whose referred inode has other 
   *            reference nodes;
   *          1 otherwise.
   */
  @VisibleForTesting
  public long removeLastINode(final INodesInPath iip) {
    final int latestSnapshot = iip.getLatestSnapshotId();
    final INode last = iip.getLastINode();
    final INodeDirectory parent = iip.getINode(-2).asDirectory();
    if (!parent.removeChild(last, latestSnapshot)) {
      return -1;
    }

    return (!last.isInLatestSnapshot(latestSnapshot)
        && INodeReference.tryRemoveReference(last) > 0) ? 0 : 1;
  }

  /**
   * Return a new collection of normalized paths from the given input
   * collection. The input collection is unmodified.
   *
   * Reserved paths, relative paths and paths with scheme are ignored.
   *
   * @param paths collection whose contents are to be normalized.
   * @return collection with all input paths normalized.
   */
  static Collection<String> normalizePaths(Collection<String> paths,
                                           String errorString) {
    if (paths.isEmpty()) {
      return paths;
    }
    final Collection<String> normalized = new ArrayList<>(paths.size());
    for (String dir : paths) {
      if (isReservedName(dir)) {
        LOG.error("{} ignoring reserved path {}", errorString, dir);
      } else {
        final Path path = new Path(dir);
        if (!path.isAbsolute()) {
          LOG.error("{} ignoring relative path {}", errorString, dir);
        } else if (path.toUri().getScheme() != null) {
          LOG.error("{} ignoring path {} with scheme", errorString, dir);
        } else {
          normalized.add(path.toString());
        }
      }
    }
    return normalized;
  }

  static String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }

  @VisibleForTesting
  public long getYieldCount() {
    return yieldCount;
  }

  void addYieldCount(long value) {
    yieldCount += value;
  }

  public INodeMap getINodeMap() {
    return inodeMap;
  }

  /**
   * This method is always called with writeLock of FSDirectory held.
   */
  public final void addToInodeMap(INode inode) {
    if (inode instanceof INodeWithAdditionalFields) {
      inodeMap.put(inode);
      if (!inode.isSymlink()) {
        final XAttrFeature xaf = inode.getXAttrFeature();
        addEncryptionZone((INodeWithAdditionalFields) inode, xaf);
      }
    }
  }

  private void addEncryptionZone(INodeWithAdditionalFields inode,
      XAttrFeature xaf) {
    if (xaf == null) {
      return;
    }
    XAttr xattr = xaf.getXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE);
    if (xattr == null) {
      return;
    }
    try {
      final HdfsProtos.ZoneEncryptionInfoProto ezProto =
          HdfsProtos.ZoneEncryptionInfoProto.parseFrom(xattr.getValue());
      ezManager.unprotectedAddEncryptionZone(inode.getId(),
          PBHelperClient.convert(ezProto.getSuite()),
          PBHelperClient.convert(ezProto.getCryptoProtocolVersion()),
          ezProto.getKeyName());
      if (ezProto.hasReencryptionProto()) {
        final ReencryptionInfoProto reProto = ezProto.getReencryptionProto();
        // inodes parents may not be loaded if this is done during fsimage
        // loading so cannot set full path now. Pass in null to indicate that.
        ezManager.getReencryptionStatus()
            .updateZoneStatus(inode.getId(), null, reProto);
      }
    } catch (InvalidProtocolBufferException e) {
      NameNode.LOG.warn("Error parsing protocol buffer of " +
          "EZ XAttr " + xattr.getName() + " dir:" + inode.getFullPathName());
    }
  }
  
  /**
   * This is to handle encryption zone for rootDir when loading from
   * fsimage, and should only be called during NN restart.
   */
  public final void addRootDirToEncryptionZone(XAttrFeature xaf) {
    addEncryptionZone(rootDir, xaf);
  }

  /**
   * This method is always called with writeLock of FSDirectory held.
   */
  public final void removeFromInodeMap(List<? extends INode> inodes) {
    if (inodes != null) {
      for (INode inode : inodes) {
        if (inode != null && inode instanceof INodeWithAdditionalFields) {
          inodeMap.remove(inode);
          ezManager.removeEncryptionZone(inode.getId());
        }
      }
    }
  }
  
  /**
   * Get the inode from inodeMap based on its inode id.
   * @param id The given id
   * @return The inode associated with the given id
   */
  public INode getInode(long id) {
    readLock();
    try {
      return inodeMap.get(id);
    } finally {
      readUnlock();
    }
  }
  
  @VisibleForTesting
  int getInodeMapSize() {
    return inodeMap.size();
  }

  long totalInodes() {
    return getInodeMapSize();
  }

  /**
   * Reset the entire namespace tree.
   */
  void reset() {
    writeLock();
    try {
      rootDir = createRoot(getFSNamesystem());
      inodeMap.clear();
      addToInodeMap(rootDir);
      nameCache.reset();
      inodeId.setCurrentValue(INodeId.LAST_RESERVED_ID);
    } finally {
      writeUnlock();
    }
  }

  static INode resolveLastINode(INodesInPath iip) throws FileNotFoundException {
    INode inode = iip.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("cannot find " + iip.getPath());
    }
    return inode;
  }

  /**
   * Caches frequently used file names to reuse file name objects and
   * reduce heap size.
   */
  void cacheName(INode inode) {
    // Name is cached only for files
    if (!inode.isFile()) {
      return;
    }
    ByteArray name = new ByteArray(inode.getLocalNameBytes());
    name = nameCache.put(name);
    if (name != null) {
      inode.setLocalName(name.getBytes());
    }
  }
  
  void shutdown() {
    nameCache.reset();
    inodeMap.clear();
  }
  
  /**
   * Given an INode get all the path complents leading to it from the root.
   * If an Inode corresponding to C is given in /A/B/C, the returned
   * patch components will be {root, A, B, C}.
   * Note that this method cannot handle scenarios where the inode is in a
   * snapshot.
   */
  public static byte[][] getPathComponents(INode inode) {
    List<byte[]> components = new ArrayList<byte[]>();
    components.add(0, inode.getLocalNameBytes());
    while(inode.getParent() != null) {
      components.add(0, inode.getParent().getLocalNameBytes());
      inode = inode.getParent();
    }
    return components.toArray(new byte[components.size()][]);
  }

  /** Check if a given inode name is reserved */
  public static boolean isReservedName(INode inode) {
    return CHECK_RESERVED_FILE_NAMES
            && Arrays.equals(inode.getLocalNameBytes(), DOT_RESERVED);
  }

  /** Check if a given path is reserved */
  public static boolean isReservedName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX + Path.SEPARATOR);
  }

  public static boolean isExactReservedName(String src) {
    return CHECK_RESERVED_FILE_NAMES && src.equals(DOT_RESERVED_PATH_PREFIX);
  }

  public static boolean isExactReservedName(byte[][] components) {
    return CHECK_RESERVED_FILE_NAMES &&
           (components.length == 2) &&
           isReservedName(components);
  }

  static boolean isReservedRawName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX +
        Path.SEPARATOR + RAW_STRING);
  }

  static boolean isReservedInodesName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX +
        Path.SEPARATOR + DOT_INODES_STRING);
  }

  static boolean isReservedName(byte[][] components) {
    return (components.length > 1) &&
            Arrays.equals(INodeDirectory.ROOT_NAME, components[0]) &&
            Arrays.equals(DOT_RESERVED, components[1]);
  }

  static boolean isReservedRawName(byte[][] components) {
    return (components.length > 2) &&
           isReservedName(components) &&
           Arrays.equals(RAW, components[2]);
  }

  /**
   * Resolve a /.reserved/... path to a non-reserved path.
   * <p/>
   * There are two special hierarchies under /.reserved/:
   * <p/>
   * /.reserved/.inodes/<inodeid> performs a path lookup by inodeid,
   * <p/>
   * /.reserved/raw/... returns the encrypted (raw) bytes of a file in an
   * encryption zone. For instance, if /ezone is an encryption zone, then
   * /ezone/a refers to the decrypted file and /.reserved/raw/ezone/a refers to
   * the encrypted (raw) bytes of /ezone/a.
   * <p/>
   * Pathnames in the /.reserved/raw directory that resolve to files not in an
   * encryption zone are equivalent to the corresponding non-raw path. Hence,
   * if /a/b/c refers to a file that is not in an encryption zone, then
   * /.reserved/raw/a/b/c is equivalent (they both refer to the same
   * unencrypted file).
   * 
   * @param pathComponents to be resolved
   * @param fsd FSDirectory
   * @return if the path indicates an inode, return path after replacing up to
   *         <inodeid> with the corresponding path of the inode, else the path
   *         in {@code pathComponents} as is. If the path refers to a path in
   *         the "raw" directory, return the non-raw pathname.
   * @throws FileNotFoundException if inodeid is invalid
   */
  static byte[][] resolveComponents(byte[][] pathComponents,
      FSDirectory fsd) throws FileNotFoundException {
    final int nComponents = pathComponents.length;
    if (nComponents < 3 || !isReservedName(pathComponents)) {
      /* This is not a /.reserved/ path so do nothing. */
    } else if (Arrays.equals(DOT_INODES, pathComponents[2])) {
      /* It's a /.reserved/.inodes path. */
      if (nComponents > 3) {
        pathComponents = resolveDotInodesPath(pathComponents, fsd);
      }
    } else if (Arrays.equals(RAW, pathComponents[2])) {
      /* It's /.reserved/raw so strip off the /.reserved/raw prefix. */
      if (nComponents == 3) {
        pathComponents = new byte[][]{INodeDirectory.ROOT_NAME};
      } else {
        if (nComponents == 4
            && Arrays.equals(DOT_RESERVED, pathComponents[3])) {
          /* It's /.reserved/raw/.reserved so don't strip */
        } else {
          pathComponents = constructRemainingPath(
              new byte[][]{INodeDirectory.ROOT_NAME}, pathComponents, 3);
        }
      }
    }
    return pathComponents;
  }

  private static byte[][] resolveDotInodesPath(
      byte[][] pathComponents, FSDirectory fsd)
      throws FileNotFoundException {
    final String inodeId = DFSUtil.bytes2String(pathComponents[3]);
    final long id;
    try {
      id = Long.parseLong(inodeId);
    } catch (NumberFormatException e) {
      throw new FileNotFoundException("Invalid inode path: " +
          DFSUtil.byteArray2PathString(pathComponents));
    }
    if (id == INodeId.ROOT_INODE_ID && pathComponents.length == 4) {
      return new byte[][]{INodeDirectory.ROOT_NAME};
    }
    INode inode = fsd.getInode(id);
    if (inode == null) {
      throw new FileNotFoundException(
          "File for given inode path does not exist: " +
              DFSUtil.byteArray2PathString(pathComponents));
    }

    // Handle single ".." for NFS lookup support.
    if ((pathComponents.length > 4)
        && Arrays.equals(pathComponents[4], DOT_DOT)) {
      INode parent = inode.getParent();
      if (parent == null || parent.getId() == INodeId.ROOT_INODE_ID) {
        // inode is root, or its parent is root.
        return new byte[][]{INodeDirectory.ROOT_NAME};
      }
      return parent.getPathComponents();
    }
    return constructRemainingPath(
        inode.getPathComponents(), pathComponents, 4);
  }

  private static byte[][] constructRemainingPath(byte[][] components,
      byte[][] extraComponents, int startAt) {
    int remainder = extraComponents.length - startAt;
    if (remainder > 0) {
      // grow the array and copy in the remaining components
      int pos = components.length;
      components = Arrays.copyOf(components, pos + remainder);
      System.arraycopy(extraComponents, startAt, components, pos, remainder);
    }
    if (NameNode.LOG.isDebugEnabled()) {
      NameNode.LOG.debug(
          "Resolved path is " + DFSUtil.byteArray2PathString(components));
    }
    return components;
  }

  INode getINode4DotSnapshot(INodesInPath iip) throws UnresolvedLinkException {
    Preconditions.checkArgument(
        iip.isDotSnapshotDir(), "%s does not end with %s",
        iip.getPath(), HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);

    final INode node = iip.getINode(-2);
    if (node != null && node.isDirectory()
        && node.asDirectory().isSnapshottable()) {
      return node;
    }
    return null;
  }

  /**
   * Resolves the given path into inodes.  Reserved paths are not handled and
   * permissions are not verified.  Client supplied paths should be
   * resolved via {@link #resolvePath(FSPermissionChecker, String, DirOp)}.
   * This method should only be used by internal methods.
   * @return the {@link INodesInPath} containing all inodes in the path.
   * @throws UnresolvedLinkException
   * @throws ParentNotDirectoryException
   * @throws AccessControlException
   */
  public INodesInPath getINodesInPath(String src, DirOp dirOp)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    return getINodesInPath(INode.getPathComponents(src), dirOp);
  }

  public INodesInPath getINodesInPath(byte[][] components, DirOp dirOp)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    INodesInPath iip = INodesInPath.resolve(rootDir, components);
    checkTraverse(null, iip, dirOp);
    return iip;
  }

  /**
   * Get {@link INode} associated with the file / directory.
   * See {@link #getINode(String, DirOp)}
   */
  @VisibleForTesting // should be removed after a lot of tests are updated
  public INode getINode(String src) throws UnresolvedLinkException,
      AccessControlException, ParentNotDirectoryException {
    return getINode(src, DirOp.READ);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   * See {@link #getINode(String, DirOp)}
   */
  @VisibleForTesting // should be removed after a lot of tests are updated
  public INode getINode4Write(String src) throws UnresolvedLinkException,
      AccessControlException, FileNotFoundException,
      ParentNotDirectoryException {
    return getINode(src, DirOp.WRITE);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INode getINode(String src, DirOp dirOp) throws UnresolvedLinkException,
      AccessControlException, ParentNotDirectoryException {
    return getINodesInPath(src, dirOp).getLastINode();
  }

  FSPermissionChecker getPermissionChecker()
    throws AccessControlException {
    try {
      return getPermissionChecker(fsOwnerShortUserName, supergroup,
          NameNode.getRemoteUser());
    } catch (IOException e) {
      throw new AccessControlException(e);
    }
  }

  @VisibleForTesting
  FSPermissionChecker getPermissionChecker(String fsOwner, String superGroup,
      UserGroupInformation ugi) throws AccessControlException {
    return new FSPermissionChecker(
        fsOwner, superGroup, ugi, getUserFilteredAttributeProvider(ugi));
  }

  void checkOwner(FSPermissionChecker pc, INodesInPath iip)
      throws AccessControlException, FileNotFoundException {
    if (iip.getLastINode() == null) {
      throw new FileNotFoundException(
          "Directory/File does not exist " + iip.getPath());
    }
    checkPermission(pc, iip, true, null, null, null, null);
  }

  void checkPathAccess(FSPermissionChecker pc, INodesInPath iip,
      FsAction access) throws AccessControlException {
    checkPermission(pc, iip, false, null, null, access, null);
  }
  void checkParentAccess(FSPermissionChecker pc, INodesInPath iip,
      FsAction access) throws AccessControlException {
    checkPermission(pc, iip, false, null, access, null, null);
  }

  void checkAncestorAccess(FSPermissionChecker pc, INodesInPath iip,
      FsAction access) throws AccessControlException {
    checkPermission(pc, iip, false, access, null, null, null);
  }

  void checkTraverse(FSPermissionChecker pc, INodesInPath iip,
      boolean resolveLink) throws AccessControlException,
        UnresolvedPathException, ParentNotDirectoryException {
    FSPermissionChecker.checkTraverse(
        isPermissionEnabled ? pc : null, iip, resolveLink);
  }

  void checkTraverse(FSPermissionChecker pc, INodesInPath iip,
      DirOp dirOp) throws AccessControlException, UnresolvedPathException,
          ParentNotDirectoryException {
    final boolean resolveLink;
    switch (dirOp) {
      case READ_LINK:
      case WRITE_LINK:
      case CREATE_LINK:
        resolveLink = false;
        break;
      default:
        resolveLink = true;
        break;
    }
    checkTraverse(pc, iip, resolveLink);
    boolean allowSnapshot = (dirOp == DirOp.READ || dirOp == DirOp.READ_LINK);
    if (!allowSnapshot && iip.isSnapshot()) {
      throw new SnapshotAccessControlException(
          "Modification on a read-only snapshot is disallowed");
    }
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  void checkPermission(FSPermissionChecker pc, INodesInPath iip,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess)
    throws AccessControlException {
    checkPermission(pc, iip, doCheckOwner, ancestorAccess,
        parentAccess, access, subAccess, false);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  void checkPermission(FSPermissionChecker pc, INodesInPath iip,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess, boolean ignoreEmptyDir)
      throws AccessControlException {
    if (!pc.isSuperUser()) {
      readLock();
      try {
        pc.checkPermission(iip, doCheckOwner, ancestorAccess,
            parentAccess, access, subAccess, ignoreEmptyDir);
      } finally {
        readUnlock();
      }
    }
  }

  void checkUnreadableBySuperuser(FSPermissionChecker pc, INodesInPath iip)
      throws IOException {
    if (pc.isSuperUser()) {
      if (FSDirXAttrOp.getXAttrByPrefixedName(this, iip,
          SECURITY_XATTR_UNREADABLE_BY_SUPERUSER) != null) {
        throw new AccessControlException(
            "Access is denied for " + pc.getUser() + " since the superuser "
            + "is not allowed to perform this operation.");
      }
    }
  }

  FileStatus getAuditFileInfo(INodesInPath iip)
      throws IOException {
    if (!namesystem.isAuditEnabled() || !namesystem.isExternalInvocation()) {
      return null;
    }

    final INode inode = iip.getLastINode();
    if (inode == null) {
      return null;
    }
    final int snapshot = iip.getPathSnapshotId();

    Path symlink = null;
    long size = 0;     // length is zero for directories
    short replication = 0;
    long blocksize = 0;

    if (inode.isFile()) {
      final INodeFile fileNode = inode.asFile();
      size = fileNode.computeFileSize(snapshot);
      replication = fileNode.getFileReplication(snapshot);
      blocksize = fileNode.getPreferredBlockSize();
    } else if (inode.isSymlink()) {
      symlink = new Path(
          DFSUtilClient.bytes2String(inode.asSymlink().getSymlink()));
    }

    return new FileStatus(
        size,
        inode.isDirectory(),
        replication,
        blocksize,
        inode.getModificationTime(snapshot),
        inode.getAccessTime(snapshot),
        inode.getFsPermission(snapshot),
        inode.getUserName(snapshot),
        inode.getGroupName(snapshot),
        symlink,
        new Path(iip.getPath()));
  }

  /**
   * Verify that parent directory of src exists.
   */
  void verifyParentDir(INodesInPath iip)
      throws FileNotFoundException, ParentNotDirectoryException {
    if (iip.length() > 2) {
      final INode parentNode = iip.getINode(-2);
      if (parentNode == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + iip.getParentPath());
      } else if (!parentNode.isDirectory()) {
        throw new ParentNotDirectoryException("Parent path is not a directory: "
            + iip.getParentPath());
      }
    }
  }

  /** Allocate a new inode ID. */
  long allocateNewInodeId() {
    return inodeId.nextValue();
  }

  /** @return the last inode ID. */
  public long getLastInodeId() {
    return inodeId.getCurrentValue();
  }

  /**
   * Set the last allocated inode id when fsimage or editlog is loaded.
   */
  void resetLastInodeId(long newValue) throws IOException {
    try {
      inodeId.skipTo(newValue);
    } catch(IllegalStateException ise) {
      throw new IOException(ise);
    }
  }

  /** Should only be used for tests to reset to any value */
  void resetLastInodeIdWithoutChecking(long newValue) {
    inodeId.setCurrentValue(newValue);
  }

  INodeAttributes getAttributes(INodesInPath iip)
      throws IOException {
    INode node = FSDirectory.resolveLastINode(iip);
    int snapshot = iip.getPathSnapshotId();
    INodeAttributes nodeAttrs = node.getSnapshotINode(snapshot);
    UserGroupInformation ugi = NameNode.getRemoteUser();
    INodeAttributeProvider ap = this.getUserFilteredAttributeProvider(ugi);
    if (ap != null) {
      // permission checking sends the full components array including the
      // first empty component for the root.  however file status
      // related calls are expected to strip out the root component according
      // to TestINodeAttributeProvider.
      byte[][] components = iip.getPathComponents();
      components = Arrays.copyOfRange(components, 1, components.length);
      nodeAttrs = ap.getAttributes(components, nodeAttrs);
    }
    return nodeAttrs;
  }

}
