/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.PartialListing;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsPathHandle;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * The ViewDistributedFileSystem is an extended class to DistributedFileSystem
 * with additional mounting functionality. The goal is to have better API
 * compatibility for HDFS users when using mounting
 * filesystem(ViewFileSystemOverloadScheme).
 * The ViewFileSystemOverloadScheme{@link ViewFileSystemOverloadScheme} is a new
 * filesystem with inherited mounting functionality from ViewFileSystem.
 * For the user who is using ViewFileSystemOverloadScheme by setting
 * fs.hdfs.impl=org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme, now
 * they can set fs.hdfs.impl=org.apache.hadoop.hdfs.ViewDistributedFileSystem.
 * So, that the hdfs users will get closely compatible API with mount
 * functionality. For the rest of all other schemes can continue to use
 * ViewFileSystemOverloadScheme class directly for mount functionality. Please
 * note that ViewFileSystemOverloadScheme provides only
 * ViewFileSystem{@link ViewFileSystem} APIs.
 * If user configured this class but no mount point configured? Then it will
 * simply work as existing DistributedFileSystem class. If user configured both
 * fs.hdfs.impl to this class and mount configurations, then users will be able
 * to make calls the APIs available in this class, they are nothing but DFS
 * APIs, but they will be delegated to viewfs functionality. Please note, APIs
 * without any path in arguments( ex: isInSafeMode), will be delegated to
 * default filesystem only, that is the configured fallback link. If you want to
 * make these API calls on specific child filesystem, you may want to initialize
 * them separately and call. In ViewDistributedFileSystem, linkFallBack is
 * mandatory when you ass mount links and it must be to your base cluster,
 * usually your current fs.defaultFS if that's pointing to hdfs.
 */
public class ViewDistributedFileSystem extends DistributedFileSystem {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ViewDistributedFileSystem.class);

  // A mounting file system.
  private ViewFileSystemOverloadScheme vfs;
  // A default DFS, which should have set via linkFallback
  private DistributedFileSystem defaultDFS;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    try {
      this.vfs = tryInitializeMountingViewFs(uri, conf);
    } catch (IOException ioe) {
      LOGGER.debug(
          "Mount tree initialization failed with the reason => {}. Falling" +
              " back to regular DFS initialization. Please" + " re-initialize" +
              " the fs after updating mount point.",
          ioe.getMessage());
      // Previous super.initialize would have skipped the dfsclient init as we
      // planned to initialize vfs. Since vfs init failed, let's init dfsClient
      // now.
      super.initDFSClient(uri, conf);
      return;
    }
    setConf(conf);
    // A child DFS with the current initialized URI. This must be same as
    // fallback fs. The fallback must point to root of your filesystems.
    // Some APIs(without path in argument, for example isInSafeMode) will
    // support only for base cluster filesystem. Only that APIs will use this
    // fs.
    defaultDFS = (DistributedFileSystem) this.vfs.getFallbackFileSystem();
    Preconditions
        .checkNotNull(defaultDFS, "In ViewHDFS fallback link is mandatory.");
    // Please don't access internal dfs directly except in tests.
    dfs = defaultDFS.dfs;
  }

  @Override
  DFSClient initDFSClient(URI uri, Configuration conf) throws IOException {
    if (this.vfs == null) {
      // There is not vfs, so let's initialise regular DFSClient.
      return super.initDFSClient(uri, conf);
    }
    return null;
  }

  public ViewDistributedFileSystem() {
  }

  private ViewFileSystemOverloadScheme tryInitializeMountingViewFs(URI theUri,
      Configuration conf) throws IOException {
    ViewFileSystemOverloadScheme viewFs = new ViewFileSystemOverloadScheme();
    viewFs.setSupportAutoAddingFallbackOnNoMounts(false);
    viewFs.initialize(theUri, conf);
    return viewFs;
  }

  @Override
  public URI getUri() {
    if (this.vfs == null) {
      return super.getUri();
    }
    return this.vfs.getUri();
  }

  @Override
  public String getScheme() {
    if (this.vfs == null) {
      return super.getScheme();
    }
    return this.vfs.getScheme();
  }

  @Override
  public Path getWorkingDirectory() {
    if (this.vfs == null) {
      return super.getWorkingDirectory();
    }
    return this.vfs.getWorkingDirectory();
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    if (this.vfs == null) {
      super.setWorkingDirectory(dir);
      return;
    }
    this.vfs.setWorkingDirectory(dir);
  }

  @Override
  public Path getHomeDirectory() {
    if (this.vfs == null) {
      return super.getHomeDirectory();
    }
    return this.vfs.getHomeDirectory();
  }

  /**
   * Returns only default cluster getHedgedReadMetrics.
   */
  @Override
  public DFSHedgedReadMetrics getHedgedReadMetrics() {
    if (this.vfs == null) {
      return super.getHedgedReadMetrics();
    }
    return defaultDFS.getHedgedReadMetrics();
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus fs, long start,
      long len) throws IOException {
    if (this.vfs == null) {
      return super.getFileBlockLocations(fs, start, len);
    }
    return this.vfs.getFileBlockLocations(fs, start, len);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, final long start,
      final long len) throws IOException {
    if (this.vfs == null) {
      return super.getFileBlockLocations(p, start, len);
    }
    return this.vfs.getFileBlockLocations(p, start, len);
  }

  @Override
  public void setVerifyChecksum(final boolean verifyChecksum) {
    if (this.vfs == null) {
      super.setVerifyChecksum(verifyChecksum);
      return;
    }
    this.vfs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public boolean recoverLease(final Path f) throws IOException {
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "recoverLease");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .recoverLease(mountPathInfo.getPathOnTarget());
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      return super.open(f, bufferSize);
    }

    return this.vfs.open(f, bufferSize);
  }

  @Override
  public FSDataInputStream open(PathHandle fd, int bufferSize)
      throws IOException {
    return this.vfs.open(fd, bufferSize);
  }

  @Override
  protected HdfsPathHandle createPathHandle(FileStatus st,
      Options.HandleOpt... opts) {
    if (this.vfs == null) {
      return super.createPathHandle(st, opts);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    if (this.vfs == null) {
      return super.append(f, bufferSize, progress);
    }
    return this.vfs.append(f, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
      final int bufferSize, final Progressable progress) throws IOException {
    if (this.vfs == null) {
      return super.append(f, flag, bufferSize, progress);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "append");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .append(mountPathInfo.getPathOnTarget(), flag, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
      final int bufferSize, final Progressable progress,
      final InetSocketAddress[] favoredNodes) throws IOException {
    if (this.vfs == null) {
      return super.append(f, flag, bufferSize, progress, favoredNodes);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "append");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .append(mountPathInfo.getPathOnTarget(), flag, bufferSize, progress,
            favoredNodes);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    if (this.vfs == null) {
      return super
          .create(f, permission, overwrite, bufferSize, replication, blockSize,
              progress);
    }
    return this.vfs
        .create(f, permission, overwrite, bufferSize, replication, blockSize,
            progress);
  }

  @Override
  public HdfsDataOutputStream create(final Path f,
      final FsPermission permission, final boolean overwrite,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress, final InetSocketAddress[] favoredNodes)
      throws IOException {
    if (this.vfs == null) {
      return super
          .create(f, permission, overwrite, bufferSize, replication, blockSize,
              progress, favoredNodes);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "create");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .create(mountPathInfo.getPathOnTarget(), permission, overwrite,
            bufferSize, replication, blockSize, progress, favoredNodes);
  }

  @Override
  //DFS specific API
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final EnumSet<CreateFlag> cflags, final int bufferSize,
      final short replication, final long blockSize,
      final Progressable progress, final Options.ChecksumOpt checksumOpt)
      throws IOException {
    if (this.vfs == null) {
      return super
          .create(f, permission, cflags, bufferSize, replication, blockSize,
              progress, checksumOpt);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "create");
    return mountPathInfo.getTargetFs()
        .create(mountPathInfo.getPathOnTarget(), permission, cflags, bufferSize,
            replication, blockSize, progress, checksumOpt);
  }

  void checkDFS(FileSystem fs, String methodName) {
    if (!(fs instanceof DistributedFileSystem)) {
      String msg = new StringBuilder("This API:").append(methodName)
          .append(" is specific to DFS. Can't run on other fs:")
          .append(fs.getUri()).toString();
      throw new UnsupportedOperationException(msg);
    }
  }

  @Override
  // DFS specific API
  protected HdfsDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
      short replication, long blockSize, Progressable progress,
      Options.ChecksumOpt checksumOpt) throws IOException {
    if (this.vfs == null) {
      return super
          .primitiveCreate(f, absolutePermission, flag, bufferSize, replication,
              blockSize, progress, checksumOpt);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "primitiveCreate");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .primitiveCreate(f, absolutePermission, flag, bufferSize, replication,
            blockSize, progress, checksumOpt);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    if (this.vfs == null) {
      return super
          .createNonRecursive(f, permission, flags, bufferSize, replication,
              bufferSize, progress);
    }
    return this.vfs
        .createNonRecursive(f, permission, flags, bufferSize, replication,
            bufferSize, progress);
  }

  @Override
  public boolean setReplication(final Path f, final short replication)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      return super.setReplication(f, replication);
    }
    return this.vfs.setReplication(f, replication);
  }

  @Override
  public void setStoragePolicy(Path src, String policyName) throws IOException {
    if (this.vfs == null) {
      super.setStoragePolicy(src, policyName);
      return;
    }
    this.vfs.setStoragePolicy(src, policyName);
  }

  @Override
  public void unsetStoragePolicy(Path src) throws IOException {
    if (this.vfs == null) {
      super.unsetStoragePolicy(src);
      return;
    }
    this.vfs.unsetStoragePolicy(src);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path src) throws IOException {
    if (this.vfs == null) {
      return super.getStoragePolicy(src);
    }
    return this.vfs.getStoragePolicy(src);
  }

  @Override
  public Collection<BlockStoragePolicy> getAllStoragePolicies()
      throws IOException {
    if (this.vfs == null) {
      return super.getAllStoragePolicies();
    }
    Collection<? extends BlockStoragePolicySpi> allStoragePolicies =
        this.vfs.getAllStoragePolicies();
    return (Collection<BlockStoragePolicy>) allStoragePolicies;
  }

  @Override
  public long getBytesWithFutureGenerationStamps() throws IOException {
    if (this.vfs == null) {
      return super.getBytesWithFutureGenerationStamps();
    }
    return defaultDFS.getBytesWithFutureGenerationStamps();
  }

  @Deprecated
  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    if (this.vfs == null) {
      return super.getStoragePolicies();
    }
    return defaultDFS.getStoragePolicies();
  }

  @Override
  //Make sure your target fs supports this API, otherwise you will get
  // Unsupported operation exception.
  public void concat(Path trg, Path[] psrcs) throws IOException {
    if (this.vfs == null) {
      super.concat(trg, psrcs);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(trg, getConf());
    mountPathInfo.getTargetFs().concat(mountPathInfo.getPathOnTarget(), psrcs);
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    if (this.vfs == null) {
      return super.rename(src, dst);
    }
    if (getMountPoints().length == 0) {
      return this.defaultDFS.rename(src, dst);
    }
    return this.vfs.rename(src, dst);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void rename(Path src, Path dst, final Options.Rename... options)
      throws IOException {
    if (this.vfs == null) {
      super.rename(src, dst, options);
      return;
    }

    // TODO: revisit
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountSrcPathInfo =
        this.vfs.getMountPathInfo(src, getConf());
    checkDFS(mountSrcPathInfo.getTargetFs(), "rename");

    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountDstPathInfo =
        this.vfs.getMountPathInfo(src, getConf());
    checkDFS(mountDstPathInfo.getTargetFs(), "rename");

    //Check both in same cluster.
    if (!mountSrcPathInfo.getTargetFs().getUri()
        .equals(mountDstPathInfo.getTargetFs().getUri())) {
      throw new HadoopIllegalArgumentException(
          "Can't rename across file systems.");
    }

    ((DistributedFileSystem) mountSrcPathInfo.getTargetFs())
        .rename(mountSrcPathInfo.getPathOnTarget(),
            mountDstPathInfo.getPathOnTarget(), options);
  }

  @Override
  public boolean truncate(final Path f, final long newLength)
      throws IOException {
    if (this.vfs == null) {
      return super.truncate(f, newLength);
    }
    return this.vfs.truncate(f, newLength);
  }

  public boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      return super.delete(f, recursive);
    }
    return this.vfs.delete(f, recursive);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    if (this.vfs == null) {
      return super.getContentSummary(f);
    }
    return this.vfs.getContentSummary(f);
  }

  @Override
  public QuotaUsage getQuotaUsage(Path f) throws IOException {
    if (this.vfs == null) {
      return super.getQuotaUsage(f);
    }
    return this.vfs.getQuotaUsage(f);
  }

  @Override
  public void setQuota(Path src, final long namespaceQuota,
      final long storagespaceQuota) throws IOException {
    if (this.vfs == null) {
      super.setQuota(src, namespaceQuota, storagespaceQuota);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(src, getConf());
    mountPathInfo.getTargetFs()
        .setQuota(mountPathInfo.getPathOnTarget(), namespaceQuota,
            storagespaceQuota);
  }

  @Override
  public void setQuotaByStorageType(Path src, final StorageType type,
      final long quota) throws IOException {
    if (this.vfs == null) {
      super.setQuotaByStorageType(src, type, quota);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(src, getConf());
    mountPathInfo.getTargetFs()
        .setQuotaByStorageType(mountPathInfo.getPathOnTarget(), type, quota);
  }

  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
    if (this.vfs == null) {
      return super.listStatus(p);
    }
    return this.vfs.listStatus(p);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter) throws FileNotFoundException, IOException {
    if (this.vfs == null) {
      return super.listLocatedStatus(f, filter);
    }
    return this.vfs.listLocatedStatus(f, filter);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path p)
      throws IOException {
    if (this.vfs == null) {
      return super.listStatusIterator(p);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(p, getConf());
    return mountPathInfo.getTargetFs()
        .listStatusIterator(mountPathInfo.getPathOnTarget());
  }

  @Override
  public RemoteIterator<PartialListing<FileStatus>> batchedListStatusIterator(
      final List<Path> paths) throws IOException {
    if (this.vfs == null) {
      return super.batchedListStatusIterator(paths);
    }
    // TODO: revisit for correct implementation.
    return this.defaultDFS.batchedListStatusIterator(paths);
  }

  @Override
  public RemoteIterator<PartialListing<LocatedFileStatus>> batchedListLocatedStatusIterator(
      final List<Path> paths) throws IOException {
    if (this.vfs == null) {
      return super.batchedListLocatedStatusIterator(paths);
    }
    // TODO: revisit for correct implementation.
    return this.defaultDFS.batchedListLocatedStatusIterator(paths);
  }

  public boolean mkdir(Path f, FsPermission permission) throws IOException {
    if (this.vfs == null) {
      return super.mkdir(f, permission);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "mkdir");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .mkdir(mountPathInfo.getPathOnTarget(), permission);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    if (this.vfs == null) {
      return super.mkdirs(f, permission);
    }
    return this.vfs.mkdirs(f, permission);
  }

  @SuppressWarnings("deprecation")
  @Override
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
      throws IOException {
    if (this.vfs == null) {
      return super.primitiveMkdir(f, absolutePermission);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "primitiveMkdir");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .primitiveMkdir(mountPathInfo.getPathOnTarget(), absolutePermission);
  }

  @Override
  public void close() throws IOException {
    if (this.vfs != null) {
      this.vfs.close();
    }
    super.close();
  }

  @InterfaceAudience.Private
  public DFSClient getClient() {
    if (this.vfs == null) {
      return super.getClient();
    }
    return defaultDFS.getClient();
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    if (this.vfs == null) {
      return super.getStatus(p);
    }
    return this.vfs.getStatus(p);
  }

  @Override
  public long getMissingBlocksCount() throws IOException {
    if (this.vfs == null) {
      return super.getMissingBlocksCount();
    }
    throw new UnsupportedOperationException(
        "getMissingBlocksCount is not supported in ViewDFS");
  }

  @Override
  public long getPendingDeletionBlocksCount() throws IOException {
    if (this.vfs == null) {
      return super.getPendingDeletionBlocksCount();
    }
    throw new UnsupportedOperationException(
        "getPendingDeletionBlocksCount is not supported in ViewDFS");
  }

  @Override
  public long getMissingReplOneBlocksCount() throws IOException {
    if (this.vfs == null) {
      return super.getMissingReplOneBlocksCount();
    }
    throw new UnsupportedOperationException(
        "getMissingReplOneBlocksCount is not supported in ViewDFS");
  }

  @Override
  public long getLowRedundancyBlocksCount() throws IOException {
    if (this.vfs == null) {
      return super.getLowRedundancyBlocksCount();
    }
    throw new UnsupportedOperationException(
        "getLowRedundancyBlocksCount is not supported in ViewDFS");
  }

  @Override
  public long getCorruptBlocksCount() throws IOException {
    if (this.vfs == null) {
      return super.getCorruptBlocksCount();
    }
    throw new UnsupportedOperationException(
        "getCorruptBlocksCount is not supported in ViewDFS");
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(final Path path)
      throws IOException {
    if (this.vfs == null) {
      return super.listCorruptFileBlocks(path);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    return mountPathInfo.getTargetFs()
        .listCorruptFileBlocks(mountPathInfo.getPathOnTarget());
  }

  @Override
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    if (this.vfs == null) {
      return super.getDataNodeStats();
    }
    return defaultDFS.getDataNodeStats();
  }

  @Override
  public DatanodeInfo[] getDataNodeStats(
      final HdfsConstants.DatanodeReportType type) throws IOException {
    if (this.vfs == null) {
      return super.getDataNodeStats(type);
    }
    return defaultDFS.getDataNodeStats(type);
  }

  @Override
  public boolean setSafeMode(HdfsConstants.SafeModeAction action)
      throws IOException {
    if (this.vfs == null) {
      return super.setSafeMode(action);
    }
    return defaultDFS.setSafeMode(action);
  }

  @Override
  public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    if (this.vfs == null) {
      return super.setSafeMode(action, isChecked);
    }
    return defaultDFS.setSafeMode(action, isChecked);
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    if (this.vfs == null) {
      return super.saveNamespace(timeWindow, txGap);
    }
    return defaultDFS.saveNamespace(timeWindow, txGap);
  }

  @Override
  public void saveNamespace() throws IOException {
    if (this.vfs == null) {
      super.saveNamespace();
      return;
    }
    defaultDFS.saveNamespace();
  }

  @Override
  public long rollEdits() throws IOException {
    if (this.vfs == null) {
      return super.rollEdits();
    }
    return defaultDFS.rollEdits();
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException {
    if (this.vfs == null) {
      return super.restoreFailedStorage(arg);
    }
    return defaultDFS.restoreFailedStorage(arg);
  }

  @Override
  public void refreshNodes() throws IOException {
    if (this.vfs == null) {
      super.refreshNodes();
      return;
    }
    defaultDFS.refreshNodes();
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    if (this.vfs == null) {
      super.finalizeUpgrade();
      return;
    }
    defaultDFS.finalizeUpgrade();
  }

  @Override
  public boolean upgradeStatus() throws IOException {
    if (this.vfs == null) {
      return super.upgradeStatus();
    }
    return defaultDFS.upgradeStatus();
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(
      HdfsConstants.RollingUpgradeAction action) throws IOException {
    if (this.vfs == null) {
      return super.rollingUpgrade(action);
    }
    return defaultDFS.rollingUpgrade(action);
  }

  @Override
  public void metaSave(String pathname) throws IOException {
    if (this.vfs == null) {
      super.metaSave(pathname);
      return;
    }
    defaultDFS.metaSave(pathname);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    if (this.vfs == null) {
      return super.getServerDefaults();
    }
    //TODO: Need to revisit.
    return defaultDFS.getServerDefaults();
  }

  @Override
  public FileStatus getFileStatus(final Path f)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      return super.getFileStatus(f);
    }
    return this.vfs.getFileStatus(f);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws IOException {
     // Regular DFS behavior
    if (this.vfs == null) {
      super.createSymlink(target, link, createParent);
      return;
    }

    // Mounting ViewHDFS behavior
    // TODO: revisit
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(target, getConf());
    mountPathInfo.getTargetFs()
        .createSymlink(mountPathInfo.getPathOnTarget(), link, createParent);
  }

  @Override
  public boolean supportsSymlinks() {
    if (this.vfs == null) {
      return super.supportsSymlinks();
    }
    // TODO: we can enabled later if we want to support symlinks.
    return false;
  }

  @Override
  public FileStatus getFileLinkStatus(final Path f) throws IOException {
    if (this.vfs == null) {
      return super.getFileLinkStatus(f);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    return mountPathInfo.getTargetFs()
        .getFileLinkStatus(mountPathInfo.getPathOnTarget());
  }

  @Override
  public Path getLinkTarget(Path path) throws IOException {
    if(this.vfs==null){
      return super.getLinkTarget(path);
    }
    return this.vfs.getLinkTarget(path);
  }

  @Override
  protected Path resolveLink(Path f) throws IOException {
    if(this.vfs==null){
      return super.resolveLink(f);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(f, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "resolveLink");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .resolveLink(mountPathInfo.getPathOnTarget());
  }

  @Override
  public FileChecksum getFileChecksum(final Path f)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      return super.getFileChecksum(f);
    }
    return this.vfs.getFileChecksum(f);
  }

  @Override
  public void setPermission(final Path f, final FsPermission permission)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      super.setPermission(f, permission);
      return;
    }
    this.vfs.setPermission(f, permission);
  }

  @Override
  public void setOwner(final Path f, final String username,
      final String groupname)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      super.setOwner(f, username, groupname);
      return;
    }
    this.vfs.setOwner(f, username, groupname);
  }

  @Override
  public void setTimes(final Path f, final long mtime, final long atime)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      super.setTimes(f, mtime, atime);
      return;
    }
    this.vfs.setTimes(f, mtime, atime);
  }

  @Override
  // DFS specific API
  protected int getDefaultPort() {
    return super.getDefaultPort();
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    if (this.vfs == null) {
      return super.getDelegationToken(renewer);
    }
    //Let applications call getDelegationTokenIssuers and get respective
    // delegation tokens from child fs.
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    if (this.vfs == null) {
      super.setBalancerBandwidth(bandwidth);
      return;
    }
    defaultDFS.setBalancerBandwidth(bandwidth);
  }

  @Override
  public String getCanonicalServiceName() {
    if (this.vfs == null) {
      return super.getCanonicalServiceName();
    }
    return defaultDFS.getCanonicalServiceName();
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    if (this.vfs == null) {
      return super.canonicalizeUri(uri);
    }

    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo = null;
    try {
      mountPathInfo = this.vfs.getMountPathInfo(new Path(uri), getConf());
    } catch (IOException e) {
      //LOG.error("Failed to resolve the uri as mount path", e);
      return null;
    }
    checkDFS(mountPathInfo.getTargetFs(), "canonicalizeUri");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .canonicalizeUri(uri);
  }

  @Override
  public boolean isInSafeMode() throws IOException {
    if (this.vfs == null) {
      return super.isInSafeMode();
    }
    return defaultDFS.isInSafeMode();
  }

  @Override
  // DFS specific API
  public void allowSnapshot(Path path) throws IOException {
    if (this.vfs == null) {
      super.allowSnapshot(path);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "allowSnapshot");
    ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .allowSnapshot(mountPathInfo.getPathOnTarget());
  }

  @Override
  public void disallowSnapshot(final Path path) throws IOException {
    if (this.vfs == null) {
      super.disallowSnapshot(path);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "disallowSnapshot");
    ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .disallowSnapshot(mountPathInfo.getPathOnTarget());
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    if (this.vfs == null) {
      return super.createSnapshot(path, snapshotName);
    }
    return this.vfs.createSnapshot(path, snapshotName);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    if (this.vfs == null) {
      super.renameSnapshot(path, snapshotOldName, snapshotOldName);
      return;
    }
    this.vfs.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }

  @Override
  //Ony for HDFS users
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    if (this.vfs == null) {
      return super.getSnapshottableDirListing();
    }
    return defaultDFS.getSnapshottableDirListing();
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName)
      throws IOException {
    if (this.vfs == null) {
      super.deleteSnapshot(path, snapshotName);
      return;
    }
    this.vfs.deleteSnapshot(path, snapshotName);
  }

  @Override
  public RemoteIterator<SnapshotDiffReportListing> snapshotDiffReportListingRemoteIterator(
      final Path snapshotDir, final String fromSnapshot,
      final String toSnapshot) throws IOException {
    if (this.vfs == null) {
      return super
          .snapshotDiffReportListingRemoteIterator(snapshotDir, fromSnapshot,
              toSnapshot);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(snapshotDir, getConf());
    checkDFS(mountPathInfo.getTargetFs(),
        "snapshotDiffReportListingRemoteIterator");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .snapshotDiffReportListingRemoteIterator(
            mountPathInfo.getPathOnTarget(), fromSnapshot, toSnapshot);
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(final Path snapshotDir,
      final String fromSnapshot, final String toSnapshot) throws IOException {
    if(this.vfs ==null){
      return super.getSnapshotDiffReport(snapshotDir, fromSnapshot,
          toSnapshot);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(snapshotDir, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "getSnapshotDiffReport");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .getSnapshotDiffReport(snapshotDir, fromSnapshot,
            toSnapshot);
  }

  @Override
  public boolean isFileClosed(final Path src) throws IOException {
    if (this.vfs == null) {
      return super.isFileClosed(src);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(src, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "isFileClosed");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .isFileClosed(mountPathInfo.getPathOnTarget());
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo info) throws IOException {
    if (this.vfs == null) {
      return super.addCacheDirective(info);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(info.getPath(), getConf());
    checkDFS(mountPathInfo.getTargetFs(), "addCacheDirective");

    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .addCacheDirective(new CacheDirectiveInfo.Builder(info)
            .setPath(mountPathInfo.getPathOnTarget()).build());
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo info,
      EnumSet<CacheFlag> flags) throws IOException {
    if (this.vfs == null) {
      return super.addCacheDirective(info, flags);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(info.getPath(), getConf());
    checkDFS(mountPathInfo.getTargetFs(), "addCacheDirective");

    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .addCacheDirective(new CacheDirectiveInfo.Builder(info)
            .setPath(mountPathInfo.getPathOnTarget()).build(), flags);
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo info) throws IOException {
    if (this.vfs == null) {
      super.modifyCacheDirective(info);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(info.getPath(), getConf());
    checkDFS(mountPathInfo.getTargetFs(), "modifyCacheDirective");

    ((DistributedFileSystem) mountPathInfo.getTargetFs()).modifyCacheDirective(
        new CacheDirectiveInfo.Builder(info)
            .setPath(mountPathInfo.getPathOnTarget()).build());
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo info,
      EnumSet<CacheFlag> flags) throws IOException {
    if (this.vfs == null) {
      super.modifyCacheDirective(info, flags);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(info.getPath(), getConf());
    checkDFS(mountPathInfo.getTargetFs(), "modifyCacheDirective");

    ((DistributedFileSystem) mountPathInfo.getTargetFs()).modifyCacheDirective(
        new CacheDirectiveInfo.Builder(info)
            .setPath(mountPathInfo.getPathOnTarget()).build(), flags);
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    if (this.vfs == null) {
      super.removeCacheDirective(id);
      return;
    }
    //defaultDFS.removeCacheDirective(id);
    //TODO: ? this can create issues in default cluster
    // if user intention is to call on specific mount.
    throw new UnsupportedOperationException();
  }

  @Override
  public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    if (this.vfs == null) {
      return super.listCacheDirectives(filter);
    }
    throw new UnsupportedOperationException(
        "listCacheDirectives is not supported in ViewDFS");
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    if (this.vfs == null) {
      super.addCachePool(info);
      return;
    }
    throw new UnsupportedOperationException(
        "addCachePool is not supported in ViewDFS");
  }

  @Override
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    if (this.vfs == null) {
      super.modifyCachePool(info);
      return;
    }
    throw new UnsupportedOperationException(
        "modifyCachePool is not supported in ViewDFS");
  }

  @Override
  public void removeCachePool(String poolName) throws IOException {
    if (this.vfs == null) {
      super.removeCachePool(poolName);
      return;
    }
    throw new UnsupportedOperationException(
        "removeCachePool is not supported in ViewDFS");
  }

  @Override
  public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
    if (this.vfs == null) {
      return super.listCachePools();
    }
    throw new UnsupportedOperationException(
        "listCachePools is not supported in ViewDFS");
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    if (this.vfs == null) {
      super.modifyAclEntries(path, aclSpec);
      return;
    }
    this.vfs.modifyAclEntries(path, aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    if (this.vfs == null) {
      super.removeAclEntries(path, aclSpec);
      return;
    }
    this.vfs.removeAclEntries(path, aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    if (this.vfs == null) {
      super.removeDefaultAcl(path);
      return;
    }
    this.vfs.removeDefaultAcl(path);
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    if (this.vfs == null) {
      super.removeAcl(path);
      return;
    }
    this.vfs.removeAcl(path);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    if (this.vfs == null) {
      super.setAcl(path, aclSpec);
      return;
    }
    this.vfs.setAcl(path, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    if (this.vfs == null) {
      return super.getAclStatus(path);
    }
    return this.vfs.getAclStatus(path);
  }

  @Override
  public void createEncryptionZone(final Path path, final String keyName)
      throws IOException {
    if (this.vfs == null) {
      super.createEncryptionZone(path, keyName);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "createEncryptionZone");
    ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .createEncryptionZone(mountPathInfo.getPathOnTarget(), keyName);
  }

  @Override
  public EncryptionZone getEZForPath(final Path path) throws IOException {
    if (this.vfs == null) {
      return super.getEZForPath(path);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "getEZForPath");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .getEZForPath(mountPathInfo.getPathOnTarget());
  }

  @Override
  public RemoteIterator<EncryptionZone> listEncryptionZones()
      throws IOException {
    if (this.vfs == null) {
      return super.listEncryptionZones();
    }
    throw new UnsupportedOperationException(
        "listEncryptionZones is not supported in ViewDFS");
  }

  @Override
  public void reencryptEncryptionZone(final Path zone,
      final HdfsConstants.ReencryptAction action) throws IOException {
    if (this.vfs == null) {
      super.reencryptEncryptionZone(zone, action);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(zone, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "reencryptEncryptionZone");
    ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .reencryptEncryptionZone(mountPathInfo.getPathOnTarget(), action);
  }

  @Override
  public RemoteIterator<ZoneReencryptionStatus> listReencryptionStatus()
      throws IOException {
    if (this.vfs == null) {
      return super.listReencryptionStatus();
    }
    throw new UnsupportedOperationException(
        "listReencryptionStatus is not supported in ViewDFS");
  }

  @Override
  public FileEncryptionInfo getFileEncryptionInfo(final Path path)
      throws IOException {
    if (this.vfs == null) {
      return super.getFileEncryptionInfo(path);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "getFileEncryptionInfo");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .getFileEncryptionInfo(mountPathInfo.getPathOnTarget());
  }

  @Override
  public void provisionEZTrash(final Path path,
      final FsPermission trashPermission) throws IOException {
    if (this.vfs == null) {
      super.provisionEZTrash(path, trashPermission);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "provisionEZTrash");
    ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .provisionEZTrash(mountPathInfo.getPathOnTarget(), trashPermission);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    if (this.vfs == null) {
      super.setXAttr(path, name, value, flag);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    mountPathInfo.getTargetFs()
        .setXAttr(mountPathInfo.getPathOnTarget(), name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    if (this.vfs == null) {
      return super.getXAttr(path, name);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    return mountPathInfo.getTargetFs()
        .getXAttr(mountPathInfo.getPathOnTarget(), name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    if (this.vfs == null) {
      return super.getXAttrs(path);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    return mountPathInfo.getTargetFs()
        .getXAttrs(mountPathInfo.getPathOnTarget());
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    if (this.vfs == null) {
      return super.getXAttrs(path, names);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    return mountPathInfo.getTargetFs()
        .getXAttrs(mountPathInfo.getPathOnTarget(), names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    if (this.vfs == null) {
      return super.listXAttrs(path);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    return mountPathInfo.getTargetFs()
        .listXAttrs(mountPathInfo.getPathOnTarget());
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    if (this.vfs == null) {
      super.removeXAttr(path, name);
      return;
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    mountPathInfo.getTargetFs()
        .removeXAttr(mountPathInfo.getPathOnTarget(), name);
  }

  @Override
  public void access(Path path, FsAction mode)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      super.access(path, mode);
      return;
    }
    this.vfs.access(path, mode);
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    if (this.vfs == null) {
      return super.getKeyProviderUri();
    }
    return defaultDFS.getKeyProviderUri();
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    if (this.vfs == null) {
      return super.getKeyProvider();
    }
    return defaultDFS.getKeyProvider();
  }

  @Override
  public DelegationTokenIssuer[] getAdditionalTokenIssuers()
      throws IOException {
    if (this.vfs == null) {
      return super.getChildFileSystems();
    }

    return this.vfs.getChildFileSystems();
  }

  @Override
  public DFSInotifyEventInputStream getInotifyEventStream() throws IOException {
    if (this.vfs == null) {
      return super.getInotifyEventStream();
    }
    throw new UnsupportedOperationException(
        "getInotifyEventStream is not supported in ViewDFS");
  }

  @Override
  public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid)
      throws IOException {
    if (this.vfs == null) {
      return super.getInotifyEventStream();
    }
    throw new UnsupportedOperationException(
        "getInotifyEventStream is not supported in ViewDFS");
  }

  @Override
  // DFS only API.
  public void setErasureCodingPolicy(final Path path, final String ecPolicyName)
      throws IOException {
    if (this.vfs == null) {
      super.setErasureCodingPolicy(path, ecPolicyName);
      return;
    }

    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "setErasureCodingPolicy");
    ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .setErasureCodingPolicy(mountPathInfo.getPathOnTarget(), ecPolicyName);
  }

  @Override
  public void satisfyStoragePolicy(Path src) throws IOException {
    if (this.vfs == null) {
      super.satisfyStoragePolicy(src);
      return;
    }
    this.vfs.satisfyStoragePolicy(src);
  }

  @Override
  public ErasureCodingPolicy getErasureCodingPolicy(final Path path)
      throws IOException {
    if (this.vfs == null) {
      return super.getErasureCodingPolicy(path);
    }

    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "getErasureCodingPolicy");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .getErasureCodingPolicy(mountPathInfo.getPathOnTarget());
  }

  @Override
  public Collection<ErasureCodingPolicyInfo> getAllErasureCodingPolicies()
      throws IOException {
    if (this.vfs == null) {
      return super.getAllErasureCodingPolicies();
    }
    return defaultDFS.getAllErasureCodingPolicies();
  }

  @Override
  public Map<String, String> getAllErasureCodingCodecs() throws IOException {
    if (this.vfs == null) {
      return super.getAllErasureCodingCodecs();
    }
    return defaultDFS.getAllErasureCodingCodecs();
  }

  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    if (this.vfs == null) {
      return super.addErasureCodingPolicies(policies);
    }
    return defaultDFS.addErasureCodingPolicies(policies);
  }

  @Override
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (this.vfs == null) {
      super.removeErasureCodingPolicy(ecPolicyName);
      return;
    }
    defaultDFS.removeErasureCodingPolicy(ecPolicyName);
  }

  @Override
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (this.vfs == null) {
      super.enableErasureCodingPolicy(ecPolicyName);
      return;
    }
    defaultDFS.enableErasureCodingPolicy(ecPolicyName);
  }

  @Override
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (this.vfs == null) {
      super.disableErasureCodingPolicy(ecPolicyName);
      return;
    }
    defaultDFS.disableErasureCodingPolicy(ecPolicyName);
  }

  @Override
  public void unsetErasureCodingPolicy(final Path path) throws IOException {

    if (this.vfs == null) {
      super.unsetErasureCodingPolicy(path);
      return;
    }

    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "unsetErasureCodingPolicy");
    ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .unsetErasureCodingPolicy(mountPathInfo.getPathOnTarget());
  }

  @Override
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      final String... policyNames) throws IOException {
    if (this.vfs == null) {
      return super.getECTopologyResultForPolicies(policyNames);
    }
    throw new UnsupportedOperationException(
        "unsetErasureCodingPolicy is not supported in ViewDFS");
  }

  @Override
  public Path getTrashRoot(Path path) {
    if (this.vfs == null) {
      return super.getTrashRoot(path);
    }
    return this.vfs.getTrashRoot(path);
  }

  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    if (this.vfs == null) {
      return super.getTrashRoots(allUsers);
    }
    List<FileStatus> trashRoots = new ArrayList<>();
    for (FileSystem fs : getChildFileSystems()) {
      trashRoots.addAll(fs.getTrashRoots(allUsers));
    }
    return trashRoots;
  }

  // Just proovided the same implementation as default in dfs as thats just
  // delegated to FileSystem parent class.
  @Override
  protected Path fixRelativePart(Path p) {
    return super.fixRelativePart(p);
  }

  Statistics getFsStatistics() {
    if (this.vfs == null) {
      return super.getFsStatistics();
    }
    return statistics;
  }

  DFSOpsCountStatistics getDFSOpsCountStatistics() {
    if (this.vfs == null) {
      return super.getDFSOpsCountStatistics();
    }
    return defaultDFS.getDFSOpsCountStatistics();
  }

  @Override
  // Works only for HDFS
  public HdfsDataOutputStreamBuilder createFile(Path path) {
    if (this.vfs == null) {
      return super.createFile(path);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo = null;
    try {
      mountPathInfo = this.vfs.getMountPathInfo(path, getConf());
    } catch (IOException e) {
      // TODO: can we return null here?
      return null;
    }
    checkDFS(mountPathInfo.getTargetFs(), "createFile");
    return (HdfsDataOutputStreamBuilder) mountPathInfo.getTargetFs()
        .createFile(mountPathInfo.getPathOnTarget());
  }

  @Deprecated
  @Override
  public RemoteIterator<OpenFileEntry> listOpenFiles() throws IOException {
    if (this.vfs == null) {
      return super.listOpenFiles();
    }
    return defaultDFS.listOpenFiles();
  }

  @Deprecated
  @Override
  public RemoteIterator<OpenFileEntry> listOpenFiles(
      EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes)
      throws IOException {
    if (this.vfs == null) {
      return super.listOpenFiles(openFilesTypes);
    }
    return defaultDFS.listOpenFiles(openFilesTypes);
  }

  @Override
  public RemoteIterator<OpenFileEntry> listOpenFiles(
      EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes, String path)
      throws IOException {
    if (this.vfs == null) {
      return super.listOpenFiles(openFilesTypes, path);
    }
    return defaultDFS.listOpenFiles(openFilesTypes, path);
  }

  @Override
  //WOrks only if it's DFS.
  public HdfsDataOutputStreamBuilder appendFile(Path path) {
    if (this.vfs == null) {
      return super.appendFile(path);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo = null;
    try {
      mountPathInfo = this.vfs.getMountPathInfo(path, getConf());
    } catch (IOException e) {
      // TODO: can we return null here?
      return null;
    }
    checkDFS(mountPathInfo.getTargetFs(), "appendFile");
    return (HdfsDataOutputStreamBuilder) mountPathInfo.getTargetFs()
        .appendFile(mountPathInfo.getPathOnTarget());
  }

  @Override
  public boolean hasPathCapability(Path path, String capability)
      throws IOException {
    if (this.vfs == null) {
      return super.hasPathCapability(path, capability);
    }
    return this.vfs.hasPathCapability(path, capability);
  }

  //Below API provided implementation in ViewFS but not there in DFS.
  @Override
  public Path resolvePath(final Path f) throws IOException {
    if (this.vfs == null) {
      return super.resolvePath(f);
    }
    return this.vfs.resolvePath(f);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean delete(final Path f)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      return super.delete(f);
    }
    return this.vfs.delete(f);
  }

  @Override
  public FileChecksum getFileChecksum(final Path f, final long length)
      throws AccessControlException, FileNotFoundException, IOException {
    if (this.vfs == null) {
      return super.getFileChecksum(f, length);
    }
    return this.vfs.getFileChecksum(f, length);
  }

  @Override
  public boolean mkdirs(Path dir) throws IOException {
    if (this.vfs == null) {
      return super.mkdirs(dir);
    }
    return this.vfs.mkdirs(dir);
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    if (this.vfs == null) {
      return super.getDefaultBlockSize(f);
    }
    return this.vfs.getDefaultBlockSize(f);
  }

  @Override
  public short getDefaultReplication(Path f) {
    if (this.vfs == null) {
      return super.getDefaultReplication(f);
    }
    return this.vfs.getDefaultReplication(f);
  }

  @Override
  public FsServerDefaults getServerDefaults(Path f) throws IOException {
    if (this.vfs == null) {
      return super.getServerDefaults(f);
    }
    return this.vfs.getServerDefaults(f);
  }

  @Override
  public void setWriteChecksum(final boolean writeChecksum) {
    if (this.vfs == null) {
      super.setWriteChecksum(writeChecksum);
      return;
    }
    this.vfs.setWriteChecksum(writeChecksum);
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    if (this.vfs == null) {
      return super.getChildFileSystems();
    }
    return this.vfs.getChildFileSystems();
  }

  public ViewFileSystem.MountPoint[] getMountPoints() {
    if (this.vfs == null) {
      return null;
    }
    return this.vfs.getMountPoints();
  }

  @Override
  public FsStatus getStatus() throws IOException {
    if (this.vfs == null) {
      return super.getStatus();
    }
    return this.vfs.getStatus();
  }

  @Override
  public long getUsed() throws IOException {
    if (this.vfs == null) {
      return super.getUsed();
    }
    return defaultDFS.getUsed();
  }
}
