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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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
import org.apache.hadoop.fs.FileUtil;
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
import org.apache.hadoop.io.MultipleIOException;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
 * them separately and call. In ViewDistributedFileSystem, we strongly recommend
 * to configure linkFallBack when you add mount links and it's recommended to
 * point be to your base cluster, usually your current fs.defaultFS if that's
 * pointing to hdfs.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
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
      LOGGER.debug(new StringBuilder("Mount tree initialization failed with ")
          .append("the reason => {}. Falling back to regular DFS")
          .append(" initialization. Please re-initialize the fs after updating")
          .append(" mount point.").toString(), ioe.getMessage());
      // Previous super.initialize would have skipped the dfsclient init and
      // setWorkingDirectory as we planned to initialize vfs. Since vfs init
      // failed, let's init dfsClient now.
      super.initDFSClient(uri, conf);
      super.setWorkingDirectory(super.getHomeDirectory());
      return;
    }

    setConf(conf);
    // A child DFS with the current initialized URI. This must be same as
    // fallback fs. The fallback must point to root of your filesystems.
    // Some APIs(without path in argument, for example isInSafeMode) will
    // support only for base cluster filesystem. Only that APIs will use this
    // fs.
    defaultDFS = (DistributedFileSystem) this.vfs.getFallbackFileSystem();
    // Please don't access internal dfs client directly except in tests.
    dfs = (defaultDFS != null) ? defaultDFS.dfs : null;
    super.setWorkingDirectory(this.vfs.getHomeDirectory());
  }

  @Override
  void initDFSClient(URI uri, Configuration conf) throws IOException {
    // Since we plan to initialize vfs in this class, we will not need to
    // initialize DFS client.
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
    if (super.dfs == null) {
      return null;
    }
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
    checkDefaultDFS(defaultDFS, "getHedgedReadMetrics");
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
    if (this.vfs == null) {
      return super.recoverLease(f);
    }

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
    if (this.vfs == null) {
      return super.open(fd, bufferSize);
    }
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
    return vfs.create(f, permission, cflags, bufferSize, replication, blockSize,
        progress, checksumOpt);
  }

  void checkDFS(FileSystem fs, String methodName) {
    if (!(fs instanceof DistributedFileSystem)) {
      String msg = new StringBuilder("This API:").append(methodName)
          .append(" is specific to DFS. Can't run on other fs:")
          .append(fs.getUri()).toString();
      throw new UnsupportedOperationException(msg);
    }
  }

  void checkDefaultDFS(FileSystem fs, String methodName) {
    if (fs == null) {
      String msg = new StringBuilder("This API:").append(methodName).append(
          " cannot be supported without default cluster(that is linkFallBack).")
          .toString();
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
    checkDefaultDFS(defaultDFS, "getBytesWithFutureGenerationStamps");
    return defaultDFS.getBytesWithFutureGenerationStamps();
  }

  @Deprecated
  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    if (this.vfs == null) {
      return super.getStoragePolicies();
    }
    checkDefaultDFS(defaultDFS, "getStoragePolicies");
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

    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountSrcPathInfo =
        this.vfs.getMountPathInfo(src, getConf());

    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountDstPathInfo =
        this.vfs.getMountPathInfo(dst, getConf());

    //Check both in same cluster.
    if (!mountSrcPathInfo.getTargetFs().getUri()
        .equals(mountDstPathInfo.getTargetFs().getUri())) {
      throw new HadoopIllegalArgumentException(
          "Can't rename across file systems.");
    }

    FileUtil.rename(mountSrcPathInfo.getTargetFs(),
        mountSrcPathInfo.getPathOnTarget(), mountDstPathInfo.getPathOnTarget(),
        options);
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
  @Override
  public DFSClient getClient() {
    if (this.vfs == null) {
      return super.getClient();
    }
    checkDefaultDFS(defaultDFS, "getClient");
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
    checkDefaultDFS(defaultDFS, "getMissingBlocksCount");
    return defaultDFS.getMissingBlocksCount();
  }

  @Override
  public long getPendingDeletionBlocksCount() throws IOException {
    if (this.vfs == null) {
      return super.getPendingDeletionBlocksCount();
    }
    checkDefaultDFS(defaultDFS, "getPendingDeletionBlocksCount");
    return defaultDFS.getPendingDeletionBlocksCount();
  }

  @Override
  public long getMissingReplOneBlocksCount() throws IOException {
    if (this.vfs == null) {
      return super.getMissingReplOneBlocksCount();
    }
    checkDefaultDFS(defaultDFS, "getMissingReplOneBlocksCount");
    return defaultDFS.getMissingReplOneBlocksCount();
  }

  @Override
  public long getLowRedundancyBlocksCount() throws IOException {
    if (this.vfs == null) {
      return super.getLowRedundancyBlocksCount();
    }
    checkDefaultDFS(defaultDFS, "getLowRedundancyBlocksCount");
    return defaultDFS.getLowRedundancyBlocksCount();
  }

  @Override
  public long getCorruptBlocksCount() throws IOException {
    if (this.vfs == null) {
      return super.getCorruptBlocksCount();
    }
    checkDefaultDFS(defaultDFS, "getCorruptBlocksCount");
    return defaultDFS.getLowRedundancyBlocksCount();
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
    checkDefaultDFS(defaultDFS, "getDataNodeStats");
    return defaultDFS.getDataNodeStats();
  }

  @Override
  public DatanodeInfo[] getDataNodeStats(
      final HdfsConstants.DatanodeReportType type) throws IOException {
    if (this.vfs == null) {
      return super.getDataNodeStats(type);
    }
    checkDefaultDFS(defaultDFS, "getDataNodeStats");
    return defaultDFS.getDataNodeStats(type);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean setSafeMode(HdfsConstants.SafeModeAction action)
      throws IOException {
    if (this.vfs == null) {
      return super.setSafeMode(action);
    }
    checkDefaultDFS(defaultDFS, "setSafeMode");
    return defaultDFS.setSafeMode(action);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    if (this.vfs == null) {
      return super.setSafeMode(action, isChecked);
    }
    checkDefaultDFS(defaultDFS, "setSafeMode");
    return defaultDFS.setSafeMode(action, isChecked);
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    if (this.vfs == null) {
      return super.saveNamespace(timeWindow, txGap);
    }
    checkDefaultDFS(defaultDFS, "saveNamespace");
    return defaultDFS.saveNamespace(timeWindow, txGap);
  }

  @Override
  public void saveNamespace() throws IOException {
    if (this.vfs == null) {
      super.saveNamespace();
      return;
    }
    checkDefaultDFS(defaultDFS, "saveNamespace");
    defaultDFS.saveNamespace();
  }

  @Override
  public long rollEdits() throws IOException {
    if (this.vfs == null) {
      return super.rollEdits();
    }
    checkDefaultDFS(defaultDFS, "rollEdits");
    return defaultDFS.rollEdits();
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException {
    if (this.vfs == null) {
      return super.restoreFailedStorage(arg);
    }
    checkDefaultDFS(defaultDFS, "restoreFailedStorage");
    return defaultDFS.restoreFailedStorage(arg);
  }

  @Override
  public void refreshNodes() throws IOException {
    if (this.vfs == null) {
      super.refreshNodes();
      return;
    }
    checkDefaultDFS(defaultDFS, "refreshNodes");
    defaultDFS.refreshNodes();
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    if (this.vfs == null) {
      super.finalizeUpgrade();
      return;
    }
    checkDefaultDFS(defaultDFS, "finalizeUpgrade");
    defaultDFS.finalizeUpgrade();
  }

  @Override
  public boolean upgradeStatus() throws IOException {
    if (this.vfs == null) {
      return super.upgradeStatus();
    }
    checkDefaultDFS(defaultDFS, "upgradeStatus");
    return defaultDFS.upgradeStatus();
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(
      HdfsConstants.RollingUpgradeAction action) throws IOException {
    if (this.vfs == null) {
      return super.rollingUpgrade(action);
    }
    checkDefaultDFS(defaultDFS, "rollingUpgrade");
    return defaultDFS.rollingUpgrade(action);
  }

  @Override
  public void metaSave(String pathname) throws IOException {
    if (this.vfs == null) {
      super.metaSave(pathname);
      return;
    }
    checkDefaultDFS(defaultDFS, "metaSave");
    defaultDFS.metaSave(pathname);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    if (this.vfs == null) {
      return super.getServerDefaults();
    }
    checkDefaultDFS(defaultDFS, "getServerDefaults");
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

    throw new UnsupportedOperationException(
        "createSymlink is not supported in ViewHDFS");
  }

  @Override
  public boolean supportsSymlinks() {
    if (this.vfs == null) {
      return super.supportsSymlinks();
    }
    // we can enabled later if we want to support symlinks.
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

  /**
   * If no mount points configured, it works same as
   * {@link DistributedFileSystem#getDelegationToken(String)}. If
   * there are mount points configured and if default fs(linkFallback)
   * configured, then it will return default fs delegation token. Otherwise
   * it will return null.
   */
  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    if (this.vfs == null) {
      return super.getDelegationToken(renewer);
    }

    if (defaultDFS != null) {
      return defaultDFS.getDelegationToken(renewer);
    }
    return null;
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    if (this.vfs == null) {
      super.setBalancerBandwidth(bandwidth);
      return;
    }
    checkDefaultDFS(defaultDFS, "setBalancerBandwidth");
    defaultDFS.setBalancerBandwidth(bandwidth);
  }

  @Override
  public String getCanonicalServiceName() {
    if (this.vfs == null) {
      return super.getCanonicalServiceName();
    }
    checkDefaultDFS(defaultDFS, "getCanonicalServiceName");
    return defaultDFS.getCanonicalServiceName();
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    if (this.vfs == null) {
      return super.canonicalizeUri(uri);
    }

    return vfs.canonicalizeUri(uri);
  }

  @Override
  public boolean isInSafeMode() throws IOException {
    if (this.vfs == null) {
      return super.isInSafeMode();
    }
    checkDefaultDFS(defaultDFS, "isInSafeMode");
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
      super.renameSnapshot(path, snapshotOldName, snapshotNewName);
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
    checkDefaultDFS(defaultDFS, "getSnapshottableDirListing");
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
    if (this.vfs == null) {
      return super.getSnapshotDiffReport(snapshotDir, fromSnapshot, toSnapshot);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(snapshotDir, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "getSnapshotDiffReport");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .getSnapshotDiffReport(mountPathInfo.getPathOnTarget(), fromSnapshot,
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
    if (info.getPath() != null) {
      ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
          this.vfs.getMountPathInfo(info.getPath(), getConf());
      checkDFS(mountPathInfo.getTargetFs(), "modifyCacheDirective");
      ((DistributedFileSystem) mountPathInfo.getTargetFs())
          .modifyCacheDirective(new CacheDirectiveInfo.Builder(info)
              .setPath(mountPathInfo.getPathOnTarget()).build());
      return;
    }

    // No path available in CacheDirectiveInfo, Let's shoot to all child fs.
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;

    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        dfs.modifyCacheDirective(info);
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo info,
      EnumSet<CacheFlag> flags) throws IOException {
    if (this.vfs == null) {
      super.modifyCacheDirective(info, flags);
      return;
    }
    if (info.getPath() != null) {
      ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
          this.vfs.getMountPathInfo(info.getPath(), getConf());
      checkDFS(mountPathInfo.getTargetFs(), "modifyCacheDirective");
      ((DistributedFileSystem) mountPathInfo.getTargetFs())
          .modifyCacheDirective(new CacheDirectiveInfo.Builder(info)
              .setPath(mountPathInfo.getPathOnTarget()).build(), flags);
      return;
    }
    // No path available in CacheDirectiveInfo, Let's shoot to all child fs.
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;
    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        dfs.modifyCacheDirective(info, flags);
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    if (this.vfs == null) {
      super.removeCacheDirective(id);
      return;
    }
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;

    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        dfs.removeCacheDirective(id);
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
  }

  @Override
  public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    if (this.vfs == null) {
      return super.listCacheDirectives(filter);
    }

    if (filter != null && filter.getPath() != null) {
      ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
          this.vfs.getMountPathInfo(filter.getPath(), getConf());
      checkDFS(mountPathInfo.getTargetFs(), "listCacheDirectives");
      return ((DistributedFileSystem) mountPathInfo.getTargetFs())
          .listCacheDirectives(new CacheDirectiveInfo.Builder(filter)
              .setPath(mountPathInfo.getPathOnTarget()).build());
    }

    // No path available in filter. Let's try to shoot to all child fs.
    final List<RemoteIterator<CacheDirectiveEntry>> iters = new ArrayList<>();
    for (FileSystem fs : getChildFileSystems()) {
      if (fs instanceof DistributedFileSystem) {
        iters.add(((DistributedFileSystem) fs).listCacheDirectives(filter));
      }
    }
    if (iters.size() == 0) {
      throw new UnsupportedOperationException(
          "No DFS found in child fs. This API can't be supported in non DFS");
    }

    return new RemoteIterator<CacheDirectiveEntry>() {
      int currIdx = 0;
      RemoteIterator<CacheDirectiveEntry> currIter = iters.get(currIdx++);

      @Override
      public boolean hasNext() throws IOException {
        if (currIter.hasNext()) {
          return true;
        }
        while (currIdx < iters.size()) {
          currIter = iters.get(currIdx++);
          if (currIter.hasNext()) {
            return true;
          }
        }
        return false;
      }

      @Override
      public CacheDirectiveEntry next() throws IOException {
        if (hasNext()) {
          return currIter.next();
        }
        throw new NoSuchElementException("No more elements");
      }
    };
  }

  //Currently Cache pool APIs supported only in default cluster.
  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    if (this.vfs == null) {
      super.addCachePool(info);
      return;
    }
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;

    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        dfs.addCachePool(info);
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
  }

  @Override
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    if (this.vfs == null) {
      super.modifyCachePool(info);
      return;
    }
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;

    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        dfs.modifyCachePool(info);
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
  }

  @Override
  public void removeCachePool(String poolName) throws IOException {
    if (this.vfs == null) {
      super.removeCachePool(poolName);
      return;
    }
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;

    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        dfs.removeCachePool(poolName);
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
  }

  @Override
  public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
    if (this.vfs == null) {
      return super.listCachePools();
    }

    List<DistributedFileSystem> childDFSs = new ArrayList<>();
    for (FileSystem fs : getChildFileSystems()) {
      if (fs instanceof DistributedFileSystem) {
        childDFSs.add((DistributedFileSystem) fs);
      }
    }
    if (childDFSs.size() == 0) {
      throw new UnsupportedOperationException(
          "No DFS found in child fs. This API can't be supported in non DFS");
    }
    return new RemoteIterator<CachePoolEntry>() {
      int curDfsIdx = 0;
      RemoteIterator<CachePoolEntry> currIter =
          childDFSs.get(curDfsIdx++).listCachePools();

      @Override
      public boolean hasNext() throws IOException {
        if (currIter.hasNext()) {
          return true;
        }
        while (curDfsIdx < childDFSs.size()) {
          currIter = childDFSs.get(curDfsIdx++).listCachePools();
          if (currIter.hasNext()) {
            return true;
          }
        }
        return false;
      }

      @Override
      public CachePoolEntry next() throws IOException {
        if (hasNext()) {
          return currIter.next();
        }
        throw new java.util.NoSuchElementException("No more entries");
      }
    };
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

  /**
   * Returns the results from default DFS (fallback). If you want the results
   * from specific clusters, please invoke them on child fs instance directly.
   */
  @Override
  public RemoteIterator<EncryptionZone> listEncryptionZones()
      throws IOException {
    if (this.vfs == null) {
      return super.listEncryptionZones();
    }
    checkDefaultDFS(defaultDFS, "listEncryptionZones");
    return defaultDFS.listEncryptionZones();
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

  /**
   * Returns the results from default DFS (fallback). If you want the results
   * from specific clusters, please invoke them on child fs instance directly.
   */
  @Override
  public RemoteIterator<ZoneReencryptionStatus> listReencryptionStatus()
      throws IOException {
    if (this.vfs == null) {
      return super.listReencryptionStatus();
    }
    checkDefaultDFS(defaultDFS, "listReencryptionStatus");
    return defaultDFS.listReencryptionStatus();
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
  public Path provisionSnapshotTrash(final Path path,
      final FsPermission trashPermission) throws IOException {
    if (this.vfs == null) {
      return super.provisionSnapshotTrash(path, trashPermission);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(path, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "provisionSnapshotTrash");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .provisionSnapshotTrash(mountPathInfo.getPathOnTarget(),
          trashPermission);
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
    checkDefaultDFS(defaultDFS, "getKeyProviderUri");
    return defaultDFS.getKeyProviderUri();
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    if (this.vfs == null) {
      return super.getKeyProvider();
    }
    checkDefaultDFS(defaultDFS, "getKeyProvider");
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
    checkDefaultDFS(defaultDFS, "getInotifyEventStream");
    return defaultDFS.getInotifyEventStream();
  }

  @Override
  public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid)
      throws IOException {
    if (this.vfs == null) {
      return super.getInotifyEventStream();
    }
    checkDefaultDFS(defaultDFS, "getInotifyEventStream");
    return defaultDFS.getInotifyEventStream();
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

  /**
   * Gets all erasure coding policies from all available child file systems.
   */
  @Override
  public Collection<ErasureCodingPolicyInfo> getAllErasureCodingPolicies()
      throws IOException {
    if (this.vfs == null) {
      return super.getAllErasureCodingPolicies();
    }
    FileSystem[] childFss = getChildFileSystems();
    List<ErasureCodingPolicyInfo> results = new ArrayList<>();
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;
    for (FileSystem fs : childFss) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        results.addAll(dfs.getAllErasureCodingPolicies());
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }

    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
    return results;
  }

  @Override
  public Map<String, String> getAllErasureCodingCodecs() throws IOException {
    if (this.vfs == null) {
      return super.getAllErasureCodingCodecs();
    }
    FileSystem[] childFss = getChildFileSystems();
    Map<String, String> results = new HashMap<>();
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;
    for (FileSystem fs : childFss) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        results.putAll(dfs.getAllErasureCodingCodecs());
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
    return results;
  }

  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    if (this.vfs == null) {
      return super.addErasureCodingPolicies(policies);
    }
    List<IOException> failedExceptions = new ArrayList<>();
    List<AddErasureCodingPolicyResponse> results = new ArrayList<>();
    boolean isDFSExistsInChilds = false;
    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        results.addAll(Arrays.asList(dfs.addErasureCodingPolicies(policies)));
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
    return results.toArray(new AddErasureCodingPolicyResponse[results.size()]);
  }

  @Override
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (this.vfs == null) {
      super.removeErasureCodingPolicy(ecPolicyName);
      return;
    }

    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;

    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        dfs.removeErasureCodingPolicy(ecPolicyName);
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
  }

  @Override
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (this.vfs == null) {
      super.enableErasureCodingPolicy(ecPolicyName);
      return;
    }
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;

    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        dfs.enableErasureCodingPolicy(ecPolicyName);
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
  }

  @Override
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (this.vfs == null) {
      super.disableErasureCodingPolicy(ecPolicyName);
      return;
    }
    List<IOException> failedExceptions = new ArrayList<>();
    boolean isDFSExistsInChilds = false;

    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      isDFSExistsInChilds = true;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        dfs.disableErasureCodingPolicy(ecPolicyName);
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (!isDFSExistsInChilds) {
      throw new UnsupportedOperationException(
          "No DFS available in child file systems.");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
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

    List<IOException> failedExceptions = new ArrayList<>();
    ECTopologyVerifierResult result = null;
    for (FileSystem fs : getChildFileSystems()) {
      if (!(fs instanceof DistributedFileSystem)) {
        continue;
      }
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        result = dfs.getECTopologyResultForPolicies(policyNames);
        if (!result.isSupported()) {
          // whenever we see negative result.
          return result;
        }
      } catch (IOException ioe) {
        failedExceptions.add(ioe);
      }
    }
    if (result == null) {
      throw new UnsupportedOperationException(
          "No DFS available in child filesystems");
    }
    if (failedExceptions.size() > 0) {
      throw MultipleIOException.createIOException(failedExceptions);
    }
    // Let's just return the last one.
    return result;
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
    checkDefaultDFS(defaultDFS, "listOpenFiles");
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
    checkDefaultDFS(defaultDFS, "listOpenFiles");
    return defaultDFS.listOpenFiles(openFilesTypes);
  }

  @Override
  public RemoteIterator<OpenFileEntry> listOpenFiles(
      EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes, String path)
      throws IOException {
    if (this.vfs == null) {
      return super.listOpenFiles(openFilesTypes, path);
    }
    Path absF = fixRelativePart(new Path(path));
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo =
        this.vfs.getMountPathInfo(absF, getConf());
    checkDFS(mountPathInfo.getTargetFs(), "listOpenFiles");
    return ((DistributedFileSystem) mountPathInfo.getTargetFs())
        .listOpenFiles(openFilesTypes,
            mountPathInfo.getPathOnTarget().toString());
  }

  @Override
  public HdfsDataOutputStreamBuilder appendFile(Path path) {
    if (this.vfs == null) {
      return super.appendFile(path);
    }
    ViewFileSystemOverloadScheme.MountPathInfo<FileSystem> mountPathInfo = null;
    try {
      mountPathInfo = this.vfs.getMountPathInfo(path, getConf());
    } catch (IOException e) {
      LOGGER.warn("Failed to resolve the path as mount path", e);
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

  //Below API provided implementations are in ViewFS but not there in DFS.
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
    return this.vfs.getUsed();
  }

  @Override
  public DatanodeInfo[] getSlowDatanodeStats() throws IOException {
    if (this.vfs == null) {
      return super.getSlowDatanodeStats();
    }
    checkDefaultDFS(defaultDFS, "getSlowDatanodeStats");
    return defaultDFS.getSlowDatanodeStats();
  }

}
