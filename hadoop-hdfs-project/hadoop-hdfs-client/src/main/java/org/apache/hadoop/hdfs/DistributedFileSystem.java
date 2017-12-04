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

package org.apache.hadoop.hdfs;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderTokenIssuer;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FSLinkResolver;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.GlobalStorageStatistics.StorageStatisticsProvider;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics.OpType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.impl.CorruptFileBlockIterator;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.ReencryptAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/****************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 *
 *****************************************************************/
@InterfaceAudience.LimitedPrivate({ "MapReduce", "HBase" })
@InterfaceStability.Unstable
public class DistributedFileSystem extends FileSystem
    implements KeyProviderTokenIssuer {
  private Path workingDir;
  private URI uri;
  private String homeDirPrefix =
      HdfsClientConfigKeys.DFS_USER_HOME_DIR_PREFIX_DEFAULT;

  DFSClient dfs;
  private boolean verifyChecksum = true;

  private DFSOpsCountStatistics storageStatistics;

  static{
    HdfsConfiguration.init();
  }

  public DistributedFileSystem() {
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>hdfs</code>
   */
  @Override
  public String getScheme() {
    return HdfsConstants.HDFS_URI_SCHEME;
  }

  @Override
  public URI getUri() { return uri; }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    String host = uri.getHost();
    if (host == null) {
      throw new IOException("Incomplete HDFS URI, no host: "+ uri);
    }
    homeDirPrefix = conf.get(
        HdfsClientConfigKeys.DFS_USER_HOME_DIR_PREFIX_KEY,
        HdfsClientConfigKeys.DFS_USER_HOME_DIR_PREFIX_DEFAULT);

    this.dfs = new DFSClient(uri, conf, statistics);
    this.uri = URI.create(uri.getScheme()+"://"+uri.getAuthority());
    this.workingDir = getHomeDirectory();

    storageStatistics = (DFSOpsCountStatistics) GlobalStorageStatistics.INSTANCE
        .put(DFSOpsCountStatistics.NAME,
          new StorageStatisticsProvider() {
            @Override
            public StorageStatistics provide() {
              return new DFSOpsCountStatistics();
            }
          });
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public long getDefaultBlockSize() {
    return dfs.getConf().getDefaultBlockSize();
  }

  @Override
  public short getDefaultReplication() {
    return dfs.getConf().getDefaultReplication();
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    String result = fixRelativePart(dir).toUri().getPath();
    if (!DFSUtilClient.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " +
          result);
    }
    workingDir = fixRelativePart(dir);
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(homeDirPrefix + "/"
        + dfs.ugi.getShortUserName()));
  }

  /**
   * Returns the hedged read metrics object for this client.
   *
   * @return object of DFSHedgedReadMetrics
   */
  public DFSHedgedReadMetrics getHedgedReadMetrics() {
    return dfs.getHedgedReadMetrics();
  }

  /**
   * Checks that the passed URI belongs to this filesystem and returns
   * just the path component. Expects a URI with an absolute path.
   *
   * @param file URI with absolute path
   * @return path component of {file}
   * @throws IllegalArgumentException if URI does not belong to this DFS
   */
  String getPathName(Path file) {
    checkPath(file);
    String result = file.toUri().getPath();
    if (!DFSUtilClient.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " +
          file+" is not a valid DFS filename.");
    }
    return result;
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (file == null) {
      return null;
    }
    return getFileBlockLocations(file.getPath(), start, len);
  }

  /**
   * The returned BlockLocation will have different formats for replicated
   * and erasure coded file.
   * Please refer to
   * {@link FileSystem#getFileBlockLocations(FileStatus, long, long)}
   * for more details.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(Path p,
      final long start, final long len) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_FILE_BLOCK_LOCATIONS);
    final Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<BlockLocation[]>() {
      @Override
      public BlockLocation[] doCall(final Path p) throws IOException {
        return dfs.getBlockLocations(getPathName(p), start, len);
      }
      @Override
      public BlockLocation[] next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getFileBlockLocations(p, start, len);
      }
    }.resolve(this, absF);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  /**
   * Start the lease recovery of a file
   *
   * @param f a file
   * @return true if the file is already closed
   * @throws IOException if an error occurs
   */
  public boolean recoverLease(final Path f) throws IOException {
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p) throws IOException{
        return dfs.recoverLease(getPathName(p));
      }
      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.recoverLease(p);
        }
        throw new UnsupportedOperationException("Cannot recoverLease through" +
            " a symlink to a non-DistributedFileSystem: " + f + " -> " + p);
      }
    }.resolve(this, absF);
  }

  @Override
  public FSDataInputStream open(Path f, final int bufferSize)
      throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.OPEN);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataInputStream>() {
      @Override
      public FSDataInputStream doCall(final Path p) throws IOException {
        final DFSInputStream dfsis =
            dfs.open(getPathName(p), bufferSize, verifyChecksum);
        return dfs.createWrappedInputStream(dfsis);
      }
      @Override
      public FSDataInputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.open(p, bufferSize);
      }
    }.resolve(this, absF);
  }

  @Override
  public FSDataOutputStream append(Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    return append(f, EnumSet.of(CreateFlag.APPEND), bufferSize, progress);
  }

  /**
   * Append to an existing file (optional operation).
   *
   * @param f the existing file to be appended.
   * @param flag Flags for the Append operation. CreateFlag.APPEND is mandatory
   *          to be present.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @return Returns instance of {@link FSDataOutputStream}
   * @throws IOException
   */
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
      final int bufferSize, final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.APPEND);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataOutputStream>() {
      @Override
      public FSDataOutputStream doCall(final Path p)
          throws IOException {
        return dfs.append(getPathName(p), bufferSize, flag, progress,
            statistics);
      }
      @Override
      public FSDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.append(p, bufferSize);
      }
    }.resolve(this, absF);
  }

  /**
   * Append to an existing file (optional operation).
   *
   * @param f the existing file to be appended.
   * @param flag Flags for the Append operation. CreateFlag.APPEND is mandatory
   *          to be present.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @param favoredNodes Favored nodes for new blocks
   * @return Returns instance of {@link FSDataOutputStream}
   * @throws IOException
   */
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
      final int bufferSize, final Progressable progress,
      final InetSocketAddress[] favoredNodes) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.APPEND);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataOutputStream>() {
      @Override
      public FSDataOutputStream doCall(final Path p)
          throws IOException {
        return dfs.append(getPathName(p), bufferSize, flag, progress,
            statistics, favoredNodes);
      }
      @Override
      public FSDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.append(p, bufferSize);
      }
    }.resolve(this, absF);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return this.create(f, permission,
        overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
            : EnumSet.of(CreateFlag.CREATE), bufferSize, replication,
        blockSize, progress, null);
  }

  /**
   * Same as
   * {@link #create(Path, FsPermission, boolean, int, short, long,
   * Progressable)} with the addition of favoredNodes that is a hint to
   * where the namenode should place the file blocks.
   * The favored nodes hint is not persisted in HDFS. Hence it may be honored
   * at the creation time only. And with favored nodes, blocks will be pinned
   * on the datanodes to prevent balancing move the block. HDFS could move the
   * blocks during replication, to move the blocks from favored nodes. A value
   * of null means no favored nodes for this create
   */
  public HdfsDataOutputStream create(final Path f,
      final FsPermission permission, final boolean overwrite,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress, final InetSocketAddress[] favoredNodes)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<HdfsDataOutputStream>() {
      @Override
      public HdfsDataOutputStream doCall(final Path p) throws IOException {
        final DFSOutputStream out = dfs.create(getPathName(f), permission,
            overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
                : EnumSet.of(CreateFlag.CREATE),
            true, replication, blockSize, progress, bufferSize, null,
            favoredNodes);
        return dfs.createWrappedOutputStream(out, statistics);
      }
      @Override
      public HdfsDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.create(p, permission, overwrite, bufferSize, replication,
              blockSize, progress, favoredNodes);
        }
        throw new UnsupportedOperationException("Cannot create with" +
            " favoredNodes through a symlink to a non-DistributedFileSystem: "
            + f + " -> " + p);
      }
    }.resolve(this, absF);
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final EnumSet<CreateFlag> cflags, final int bufferSize,
      final short replication, final long blockSize,
      final Progressable progress, final ChecksumOpt checksumOpt)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataOutputStream>() {
      @Override
      public FSDataOutputStream doCall(final Path p) throws IOException {
        final DFSOutputStream dfsos = dfs.create(getPathName(p), permission,
            cflags, replication, blockSize, progress, bufferSize,
            checksumOpt);
        return dfs.createWrappedOutputStream(dfsos, statistics);
      }
      @Override
      public FSDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.create(p, permission, cflags, bufferSize,
            replication, blockSize, progress, checksumOpt);
      }
    }.resolve(this, absF);
  }

  /**
   * Same as
   * {@link #create(Path, FsPermission, EnumSet<CreateFlag>, int, short, long,
   * Progressable, ChecksumOpt)} with a few additions. First, addition of
   * favoredNodes that is a hint to where the namenode should place the file
   * blocks. The favored nodes hint is not persisted in HDFS. Hence it may be
   * honored at the creation time only. And with favored nodes, blocks will be
   * pinned on the datanodes to prevent balancing move the block. HDFS could
   * move the blocks during replication, to move the blocks from favored nodes.
   * A value of null means no favored nodes for this create.
   * The second addition is ecPolicyName. A non-null ecPolicyName specifies an
   * explicit erasure coding policy for this file, overriding the inherited
   * policy. A null ecPolicyName means the file will inherit its EC policy or
   * replication policy from its ancestor (the default).
   * ecPolicyName and SHOULD_REPLICATE CreateFlag are mutually exclusive. It's
   * invalid to set both SHOULD_REPLICATE and a non-null ecPolicyName.
   *
   */
  private HdfsDataOutputStream create(final Path f,
      final FsPermission permission, final EnumSet<CreateFlag> flag,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress, final ChecksumOpt checksumOpt,
      final InetSocketAddress[] favoredNodes, final String ecPolicyName)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<HdfsDataOutputStream>() {
      @Override
      public HdfsDataOutputStream doCall(final Path p) throws IOException {
        final DFSOutputStream out = dfs.create(getPathName(f), permission,
            flag, true, replication, blockSize, progress, bufferSize,
            checksumOpt, favoredNodes, ecPolicyName);
        return dfs.createWrappedOutputStream(out, statistics);
      }
      @Override
      public HdfsDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.create(p, permission, flag, bufferSize, replication,
              blockSize, progress, checksumOpt, favoredNodes, ecPolicyName);
        }
        throw new UnsupportedOperationException("Cannot create with" +
            " favoredNodes through a symlink to a non-DistributedFileSystem: "
            + f + " -> " + p);
      }
    }.resolve(this, absF);
  }

  @Override
  protected HdfsDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
      short replication, long blockSize, Progressable progress,
      ChecksumOpt checksumOpt) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.PRIMITIVE_CREATE);
    final DFSOutputStream dfsos = dfs.primitiveCreate(
        getPathName(fixRelativePart(f)),
        absolutePermission, flag, true, replication, blockSize,
        progress, bufferSize, checksumOpt);
    return dfs.createWrappedOutputStream(dfsos, statistics);
  }

  /**
   * Similar to {@link #create(Path, FsPermission, EnumSet, int, short, long,
   * Progressable, ChecksumOpt, InetSocketAddress[], String)}, it provides a
   * HDFS-specific version of {@link #createNonRecursive(Path, FsPermission,
   * EnumSet, int, short, long, Progressable)} with a few additions.
   *
   * @see #create(Path, FsPermission, EnumSet, int, short, long, Progressable,
   * ChecksumOpt, InetSocketAddress[], String) for the descriptions of
   * additional parameters, i.e., favoredNodes and ecPolicyName.
   */
  private HdfsDataOutputStream createNonRecursive(final Path f,
      final FsPermission permission, final EnumSet<CreateFlag> flag,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress, final ChecksumOpt checksumOpt,
      final InetSocketAddress[] favoredNodes, final String ecPolicyName)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<HdfsDataOutputStream>() {
      @Override
      public HdfsDataOutputStream doCall(final Path p) throws IOException {
        final DFSOutputStream out = dfs.create(getPathName(f), permission,
            flag, false, replication, blockSize, progress, bufferSize,
            checksumOpt, favoredNodes, ecPolicyName);
        return dfs.createWrappedOutputStream(out, statistics);
      }
      @Override
      public HdfsDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.createNonRecursive(p, permission, flag, bufferSize,
              replication, blockSize, progress, checksumOpt, favoredNodes,
              ecPolicyName);
        }
        throw new UnsupportedOperationException("Cannot create with" +
            " favoredNodes through a symlink to a non-DistributedFileSystem: "
            + f + " -> " + p);
      }
    }.resolve(this, absF);
  }

  /**
   * Same as create(), except fails if parent directory doesn't already exist.
   */
  @Override
  public FSDataOutputStream createNonRecursive(final Path f,
      final FsPermission permission, final EnumSet<CreateFlag> flag,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE_NON_RECURSIVE);
    if (flag.contains(CreateFlag.OVERWRITE)) {
      flag.add(CreateFlag.CREATE);
    }
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataOutputStream>() {
      @Override
      public FSDataOutputStream doCall(final Path p) throws IOException {
        final DFSOutputStream dfsos = dfs.create(getPathName(p), permission,
            flag, false, replication, blockSize, progress, bufferSize, null);
        return dfs.createWrappedOutputStream(dfsos, statistics);
      }

      @Override
      public FSDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.createNonRecursive(p, permission, flag, bufferSize,
            replication, blockSize, progress);
      }
    }.resolve(this, absF);
  }

  @Override
  public boolean setReplication(Path src, final short replication)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_REPLICATION);
    Path absF = fixRelativePart(src);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p) throws IOException {
        return dfs.setReplication(getPathName(p), replication);
      }
      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.setReplication(p, replication);
      }
    }.resolve(this, absF);
  }

  /**
   * Set the source path to the specified storage policy.
   *
   * @param src The source path referring to either a directory or a file.
   * @param policyName The name of the storage policy.
   */
  @Override
  public void setStoragePolicy(final Path src, final String policyName)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_STORAGE_POLICY);
    Path absF = fixRelativePart(src);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.setStoragePolicy(getPathName(p), policyName);
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setStoragePolicy(p, policyName);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void unsetStoragePolicy(final Path src)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.UNSET_STORAGE_POLICY);
    Path absF = fixRelativePart(src);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.unsetStoragePolicy(getPathName(p));
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        if (fs instanceof DistributedFileSystem) {
          ((DistributedFileSystem) fs).unsetStoragePolicy(p);
          return null;
        } else {
          throw new UnsupportedOperationException(
              "Cannot perform unsetStoragePolicy on a "
                  + "non-DistributedFileSystem: " + src + " -> " + p);
        }
      }
    }.resolve(this, absF);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path path) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_STORAGE_POLICY);
    Path absF = fixRelativePart(path);

    return new FileSystemLinkResolver<BlockStoragePolicySpi>() {
      @Override
      public BlockStoragePolicySpi doCall(final Path p) throws IOException {
        return getClient().getStoragePolicy(getPathName(p));
      }

      @Override
      public BlockStoragePolicySpi next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getStoragePolicy(p);
      }
    }.resolve(this, absF);
  }

  @Override
  public Collection<BlockStoragePolicy> getAllStoragePolicies()
      throws IOException {
    return Arrays.asList(dfs.getStoragePolicies());
  }

  /**
   * Returns number of bytes within blocks with future generation stamp. These
   * are bytes that will be potentially deleted if we forceExit from safe mode.
   *
   * @return number of bytes.
   */
  public long getBytesWithFutureGenerationStamps() throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_BYTES_WITH_FUTURE_GS);
    return dfs.getBytesInFutureBlocks();
  }

  /**
   * Deprecated. Prefer {@link FileSystem#getAllStoragePolicies()}
   * @throws IOException
   */
  @Deprecated
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_STORAGE_POLICIES);
    return dfs.getStoragePolicies();
  }

  /**
   * Move blocks from srcs to trg and delete srcs afterwards.
   * The file block sizes must be the same.
   *
   * @param trg existing file to append to
   * @param psrcs list of files (same block size, same replication)
   * @throws IOException
   */
  @Override
  public void concat(Path trg, Path [] psrcs) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CONCAT);
    // Make target absolute
    Path absF = fixRelativePart(trg);
    // Make all srcs absolute
    Path[] srcs = new Path[psrcs.length];
    for (int i=0; i<psrcs.length; i++) {
      srcs[i] = fixRelativePart(psrcs[i]);
    }
    // Try the concat without resolving any links
    String[] srcsStr = new String[psrcs.length];
    try {
      for (int i=0; i<psrcs.length; i++) {
        srcsStr[i] = getPathName(srcs[i]);
      }
      dfs.concat(getPathName(absF), srcsStr);
    } catch (UnresolvedLinkException e) {
      // Exception could be from trg or any src.
      // Fully resolve trg and srcs. Fail if any of them are a symlink.
      FileStatus stat = getFileLinkStatus(absF);
      if (stat.isSymlink()) {
        throw new IOException("Cannot concat with a symlink target: "
            + trg + " -> " + stat.getPath());
      }
      absF = fixRelativePart(stat.getPath());
      for (int i=0; i<psrcs.length; i++) {
        stat = getFileLinkStatus(srcs[i]);
        if (stat.isSymlink()) {
          throw new IOException("Cannot concat with a symlink src: "
              + psrcs[i] + " -> " + stat.getPath());
        }
        srcs[i] = fixRelativePart(stat.getPath());
      }
      // Try concat again. Can still race with another symlink.
      for (int i=0; i<psrcs.length; i++) {
        srcsStr[i] = getPathName(srcs[i]);
      }
      dfs.concat(getPathName(absF), srcsStr);
    }
  }


  @SuppressWarnings("deprecation")
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.RENAME);

    final Path absSrc = fixRelativePart(src);
    final Path absDst = fixRelativePart(dst);

    // Try the rename without resolving first
    try {
      return dfs.rename(getPathName(absSrc), getPathName(absDst));
    } catch (UnresolvedLinkException e) {
      // Fully resolve the source
      final Path source = getFileLinkStatus(absSrc).getPath();
      // Keep trying to resolve the destination
      return new FileSystemLinkResolver<Boolean>() {
        @Override
        public Boolean doCall(final Path p) throws IOException {
          return dfs.rename(getPathName(source), getPathName(p));
        }
        @Override
        public Boolean next(final FileSystem fs, final Path p)
            throws IOException {
          // Should just throw an error in FileSystem#checkPath
          return doCall(p);
        }
      }.resolve(this, absDst);
    }
  }

  /**
   * This rename operation is guaranteed to be atomic.
   */
  @SuppressWarnings("deprecation")
  @Override
  public void rename(Path src, Path dst, final Options.Rename... options)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.RENAME);
    final Path absSrc = fixRelativePart(src);
    final Path absDst = fixRelativePart(dst);
    // Try the rename without resolving first
    try {
      dfs.rename(getPathName(absSrc), getPathName(absDst), options);
    } catch (UnresolvedLinkException e) {
      // Fully resolve the source
      final Path source = getFileLinkStatus(absSrc).getPath();
      // Keep trying to resolve the destination
      new FileSystemLinkResolver<Void>() {
        @Override
        public Void doCall(final Path p) throws IOException {
          dfs.rename(getPathName(source), getPathName(p), options);
          return null;
        }
        @Override
        public Void next(final FileSystem fs, final Path p)
            throws IOException {
          // Should just throw an error in FileSystem#checkPath
          return doCall(p);
        }
      }.resolve(this, absDst);
    }
  }

  @Override
  public boolean truncate(Path f, final long newLength) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.TRUNCATE);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p) throws IOException {
        return dfs.truncate(getPathName(p), newLength);
      }
      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.truncate(p, newLength);
      }
    }.resolve(this, absF);
  }

  @Override
  public boolean delete(Path f, final boolean recursive) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.DELETE);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p) throws IOException {
        return dfs.delete(getPathName(p), recursive);
      }
      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.delete(p, recursive);
      }
    }.resolve(this, absF);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_CONTENT_SUMMARY);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<ContentSummary>() {
      @Override
      public ContentSummary doCall(final Path p) throws IOException {
        return dfs.getContentSummary(getPathName(p));
      }
      @Override
      public ContentSummary next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getContentSummary(p);
      }
    }.resolve(this, absF);
  }

  @Override
  public QuotaUsage getQuotaUsage(Path f) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_QUOTA_USAGE);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<QuotaUsage>() {
      @Override
      public QuotaUsage doCall(final Path p)
              throws IOException, UnresolvedLinkException {
        return dfs.getQuotaUsage(getPathName(p));
      }
      @Override
      public QuotaUsage next(final FileSystem fs, final Path p)
              throws IOException {
        return fs.getQuotaUsage(p);
      }
    }.resolve(this, absF);
  }

  /** Set a directory's quotas
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String,
   * long, long, StorageType)
   */
  public void setQuota(Path src, final long namespaceQuota,
      final long storagespaceQuota) throws IOException {
    Path absF = fixRelativePart(src);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.setQuota(getPathName(p), namespaceQuota, storagespaceQuota);
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        // setQuota is not defined in FileSystem, so we only can resolve
        // within this DFS
        return doCall(p);
      }
    }.resolve(this, absF);
  }

  /**
   * Set the per type storage quota of a directory.
   *
   * @param src target directory whose quota is to be modified.
   * @param type storage type of the specific storage type quota to be modified.
   * @param quota value of the specific storage type quota to be modified.
   * Maybe {@link HdfsConstants#QUOTA_RESET} to clear quota by storage type.
   */
  public void setQuotaByStorageType(Path src, final StorageType type,
      final long quota)
      throws IOException {
    Path absF = fixRelativePart(src);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.setQuotaByStorageType(getPathName(p), type, quota);
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        // setQuotaByStorageType is not defined in FileSystem, so we only can
        // resolve within this DFS
        return doCall(p);
      }
    }.resolve(this, absF);
  }

  private FileStatus[] listStatusInternal(Path p) throws IOException {
    String src = getPathName(p);

    // fetch the first batch of entries in the directory
    DirectoryListing thisListing = dfs.listPaths(
        src, HdfsFileStatus.EMPTY_NAME);

    if (thisListing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }

    HdfsFileStatus[] partialListing = thisListing.getPartialListing();
    if (!thisListing.hasMore()) { // got all entries of the directory
      FileStatus[] stats = new FileStatus[partialListing.length];
      for (int i = 0; i < partialListing.length; i++) {
        stats[i] = partialListing[i].makeQualified(getUri(), p);
      }
      statistics.incrementReadOps(1);
      storageStatistics.incrementOpCounter(OpType.LIST_STATUS);
      return stats;
    }

    // The directory size is too big that it needs to fetch more
    // estimate the total number of entries in the directory
    int totalNumEntries =
        partialListing.length + thisListing.getRemainingEntries();
    ArrayList<FileStatus> listing =
        new ArrayList<>(totalNumEntries);
    // add the first batch of entries to the array list
    for (HdfsFileStatus fileStatus : partialListing) {
      listing.add(fileStatus.makeQualified(getUri(), p));
    }
    statistics.incrementLargeReadOps(1);
    storageStatistics.incrementOpCounter(OpType.LIST_STATUS);

    // now fetch more entries
    do {
      thisListing = dfs.listPaths(src, thisListing.getLastName());

      if (thisListing == null) { // the directory is deleted
        throw new FileNotFoundException("File " + p + " does not exist.");
      }

      partialListing = thisListing.getPartialListing();
      for (HdfsFileStatus fileStatus : partialListing) {
        listing.add(fileStatus.makeQualified(getUri(), p));
      }
      statistics.incrementLargeReadOps(1);
      storageStatistics.incrementOpCounter(OpType.LIST_STATUS);
    } while (thisListing.hasMore());

    return listing.toArray(new FileStatus[listing.size()]);
  }

  /**
   * List all the entries of a directory
   *
   * Note that this operation is not atomic for a large directory.
   * The entries of a directory may be fetched from NameNode multiple times.
   * It only guarantees that  each name occurs once if a directory
   * undergoes changes between the calls.
   */
  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
    Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<FileStatus[]>() {
      @Override
      public FileStatus[] doCall(final Path p) throws IOException {
        return listStatusInternal(p);
      }
      @Override
      public FileStatus[] next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.listStatus(p);
      }
    }.resolve(this, absF);
  }

  /**
   * The BlockLocation of returned LocatedFileStatus will have different
   * formats for replicated and erasure coded file.
   * Please refer to
   * {@link FileSystem#getFileBlockLocations(FileStatus, long, long)} for
   * more details.
   */
  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path p,
      final PathFilter filter)
      throws IOException {
    Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<RemoteIterator<LocatedFileStatus>>() {
      @Override
      public RemoteIterator<LocatedFileStatus> doCall(final Path p)
          throws IOException {
        return new DirListingIterator<>(p, filter, true);
      }

      @Override
      public RemoteIterator<LocatedFileStatus> next(final FileSystem fs,
          final Path p) throws IOException {
        if (fs instanceof DistributedFileSystem) {
          return ((DistributedFileSystem)fs).listLocatedStatus(p, filter);
        }
        // symlink resolution for this methos does not work cross file systems
        // because it is a protected method.
        throw new IOException("Link resolution does not work with multiple " +
            "file systems for listLocatedStatus(): " + p);
      }
    }.resolve(this, absF);
  }


  /**
   * Returns a remote iterator so that followup calls are made on demand
   * while consuming the entries. This reduces memory consumption during
   * listing of a large directory.
   *
   * @param p target path
   * @return remote iterator
   */
  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path p)
      throws IOException {
    Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<RemoteIterator<FileStatus>>() {
      @Override
      public RemoteIterator<FileStatus> doCall(final Path p)
          throws IOException {
        return new DirListingIterator<>(p, false);
      }

      @Override
      public RemoteIterator<FileStatus> next(final FileSystem fs, final Path p)
          throws IOException {
        return ((DistributedFileSystem)fs).listStatusIterator(p);
      }
    }.resolve(this, absF);

  }

  /**
   * This class defines an iterator that returns
   * the file status of each file/subdirectory of a directory
   *
   * if needLocation, status contains block location if it is a file
   * throws a RuntimeException with the error as its cause.
   *
   * @param <T> the type of the file status
   */
  private class  DirListingIterator<T extends FileStatus>
      implements RemoteIterator<T> {
    private DirectoryListing thisListing;
    private int i;
    private Path p;
    private String src;
    private T curStat = null;
    private PathFilter filter;
    private boolean needLocation;

    private DirListingIterator(Path p, PathFilter filter,
        boolean needLocation) throws IOException {
      this.p = p;
      this.src = getPathName(p);
      this.filter = filter;
      this.needLocation = needLocation;
      // fetch the first batch of entries in the directory
      thisListing = dfs.listPaths(src, HdfsFileStatus.EMPTY_NAME,
          needLocation);
      statistics.incrementReadOps(1);
      storageStatistics.incrementOpCounter(OpType.LIST_LOCATED_STATUS);
      if (thisListing == null) { // the directory does not exist
        throw new FileNotFoundException("File " + p + " does not exist.");
      }
      i = 0;
    }

    private DirListingIterator(Path p, boolean needLocation)
        throws IOException {
      this(p, null, needLocation);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean hasNext() throws IOException {
      while (curStat == null && hasNextNoFilter()) {
        T next;
        HdfsFileStatus fileStat = thisListing.getPartialListing()[i++];
        if (needLocation) {
          next = (T)((HdfsLocatedFileStatus)fileStat)
              .makeQualifiedLocated(getUri(), p);
        } else {
          next = (T)fileStat.makeQualified(getUri(), p);
        }
        // apply filter if not null
        if (filter == null || filter.accept(next.getPath())) {
          curStat = next;
        }
      }
      return curStat != null;
    }

    /** Check if there is a next item before applying the given filter */
    private boolean hasNextNoFilter() throws IOException {
      if (thisListing == null) {
        return false;
      }
      if (i >= thisListing.getPartialListing().length
          && thisListing.hasMore()) {
        // current listing is exhausted & fetch a new listing
        thisListing = dfs.listPaths(src, thisListing.getLastName(),
            needLocation);
        statistics.incrementReadOps(1);
        if (thisListing == null) {
          throw new FileNotFoundException("File " + p + " does not exist.");
        }
        i = 0;
      }
      return (i < thisListing.getPartialListing().length);
    }

    @Override
    public T next() throws IOException {
      if (hasNext()) {
        T tmp = curStat;
        curStat = null;
        return tmp;
      }
      throw new java.util.NoSuchElementException("No more entry in " + p);
    }
  }

  /**
   * Create a directory, only when the parent directories exist.
   *
   * See {@link FsPermission#applyUMask(FsPermission)} for details of how
   * the permission is applied.
   *
   * @param f           The path to create
   * @param permission  The permission.  See FsPermission#applyUMask for
   *                    details about how this is used to calculate the
   *                    effective permission.
   */
  public boolean mkdir(Path f, FsPermission permission) throws IOException {
    return mkdirsInternal(f, permission, false);
  }

  /**
   * Create a directory and its parent directories.
   *
   * See {@link FsPermission#applyUMask(FsPermission)} for details of how
   * the permission is applied.
   *
   * @param f           The path to create
   * @param permission  The permission.  See FsPermission#applyUMask for
   *                    details about how this is used to calculate the
   *                    effective permission.
   */
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return mkdirsInternal(f, permission, true);
  }

  private boolean mkdirsInternal(Path f, final FsPermission permission,
      final boolean createParent) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.MKDIRS);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p) throws IOException {
        return dfs.mkdirs(getPathName(p), permission, createParent);
      }

      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        // FileSystem doesn't have a non-recursive mkdir() method
        // Best we can do is error out
        if (!createParent) {
          throw new IOException("FileSystem does not support non-recursive"
              + "mkdir");
        }
        return fs.mkdirs(p, permission);
      }
    }.resolve(this, absF);
  }

  @SuppressWarnings("deprecation")
  @Override
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.PRIMITIVE_MKDIR);
    return dfs.primitiveMkdir(getPathName(f), absolutePermission);
  }


  @Override
  public void close() throws IOException {
    try {
      dfs.closeOutputStreams(false);
      super.close();
    } finally {
      dfs.close();
    }
  }

  @Override
  public String toString() {
    return "DFS[" + dfs + "]";
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public DFSClient getClient() {
    return dfs;
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_STATUS);
    return dfs.getDiskStatus();
  }

  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   *
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    return dfs.getMissingBlocksCount();
  }

  /**
   * Returns count of blocks pending on deletion.
   *
   * @throws IOException
   */
  public long getPendingDeletionBlocksCount() throws IOException {
    return dfs.getPendingDeletionBlocksCount();
  }

  /**
   * Returns count of blocks with replication factor 1 and have
   * lost the only replica.
   *
   * @throws IOException
   */
  public long getMissingReplOneBlocksCount() throws IOException {
    return dfs.getMissingReplOneBlocksCount();
  }

  /**
   * Returns aggregated count of blocks with less redundancy.
   *
   * @throws IOException
   */
  public long getLowRedundancyBlocksCount() throws IOException {
    return dfs.getLowRedundancyBlocksCount();
  }

  /**
   * Returns count of blocks with at least one replica marked corrupt.
   *
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    return dfs.getCorruptBlocksCount();
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(final Path path)
      throws IOException {
    Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<RemoteIterator<Path>>() {
      @Override
      public RemoteIterator<Path> doCall(final Path path) throws IOException,
          UnresolvedLinkException {
        return new CorruptFileBlockIterator(dfs, path);
      }

      @Override
      public RemoteIterator<Path> next(final FileSystem fs, final Path path)
          throws IOException {
        return fs.listCorruptFileBlocks(path);
      }
    }.resolve(this, absF);
  }

  /** @return datanode statistics. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return getDataNodeStats(DatanodeReportType.ALL);
  }

  /** @return datanode statistics for the given type. */
  public DatanodeInfo[] getDataNodeStats(final DatanodeReportType type)
      throws IOException {
    return dfs.datanodeReport(type);
  }

  /**
   * Enter, leave or get safe mode.
   *
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(
   *    HdfsConstants.SafeModeAction,boolean)
   */
  public boolean setSafeMode(HdfsConstants.SafeModeAction action)
      throws IOException {
    return setSafeMode(action, false);
  }

  /**
   * Enter, leave or get safe mode.
   *
   * @param action
   *          One of SafeModeAction.ENTER, SafeModeAction.LEAVE and
   *          SafeModeAction.GET
   * @param isChecked
   *          If true check only for Active NNs status, else check first NN's
   *          status
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(SafeModeAction, boolean)
   */
  public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    return dfs.setSafeMode(action, isChecked);
  }

  /**
   * Save namespace image.
   *
   * @param timeWindow NameNode can ignore this command if the latest
   *                   checkpoint was done within the given time period (in
   *                   seconds).
   * @return true if a new checkpoint has been made
   * @see ClientProtocol#saveNamespace(long, long)
   */
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    return dfs.saveNamespace(timeWindow, txGap);
  }

  /**
   * Save namespace image. NameNode always does the checkpoint.
   */
  public void saveNamespace() throws IOException {
    saveNamespace(0, 0);
  }

  /**
   * Rolls the edit log on the active NameNode.
   * Requires super-user privileges.
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#rollEdits()
   * @return the transaction ID of the newly created segment
   */
  public long rollEdits() throws IOException {
    return dfs.rollEdits();
  }

  /**
   * enable/disable/check restoreFaileStorage
   *
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#restoreFailedStorage(String arg)
   */
  public boolean restoreFailedStorage(String arg) throws IOException {
    return dfs.restoreFailedStorage(arg);
  }


  /**
   * Refreshes the list of hosts and excluded hosts from the configured
   * files.
   */
  public void refreshNodes() throws IOException {
    dfs.refreshNodes();
  }

  /**
   * Finalize previously upgraded files system state.
   * @throws IOException
   */
  public void finalizeUpgrade() throws IOException {
    dfs.finalizeUpgrade();
  }

  /**
   * Rolling upgrade: prepare/finalize/query.
   */
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException {
    return dfs.rollingUpgrade(action);
  }

  /*
   * Requests the namenode to dump data strcutures into specified
   * file.
   */
  public void metaSave(String pathname) throws IOException {
    dfs.metaSave(pathname);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return dfs.getServerDefaults();
  }

  /**
   * Returns the stat information about the file.
   * @throws FileNotFoundException if the file does not exist.
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_FILE_STATUS);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FileStatus>() {
      @Override
      public FileStatus doCall(final Path p) throws IOException {
        HdfsFileStatus fi = dfs.getFileInfo(getPathName(p));
        if (fi != null) {
          return fi.makeQualified(getUri(), p);
        } else {
          throw new FileNotFoundException("File does not exist: " + p);
        }
      }
      @Override
      public FileStatus next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getFileStatus(p);
      }
    }.resolve(this, absF);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws IOException {
    if (!FileSystem.areSymlinksEnabled()) {
      throw new UnsupportedOperationException("Symlinks not supported");
    }
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE_SYM_LINK);
    final Path absF = fixRelativePart(link);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.createSymlink(target.toString(), getPathName(p), createParent);
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        fs.createSymlink(target, p, createParent);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public boolean supportsSymlinks() {
    return true;
  }

  @Override
  public FileStatus getFileLinkStatus(final Path f) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_FILE_LINK_STATUS);
    final Path absF = fixRelativePart(f);
    FileStatus status = new FileSystemLinkResolver<FileStatus>() {
      @Override
      public FileStatus doCall(final Path p) throws IOException {
        HdfsFileStatus fi = dfs.getFileLinkInfo(getPathName(p));
        if (fi != null) {
          return fi.makeQualified(getUri(), p);
        } else {
          throw new FileNotFoundException("File does not exist: " + p);
        }
      }
      @Override
      public FileStatus next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getFileLinkStatus(p);
      }
    }.resolve(this, absF);
    // Fully-qualify the symlink
    if (status.isSymlink()) {
      Path targetQual = FSLinkResolver.qualifySymlinkTarget(this.getUri(),
          status.getPath(), status.getSymlink());
      status.setSymlink(targetQual);
    }
    return status;
  }

  @Override
  public Path getLinkTarget(final Path f) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_LINK_TARGET);
    final Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Path>() {
      @Override
      public Path doCall(final Path p) throws IOException {
        HdfsFileStatus fi = dfs.getFileLinkInfo(getPathName(p));
        if (fi != null) {
          return fi.makeQualified(getUri(), p).getSymlink();
        } else {
          throw new FileNotFoundException("File does not exist: " + p);
        }
      }
      @Override
      public Path next(final FileSystem fs, final Path p) throws IOException {
        return fs.getLinkTarget(p);
      }
    }.resolve(this, absF);
  }

  @Override
  protected Path resolveLink(Path f) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.RESOLVE_LINK);
    String target = dfs.getLinkTarget(getPathName(fixRelativePart(f)));
    if (target == null) {
      throw new FileNotFoundException("File does not exist: " + f.toString());
    }
    return new Path(target);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_FILE_CHECKSUM);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FileChecksum>() {
      @Override
      public FileChecksum doCall(final Path p) throws IOException {
        return dfs.getFileChecksum(getPathName(p), Long.MAX_VALUE);
      }

      @Override
      public FileChecksum next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getFileChecksum(p);
      }
    }.resolve(this, absF);
  }

  @Override
  public FileChecksum getFileChecksum(Path f, final long length)
      throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_FILE_CHECKSUM);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FileChecksum>() {
      @Override
      public FileChecksum doCall(final Path p) throws IOException {
        return dfs.getFileChecksum(getPathName(p), length);
      }

      @Override
      public FileChecksum next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          return fs.getFileChecksum(p, length);
        } else {
          throw new UnsupportedFileSystemException(
              "getFileChecksum(Path, long) is not supported by "
                  + fs.getClass().getSimpleName());
        }
      }
    }.resolve(this, absF);
  }

  @Override
  public void setPermission(Path p, final FsPermission permission
  ) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_PERMISSION);
    Path absF = fixRelativePart(p);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.setPermission(getPathName(p), permission);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setPermission(p, permission);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void setOwner(Path p, final String username, final String groupname)
      throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_OWNER);
    Path absF = fixRelativePart(p);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.setOwner(getPathName(p), username, groupname);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setOwner(p, username, groupname);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void setTimes(Path p, final long mtime, final long atime)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_TIMES);
    Path absF = fixRelativePart(p);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.setTimes(getPathName(p), mtime, atime);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setTimes(p, mtime, atime);
        return null;
      }
    }.resolve(this, absF);
  }


  @Override
  protected int getDefaultPort() {
    return HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT;
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    return dfs.getDelegationToken(renewer == null ? null : new Text(renewer));
  }

  /**
   * Requests the namenode to tell all datanodes to use a new, non-persistent
   * bandwidth value for dfs.datanode.balance.bandwidthPerSec.
   * The bandwidth parameter is the max number of bytes per second of network
   * bandwidth to be used by a datanode during balancing.
   *
   * @param bandwidth Balancer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    dfs.setBalancerBandwidth(bandwidth);
  }

  /**
   * Get a canonical service name for this file system. If the URI is logical,
   * the hostname part of the URI will be returned.
   * @return a service string that uniquely identifies this file system.
   */
  @Override
  public String getCanonicalServiceName() {
    return dfs.getCanonicalServiceName();
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    if (HAUtilClient.isLogicalUri(getConf(), uri)) {
      // Don't try to DNS-resolve logical URIs, since the 'authority'
      // portion isn't a proper hostname
      return uri;
    } else {
      return NetUtils.getCanonicalUri(uri, getDefaultPort());
    }
  }

  /**
   * Utility function that returns if the NameNode is in safemode or not. In HA
   * mode, this API will return only ActiveNN's safemode status.
   *
   * @return true if NameNode is in safemode, false otherwise.
   * @throws IOException
   *           when there is an issue communicating with the NameNode
   */
  public boolean isInSafeMode() throws IOException {
    return setSafeMode(SafeModeAction.SAFEMODE_GET, true);
  }

  /** @see HdfsAdmin#allowSnapshot(Path) */
  public void allowSnapshot(final Path path) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.ALLOW_SNAPSHOT);
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.allowSnapshot(getPathName(p));
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.allowSnapshot(p);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + path + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }

  /** @see HdfsAdmin#disallowSnapshot(Path) */
  public void disallowSnapshot(final Path path) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.DISALLOW_SNAPSHOT);
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.disallowSnapshot(getPathName(p));
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.disallowSnapshot(p);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + path + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE_SNAPSHOT);
    Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<Path>() {
      @Override
      public Path doCall(final Path p) throws IOException {
        return new Path(dfs.createSnapshot(getPathName(p), snapshotName));
      }

      @Override
      public Path next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.createSnapshot(p);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + path + " -> " + p);
        }
      }
    }.resolve(this, absF);
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.RENAME_SNAPSHOT);
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.renameSnapshot(getPathName(p), snapshotOldName, snapshotNewName);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.renameSnapshot(p, snapshotOldName, snapshotNewName);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + path + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * @return All the snapshottable directories
   * @throws IOException
   */
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    return dfs.getSnapshottableDirListing();
  }

  @Override
  public void deleteSnapshot(final Path snapshotDir, final String snapshotName)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.DELETE_SNAPSHOT);
    Path absF = fixRelativePart(snapshotDir);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.deleteSnapshot(getPathName(p), snapshotName);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.deleteSnapshot(p, snapshotName);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + snapshotDir + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Get the difference between two snapshots, or between a snapshot and the
   * current tree of a directory.
   *
   * @see DFSClient#getSnapshotDiffReport(String, String, String)
   */
  public SnapshotDiffReport getSnapshotDiffReport(final Path snapshotDir,
      final String fromSnapshot, final String toSnapshot) throws IOException {
    Path absF = fixRelativePart(snapshotDir);
    return new FileSystemLinkResolver<SnapshotDiffReport>() {
      @Override
      public SnapshotDiffReport doCall(final Path p) throws IOException {
        return dfs.getSnapshotDiffReport(getPathName(p), fromSnapshot,
            toSnapshot);
      }

      @Override
      public SnapshotDiffReport next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          myDfs.getSnapshotDiffReport(p, fromSnapshot, toSnapshot);
        } else {
          throw new UnsupportedOperationException("Cannot perform snapshot"
              + " operations on a symlink to a non-DistributedFileSystem: "
              + snapshotDir + " -> " + p);
        }
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Get the close status of a file
   * @param src The path to the file
   *
   * @return return true if file is closed
   * @throws FileNotFoundException if the file does not exist.
   * @throws IOException If an I/O error occurred
   */
  public boolean isFileClosed(final Path src) throws IOException {
    Path absF = fixRelativePart(src);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p) throws IOException {
        return dfs.isFileClosed(getPathName(p));
      }

      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.isFileClosed(p);
        } else {
          throw new UnsupportedOperationException("Cannot call isFileClosed"
              + " on a symlink to a non-DistributedFileSystem: "
              + src + " -> " + p);
        }
      }
    }.resolve(this, absF);
  }

  /**
   * @see {@link #addCacheDirective(CacheDirectiveInfo, EnumSet)}
   */
  public long addCacheDirective(CacheDirectiveInfo info) throws IOException {
    return addCacheDirective(info, EnumSet.noneOf(CacheFlag.class));
  }

  /**
   * Add a new CacheDirective.
   *
   * @param info Information about a directive to add.
   * @param flags {@link CacheFlag}s to use for this operation.
   * @return the ID of the directive that was created.
   * @throws IOException if the directive could not be added
   */
  public long addCacheDirective(
      CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
    Preconditions.checkNotNull(info.getPath());
    Path path = new Path(getPathName(fixRelativePart(info.getPath()))).
        makeQualified(getUri(), getWorkingDirectory());
    return dfs.addCacheDirective(
        new CacheDirectiveInfo.Builder(info).
            setPath(path).
            build(),
        flags);
  }

  /**
   * @see {@link #modifyCacheDirective(CacheDirectiveInfo, EnumSet)}
   */
  public void modifyCacheDirective(CacheDirectiveInfo info) throws IOException {
    modifyCacheDirective(info, EnumSet.noneOf(CacheFlag.class));
  }

  /**
   * Modify a CacheDirective.
   *
   * @param info Information about the directive to modify. You must set the ID
   *          to indicate which CacheDirective you want to modify.
   * @param flags {@link CacheFlag}s to use for this operation.
   * @throws IOException if the directive could not be modified
   */
  public void modifyCacheDirective(
      CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
    if (info.getPath() != null) {
      info = new CacheDirectiveInfo.Builder(info).
          setPath(new Path(getPathName(fixRelativePart(info.getPath()))).
              makeQualified(getUri(), getWorkingDirectory())).build();
    }
    dfs.modifyCacheDirective(info, flags);
  }

  /**
   * Remove a CacheDirectiveInfo.
   *
   * @param id identifier of the CacheDirectiveInfo to remove
   * @throws IOException if the directive could not be removed
   */
  public void removeCacheDirective(long id)
      throws IOException {
    dfs.removeCacheDirective(id);
  }

  /**
   * List cache directives.  Incrementally fetches results from the server.
   *
   * @param filter Filter parameters to use when listing the directives, null to
   *               list all directives visible to us.
   * @return A RemoteIterator which returns CacheDirectiveInfo objects.
   */
  public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    if (filter == null) {
      filter = new CacheDirectiveInfo.Builder().build();
    }
    if (filter.getPath() != null) {
      filter = new CacheDirectiveInfo.Builder(filter).
          setPath(new Path(getPathName(fixRelativePart(filter.getPath())))).
          build();
    }
    final RemoteIterator<CacheDirectiveEntry> iter =
        dfs.listCacheDirectives(filter);
    return new RemoteIterator<CacheDirectiveEntry>() {
      @Override
      public boolean hasNext() throws IOException {
        return iter.hasNext();
      }

      @Override
      public CacheDirectiveEntry next() throws IOException {
        // Although the paths we get back from the NameNode should always be
        // absolute, we call makeQualified to add the scheme and authority of
        // this DistributedFilesystem.
        CacheDirectiveEntry desc = iter.next();
        CacheDirectiveInfo info = desc.getInfo();
        Path p = info.getPath().makeQualified(getUri(), getWorkingDirectory());
        return new CacheDirectiveEntry(
            new CacheDirectiveInfo.Builder(info).setPath(p).build(),
            desc.getStats());
      }
    };
  }

  /**
   * Add a cache pool.
   *
   * @param info
   *          The request to add a cache pool.
   * @throws IOException
   *          If the request could not be completed.
   */
  public void addCachePool(CachePoolInfo info) throws IOException {
    CachePoolInfo.validate(info);
    dfs.addCachePool(info);
  }

  /**
   * Modify an existing cache pool.
   *
   * @param info
   *          The request to modify a cache pool.
   * @throws IOException
   *          If the request could not be completed.
   */
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    CachePoolInfo.validate(info);
    dfs.modifyCachePool(info);
  }

  /**
   * Remove a cache pool.
   *
   * @param poolName
   *          Name of the cache pool to remove.
   * @throws IOException
   *          if the cache pool did not exist, or could not be removed.
   */
  public void removeCachePool(String poolName) throws IOException {
    CachePoolInfo.validateName(poolName);
    dfs.removeCachePool(poolName);
  }

  /**
   * List all cache pools.
   *
   * @return A remote iterator from which you can get CachePoolEntry objects.
   *          Requests will be made as needed.
   * @throws IOException
   *          If there was an error listing cache pools.
   */
  public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
    return dfs.listCachePools();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void modifyAclEntries(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.MODIFY_ACL_ENTRIES);
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.modifyAclEntries(getPathName(p), aclSpec);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        fs.modifyAclEntries(p, aclSpec);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeAclEntries(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.REMOVE_ACL_ENTRIES);
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.removeAclEntries(getPathName(p), aclSpec);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        fs.removeAclEntries(p, aclSpec);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.REMOVE_DEFAULT_ACL);
    final Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.removeDefaultAcl(getPathName(p));
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        fs.removeDefaultAcl(p);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.REMOVE_ACL);
    final Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.removeAcl(getPathName(p));
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        fs.removeAcl(p);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAcl(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_ACL);
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.setAcl(getPathName(p), aclSpec);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        fs.setAcl(p, aclSpec);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    final Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<AclStatus>() {
      @Override
      public AclStatus doCall(final Path p) throws IOException {
        return dfs.getAclStatus(getPathName(p));
      }
      @Override
      public AclStatus next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getAclStatus(p);
      }
    }.resolve(this, absF);
  }

  /* HDFS only */
  public void createEncryptionZone(final Path path, final String keyName)
      throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.createEncryptionZone(getPathName(p), keyName);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem) fs;
          myDfs.createEncryptionZone(p, keyName);
          return null;
        } else {
          throw new UnsupportedOperationException(
              "Cannot call createEncryptionZone"
                  + " on a symlink to a non-DistributedFileSystem: " + path
                  + " -> " + p);
        }
      }
    }.resolve(this, absF);
  }

  /* HDFS only */
  public EncryptionZone getEZForPath(final Path path)
      throws IOException {
    Preconditions.checkNotNull(path);
    Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<EncryptionZone>() {
      @Override
      public EncryptionZone doCall(final Path p) throws IOException {
        return dfs.getEZForPath(getPathName(p));
      }

      @Override
      public EncryptionZone next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem) fs;
          return myDfs.getEZForPath(p);
        } else {
          throw new UnsupportedOperationException(
              "Cannot call getEZForPath"
                  + " on a symlink to a non-DistributedFileSystem: " + path
                  + " -> " + p);
        }
      }
    }.resolve(this, absF);
  }

  /* HDFS only */
  public RemoteIterator<EncryptionZone> listEncryptionZones()
      throws IOException {
    return dfs.listEncryptionZones();
  }

  /* HDFS only */
  public void reencryptEncryptionZone(final Path zone,
      final ReencryptAction action) throws IOException {
    final Path absF = fixRelativePart(zone);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.reencryptEncryptionZone(getPathName(p), action);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem) fs;
          myDfs.reencryptEncryptionZone(p, action);
          return null;
        }
        throw new UnsupportedOperationException(
            "Cannot call reencryptEncryptionZone"
                + " on a symlink to a non-DistributedFileSystem: " + zone
                + " -> " + p);
      }
    }.resolve(this, absF);
  }

  /* HDFS only */
  public RemoteIterator<ZoneReencryptionStatus> listReencryptionStatus()
      throws IOException {
    return dfs.listReencryptionStatus();
  }

  /* HDFS only */
  public FileEncryptionInfo getFileEncryptionInfo(final Path path)
      throws IOException {
    Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<FileEncryptionInfo>() {
      @Override
      public FileEncryptionInfo doCall(final Path p) throws IOException {
        final HdfsFileStatus fi = dfs.getFileInfo(getPathName(p));
        if (fi == null) {
          throw new FileNotFoundException("File does not exist: " + p);
        }
        return fi.getFileEncryptionInfo();
      }

      @Override
      public FileEncryptionInfo next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem)fs;
          return myDfs.getFileEncryptionInfo(p);
        }
        throw new UnsupportedOperationException(
            "Cannot call getFileEncryptionInfo"
                + " on a symlink to a non-DistributedFileSystem: " + path
                + " -> " + p);
      }
    }.resolve(this, absF);
  }

  @Override
  public void setXAttr(Path path, final String name, final byte[] value,
      final EnumSet<XAttrSetFlag> flag) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_XATTR);
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {

      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.setXAttr(getPathName(p), name, value, flag);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        fs.setXAttr(p, name, value, flag);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public byte[] getXAttr(Path path, final String name) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_XATTR);
    final Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<byte[]>() {
      @Override
      public byte[] doCall(final Path p) throws IOException {
        return dfs.getXAttr(getPathName(p), name);
      }
      @Override
      public byte[] next(final FileSystem fs, final Path p) throws IOException {
        return fs.getXAttr(p, name);
      }
    }.resolve(this, absF);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    final Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<Map<String, byte[]>>() {
      @Override
      public Map<String, byte[]> doCall(final Path p) throws IOException {
        return dfs.getXAttrs(getPathName(p));
      }
      @Override
      public Map<String, byte[]> next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getXAttrs(p);
      }
    }.resolve(this, absF);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, final List<String> names)
      throws IOException {
    final Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<Map<String, byte[]>>() {
      @Override
      public Map<String, byte[]> doCall(final Path p) throws IOException {
        return dfs.getXAttrs(getPathName(p), names);
      }
      @Override
      public Map<String, byte[]> next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getXAttrs(p, names);
      }
    }.resolve(this, absF);
  }

  @Override
  public List<String> listXAttrs(Path path)
      throws IOException {
    final Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<List<String>>() {
      @Override
      public List<String> doCall(final Path p) throws IOException {
        return dfs.listXAttrs(getPathName(p));
      }
      @Override
      public List<String> next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.listXAttrs(p);
      }
    }.resolve(this, absF);
  }

  @Override
  public void removeXAttr(Path path, final String name) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.REMOVE_XATTR);
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.removeXAttr(getPathName(p), name);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        fs.removeXAttr(p, name);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void access(Path path, final FsAction mode) throws IOException {
    final Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.checkAccess(getPathName(p), mode);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.access(p, mode);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    return dfs.getKeyProviderUri();
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return dfs.getKeyProvider();
  }

  @Override
  public Token<?>[] addDelegationTokens(
      final String renewer, Credentials credentials) throws IOException {
    Token<?>[] tokens = super.addDelegationTokens(renewer, credentials);
    return HdfsKMSUtil.addDelegationTokensForKeyProvider(
        this, renewer, credentials, uri, tokens);
  }

  public DFSInotifyEventInputStream getInotifyEventStream() throws IOException {
    return dfs.getInotifyEventStream();
  }

  public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid)
      throws IOException {
    return dfs.getInotifyEventStream(lastReadTxid);
  }

  /**
   * Set the source path to the specified erasure coding policy.
   *
   * @param path     The directory to set the policy
   * @param ecPolicyName The erasure coding policy name.
   * @throws IOException
   */
  public void setErasureCodingPolicy(final Path path,
      final String ecPolicyName) throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.setErasureCodingPolicy(getPathName(p), ecPolicyName);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem) fs;
          myDfs.setErasureCodingPolicy(p, ecPolicyName);
          return null;
        }
        throw new UnsupportedOperationException(
            "Cannot setErasureCodingPolicy through a symlink to a "
                + "non-DistributedFileSystem: " + path + " -> " + p);
      }
    }.resolve(this, absF);
  }

  /**
   * Get erasure coding policy information for the specified path
   *
   * @param path The path of the file or directory
   * @return Returns the policy information if file or directory on the path
   * is erasure coded, null otherwise. Null will be returned if directory or
   * file has REPLICATION policy.
   * @throws IOException
   */
  public ErasureCodingPolicy getErasureCodingPolicy(final Path path)
      throws IOException {
    Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<ErasureCodingPolicy>() {
      @Override
      public ErasureCodingPolicy doCall(final Path p) throws IOException {
        return dfs.getErasureCodingPolicy(getPathName(p));
      }

      @Override
      public ErasureCodingPolicy next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem) fs;
          return myDfs.getErasureCodingPolicy(p);
        }
        throw new UnsupportedOperationException(
            "Cannot getErasureCodingPolicy through a symlink to a "
                + "non-DistributedFileSystem: " + path + " -> " + p);
      }
    }.resolve(this, absF);
  }

  /**
   * Retrieve all the erasure coding policies supported by this file system,
   * including enabled, disabled and removed policies, but excluding
   * REPLICATION policy.
   *
   * @return all erasure coding policies supported by this file system.
   * @throws IOException
   */
  public Collection<ErasureCodingPolicyInfo> getAllErasureCodingPolicies()
      throws IOException {
    return Arrays.asList(dfs.getErasureCodingPolicies());
  }

  /**
   * Retrieve all the erasure coding codecs and coders supported by this file
   * system.
   *
   * @return all erasure coding codecs and coders supported by this file system.
   * @throws IOException
   */
  public Map<String, String> getAllErasureCodingCodecs()
      throws IOException {
    return dfs.getErasureCodingCodecs();
  }

  /**
   * Add Erasure coding policies to HDFS. For each policy input, schema and
   * cellSize are musts, name and id are ignored. They will be automatically
   * created and assigned by Namenode once the policy is successfully added,
   * and will be returned in the response; policy states will be set to
   * DISABLED automatically.
   *
   * @param policies The user defined ec policy list to add.
   * @return Return the response list of adding operations.
   * @throws IOException
   */
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies)  throws IOException {
    return dfs.addErasureCodingPolicies(policies);
  }

  /**
   * Remove erasure coding policy.
   *
   * @param ecPolicyName The name of the policy to be removed.
   * @throws IOException
   */
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    dfs.removeErasureCodingPolicy(ecPolicyName);
  }

  /**
   * Enable erasure coding policy.
   *
   * @param ecPolicyName The name of the policy to be enabled.
   * @throws IOException
   */
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    dfs.enableErasureCodingPolicy(ecPolicyName);
  }

  /**
   * Disable erasure coding policy.
   *
   * @param ecPolicyName The name of the policy to be disabled.
   * @throws IOException
   */
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    dfs.disableErasureCodingPolicy(ecPolicyName);
  }

  /**
   * Unset the erasure coding policy from the source path.
   *
   * @param path     The directory to unset the policy
   * @throws IOException
   */
  public void unsetErasureCodingPolicy(final Path path) throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.unsetErasureCodingPolicy(getPathName(p));
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem) fs;
          myDfs.unsetErasureCodingPolicy(p);
          return null;
        }
        throw new UnsupportedOperationException(
            "Cannot unsetErasureCodingPolicy through a symlink to a "
                + "non-DistributedFileSystem: " + path + " -> " + p);
      }
    }.resolve(this, absF);
  }

  /**
   * Get the root directory of Trash for a path in HDFS.
   * 1. File in encryption zone returns /ez1/.Trash/username
   * 2. File not in encryption zone, or encountered exception when checking
   *    the encryption zone of the path, returns /users/username/.Trash
   * Caller appends either Current or checkpoint timestamp for trash destination
   * @param path the trash root of the path to be determined.
   * @return trash root
   */
  @Override
  public Path getTrashRoot(Path path) {
    try {
      if ((path == null) || !dfs.isHDFSEncryptionEnabled()) {
        return super.getTrashRoot(path);
      }
    } catch (IOException ioe) {
      DFSClient.LOG.warn("Exception while checking whether encryption zone is "
          + "supported", ioe);
    }

    String parentSrc = path.isRoot()?
        path.toUri().getPath():path.getParent().toUri().getPath();
    try {
      EncryptionZone ez = dfs.getEZForPath(parentSrc);
      if ((ez != null)) {
        return this.makeQualified(
            new Path(new Path(ez.getPath(), FileSystem.TRASH_PREFIX),
                dfs.ugi.getShortUserName()));
      }
    } catch (IOException e) {
      DFSClient.LOG.warn("Exception in checking the encryption zone for the " +
          "path " + parentSrc + ". " + e.getMessage());
    }
    return super.getTrashRoot(path);
  }

  /**
   * Get all the trash roots of HDFS for current user or for all the users.
   * 1. File deleted from non-encryption zone /user/username/.Trash
   * 2. File deleted from encryption zones
   *    e.g., ez1 rooted at /ez1 has its trash root at /ez1/.Trash/$USER
   * @param allUsers return trashRoots of all users if true, used by emptier
   * @return trash roots of HDFS
   */
  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    List<FileStatus> ret = new ArrayList<>();
    // Get normal trash roots
    ret.addAll(super.getTrashRoots(allUsers));

    try {
      // Get EZ Trash roots
      final RemoteIterator<EncryptionZone> it = dfs.listEncryptionZones();
      while (it.hasNext()) {
        Path ezTrashRoot = new Path(it.next().getPath(),
            FileSystem.TRASH_PREFIX);
        if (!exists(ezTrashRoot)) {
          continue;
        }
        if (allUsers) {
          for (FileStatus candidate : listStatus(ezTrashRoot)) {
            if (exists(candidate.getPath())) {
              ret.add(candidate);
            }
          }
        } else {
          Path userTrash = new Path(ezTrashRoot, dfs.ugi.getShortUserName());
          try {
            ret.add(getFileStatus(userTrash));
          } catch (FileNotFoundException ignored) {
          }
        }
      }
    } catch (IOException e){
      DFSClient.LOG.warn("Cannot get all encrypted trash roots", e);
    }
    return ret;
  }

  @Override
  protected Path fixRelativePart(Path p) {
    return super.fixRelativePart(p);
  }

  Statistics getFsStatistics() {
    return statistics;
  }

  DFSOpsCountStatistics getDFSOpsCountStatistics() {
    return storageStatistics;
  }

  /**
   * HdfsDataOutputStreamBuilder provides the HDFS-specific capabilities to
   * write file on HDFS.
   */
  public static final class HdfsDataOutputStreamBuilder
      extends FSDataOutputStreamBuilder<
      FSDataOutputStream, HdfsDataOutputStreamBuilder> {
    private final DistributedFileSystem dfs;
    private InetSocketAddress[] favoredNodes = null;
    private String ecPolicyName = null;

    /**
     * Construct a HdfsDataOutputStream builder for a file.
     * @param dfs the {@link DistributedFileSystem} instance.
     * @param path the path of the file to create / append.
     */
    private HdfsDataOutputStreamBuilder(DistributedFileSystem dfs, Path path) {
      super(dfs, path);
      this.dfs = dfs;
    }

    @Override
    protected HdfsDataOutputStreamBuilder getThisBuilder() {
      return this;
    }

    private InetSocketAddress[] getFavoredNodes() {
      return favoredNodes;
    }

    /**
     * Set favored DataNodes.
     * @param nodes the addresses of the favored DataNodes.
     */
    public HdfsDataOutputStreamBuilder favoredNodes(
        @Nonnull final InetSocketAddress[] nodes) {
      Preconditions.checkNotNull(nodes);
      favoredNodes = nodes.clone();
      return this;
    }

    /**
     * Force closed blocks to disk.
     *
     * @see CreateFlag for the details.
     */
    public HdfsDataOutputStreamBuilder syncBlock() {
      getFlags().add(CreateFlag.SYNC_BLOCK);
      return this;
    }

    /**
     * Create the block on transient storage if possible.
     *
     * @see CreateFlag for the details.
     */
    public HdfsDataOutputStreamBuilder lazyPersist() {
      getFlags().add(CreateFlag.LAZY_PERSIST);
      return this;
    }

    /**
     * Append data to a new block instead of the end of the last partial block.
     *
     * @see CreateFlag for the details.
     */
    public HdfsDataOutputStreamBuilder newBlock() {
      getFlags().add(CreateFlag.NEW_BLOCK);
      return this;
    }

    /**
     * Advise that a block replica NOT be written to the local DataNode.
     *
     * @see CreateFlag for the details.
     */
    public HdfsDataOutputStreamBuilder noLocalWrite() {
      getFlags().add(CreateFlag.NO_LOCAL_WRITE);
      return this;
    }

    @VisibleForTesting
    String getEcPolicyName() {
      return ecPolicyName;
    }

    /**
     * Enforce the file to be a striped file with erasure coding policy
     * 'policyName', no matter what its parent directory's replication
     * or erasure coding policy is. Don't call this function and
     * enforceReplicate() in the same builder since they have conflict
     * of interest.
     */
    public HdfsDataOutputStreamBuilder ecPolicyName(
        @Nonnull final String policyName) {
      Preconditions.checkNotNull(policyName);
      ecPolicyName = policyName;
      return this;
    }

    @VisibleForTesting
    boolean shouldReplicate() {
      return getFlags().contains(CreateFlag.SHOULD_REPLICATE);
    }

    /**
     * Enforce the file to be a replicated file, no matter what its parent
     * directory's replication or erasure coding policy is. Don't call this
     * function and setEcPolicyName() in the same builder since they have
     * conflict of interest.
     */
    public HdfsDataOutputStreamBuilder replicate() {
      getFlags().add(CreateFlag.SHOULD_REPLICATE);
      return this;
    }

    @VisibleForTesting
    @Override
    protected EnumSet<CreateFlag> getFlags() {
      return super.getFlags();
    }

    /**
     * Build HdfsDataOutputStream to write.
     *
     * @return a fully-initialized OutputStream.
     * @throws IOException on I/O errors.
     */
    @Override
    public FSDataOutputStream build() throws IOException {
      if (getFlags().contains(CreateFlag.CREATE) ||
          getFlags().contains(CreateFlag.OVERWRITE)) {
        if (isRecursive()) {
          return dfs.create(getPath(), getPermission(), getFlags(),
              getBufferSize(), getReplication(), getBlockSize(),
              getProgress(), getChecksumOpt(), getFavoredNodes(),
              getEcPolicyName());
        } else {
          return dfs.createNonRecursive(getPath(), getPermission(), getFlags(),
              getBufferSize(), getReplication(), getBlockSize(), getProgress(),
              getChecksumOpt(), getFavoredNodes(), getEcPolicyName());
        }
      } else if (getFlags().contains(CreateFlag.APPEND)) {
        return dfs.append(getPath(), getFlags(), getBufferSize(), getProgress(),
            getFavoredNodes());
      }
      throw new HadoopIllegalArgumentException(
          "Must specify either create or append");
    }
  }

  /**
   * Create a HdfsDataOutputStreamBuilder to create a file on DFS.
   * Similar to {@link #create(Path)}, file is overwritten by default.
   *
   * @param path the path of the file to create.
   * @return A HdfsDataOutputStreamBuilder for creating a file.
   */
  @Override
  public HdfsDataOutputStreamBuilder createFile(Path path) {
    return new HdfsDataOutputStreamBuilder(this, path).create().overwrite(true);
  }

  /**
   * Returns a RemoteIterator which can be used to list all open files
   * currently managed by the NameNode. For large numbers of open files,
   * iterator will fetch the list in batches of configured size.
   * <p/>
   * Since the list is fetched in batches, it does not represent a
   * consistent snapshot of the all open files.
   * <p/>
   * This method can only be called by HDFS superusers.
   */
  public RemoteIterator<OpenFileEntry> listOpenFiles() throws IOException {
    return dfs.listOpenFiles();
  }

  /**
   * Create a {@link HdfsDataOutputStreamBuilder} to append a file on DFS.
   *
   * @param path file path.
   * @return A {@link HdfsDataOutputStreamBuilder} for appending a file.
   */
  @Override
  public HdfsDataOutputStreamBuilder appendFile(Path path) {
    return new HdfsDataOutputStreamBuilder(this, path).append();
  }
}
