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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ProvidedVolumeImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReadThroughInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.SynchronousReadThroughInputStream;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * This abstract class is used as a base class for provided replicas.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class ProvidedReplica extends ReplicaInfo {

  public static final Logger LOG =
      LoggerFactory.getLogger(ProvidedReplica.class);

  // Null checksum information for provided replicas.
  // Shared across all replicas.
  static final byte[] NULL_CHECKSUM_ARRAY =
      FsDatasetUtil.createNullChecksumByteArray();
  private URI fileURI;
  private Path pathPrefix;
  private String pathSuffix;
  private long fileOffset;
  private Configuration conf;
  private PathHandle pathHandle;
  private FileSystem remoteFS;

  /**
   * Constructor.
   *
   * @param blockId block id
   * @param fileURI remote URI this block is to be read from
   * @param fileOffset the offset in the remote URI
   * @param blockLen the length of the block
   * @param genStamp the generation stamp of the block
   * @param volume the volume this block belongs to
   * @param conf the configuration
   * @param remoteFS reference to the remote filesystem to use for this replica.
   */
  public ProvidedReplica(long blockId, URI fileURI, long fileOffset,
      long blockLen, long genStamp, PathHandle pathHandle, FsVolumeSpi volume,
      Configuration conf, FileSystem remoteFS) {
    super(volume, blockId, blockLen, genStamp);
    this.fileURI = fileURI;
    this.fileOffset = fileOffset;
    this.conf = conf;
    this.pathHandle = pathHandle;
    if (remoteFS != null && containsBlock(remoteFS.getUri(), fileURI)) {
      this.remoteFS = remoteFS;
    } else {
      LOG.warn(
          "Creating an reference to the remote FS for provided block " + this);
      try {
        this.remoteFS = FileSystem.get(fileURI, this.conf);
      } catch (IOException e) {
        LOG.warn("Failed to obtain filesystem for " + fileURI);
        this.remoteFS = null;
      }
    }
  }

  /**
   * Constructor.
   *
   * @param blockId block id
   * @param pathPrefix A prefix of the {@link Path} associated with this replica
   *          on the remote {@link FileSystem}.
   * @param pathSuffix A suffix of the {@link Path} associated with this replica
   *          on the remote {@link FileSystem}. Resolving the {@code pathSuffix}
   *          against the {@code pathPrefix} should provide the exact
   *          {@link Path} of the data associated with this replica on the
   *          remote {@link FileSystem}.
   * @param fileOffset the offset in the remote URI
   * @param blockLen the length of the block
   * @param genStamp the generation stamp of the block
   * @param volume the volume this block belongs to
   * @param conf the configuration
   * @param remoteFS reference to the remote filesystem to use for this replica.
   */
  public ProvidedReplica(long blockId, Path pathPrefix, String pathSuffix,
      long fileOffset, long blockLen, long genStamp, PathHandle pathHandle,
      FsVolumeSpi volume, Configuration conf, FileSystem remoteFS) {
    super(volume, blockId, blockLen, genStamp);
    this.fileURI = null;
    this.pathPrefix = pathPrefix;
    this.pathSuffix = pathSuffix;
    this.fileOffset = fileOffset;
    this.conf = conf;
    this.pathHandle = pathHandle;
    if (remoteFS != null &&
        containsBlock(remoteFS.getUri(), pathPrefix.toUri())) {
      this.remoteFS = remoteFS;
    } else {
      LOG.warn(
          "Creating an reference to the remote FS for provided block " + this);
      try {
        this.remoteFS = FileSystem.get(pathPrefix.toUri(), this.conf);
      } catch (IOException e) {
        LOG.warn("Failed to obtain filesystem for " + pathPrefix);
        this.remoteFS = null;
      }
    }
  }

  public ProvidedReplica(ProvidedReplica r) {
    super(r);
    this.fileURI = r.fileURI;
    this.fileOffset = r.fileOffset;
    this.conf = r.conf;
    this.remoteFS = r.remoteFS;
    this.pathHandle = r.pathHandle;
    this.pathPrefix = r.pathPrefix;
    this.pathSuffix = r.pathSuffix;
  }


  private static URI getAbsoluteURI(URI uri) {
    if (!uri.isAbsolute() || uri.getScheme().equals("file")) {
      // URI is not absolute implies it is for a local file
      // normalize the URI
      return StorageLocation.normalizeFileURI(uri);
    } else {
      return uri;
    }
  }

  /**
   * Checks if the {@code blockURI} is contained in the {@code volumeURI}.
   * @param volumeURI
   * @param blockURI
   * @return true if the block URI is contained within the volume URI.
   */
  @VisibleForTesting
  static boolean containsBlock(URI volumeURI, URI blockURI) {
    if (volumeURI == null && blockURI == null){
      return true;
    }
    if (volumeURI == null || blockURI == null) {
      return false;
    }
    volumeURI = getAbsoluteURI(volumeURI);
    blockURI = getAbsoluteURI(blockURI);
    return !volumeURI.relativize(blockURI).equals(blockURI);
  }

  @VisibleForTesting
  FileSystem getRemoteFS() {
    return remoteFS;
  }

  @Override
  public URI getBlockURI() {
    return getRemoteURI();
  }

  @VisibleForTesting
  public String getPathSuffix() {
    return pathSuffix;
  }

  @VisibleForTesting
  public Path getPathPrefix() {
    return pathPrefix;
  }

  private URI getRemoteURI() {
    if (fileURI != null) {
      return fileURI;
    } else if (pathPrefix == null) {
      return new Path(pathSuffix).toUri();
    } else {
      return new Path(pathPrefix, pathSuffix).toUri();
    }
  }

  static InputStream createInputStream(ProvidedReplica replica,
      FSDataInputStream fsIns, String bpid, boolean readThough,
      long fileOffset, long seekOffset, Configuration conf)
      throws IOException {
    if (readThough) {
      ReadThroughInputStream readThroughIns = ReflectionUtils.newInstance(
          conf.getClass(
              DFSConfigKeys.DFS_DATANODE_PROVIDED_READ_CACHE_TASK_CLASS,
              SynchronousReadThroughInputStream.class,
              ReadThroughInputStream.class),
          conf);
      try {
        ExtendedBlock eb = new ExtendedBlock(bpid, replica);
        readThroughIns.init(eb, conf,
            replica.getVolume().getDataset(), fsIns, fileOffset,
            seekOffset, replica.getNumBytes());
        return readThroughIns;
      } catch (IOException e) {
        LOG.warn("Caching task initialization failed; Error " + e);
      }
    }
    return new BoundedInputStream(new FSDataInputStream(fsIns),
        replica.getNumBytes());
  }

  @Override
  public InputStream getDataInputStream(long seekOffset) throws IOException {
    return getDataInputStream(seekOffset, false, null);
  }

  @Override
  public InputStream getDataInputStream(long seekOffset, boolean readThrough,
      String bpid) throws IOException {
    if (remoteFS != null) {
      getProvidedVolume().incrProvidedOpenCalls();
      FSDataInputStream ins;
      try {
        if (pathHandle != null && pathHandle.toByteArray().length > 0) {
          ins = remoteFS.open(pathHandle, conf.getInt(IO_FILE_BUFFER_SIZE_KEY,
              IO_FILE_BUFFER_SIZE_DEFAULT));
        } else {
          ins = remoteFS.open(new Path(getRemoteURI()));
        }
      } catch (UnsupportedOperationException e) {
        throw new IOException("PathHandle specified, but unsuported", e);
      }

      ins.seek(fileOffset + seekOffset);
      return createInputStream(this, ins, bpid, readThrough, fileOffset,
          seekOffset, conf);
    } else {
      throw new IOException("Remote filesystem for provided replica " + this +
          " does not exist");
    }
  }

  @Override
  public OutputStream getDataOutputStream(boolean append) throws IOException {
    throw new UnsupportedOperationException(
        "OutputDataStream is not implemented for ProvidedReplica");
  }

  @Override
  public URI getMetadataURI() {
    return null;
  }

  @Override
  public OutputStream getMetadataOutputStream(boolean append)
      throws IOException {
    return null;
  }

  @Override
  public boolean blockDataExists() {
    if(remoteFS != null) {
      try {
        URI remoteURI = getRemoteURI();
        if (remoteURI == null) {
          LOG.warn("URI of provided replica {} is null", this);
          return false;
        }
        getProvidedVolume().incrProvidedExistsChecks();
        return remoteFS.exists(new Path(remoteURI));
      } catch (IOException e) {
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public boolean deleteBlockData() {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support deleting block data");
  }

  @Override
  public long getBlockDataLength() {
    return this.getNumBytes();
  }

  @Override
  public LengthInputStream getMetadataInputStream(long offset)
      throws IOException {
    return new LengthInputStream(new ByteArrayInputStream(NULL_CHECKSUM_ARRAY),
        NULL_CHECKSUM_ARRAY.length);
  }

  @Override
  public boolean metadataExists() {
    return NULL_CHECKSUM_ARRAY == null ? false : true;
  }

  @Override
  public boolean deleteMetadata() {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support deleting metadata");
  }

  @Override
  public long getMetadataLength() {
    return NULL_CHECKSUM_ARRAY == null ? 0 : NULL_CHECKSUM_ARRAY.length;
  }

  @Override
  public boolean renameMeta(URI destURI) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support renaming metadata");
  }

  @Override
  public boolean renameData(URI destURI) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support renaming data");
  }

  @Override
  public boolean getPinning(LocalFileSystem localFS) throws IOException {
    return false;
  }

  @Override
  public void setPinning(LocalFileSystem localFS) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support pinning");
  }

  @Override
  public void bumpReplicaGS(long newGS) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support writes");
  }

  @Override
  public boolean breakHardLinksIfNeeded() throws IOException {
    return false;
  }

  @Override
  public ReplicaRecoveryInfo createInfo()
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support writes");
  }

  @Override
  public int compareWith(ScanInfo info) {
    if (info.getFileRegion().equals(
        new FileRegion(this.getBlockId(), new Path(getRemoteURI()),
            fileOffset, this.getNumBytes(), this.getGenerationStamp()))) {
      return 0;
    } else {
      return (int) (info.getBlockLength() - getNumBytes());
    }
  }

  @Override
  public void truncateBlock(long newLength) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support truncate");
  }

  @Override
  public void updateWithReplica(StorageLocation replicaLocation) {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support update");
  }

  @Override
  public void copyMetadata(URI destination) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support copy metadata");
  }

  @Override
  public void copyBlockdata(URI destination) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support copy data");
  }

  @VisibleForTesting
  public void setPathHandle(PathHandle pathHandle) {
    this.pathHandle = pathHandle;
  }

  private ProvidedVolumeImpl getProvidedVolume() {
    return (ProvidedVolumeImpl) getVolume();
  }
}
