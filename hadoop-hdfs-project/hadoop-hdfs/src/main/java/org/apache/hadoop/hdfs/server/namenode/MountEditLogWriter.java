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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedStorageMap;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.BlockResolver;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.TreePath;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.UGIResolver;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_WRITER_BLOCK_RESOLVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_WRITER_BLOCK_RESOLVER_CLASS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_WRITER_UGI_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_WRITER_UGI_CLASS_DEFAULT;

/**
 * This class creates the local directories or files that are equivalent
 * to the remote paths specified. The creation includes
 * the appropriate edit logs to ensure the operations allow for HA.
 */
public class MountEditLogWriter implements Closeable {

  public static final Logger LOG =
      LoggerFactory.getLogger(MountEditLogWriter.class);

  private FSNamesystem namesystem;
  private final UGIResolver ugis;
  private final BlockAliasMap.Writer<FileRegion> aliasMapWriter;
  private final BlockResolver blockResolver;
  private final BlockManager blockManager;
  private final DatanodeStorage providedDatanodeStorage;
  private final DatanodeStorageInfo providedStorageInfo;

  public MountEditLogWriter(FSNamesystem namesystem, Configuration conf) {
    this.namesystem = namesystem;
    ugis = ReflectionUtils.newInstance(
        conf.getClass(DFS_IMAGE_WRITER_UGI_CLASS,
            DFS_IMAGE_WRITER_UGI_CLASS_DEFAULT, UGIResolver.class), conf);
    Class<? extends BlockResolver> blockIdsClass = conf.getClass(
        DFS_IMAGE_WRITER_BLOCK_RESOLVER_CLASS,
        DFS_IMAGE_WRITER_BLOCK_RESOLVER_CLASS_DEFAULT, BlockResolver.class);
    blockResolver = ReflectionUtils.newInstance(blockIdsClass, conf);

    blockManager = namesystem.getBlockManager();
    ProvidedStorageMap providedStorageMap =
        blockManager.getProvidedStorageMap();
    if (!providedStorageMap.containsActiveProvidedNodes()) {
      throw new UnsupportedOperationException("Mount failed! " +
          "Datanodes with PROVIDED storage unavailable.");
    }
    providedStorageInfo = providedStorageMap.getProvidedStorageInfo();
    if (providedStorageInfo == null) {
      throw new UnsupportedOperationException("Mount failed! " +
          "Provided storage has not been configured.");
    }
    providedDatanodeStorage = new DatanodeStorage(
        providedStorageInfo.getStorageID(), DatanodeStorage.State.NORMAL,
        providedStorageInfo.getStorageType());

    BlockAliasMap aliasMap = providedStorageMap.getAliasMap();
    if (aliasMap == null) {
      throw new UnsupportedOperationException("Mount failed! " +
          "Aliasmap has not been configured properly.");
    }
    try {
      aliasMapWriter = aliasMap.getWriter(null, namesystem.getBlockPoolId());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create the path corresponding to the remote {@link TreePath}, and create
   * the necessary edits for it.
   * @param treePath remote path
   * @throws IOException
   */
  public void addToEdits(TreePath treePath) throws IOException {
    FileStatus remoteStatus = treePath.getFileStatus();
    AclStatus aclStatus = treePath.getAclStatus();
    String localPath = getLocalPath(treePath);
    String storagePolicyName = treePath.getStoragePolicyName();
    if (remoteStatus.isFile()) {
      createFile(remoteStatus, aclStatus, localPath);
    } else if (remoteStatus.isDirectory()) {
      createDirectory(remoteStatus, aclStatus, localPath, storagePolicyName);
    } else if (remoteStatus.isSymlink()) {
      throw new UnsupportedOperationException("symlinks not supported");
    } else {
      throw new UnsupportedOperationException("Unknown type: " + remoteStatus);
    }
  }

  private void setAcls(String localPath, AclStatus aclStatus)
      throws IOException {
    if (aclStatus != null) {
      namesystem.modifyAclEntries(localPath, ugis.aclEntries(aclStatus));
    }
  }

  private HdfsFileStatus createDirectory(FileStatus remoteStatus,
      AclStatus aclStatus, String localPath, String storagePolicyName)
      throws IOException {
    PermissionStatus perms = ugis.getPermissions(remoteStatus, aclStatus);
    try {
      if (!namesystem.mkdirsChecked(localPath, perms, false, path -> {})) {
        LOG.error("Unable to create directory {}.", localPath);
        // clean up?
        throw new IOException("Unable to create directory " + localPath
            + " during mount");
      }
    } catch (FileAlreadyExistsException e) {
      LOG.warn("Existing directory found for path {}. Exception: {}",
          remoteStatus.getPath(), e);
    }
    setAcls(localPath, aclStatus);
    if (storagePolicyName != null) {
      namesystem.setStoragePolicy(localPath, storagePolicyName);
    }
    return namesystem.getFileInfo(localPath, false, false, false);
  }

  private HdfsFileStatus createFile(FileStatus remoteStatus,
      AclStatus aclStatus, String localPath) throws IOException {
    PermissionStatus perms = ugis.getPermissions(remoteStatus, aclStatus);
    // create the file
    String client = "MountClient";
    HdfsFileStatus hdfsFileStatus = namesystem.startFileChecked(
        localPath, perms, client, client,
        EnumSet.of(CreateFlag.CREATE), false,
        (short) blockResolver.getReplication(remoteStatus),
        blockResolver.preferredBlockSize(remoteStatus), null, null, null,
        false, path -> {});
    long inodeId = hdfsFileStatus.getFileId();
    long offset = 0L;
    ExtendedBlock prev = null;

    for (HdfsProtos.BlockProto block : blockResolver.resolve(remoteStatus)) {
      LocatedBlock lb = namesystem.getAdditionalBlock(
          localPath, inodeId, client, prev, null, null, null);
      prev = lb.getBlock();
      aliasMapWriter.store(new FileRegion(prev.getBlockId(),
          remoteStatus.getPath(), offset, block.getNumBytes(),
          prev.getGenerationStamp()));
      offset += block.getNumBytes();
      prev.setNumBytes(block.getNumBytes());
      ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(
          prev.getLocalBlock(),
          ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK,
          null);

      StorageReceivedDeletedBlocks receivedDeletedBlocks =
          new StorageReceivedDeletedBlocks(providedDatanodeStorage,
              new ReceivedDeletedBlockInfo[] {bInfo});

      blockManager.processIncrementalBlockReport(
          providedStorageInfo.getDatanodeDescriptor(), receivedDeletedBlocks);
    }
    boolean fileCompleted = namesystem.completeFile(
        localPath, client, prev, inodeId);
    if (!fileCompleted) {
      LOG.warn("File isn't completed {}", localPath);
    }
    setAcls(localPath, aclStatus);
    return hdfsFileStatus;
  }

  private String getLocalPath(TreePath e) {
    Path localMount = e.getLocalMountPath();
    Path remoteRoot = e.getRemoteRoot();
    Path fullRemotePath = e.getFileStatus().getPath();
    if (remoteRoot == null) {
      return fullRemotePath.toString();
    }
    URI relativeURI = remoteRoot.toUri().relativize(fullRemotePath.toUri());
    String relativePath = relativeURI.getPath();
    return relativePath.length() > 0
        ? new Path(localMount, relativePath).toString()
        : fullRemotePath.toString();
  }

  @Override
  public void close() throws IOException {
    aliasMapWriter.close();
  }
}
