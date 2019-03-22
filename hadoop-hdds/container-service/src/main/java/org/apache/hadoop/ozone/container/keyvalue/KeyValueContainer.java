/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerPacker;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers
    .KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.utils.MetadataStore;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_ALREADY_EXISTS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_FILES_CREATE_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_OPEN;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.DISK_OUT_OF_SPACE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.ERROR_IN_COMPACT_DB;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.UNSUPPORTED_REQUEST;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to perform KeyValue Container operations.
 */
public class KeyValueContainer implements Container<KeyValueContainerData> {

  private static final Logger LOG = LoggerFactory.getLogger(Container.class);

  // Use a non-fair RW lock for better throughput, we may revisit this decision
  // if this causes fairness issues.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final KeyValueContainerData containerData;
  private Configuration config;

  public KeyValueContainer(KeyValueContainerData containerData, Configuration
      ozoneConfig) {
    Preconditions.checkNotNull(containerData, "KeyValueContainerData cannot " +
        "be null");
    Preconditions.checkNotNull(ozoneConfig, "Ozone configuration cannot " +
        "be null");
    this.config = ozoneConfig;
    this.containerData = containerData;
  }

  @Override
  public void create(VolumeSet volumeSet, VolumeChoosingPolicy
      volumeChoosingPolicy, String scmId) throws StorageContainerException {
    Preconditions.checkNotNull(volumeChoosingPolicy, "VolumeChoosingPolicy " +
        "cannot be null");
    Preconditions.checkNotNull(volumeSet, "VolumeSet cannot be null");
    Preconditions.checkNotNull(scmId, "scmId cannot be null");

    File containerMetaDataPath = null;
    //acquiring volumeset read lock
    long maxSize = containerData.getMaxSize();
    volumeSet.readLock();
    try {
      HddsVolume containerVolume = volumeChoosingPolicy.chooseVolume(volumeSet
          .getVolumesList(), maxSize);
      String hddsVolumeDir = containerVolume.getHddsRootDir().toString();

      long containerID = containerData.getContainerID();

      containerMetaDataPath = KeyValueContainerLocationUtil
          .getContainerMetaDataPath(hddsVolumeDir, scmId, containerID);
      containerData.setMetadataPath(containerMetaDataPath.getPath());

      File chunksPath = KeyValueContainerLocationUtil.getChunksLocationPath(
          hddsVolumeDir, scmId, containerID);

      // Check if it is new Container.
      ContainerUtils.verifyIsNewContainer(containerMetaDataPath);

      //Create Metadata path chunks path and metadata db
      File dbFile = getContainerDBFile();
      KeyValueContainerUtil.createContainerMetaData(containerMetaDataPath,
          chunksPath, dbFile, config);

      String impl = config.getTrimmed(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL,
          OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_DEFAULT);

      //Set containerData for the KeyValueContainer.
      containerData.setChunksPath(chunksPath.getPath());
      containerData.setContainerDBType(impl);
      containerData.setDbFile(dbFile);
      containerData.setVolume(containerVolume);

      // Create .container file
      File containerFile = getContainerFile();
      createContainerFile(containerFile);

    } catch (StorageContainerException ex) {
      if (containerMetaDataPath != null && containerMetaDataPath.getParentFile()
          .exists()) {
        FileUtil.fullyDelete(containerMetaDataPath.getParentFile());
      }
      throw ex;
    } catch (DiskOutOfSpaceException ex) {
      throw new StorageContainerException("Container creation failed, due to " +
          "disk out of space", ex, DISK_OUT_OF_SPACE);
    } catch (FileAlreadyExistsException ex) {
      throw new StorageContainerException("Container creation failed because " +
          "ContainerFile already exists", ex, CONTAINER_ALREADY_EXISTS);
    } catch (IOException ex) {
      if (containerMetaDataPath != null && containerMetaDataPath.getParentFile()
          .exists()) {
        FileUtil.fullyDelete(containerMetaDataPath.getParentFile());
      }
      throw new StorageContainerException("Container creation failed.", ex,
          CONTAINER_INTERNAL_ERROR);
    } finally {
      volumeSet.readUnlock();
    }
  }

  /**
   * Set all of the path realted container data fields based on the name
   * conventions.
   *
   * @param scmId
   * @param containerVolume
   * @param hddsVolumeDir
   */
  public void populatePathFields(String scmId,
      HddsVolume containerVolume, String hddsVolumeDir) {

    long containerId = containerData.getContainerID();

    File containerMetaDataPath = KeyValueContainerLocationUtil
        .getContainerMetaDataPath(hddsVolumeDir, scmId, containerId);

    File chunksPath = KeyValueContainerLocationUtil.getChunksLocationPath(
        hddsVolumeDir, scmId, containerId);
    File dbFile = KeyValueContainerLocationUtil.getContainerDBFile(
        containerMetaDataPath, containerId);

    //Set containerData for the KeyValueContainer.
    containerData.setMetadataPath(containerMetaDataPath.getPath());
    containerData.setChunksPath(chunksPath.getPath());
    containerData.setDbFile(dbFile);
    containerData.setVolume(containerVolume);
  }

  /**
   * Writes to .container file.
   *
   * @param containerFile container file name
   * @param isCreate True if creating a new file. False is updating an
   *                 existing container file.
   * @throws StorageContainerException
   */
  private void writeToContainerFile(File containerFile, boolean isCreate)
      throws StorageContainerException {
    File tempContainerFile = null;
    long containerId = containerData.getContainerID();
    try {
      tempContainerFile = createTempFile(containerFile);
      ContainerDataYaml.createContainerFile(
          ContainerType.KeyValueContainer, containerData, tempContainerFile);

      // NativeIO.renameTo is an atomic function. But it might fail if the
      // container file already exists. Hence, we handle the two cases
      // separately.
      if (isCreate) {
        NativeIO.renameTo(tempContainerFile, containerFile);
      } else {
        Files.move(tempContainerFile.toPath(), containerFile.toPath(),
            StandardCopyOption.REPLACE_EXISTING);
      }

    } catch (IOException ex) {
      throw new StorageContainerException("Error while creating/ updating " +
          ".container file. ContainerID: " + containerId, ex,
          CONTAINER_FILES_CREATE_ERROR);
    } finally {
      if (tempContainerFile != null && tempContainerFile.exists()) {
        if (!tempContainerFile.delete()) {
          LOG.warn("Unable to delete container temporary file: {}.",
              tempContainerFile.getAbsolutePath());
        }
      }
    }
  }

  private void createContainerFile(File containerFile)
      throws StorageContainerException {
    writeToContainerFile(containerFile, true);
  }

  private void updateContainerFile(File containerFile)
      throws StorageContainerException {
    writeToContainerFile(containerFile, false);
  }


  @Override
  public void delete() throws StorageContainerException {
    long containerId = containerData.getContainerID();
    try {
      KeyValueContainerUtil.removeContainer(containerData, config);
    } catch (StorageContainerException ex) {
      throw ex;
    } catch (IOException ex) {
      // TODO : An I/O error during delete can leave partial artifacts on the
      // disk. We will need the cleaner thread to cleanup this information.
      String errMsg = String.format("Failed to cleanup container. ID: %d",
          containerId);
      LOG.error(errMsg, ex);
      throw new StorageContainerException(errMsg, ex, CONTAINER_INTERNAL_ERROR);
    }
  }

  @Override
  public void markContainerForClose() throws StorageContainerException {
    writeLock();
    try {
      if (getContainerState() != ContainerDataProto.State.OPEN) {
        throw new StorageContainerException(
            "Attempting to close a " + getContainerState() + " container.",
            CONTAINER_NOT_OPEN);
      }
      updateContainerData(() ->
          containerData.setState(ContainerDataProto.State.CLOSING));
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void markContainerUnhealthy() throws StorageContainerException {
    writeLock();
    try {
      updateContainerData(() ->
          containerData.setState(ContainerDataProto.State.UNHEALTHY));
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void quasiClose() throws StorageContainerException {
    writeLock();
    try {
      updateContainerData(containerData::quasiCloseContainer);
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void close() throws StorageContainerException {
    writeLock();
    try {
      updateContainerData(containerData::closeContainer);
    } finally {
      writeUnlock();
    }

    // It is ok if this operation takes a bit of time.
    // Close container is not expected to be instantaneous.
    compactDB();
  }

  /**
   *
   * Must be invoked with the writeLock held.
   *
   * @param update
   * @throws StorageContainerException
   */
  private void updateContainerData(Runnable update)
      throws StorageContainerException {
    Preconditions.checkState(hasWriteLock());
    ContainerDataProto.State oldState = null;
    try {
      oldState = containerData.getState();
      update.run();
      File containerFile = getContainerFile();
      // update the new container data to .container File
      updateContainerFile(containerFile);

    } catch (StorageContainerException ex) {
      if (oldState != null) {
        // Failed to update .container file. Reset the state to CLOSING
        containerData.setState(oldState);
      }
      throw ex;
    }
  }

  void compactDB() throws StorageContainerException {
    try {
      MetadataStore db = BlockUtils.getDB(containerData, config);
      db.compactDB();
      LOG.info("Container {} is closed with bcsId {}.",
          containerData.getContainerID(),
          containerData.getBlockCommitSequenceId());
    } catch (StorageContainerException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error("Error in DB compaction while closing container", ex);
      throw new StorageContainerException(ex, ERROR_IN_COMPACT_DB);
    }
  }

  @Override
  public KeyValueContainerData getContainerData()  {
    return containerData;
  }

  @Override
  public ContainerProtos.ContainerDataProto.State getContainerState() {
    return containerData.getState();
  }

  @Override
  public ContainerType getContainerType() {
    return ContainerType.KeyValueContainer;
  }

  @Override
  public void update(
      Map<String, String> metadata, boolean forceUpdate)
      throws StorageContainerException {

    // TODO: Now, when writing the updated data to .container file, we are
    // holding lock and writing data to disk. We can have async implementation
    // to flush the update container data to disk.
    long containerId = containerData.getContainerID();
    if(!containerData.isValid()) {
      LOG.debug("Invalid container data. ContainerID: {}", containerId);
      throw new StorageContainerException("Invalid container data. " +
          "ContainerID: " + containerId, INVALID_CONTAINER_STATE);
    }
    if (!forceUpdate && !containerData.isOpen()) {
      throw new StorageContainerException(
          "Updating a closed container without force option is not allowed. " +
              "ContainerID: " + containerId, UNSUPPORTED_REQUEST);
    }

    Map<String, String> oldMetadata = containerData.getMetadata();
    try {
      writeLock();
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        containerData.addMetadata(entry.getKey(), entry.getValue());
      }

      File containerFile = getContainerFile();
      // update the new container data to .container File
      updateContainerFile(containerFile);
    } catch (StorageContainerException  ex) {
      containerData.setMetadata(oldMetadata);
      throw ex;
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void updateDeleteTransactionId(long deleteTransactionId) {
    containerData.updateDeleteTransactionId(deleteTransactionId);
  }

  @Override
  public KeyValueBlockIterator blockIterator() throws IOException{
    return new KeyValueBlockIterator(containerData.getContainerID(), new File(
        containerData.getContainerPath()));
  }

  @Override
  public void importContainerData(InputStream input,
      ContainerPacker<KeyValueContainerData> packer) throws IOException {
    writeLock();
    try {
      if (getContainerFile().exists()) {
        String errorMessage = String.format(
            "Can't import container (cid=%d) data to a specific location"
                + " as the container descriptor (%s) has already been exist.",
            getContainerData().getContainerID(),
            getContainerFile().getAbsolutePath());
        throw new IOException(errorMessage);
      }
      //copy the values from the input stream to the final destination
      // directory.
      byte[] descriptorContent = packer.unpackContainerData(this, input);

      Preconditions.checkNotNull(descriptorContent,
          "Container descriptor is missing from the container archive: "
              + getContainerData().getContainerID());

      //now, we have extracted the container descriptor from the previous
      //datanode. We can load it and upload it with the current data
      // (original metadata + current filepath fields)
      KeyValueContainerData originalContainerData =
          (KeyValueContainerData) ContainerDataYaml
              .readContainer(descriptorContent);


      containerData.setState(originalContainerData.getState());
      containerData
          .setContainerDBType(originalContainerData.getContainerDBType());
      containerData.setBytesUsed(originalContainerData.getBytesUsed());

      //rewriting the yaml file with new checksum calculation.
      update(originalContainerData.getMetadata(), true);

      //fill in memory stat counter (keycount, byte usage)
      KeyValueContainerUtil.parseKVContainerData(containerData, config);

    } catch (Exception ex) {
      //delete all the temporary data in case of any exception.
      try {
        FileUtils.deleteDirectory(new File(containerData.getMetadataPath()));
        FileUtils.deleteDirectory(new File(containerData.getChunksPath()));
        FileUtils.deleteDirectory(getContainerFile());
      } catch (Exception deleteex) {
        LOG.error(
            "Can not cleanup destination directories after a container import"
                + " error (cid" +
                containerData.getContainerID() + ")", deleteex);
      }
      throw ex;
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void exportContainerData(OutputStream destination,
      ContainerPacker<KeyValueContainerData> packer) throws IOException {
    if (getContainerData().getState() !=
        ContainerProtos.ContainerDataProto.State.CLOSED) {
      throw new IllegalStateException(
          "Only closed containers could be exported: ContainerId="
              + getContainerData().getContainerID());
    }
    packer.pack(this, destination);
  }

  /**
   * Acquire read lock.
   */
  public void readLock() {
    this.lock.readLock().lock();

  }

  /**
   * Release read lock.
   */
  public void readUnlock() {
    this.lock.readLock().unlock();
  }

  /**
   * Check if the current thread holds read lock.
   */
  public boolean hasReadLock() {
    return this.lock.readLock().tryLock();
  }

  /**
   * Acquire write lock.
   */
  public void writeLock() {
    this.lock.writeLock().lock();
  }

  /**
   * Release write lock.
   */
  public void writeUnlock() {
    this.lock.writeLock().unlock();

  }

  /**
   * Check if the current thread holds write lock.
   */
  public boolean hasWriteLock() {
    return this.lock.writeLock().isHeldByCurrentThread();
  }

  /**
   * Acquire read lock, unless interrupted while waiting.
   * @throws InterruptedException
   */
  @Override
  public void readLockInterruptibly() throws InterruptedException {
    this.lock.readLock().lockInterruptibly();
  }

  /**
   * Acquire write lock, unless interrupted while waiting.
   * @throws InterruptedException
   */
  @Override
  public void writeLockInterruptibly() throws InterruptedException {
    this.lock.writeLock().lockInterruptibly();

  }

  /**
   * Returns containerFile.
   * @return .container File name
   */
  @Override
  public File getContainerFile() {
    return getContainerFile(containerData.getMetadataPath(),
            containerData.getContainerID());
  }

  static File getContainerFile(String metadataPath, long containerId) {
    return new File(metadataPath,
        containerId + OzoneConsts.CONTAINER_EXTENSION);
  }

  @Override
  public void updateBlockCommitSequenceId(long blockCommitSequenceId) {
    containerData.updateBlockCommitSequenceId(blockCommitSequenceId);
  }


  /**
   * Returns KeyValueContainerReport for the KeyValueContainer.
   */
  @Override
  public ContainerReplicaProto getContainerReport()
      throws StorageContainerException {
    ContainerReplicaProto.Builder ciBuilder =
        ContainerReplicaProto.newBuilder();
    ciBuilder.setContainerID(containerData.getContainerID())
        .setReadCount(containerData.getReadCount())
        .setWriteCount(containerData.getWriteCount())
        .setReadBytes(containerData.getReadBytes())
        .setWriteBytes(containerData.getWriteBytes())
        .setKeyCount(containerData.getKeyCount())
        .setUsed(containerData.getBytesUsed())
        .setState(getHddsState())
        .setDeleteTransactionId(containerData.getDeleteTransactionId())
        .setBlockCommitSequenceId(containerData.getBlockCommitSequenceId())
        .setOriginNodeId(containerData.getOriginNodeId());
    return ciBuilder.build();
  }

  /**
   * Returns LifeCycle State of the container.
   * @return LifeCycle State of the container in HddsProtos format
   * @throws StorageContainerException
   */
  private ContainerReplicaProto.State getHddsState()
      throws StorageContainerException {
    ContainerReplicaProto.State state;
    switch (containerData.getState()) {
    case OPEN:
      state = ContainerReplicaProto.State.OPEN;
      break;
    case CLOSING:
      state = ContainerReplicaProto.State.CLOSING;
      break;
    case QUASI_CLOSED:
      state = ContainerReplicaProto.State.QUASI_CLOSED;
      break;
    case CLOSED:
      state = ContainerReplicaProto.State.CLOSED;
      break;
    case UNHEALTHY:
      state = ContainerReplicaProto.State.UNHEALTHY;
      break;
    default:
      throw new StorageContainerException("Invalid Container state found: " +
          containerData.getContainerID(), INVALID_CONTAINER_STATE);
    }
    return state;
  }

  /**
   * Returns container DB file.
   * @return
   */
  public File getContainerDBFile() {
    return new File(containerData.getMetadataPath(), containerData
        .getContainerID() + OzoneConsts.DN_CONTAINER_DB);
  }

  /**
   * run integrity checks on the Container metadata.
   */
  public void check() throws StorageContainerException {
    ContainerCheckLevel level = ContainerCheckLevel.NO_CHECK;
    long containerId = containerData.getContainerID();

    switch (containerData.getState()) {
    case OPEN:
      level = ContainerCheckLevel.FAST_CHECK;
      LOG.info("Doing Fast integrity checks for Container ID : {},"
          + " because it is OPEN", containerId);
      break;
    case CLOSING:
      level = ContainerCheckLevel.FAST_CHECK;
      LOG.info("Doing Fast integrity checks for Container ID : {},"
          + " because it is CLOSING", containerId);
      break;
    case CLOSED:
    case QUASI_CLOSED:
      level = ContainerCheckLevel.FULL_CHECK;
      LOG.debug("Doing Full integrity checks for Container ID : {},"
              + " because it is in {} state", containerId,
          containerData.getState());
      break;
    default:
      throw new StorageContainerException(
          "Invalid Container state found for Container : " + containerData
              .getContainerID(), INVALID_CONTAINER_STATE);
    }

    if (level == ContainerCheckLevel.NO_CHECK) {
      LOG.debug("Skipping integrity checks for Container Id : {}", containerId);
      return;
    }

    KeyValueContainerCheck checker =
        new KeyValueContainerCheck(containerData.getMetadataPath(), config,
            containerId);

    switch (level) {
    case FAST_CHECK:
      checker.fastCheck();
      break;
    case FULL_CHECK:
      checker.fullCheck();
      break;
    case NO_CHECK:
      LOG.debug("Skipping integrity checks for Container Id : {}", containerId);
      break;
    default:
      // we should not be here at all, scuttle the ship!
      Preconditions.checkNotNull(0, "Invalid Containercheck level");
    }
  }

  private enum ContainerCheckLevel {
    NO_CHECK, FAST_CHECK, FULL_CHECK
  }

  /**
   * Creates a temporary file.
   * @param file
   * @return
   * @throws IOException
   */
  private File createTempFile(File file) throws IOException{
    return File.createTempFile("tmp_" + System.currentTimeMillis() + "_",
        file.getName(), file.getParentFile());
  }

}
