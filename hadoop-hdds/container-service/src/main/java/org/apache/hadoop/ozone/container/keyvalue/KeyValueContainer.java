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

import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerLifeCycleState;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers
    .KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.utils.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_ALREADY_EXISTS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_FILES_CREATE_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.DISK_OUT_OF_SPACE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.ERROR_IN_COMPACT_DB;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.UNSUPPORTED_REQUEST;

/**
 * Class to perform KeyValue Container operations.
 */
public class KeyValueContainer implements Container {

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
    //acquiring volumeset lock and container lock
    volumeSet.acquireLock();
    long maxSize = (containerData.getMaxSizeGB() * 1024L * 1024L * 1024L);
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
      volumeSet.releaseLock();
    }
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
  public void delete(boolean forceDelete)
      throws StorageContainerException {
    long containerId = containerData.getContainerID();
    try {
      KeyValueContainerUtil.removeContainer(containerData, config, forceDelete);
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
  public void close() throws StorageContainerException {

    //TODO: writing .container file and compaction can be done
    // asynchronously, otherwise rpc call for this will take a lot of time to
    // complete this action
    try {
      writeLock();

      containerData.closeContainer();
      File containerFile = getContainerFile();
      // update the new container data to .container File
      updateContainerFile(containerFile);

    } catch (StorageContainerException ex) {
      // Failed to update .container file. Reset the state to CLOSING
      containerData.setState(ContainerLifeCycleState.CLOSING);
      throw ex;
    } finally {
      writeUnlock();
    }

    // It is ok if this operation takes a bit of time.
    // Close container is not expected to be instantaneous.
    try {
      MetadataStore db = KeyUtils.getDB(containerData, config);
      db.compactDB();
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
  public ContainerLifeCycleState getContainerState() {
    return containerData.getState();
  }

  @Override
  public ContainerType getContainerType() {
    return ContainerType.KeyValueContainer;
  }

  @Override
  public void update(Map<String, String> metadata, boolean forceUpdate)
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
    return new File(containerData.getMetadataPath(), containerData
        .getContainerID() + OzoneConsts.CONTAINER_EXTENSION);
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
