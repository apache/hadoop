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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerLifeCycleState;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_ALREADY_EXISTS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_METADATA_ERROR;
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
  private long containerMaxSize;
  private Configuration config;

  public KeyValueContainer(KeyValueContainerData containerData, Configuration
      ozoneConfig) {
    Preconditions.checkNotNull(containerData, "KeyValueContainerData cannot " +
        "be null");
    Preconditions.checkNotNull(ozoneConfig, "Ozone configuration cannot " +
        "be null");
    this.config = ozoneConfig;
    this.containerData = containerData;
    this.containerMaxSize = (long) ozoneConfig.getInt(ScmConfigKeys
        .OZONE_SCM_CONTAINER_SIZE_GB, ScmConfigKeys
        .OZONE_SCM_CONTAINER_SIZE_DEFAULT) * 1024L * 1024L * 1024L;
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
    try {
      HddsVolume containerVolume = volumeChoosingPolicy.chooseVolume(volumeSet
          .getVolumesList(), containerMaxSize);
      String containerBasePath = containerVolume.getHddsRootDir().toString();

      long containerId = containerData.getContainerId();
      String containerName = Long.toString(containerId);

      containerMetaDataPath = KeyValueContainerLocationUtil
          .getContainerMetaDataPath(containerBasePath, scmId, containerId);
      File chunksPath = KeyValueContainerLocationUtil.getChunksLocationPath(
          containerBasePath, scmId, containerId);
      File containerFile = KeyValueContainerLocationUtil.getContainerFile(
          containerMetaDataPath, containerName);
      File containerCheckSumFile = KeyValueContainerLocationUtil
          .getContainerCheckSumFile(containerMetaDataPath, containerName);
      File dbFile = KeyValueContainerLocationUtil.getContainerDBFile(
          containerMetaDataPath, containerName);

      // Check if it is new Container.
      KeyValueContainerUtil.verifyIsNewContainer(containerMetaDataPath);

      //Create Metadata path chunks path and metadata db
      KeyValueContainerUtil.createContainerMetaData(containerMetaDataPath,
          chunksPath, dbFile, containerName, config);

      String impl = config.getTrimmed(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL,
          OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_DEFAULT);

      //Set containerData for the KeyValueContainer.
      containerData.setMetadataPath(containerMetaDataPath.getPath());
      containerData.setChunksPath(chunksPath.getPath());
      containerData.setContainerDBType(impl);
      containerData.setDbFile(dbFile);

      // Create .container file and .chksm file
      createContainerFile(containerFile, containerCheckSumFile);


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
   * Creates .container file and checksum file.
   *
   * @param containerFile
   * @param containerCheckSumFile
   * @throws StorageContainerException
   */
  private void createContainerFile(File containerFile, File
      containerCheckSumFile) throws StorageContainerException {
    File tempContainerFile = null;
    File tempCheckSumFile = null;
    FileOutputStream containerCheckSumStream = null;
    Writer writer = null;
    long containerId = containerData.getContainerId();
    try {
      tempContainerFile = createTempFile(containerFile);
      tempCheckSumFile = createTempFile(containerCheckSumFile);
      ContainerDataYaml.createContainerFile(ContainerProtos.ContainerType
              .KeyValueContainer, tempContainerFile, containerData);

      //Compute Checksum for container file
      String checksum = KeyValueContainerUtil.computeCheckSum(containerId,
          tempContainerFile);
      containerCheckSumStream = new FileOutputStream(tempCheckSumFile);
      writer = new OutputStreamWriter(containerCheckSumStream, "UTF-8");
      writer.write(checksum);
      writer.flush();

      NativeIO.renameTo(tempContainerFile, containerFile);
      NativeIO.renameTo(tempCheckSumFile, containerCheckSumFile);

    } catch (IOException ex) {
      throw new StorageContainerException("Error during creation of " +
          "required files(.container, .chksm) for container. Container Name: "
          + containerId, ex, CONTAINER_FILES_CREATE_ERROR);
    } finally {
      IOUtils.closeStream(containerCheckSumStream);
      if (tempContainerFile != null && tempContainerFile.exists()) {
        if (!tempContainerFile.delete()) {
          LOG.warn("Unable to delete container temporary file: {}.",
              tempContainerFile.getAbsolutePath());
        }
      }
      if (tempCheckSumFile != null && tempCheckSumFile.exists()) {
        if (!tempCheckSumFile.delete()) {
          LOG.warn("Unable to delete container temporary checksum file: {}.",
              tempContainerFile.getAbsolutePath());
        }
      }
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException ex) {
        LOG.warn("Error occurred during closing the writer.  Container " +
            "Name:" + containerId);
      }

    }
  }


  private void updateContainerFile(File containerFile, File
      containerCheckSumFile) throws StorageContainerException {

    File containerBkpFile = null;
    File checkSumBkpFile = null;
    long containerId = containerData.getContainerId();

    try {
      if (containerFile.exists() && containerCheckSumFile.exists()) {
        //Take backup of original files (.container and .chksm files)
        containerBkpFile = new File(containerFile + ".bkp");
        checkSumBkpFile = new File(containerCheckSumFile + ".bkp");
        NativeIO.renameTo(containerFile, containerBkpFile);
        NativeIO.renameTo(containerCheckSumFile, checkSumBkpFile);
        createContainerFile(containerFile, containerCheckSumFile);
      } else {
        containerData.setState(ContainerProtos.ContainerLifeCycleState.INVALID);
        throw new StorageContainerException("Container is an Inconsistent " +
            "state, missing required files(.container, .chksm)",
            INVALID_CONTAINER_STATE);
      }
    } catch (StorageContainerException ex) {
      throw ex;
    } catch (IOException ex) {
      // Restore from back up files.
      try {
        if (containerBkpFile != null && containerBkpFile
            .exists() && containerFile.delete()) {
          LOG.info("update failed for container Name: {}, restoring container" +
              " file", containerId);
          NativeIO.renameTo(containerBkpFile, containerFile);
        }
        if (checkSumBkpFile != null && checkSumBkpFile.exists() &&
            containerCheckSumFile.delete()) {
          LOG.info("update failed for container Name: {}, restoring checksum" +
              " file", containerId);
          NativeIO.renameTo(checkSumBkpFile, containerCheckSumFile);
        }
        throw new StorageContainerException("Error during updating of " +
            "required files(.container, .chksm) for container. Container Name: "
            + containerId, ex, CONTAINER_FILES_CREATE_ERROR);
      } catch (IOException e) {
        containerData.setState(ContainerProtos.ContainerLifeCycleState.INVALID);
        LOG.error("During restore failed for container Name: " +
            containerId);
        throw new StorageContainerException(
            "Failed to restore container data from the backup. ID: "
                + containerId, CONTAINER_FILES_CREATE_ERROR);
      }
    } finally {
      if (containerBkpFile != null && containerBkpFile
          .exists()) {
        if(!containerBkpFile.delete()) {
          LOG.warn("Unable to delete container backup file: {}",
              containerBkpFile);
        }
      }
      if (checkSumBkpFile != null && checkSumBkpFile.exists()) {
        if(!checkSumBkpFile.delete()) {
          LOG.warn("Unable to delete container checksum backup file: {}",
              checkSumBkpFile);
        }
      }
    }
  }


  @Override
  public void delete(boolean forceDelete)
      throws StorageContainerException {
    long containerId = containerData.getContainerId();
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
      long containerId = containerData.getContainerId();
      if(!containerData.isValid()) {
        LOG.debug("Invalid container data. Container Id: {}", containerId);
        throw new StorageContainerException("Invalid container data. Name : " +
            containerId, INVALID_CONTAINER_STATE);
      }
      containerData.closeContainer();
      File containerFile = getContainerFile();
      File containerCheckSumFile = getContainerCheckSumFile();

      // update the new container data to .container File
      updateContainerFile(containerFile, containerCheckSumFile);

    } catch (StorageContainerException ex) {
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
  public ContainerProtos.ContainerType getContainerType() {
    return ContainerProtos.ContainerType.KeyValueContainer;
  }

  @Override
  public void update(Map<String, String> metadata, boolean forceUpdate)
      throws StorageContainerException {

    // TODO: Now, when writing the updated data to .container file, we are
    // holding lock and writing data to disk. We can have async implementation
    // to flush the update container data to disk.
    long containerId = containerData.getContainerId();
    if(!containerData.isValid()) {
      LOG.debug("Invalid container data. ID: {}", containerId);
      throw new StorageContainerException("Invalid container data. " +
          "Container Name : " + containerId, INVALID_CONTAINER_STATE);
    }
    if (!forceUpdate && !containerData.isOpen()) {
      throw new StorageContainerException(
          "Updating a closed container is not allowed. ID: " + containerId,
          UNSUPPORTED_REQUEST);
    }
    try {
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        containerData.addMetadata(entry.getKey(), entry.getValue());
      }
    } catch (IOException ex) {
      throw new StorageContainerException("Container Metadata update error" +
          ". Container Name:" + containerId, ex, CONTAINER_METADATA_ERROR);
    }
    try {
      writeLock();
      String containerName = String.valueOf(containerId);
      File containerFile = getContainerFile();
      File containerCheckSumFile = getContainerCheckSumFile();
      // update the new container data to .container File
      updateContainerFile(containerFile, containerCheckSumFile);
    } catch (StorageContainerException  ex) {
      throw ex;
    } finally {
      writeUnlock();
    }
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
  private File getContainerFile() {
    return new File(containerData.getMetadataPath(), containerData
        .getContainerId() + OzoneConsts.CONTAINER_EXTENSION);
  }

  /**
   * Returns container checksum file.
   * @return container checksum file
   */
  private File getContainerCheckSumFile() {
    return new File(containerData.getMetadataPath(), containerData
        .getContainerId() + OzoneConsts.CONTAINER_FILE_CHECKSUM_EXTENSION);
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
