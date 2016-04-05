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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerLocationManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.interfaces.KeyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_EXTENSION;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_META;

/**
 * A Generic ContainerManagerImpl that will be called from Ozone
 * ContainerManagerImpl. This allows us to support delta changes to ozone
 * version without having to rewrite the containerManager.
 */
public class ContainerManagerImpl implements ContainerManager {
  static final Logger LOG =
      LoggerFactory.getLogger(ContainerManagerImpl.class);

  private final ConcurrentSkipListMap<String, ContainerStatus>
      containerMap = new ConcurrentSkipListMap<>();

  // This lock follows fair locking policy of first-come first-serve
  // for waiting threads.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private ContainerLocationManager locationManager;
  private ChunkManager chunkManager;
  private KeyManager keyManager;

  /**
   * Init call that sets up a container Manager.
   *
   * @param config        - Configuration.
   * @param containerDirs - List of Metadata Container locations.
   * @throws IOException
   */
  @Override
  public void init(Configuration config, List<Path> containerDirs,
                   FsDatasetSpi dataset)
      throws IOException {

    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(containerDirs);
    Preconditions.checkState(containerDirs.size() > 0);

    readLock();
    try {
      for (Path path : containerDirs) {
        File directory = path.toFile();
        if (!directory.isDirectory()) {
          LOG.error("Invalid path to container metadata directory. path: {}",
              path.toString());
          throw new IOException("Invalid path to container metadata directory" +
              ". " + path);
        }
        File[] files = directory.listFiles(new ContainerFilter());
        if (files != null) {
          for (File containerFile : files) {
            String containerPath =
                ContainerUtils.getContainerNameFromFile(containerFile);
            Preconditions.checkNotNull(containerPath);
            readContainerInfo(containerPath);
          }
        }
      }
      this.locationManager = new ContainerLocationManagerImpl(config,
          containerDirs, dataset);

    } finally {
      readUnlock();
    }
  }

  /**
   * Reads the Container Info from a file and verifies that checksum match. If
   * the checksums match, then that file is added to containerMap.
   *
   * @param containerName - Name which points to the persisted container.
   */
  private void readContainerInfo(String containerName)
      throws IOException {
    Preconditions.checkState(containerName.length() > 0);
    FileInputStream containerStream = null;
    DigestInputStream dis = null;
    FileInputStream metaStream = null;
    Path cPath = Paths.get(containerName).getFileName();
    String keyName = null;
    if (cPath != null) {
      keyName = cPath.toString();
    }
    Preconditions.checkNotNull(keyName);

    try {
      String containerFileName = containerName.concat(CONTAINER_EXTENSION);
      String metaFileName = containerName.concat(CONTAINER_META);

      containerStream = new FileInputStream(containerFileName);

      metaStream = new FileInputStream(metaFileName);

      MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);

      dis = new DigestInputStream(containerStream, sha);

      ContainerData containerData = ContainerData.getFromProtBuf(
          ContainerProtos.ContainerData.parseDelimitedFrom(dis));


      ContainerProtos.ContainerMeta meta = ContainerProtos.ContainerMeta
          .parseDelimitedFrom(metaStream);

      if (meta != null && !DigestUtils.sha256Hex(sha.digest()).equals(meta
          .getHash())) {
        throw new IOException("Invalid SHA found for file.");
      }

      containerMap.put(keyName, new ContainerStatus(containerData, true));

    } catch (IOException | NoSuchAlgorithmException ex) {
      LOG.error("read failed for file: {} ex: {}",
          containerName, ex.getMessage());

      // TODO : Add this file to a recovery Queue.

      // Remember that this container is busted and we cannot use it.
      containerMap.put(keyName, new ContainerStatus(null, false));
    } finally {
      IOUtils.closeStream(dis);
      IOUtils.closeStream(containerStream);
      IOUtils.closeStream(metaStream);
    }
  }

  /**
   * Creates a container with the given name.
   *
   * @param pipeline      -- Nodes which make up this container.
   * @param containerData - Container Name and metadata.
   * @throws IOException
   */
  @Override
  public void createContainer(Pipeline pipeline, ContainerData containerData)
      throws IOException {
    Preconditions.checkNotNull(containerData);

    writeLock();
    try {
      if (containerMap.containsKey(containerData.getName())) {
        throw new FileAlreadyExistsException("container already exists.");
      }

      // This is by design. We first write and close the
      // container Info and metadata to a directory.
      // Then read back and put that info into the containerMap.
      // This allows us to make sure that our write is consistent.

      writeContainerInfo(containerData);
      File cFile = new File(containerData.getContainerPath());
      readContainerInfo(ContainerUtils.getContainerNameFromFile(cFile));
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException("failed to create container", ex);

    } finally {
      writeUnlock();
    }

  }

  /**
   * Writes a container to a chosen location and updates the container Map.
   *
   * The file formats of ContainerData and Container Meta is the following.
   *
   * message ContainerData {
   * required string name = 1;
   * repeated KeyValue metadata = 2;
   * optional string dbPath = 3;
   * optional string containerPath = 4;
   * }
   *
   * message ContainerMeta {
   * required string fileName = 1;
   * required string hash = 2;
   * }
   *
   * @param containerData - container Data
   */
  private void writeContainerInfo(ContainerData containerData)
      throws IOException, NoSuchAlgorithmException {

    Preconditions.checkNotNull(this.locationManager);

    FileOutputStream containerStream = null;
    DigestOutputStream dos = null;
    FileOutputStream metaStream = null;
    Path location = locationManager.getContainerPath();

    File containerFile = ContainerUtils.getContainerFile(containerData,
        location);
    File metadataFile = ContainerUtils.getMetadataFile(containerData, location);

    try {
      ContainerUtils.verifyIsNewContainer(containerFile, metadataFile);

      Path metadataPath = this.locationManager.getDataPath(
          containerData.getContainerName());
      metadataPath = ContainerUtils.createMetadata(metadataPath);

      containerStream = new FileOutputStream(containerFile);
      metaStream = new FileOutputStream(metadataFile);
      MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);

      dos = new DigestOutputStream(containerStream, sha);
      containerData.setDBPath(metadataPath.resolve(OzoneConsts.CONTAINER_DB)
          .toString());
      containerData.setContainerPath(containerFile.toString());

      ContainerProtos.ContainerData protoData = containerData
          .getProtoBufMessage();
      protoData.writeDelimitedTo(dos);

      ContainerProtos.ContainerMeta protoMeta = ContainerProtos
          .ContainerMeta.newBuilder()
          .setFileName(containerFile.toString())
          .setHash(DigestUtils.sha256Hex(sha.digest()))
          .build();
      protoMeta.writeDelimitedTo(metaStream);

    } catch (IOException ex) {

      // TODO : we need to clean up partially constructed files
      // The proper way to do would be for a thread
      // to read all these 3 artifacts and make sure they are
      // sane. That info needs to come from the replication
      // pipeline, and if not consistent delete these file.

      // In case of ozone this is *not* a deal breaker since
      // SCM is guaranteed to generate unique container names.

      LOG.error("creation of container failed. Name: {} "
          , containerData.getContainerName());
      throw ex;
    } finally {
      IOUtils.closeStream(dos);
      IOUtils.closeStream(containerStream);
      IOUtils.closeStream(metaStream);
    }
  }



  /**
   * Deletes an existing container.
   *
   * @param pipeline      - nodes that make this container.
   * @param containerName - name of the container.
   * @throws IOException
   */
  @Override
  public void deleteContainer(Pipeline pipeline, String containerName) throws
      IOException {
    Preconditions.checkState(containerName.length() > 0);
    writeLock();
    try {
      ContainerStatus status = containerMap.get(containerName);
      if (status == null) {
        LOG.info("No such container. Name: {}", containerName);
        throw new IOException("No such container. Name : " + containerName);
      }
      ContainerUtils.removeContainer(status.containerData);
      containerMap.remove(containerName);
    } finally {
      writeUnlock();
    }

  }

  /**
   * A simple interface for container Iterations.
   * <p/>
   * This call make no guarantees about consistency of the data between
   * different list calls. It just returns the best known data at that point of
   * time. It is possible that using this iteration you can miss certain
   * container from the listing.
   *
   * @param prevKey - Previous Key Value or empty String.
   * @param count   - how many to return
   * @param data    - Actual containerData
   * @throws IOException
   */
  @Override
  public void listContainer(String prevKey, long count,
                            List<ContainerData> data) throws IOException {
    Preconditions.checkNotNull(data);
    readLock();
    try {
      ConcurrentNavigableMap<String, ContainerStatus> map = null;
      if (prevKey.length() == 0) {
        map = containerMap.tailMap(containerMap.firstKey(), true);
      } else {
        map = containerMap.tailMap(prevKey, false);
      }

      int currentCount = 0;
      for (ContainerStatus entry : map.values()) {
        if (currentCount < count) {
          data.add(entry.getContainer());
          currentCount++;
        } else {
          return;
        }
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * Get metadata about a specific container.
   *
   * @param containerName - Name of the container
   * @return ContainerData - Container Data.
   * @throws IOException
   */
  @Override
  public ContainerData readContainer(String containerName) throws IOException {
    if(!containerMap.containsKey(containerName)) {
      throw new IOException("Unable to find the container. Name: "
          + containerName);
    }
    return containerMap.get(containerName).getContainer();
  }

  /**
   * Supports clean shutdown of container.
   *
   * @throws IOException
   */
  @Override
  public void shutdown() throws IOException {

  }


  @VisibleForTesting
  ConcurrentSkipListMap<String, ContainerStatus> getContainerMap() {
    return containerMap;
  }

  /**
   * Acquire read lock.
   */
  @Override
  public void readLock() {
    this.lock.readLock().lock();

  }

  /**
   * Release read lock.
   */
  @Override
  public void readUnlock() {
    this.lock.readLock().unlock();
  }

  /**
   * Check if the current thread holds read lock.
   */
  @Override
  public boolean hasReadLock() {
    return this.lock.readLock().tryLock();
  }

  /**
   * Acquire write lock.
   */
  @Override
  public void writeLock() {
    this.lock.writeLock().lock();
  }

  /**
   * Acquire write lock, unless interrupted while waiting.
   */
  @Override
  public void writeLockInterruptibly() throws InterruptedException {
    this.lock.writeLock().lockInterruptibly();

  }

  /**
   * Release write lock.
   */
  @Override
  public void writeUnlock() {
    this.lock.writeLock().unlock();

  }

  /**
   * Check if the current thread holds write lock.
   */
  @Override
  public boolean hasWriteLock() {
    return this.lock.writeLock().isHeldByCurrentThread();
  }

  /**
   * Sets the chunk Manager.
   * @param chunkManager
   */
  public void setChunkManager(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
  }

  public ChunkManager getChunkManager() {
    return this.chunkManager;
  }

  /**
   * Sets the Key Manager.
   *
   * @param keyManager - Key Manager.
   */
  @Override
  public void setKeyManager(KeyManager keyManager) {
    this.keyManager = keyManager;
  }

  /**
   * Gets the Key Manager.
   *
   * @return KeyManager.
   */
  @Override
  public KeyManager getKeyManager() {
    return this.keyManager;
  }

  /**
   * Filter out only container files from the container metadata dir.
   */
  private static class ContainerFilter implements FilenameFilter {
    /**
     * Tests if a specified file should be included in a file list.
     *
     * @param dir  the directory in which the file was found.
     * @param name the name of the file.
     * @return <code>true</code> if and only if the name should be included in
     * the file list; <code>false</code> otherwise.
     */
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(CONTAINER_EXTENSION);
    }
  }

  /**
   * This is an immutable class that represents the state of a container. if the
   * container reading encountered an error when we boot up we will post that
   * info to a recovery queue and keep the info in the containerMap.
   * <p/>
   * if and when the issue is fixed, the expectation is that this entry will be
   * deleted by the recovery thread from the containerMap and will insert entry
   * instead of modifying this class.
   */
  @VisibleForTesting
  static class ContainerStatus {
    private final ContainerData containerData;
    private final boolean active;

    /**
     * Creates a Container Status class.
     *
     * @param containerData - ContainerData.
     * @param active        - Active or not active.
     */
    public ContainerStatus(ContainerData containerData, boolean active) {
      this.containerData = containerData;
      this.active = active;
    }

    /**
     * Returns container if it is active. It is not active if we have had an
     * error and we are waiting for the background threads to fix the issue.
     *
     * @return ContainerData.
     */
    public ContainerData getContainer() {
      if (active) {
        return containerData;
      }
      return null;
    }

    /**
     * Indicates if a container is Active.
     *
     * @return
     */
    public boolean isActive() {
      return active;
    }
  }
}
