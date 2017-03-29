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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMStorageReport;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces
    .ContainerLocationManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.interfaces.KeyManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.CONTAINER_EXISTS;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.INVALID_CONFIG;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.IO_EXCEPTION;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.UNABLE_TO_READ_METADATA_DB;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.UNSUPPORTED_REQUEST;
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
  private Configuration conf;

  /**
   * Init call that sets up a container Manager.
   *
   * @param config - Configuration.
   * @param containerDirs - List of Metadata Container locations.
   * @throws IOException
   */
  @Override
  public void init(
      Configuration config, List<StorageLocation> containerDirs)
      throws IOException {
    Preconditions.checkNotNull(config, "Config must not be null");
    Preconditions.checkNotNull(containerDirs, "Container directories cannot " +
        "be null");
    Preconditions.checkState(containerDirs.size() > 0, "Number of container" +
        " directories must be greater than zero.");

    this.conf = config;
    readLock();
    try {
      for (StorageLocation path : containerDirs) {
        File directory = Paths.get(path.getNormalizedUri()).toFile();
        // TODO: This will fail if any directory is invalid.
        // We should fix this to handle invalid directories and continue.
        // Leaving it this way to fail fast for time being.
        if (!directory.isDirectory()) {
          LOG.error("Invalid path to container metadata directory. path: {}",
              path.toString());
          throw new StorageContainerException("Invalid path to container " +
              "metadata directory." + path, INVALID_CONFIG);
        }
        File[] files = directory.listFiles(new ContainerFilter());
        if (files != null) {
          for (File containerFile : files) {
            String containerPath =
                ContainerUtils.getContainerNameFromFile(containerFile);
            Preconditions.checkNotNull(containerPath, "Container path cannot" +
                " be null");
            readContainerInfo(containerPath);
          }
        }
      }

      List<StorageLocation> dataDirs = new LinkedList<>();
      for (String dir : config.getStrings(DFS_DATANODE_DATA_DIR_KEY)) {
        StorageLocation location = StorageLocation.parse(dir);
        dataDirs.add(location);
      }
      this.locationManager =
          new ContainerLocationManagerImpl(containerDirs, dataDirs, config);

    } finally {
      readUnlock();
    }
  }

  /**
   * Reads the Container Info from a file and verifies that checksum match. If
   * the checksums match, then that file is added to containerMap.
   *
   * @param containerName - Name which points to the persisted container.
   * @throws StorageContainerException
   */
  private void readContainerInfo(String containerName)
      throws StorageContainerException {
    Preconditions.checkState(containerName.length() > 0,
        "Container name length cannot be zero.");
    FileInputStream containerStream = null;
    DigestInputStream dis = null;
    FileInputStream metaStream = null;
    Path cPath = Paths.get(containerName).getFileName();
    String keyName = null;
    if (cPath != null) {
      keyName = cPath.toString();
    }
    Preconditions.checkNotNull(keyName,
        "Container Name  to container key mapping is null");

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

      if (meta != null &&
          !DigestUtils.sha256Hex(sha.digest()).equals(meta.getHash())) {
        // This means we were not able read data from the disk when booted the
        // datanode. We are going to rely on SCM understanding that we don't
        // have
        // valid data for this container when we send container reports.
        // Hopefully SCM will ask us to delete this container and rebuild it.
        LOG.error("Invalid SHA found for container data. Name :{}" +
            "cowardly refusing to read invalid data", containerName);
        containerMap.put(keyName, new ContainerStatus(null, false));
        return;
      }

      containerMap.put(keyName, new ContainerStatus(containerData, true));

    } catch (IOException | NoSuchAlgorithmException ex) {
      LOG.error("read failed for file: {} ex: {}", containerName,
          ex.getMessage());

      // TODO : Add this file to a recovery Queue.

      // Remember that this container is busted and we cannot use it.
      containerMap.put(keyName, new ContainerStatus(null, false));
      throw new StorageContainerException("Unable to read container info",
          UNABLE_TO_READ_METADATA_DB);
    } finally {
      IOUtils.closeStream(dis);
      IOUtils.closeStream(containerStream);
      IOUtils.closeStream(metaStream);
    }
  }

  /**
   * Creates a container with the given name.
   *
   * @param pipeline -- Nodes which make up this container.
   * @param containerData - Container Name and metadata.
   * @throws StorageContainerException - Exception
   */
  @Override
  public void createContainer(Pipeline pipeline, ContainerData containerData)
      throws StorageContainerException {
    Preconditions.checkNotNull(containerData, "Container data cannot be null");
    writeLock();
    try {
      if (containerMap.containsKey(containerData.getName())) {
        LOG.debug("container already exists. {}", containerData.getName());
        throw new StorageContainerException("container already exists.",
            CONTAINER_EXISTS);
      }

      // This is by design. We first write and close the
      // container Info and metadata to a directory.
      // Then read back and put that info into the containerMap.
      // This allows us to make sure that our write is consistent.

      writeContainerInfo(containerData, false);
      File cFile = new File(containerData.getContainerPath());
      readContainerInfo(ContainerUtils.getContainerNameFromFile(cFile));
    } catch (NoSuchAlgorithmException ex) {
      LOG.error("Internal error: We seem to be running a JVM without a " +
          "needed hash algorithm.");
      throw new StorageContainerException("failed to create container",
          NO_SUCH_ALGORITHM);
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
   * @param overwrite - Whether we are overwriting.
   * @throws StorageContainerException, NoSuchAlgorithmException
   */
  private void writeContainerInfo(ContainerData containerData,
      boolean  overwrite)
      throws StorageContainerException, NoSuchAlgorithmException {

    Preconditions.checkNotNull(this.locationManager,
        "Internal error: location manager cannot be null");

    FileOutputStream containerStream = null;
    DigestOutputStream dos = null;
    FileOutputStream metaStream = null;

    try {
      Path location = locationManager.getContainerPath();
      File containerFile = ContainerUtils.getContainerFile(containerData,
          location);
      File metadataFile = ContainerUtils.getMetadataFile(containerData,
          location);
      if(!overwrite) {
        ContainerUtils.verifyIsNewContainer(containerFile, metadataFile);
      }

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
      // The saving grace is that we check if we have residue files
      // lying around when creating a new container. We need to queue
      // this information to a cleaner thread.

      LOG.error("Creation of container failed. Name: {}, we might need to " +
              "cleanup partially created artifacts. ",
          containerData.getContainerName(), ex);
      throw new StorageContainerException("Container creation failed. ",
          ex, CONTAINER_INTERNAL_ERROR);
    } finally {
      IOUtils.closeStream(dos);
      IOUtils.closeStream(containerStream);
      IOUtils.closeStream(metaStream);
    }
  }

  /**
   * Deletes an existing container.
   *
   * @param pipeline - nodes that make this container.
   * @param containerName - name of the container.
   * @throws StorageContainerException
   */
  @Override
  public void deleteContainer(Pipeline pipeline, String containerName) throws
      StorageContainerException {
    Preconditions.checkNotNull(containerName, "Container name cannot be null");
    Preconditions.checkState(containerName.length() > 0,
        "Container name length cannot be zero.");
    writeLock();
    try {
      ContainerStatus status = containerMap.get(containerName);
      if (status == null) {
        LOG.debug("No such container. Name: {}", containerName);
        throw new StorageContainerException("No such container. Name : " +
            containerName, CONTAINER_NOT_FOUND);
      }
      ContainerUtils.removeContainer(status.containerData, conf);
      containerMap.remove(containerName);
    } catch (StorageContainerException e) {
      throw e;
    } catch (IOException e) {
      // TODO : An I/O error during delete can leave partial artifacts on the
      // disk. We will need the cleaner thread to cleanup this information.
      LOG.error("Failed to cleanup container. Name: {}", containerName, e);
      throw new StorageContainerException(containerName, e, IO_EXCEPTION);
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
   * @param prefix -  Return keys that match this prefix.
   * @param count - how many to return
   * @param prevKey - Previous Key Value or empty String.
   * @param data - Actual containerData
   * @throws StorageContainerException
   */
  @Override
  public void listContainer(String prefix, long count, String prevKey,
      List<ContainerData> data) throws StorageContainerException {
    // TODO : Support list with Prefix and PrevKey
    Preconditions.checkNotNull(data,
        "Internal assertion: data cannot be null");
    readLock();
    try {
      ConcurrentNavigableMap<String, ContainerStatus> map;
      if (prevKey == null || prevKey.isEmpty()) {
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
   * @throws StorageContainerException
   */
  @Override
  public ContainerData readContainer(String containerName) throws
      StorageContainerException {
    Preconditions.checkNotNull(containerName, "Container name cannot be null");
    Preconditions.checkState(containerName.length() > 0,
        "Container name length cannot be zero.");
    if (!containerMap.containsKey(containerName)) {
      throw new StorageContainerException("Unable to find the container. Name: "
          + containerName, CONTAINER_NOT_FOUND);
    }
    return containerMap.get(containerName).getContainer();
  }

  /**
   * Closes a open container, if it is already closed or does not exist a
   * StorageContainerException is thrown.
   *
   * @param containerName - Name of the container.
   * @throws StorageContainerException
   */
  @Override
  public void closeContainer(String containerName)
      throws StorageContainerException, NoSuchAlgorithmException {
    ContainerData containerData = readContainer(containerName);
    containerData.closeContainer();
    writeContainerInfo(containerData, true);

    // Active is different from closed. Closed means it is immutable, active
    // false means we have some internal error that is happening to this
    // container. This is a way to track damaged containers if we have an
    // I/O failure, this allows us to take quick action in case of container
    // issues.

    ContainerStatus status = new ContainerStatus(containerData, true);
    containerMap.put(containerName, status);
  }

  @Override
  public void updateContainer(Pipeline pipeline, String containerName,
      ContainerData data) throws StorageContainerException{
    Preconditions.checkNotNull(pipeline, "Pipeline cannot be null");
    Preconditions.checkNotNull(containerName, "Container name cannot be null");
    Preconditions.checkNotNull(data, "Container data cannot be null");
    FileOutputStream containerStream = null;
    DigestOutputStream dos = null;
    MessageDigest sha = null;
    File containerFileBK = null, containerFile = null;
    boolean deleted = false;

    if(!containerMap.containsKey(containerName)) {
      throw new StorageContainerException("Container doesn't exist. Name :"
          + containerName, CONTAINER_NOT_FOUND);
    }

    try {
      sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    } catch (NoSuchAlgorithmException e) {
      throw new StorageContainerException("Unable to create Message Digest,"
          + " usually this is a java configuration issue.",
          NO_SUCH_ALGORITHM);
    }

    try {
      Path location = locationManager.getContainerPath();
      ContainerData orgData = containerMap.get(containerName).getContainer();
      if (!orgData.isOpen()) {
        throw new StorageContainerException(
            "Update a closed container is not allowed. Name: " + containerName,
            UNSUPPORTED_REQUEST);
      }

      containerFile = ContainerUtils.getContainerFile(orgData, location);
      if (!containerFile.exists() || !containerFile.canWrite()) {
        throw new StorageContainerException(
            "Container file not exists or corrupted. Name: " + containerName,
            CONTAINER_INTERNAL_ERROR);
      }

      // Backup the container file
      containerFileBK = File.createTempFile(
          "tmp_" + System.currentTimeMillis() + "_",
          containerFile.getName(), containerFile.getParentFile());
      FileUtils.copyFile(containerFile, containerFileBK);

      deleted = containerFile.delete();
      containerStream = new FileOutputStream(containerFile);
      dos = new DigestOutputStream(containerStream, sha);

      ContainerProtos.ContainerData protoData = data.getProtoBufMessage();
      protoData.writeDelimitedTo(dos);

      // Update the in-memory map
      ContainerStatus newStatus = new ContainerStatus(data, true);
      containerMap.replace(containerName, newStatus);
    } catch (IOException e) {
      // Restore the container file from backup
      if(containerFileBK != null && containerFileBK.exists() && deleted) {
        if(containerFile.delete()
            && containerFileBK.renameTo(containerFile)) {
          throw new StorageContainerException("Container update failed,"
              + " container data restored from the backup.",
              CONTAINER_INTERNAL_ERROR);
        } else {
          throw new StorageContainerException(
              "Failed to restore container data from the backup. Name: "
                  + containerName, CONTAINER_INTERNAL_ERROR);
        }
      }
    } finally {
      if (containerFileBK != null && containerFileBK.exists()) {
        if(!containerFileBK.delete()) {
          LOG.warn("Unable to delete container file backup : {}.",
              containerFileBK.getAbsolutePath());
        }
      }
      IOUtils.closeStream(dos);
      IOUtils.closeStream(containerStream);
    }
  }

  @VisibleForTesting
  protected File getContainerFile(ContainerData data) throws IOException {
    return ContainerUtils.getContainerFile(data,
        this.locationManager.getContainerPath());
  }

  /**
   * Checks if a container exists.
   *
   * @param containerName - Name of the container.
   * @return true if the container is open false otherwise.
   * @throws StorageContainerException - Throws Exception if we are not able to
   *                                   find the container.
   */
  @Override
  public boolean isOpen(String containerName) throws StorageContainerException {
    ContainerData cData = containerMap.get(containerName).getContainer();
    if (cData == null) {
      throw new StorageContainerException("Container not found",
          CONTAINER_NOT_FOUND);
    }
    return cData.isOpen();
  }

  /**
   * Supports clean shutdown of container.
   *
   * @throws IOException
   */
  @Override
  public void shutdown() throws IOException {
    Preconditions.checkState(this.hasWriteLock(),
        "Assumption that we are holding the lock violated.");
    this.containerMap.clear();
    this.locationManager.shutdown();
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

  public ChunkManager getChunkManager() {
    return this.chunkManager;
  }

  /**
   * Sets the chunk Manager.
   *
   * @param chunkManager - Chunk Manager
   */
  public void setChunkManager(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
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
   * Get the node report.
   * @return node report.
   */
  @Override
  public SCMNodeReport getNodeReport() throws IOException {
    StorageLocationReport[] reports = locationManager.getLocationReport();
    SCMNodeReport.Builder nrb = SCMNodeReport.newBuilder();
    for (int i = 0; i < reports.length; i++) {
      SCMStorageReport.Builder srb = SCMStorageReport.newBuilder();
      nrb.addStorageReport(i, srb.setStorageUuid(reports[i].getId())
          .setCapacity(reports[i].getCapacity())
          .setScmUsed(reports[i].getScmUsed())
          .setRemaining(reports[i].getRemaining())
          .build());
    }
    return nrb.build();
  }

  /**
   * Gets container reports.
   *
   * @return List of all closed containers.
   * @throws IOException
   */
  @Override
  public List<ContainerData> getContainerReports() throws IOException {
    LOG.debug("Starting container report iteration.");
    // No need for locking since containerMap is a ConcurrentSkipListMap
    // And we can never get the exact state since close might happen
    // after we iterate a point.
    return containerMap.entrySet().stream()
        .filter(containerStatus ->
            containerStatus.getValue().getContainer().isOpen())
        .map(containerStatus -> containerStatus.getValue().getContainer())
        .collect(Collectors.toList());
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
   * Filter out only container files from the container metadata dir.
   */
  private static class ContainerFilter implements FilenameFilter {
    /**
     * Tests if a specified file should be included in a file list.
     *
     * @param dir the directory in which the file was found.
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
     * @param active - Active or not active.
     */
    ContainerStatus(ContainerData containerData, boolean active) {
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
     * @return true if it is active.
     */
    public boolean isActive() {
      return active;
    }
  }
}
