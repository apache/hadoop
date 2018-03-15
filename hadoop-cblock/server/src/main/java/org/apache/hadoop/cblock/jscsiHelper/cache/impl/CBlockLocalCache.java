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
package org.apache.hadoop.cblock.jscsiHelper.cache.impl;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.cblock.jscsiHelper.ContainerCacheFlusher;
import org.apache.hadoop.cblock.jscsiHelper.cache.CacheModule;
import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.cblock.jscsiHelper.CBlockTargetMetrics;
import org.apache.hadoop.utils.LevelDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_DISK_CACHE_PATH_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_DISK_CACHE_PATH_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_TRACE_IO;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_TRACE_IO_DEFAULT;

/**
 * A local cache used by the CBlock ISCSI server. This class is enabled or
 * disabled via config settings.
 */
public class CBlockLocalCache implements CacheModule {
  private static final Logger LOG =
      LoggerFactory.getLogger(CBlockLocalCache.class);
  private static final Logger TRACER =
      LoggerFactory.getLogger("TraceIO");

  private final Configuration conf;
  /**
   * LevelDB cache file.
   */
  private final LevelDBStore cacheDB;

  /**
   * AsyncBlock writer updates the cacheDB and writes the blocks async to
   * remote containers.
   */
  private final AsyncBlockWriter blockWriter;

  /**
   * Sync block reader tries to read from the cache and if we get a cache
   * miss we will fetch the block from remote location. It will asynchronously
   * update the cacheDB.
   */
  private final SyncBlockReader blockReader;
  private final String userName;
  private final String volumeName;

  /**
   * From a block ID we are able to get the pipeline by indexing this array.
   */
  private final Pipeline[] containerList;
  private final int blockSize;
  private XceiverClientManager clientManager;
  /**
   * If this flag is enabled then cache traces all I/O, all reads and writes
   * are visible in the log with sha of the block written. Makes the system
   * slower use it only for debugging or creating trace simulations.
   */
  private final boolean traceEnabled;
  private final boolean enableShortCircuitIO;
  private final long volumeSize;
  private long currentCacheSize;
  private File dbPath;
  private final ContainerCacheFlusher flusher;
  private CBlockTargetMetrics cblockTargetMetrics;

  /**
   * Get Db Path.
   * @return the file instance of the db.
   */
  public File getDbPath() {
    return dbPath;
  }

  /**
   * Constructor for CBlockLocalCache invoked via the builder.
   *
   * @param conf -  Configuration
   * @param volumeName - volume Name
   * @param userName - user name
   * @param containerPipelines - Pipelines that make up this contianer
   * @param blockSize - blockSize
   * @param flusher - flusher to flush data to container
   * @throws IOException
   */
  CBlockLocalCache(
      Configuration conf, String volumeName,
      String userName, List<Pipeline> containerPipelines, int blockSize,
      long volumeSize, ContainerCacheFlusher flusher) throws IOException {
    this.conf = conf;
    this.userName = userName;
    this.volumeName = volumeName;
    this.blockSize = blockSize;
    this.flusher = flusher;
    this.traceEnabled = conf.getBoolean(DFS_CBLOCK_TRACE_IO,
        DFS_CBLOCK_TRACE_IO_DEFAULT);
    this.enableShortCircuitIO = conf.getBoolean(
        DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO,
        DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO_DEFAULT);
    dbPath = Paths.get(conf.get(DFS_CBLOCK_DISK_CACHE_PATH_KEY,
        DFS_CBLOCK_DISK_CACHE_PATH_DEFAULT), userName, volumeName).toFile();

    if (!dbPath.exists() && !dbPath.mkdirs()) {
      LOG.error("Unable to create the cache paths. Path: {}", dbPath);
      throw new IllegalArgumentException("Unable to create paths. Path: " +
          dbPath);
    }
    cacheDB = flusher.getCacheDB(dbPath.toString());
    this.containerList = containerPipelines.toArray(new
        Pipeline[containerPipelines.size()]);
    this.volumeSize = volumeSize;

    blockWriter = new AsyncBlockWriter(conf, this);
    blockReader = new SyncBlockReader(conf, this);
    if (this.traceEnabled) {
      getTracer().info("Task=StartingCache");
    }
  }

  private void setClientManager(XceiverClientManager manager) {
    this.clientManager = manager;
  }

  private void setCblockTargetMetrics(CBlockTargetMetrics targetMetrics) {
    this.cblockTargetMetrics = targetMetrics;
  }

  /**
   * Returns new builder class that builds a CBlockLocalCache.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public void processDirtyMessage(String fileName) {
    flusher.processDirtyBlocks(dbPath.toString(), fileName);
  }

  /**
   * Get usable disk space.
   *
   * @param dbPathString - Path to db
   * @return long bytes remaining.
   */
  private static long getRemainingDiskSpace(String dbPathString) {
    try {
      URI fileUri = new URI("file:///");
      Path dbPath = Paths.get(fileUri).resolve(dbPathString);
      FileStore disk = Files.getFileStore(dbPath);
      return disk.getUsableSpace();
    } catch (URISyntaxException | IOException ex) {
      LOG.error("Unable to get free space on for path :" + dbPathString);
    }
    return 0L;
  }

  /**
   * Returns the Max current CacheSize.
   *
   * @return - Cache Size
   */
  public long getCurrentCacheSize() {
    return currentCacheSize;
  }

  /**
   * Sets the Maximum Cache Size.
   *
   * @param currentCacheSize - Max current Cache Size.
   */
  public void setCurrentCacheSize(long currentCacheSize) {
    this.currentCacheSize = currentCacheSize;
  }

  /**
   * True if block tracing is enabled.
   *
   * @return - bool
   */
  public boolean isTraceEnabled() {
    return traceEnabled;
  }

  /**
   * Checks if Short Circuit I/O is enabled.
   *
   * @return - true if it is enabled.
   */
  public boolean isShortCircuitIOEnabled() {
    return enableShortCircuitIO;
  }

  /**
   * Returns the default block size of this device.
   *
   * @return - int
   */
  public int getBlockSize() {
    return blockSize;
  }

  /**
   * Gets the client manager.
   *
   * @return XceiverClientManager
   */
  public XceiverClientManager getClientManager() {
    return clientManager;
  }

  /**
   * check if the key is cached, if yes, returned the cached object.
   * otherwise, load from data source. Then put it into cache.
   *
   * @param blockID
   * @return the block associated to the blockID
   */
  @Override
  public LogicalBlock get(long blockID) throws IOException {
    cblockTargetMetrics.incNumReadOps();
    return blockReader.readBlock(blockID);
  }

  /**
   * put the value of the key into cache and remote container.
   *
   * @param blockID - BlockID
   * @param data - byte[]
   */
  @Override
  public void put(long blockID, byte[] data) throws IOException {
    cblockTargetMetrics.incNumWriteOps();
    LogicalBlock block = new DiskBlock(blockID, data, false);
    blockWriter.writeBlock(block);
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public void start() throws IOException {
    flusher.register(getDbPath().getPath(), containerList);
    blockWriter.start();
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public void close() throws IOException {
    blockReader.shutdown();
    blockWriter.shutdown();
    this.flusher.releaseCacheDB(dbPath.toString());
    if (this.traceEnabled) {
      getTracer().info("Task=ShutdownCache");
    }
  }

  /**
   * Returns true if cache still has blocks pending to write.
   *
   * @return false if we have no pending blocks to write.
   */
  @Override
  public boolean isDirtyCache() {
    return false;
  }

  /**
   * Returns the local cache DB.
   *
   * @return - DB
   */
  LevelDBStore getCacheDB() {
    return this.cacheDB;
  }

  /**
   * Returns the current userName.
   *
   * @return - UserName
   */
  String getUserName() {
    return this.userName;
  }

  /**
   * Returns the volume name.
   *
   * @return VolumeName.
   */
  String getVolumeName() {
    return this.volumeName;
  }

  /**
   * Returns the target metrics.
   *
   * @return CBlock Target Metrics.
   */
  CBlockTargetMetrics getTargetMetrics() {
    return this.cblockTargetMetrics;
  }

  /**
   * Returns the pipeline to use given a container.
   *
   * @param blockId - blockID
   * @return - pipeline.
   */
  Pipeline getPipeline(long blockId) {
    int containerIdx = (int) blockId % containerList.length;
    long cBlockIndex =
        Longs.fromByteArray(containerList[containerIdx].getData());
    if (cBlockIndex > 0) {
      // This catches the case when we get a wrong container in the ordering
      // of the containers.
      Preconditions.checkState(containerIdx % cBlockIndex == 0,
          "The container ID computed should match with the container index " +
              "returned from cBlock Server.");
    }
    return containerList[containerIdx];
  }

  String getTraceID(long blockID) {
    return flusher.getTraceID(dbPath, blockID);
  }

  /**
   * Returns tracer.
   *
   * @return - Logger
   */
  Logger getTracer() {
    return TRACER;
  }

  /**
   * Builder class for CBlocklocalCache.
   */
  public static class Builder {
    private Configuration configuration;
    private String userName;
    private String volumeName;
    private List<Pipeline> pipelines;
    private XceiverClientManager clientManager;
    private int blockSize;
    private long volumeSize;
    private ContainerCacheFlusher flusher;
    private CBlockTargetMetrics metrics;

    /**
     * Ctor.
     */
    Builder() {
    }

    /**
     * Computes a cache size based on the configuration and available disk
     * space.
     *
     * @param configuration - Config
     * @param volumeSize - Size of Volume
     * @param blockSize - Size of the block
     * @return - cache size in bytes.
     */
    private static long computeCacheSize(Configuration configuration,
        long volumeSize, int blockSize) {
      long cacheSize = 0;
      String dbPath = configuration.get(DFS_CBLOCK_DISK_CACHE_PATH_KEY,
          DFS_CBLOCK_DISK_CACHE_PATH_DEFAULT);
      if (StringUtils.isBlank(dbPath)) {
        return cacheSize;
      }
      long spaceRemaining = getRemainingDiskSpace(dbPath);
      double cacheRatio = 1.0;

      if (spaceRemaining < volumeSize) {
        cacheRatio = (double)spaceRemaining / volumeSize;
      }

      // if cache is going to be at least 10% of the volume size it is worth
      // doing, otherwise skip creating the  cache.
      if (cacheRatio >= 0.10) {
        cacheSize = Double.doubleToLongBits(volumeSize * cacheRatio);
      }
      return cacheSize;
    }

    /**
     * Sets the Config to be used by this cache.
     *
     * @param conf - Config
     * @return Builder
     */
    public Builder setConfiguration(Configuration conf) {
      this.configuration = conf;
      return this;
    }

    /**
     * Sets the user name who is the owner of this volume.
     *
     * @param user - name of the owner, please note this is not the current
     * user name.
     * @return - Builder
     */
    public Builder setUserName(String user) {
      this.userName = user;
      return this;
    }

    /**
     * Sets the VolumeName.
     *
     * @param volume - Name of the volume
     * @return Builder
     */
    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    /**
     * Sets the Pipelines that form this volume.
     *
     * @param pipelineList - list of pipelines
     * @return Builder
     */
    public Builder setPipelines(List<Pipeline> pipelineList) {
      this.pipelines = pipelineList;
      return this;
    }

    /**
     * Sets the Client Manager that manages the communication with containers.
     *
     * @param xceiverClientManager - clientManager.
     * @return - Builder
     */
    public Builder setClientManager(XceiverClientManager xceiverClientManager) {
      this.clientManager = xceiverClientManager;
      return this;
    }

    /**
     * Sets the block size -- Typical sizes are 4KB, 8KB etc.
     *
     * @param size - BlockSize.
     * @return - Builder
     */
    public Builder setBlockSize(int size) {
      this.blockSize = size;
      return this;
    }

    /**
     * Sets the volumeSize.
     *
     * @param size - VolumeSize
     * @return - Builder
     */
    public Builder setVolumeSize(long size) {
      this.volumeSize = size;
      return this;
    }

    /**
     * Set flusher.
     * @param containerCacheFlusher - cache Flusher
     * @return Builder.
     */
    public Builder setFlusher(ContainerCacheFlusher containerCacheFlusher) {
      this.flusher = containerCacheFlusher;
      return this;
    }

    /**
     * Sets the cblock Metrics.
     *
     * @param targetMetrics - CBlock Target Metrics
     * @return - Builder
     */
    public Builder setCBlockTargetMetrics(CBlockTargetMetrics targetMetrics) {
      this.metrics = targetMetrics;
      return this;
    }

    /**
     * Constructs a CBlockLocalCache.
     *
     * @return the CBlockLocalCache with the preset properties.
     * @throws IOException
     */
    public CBlockLocalCache build() throws IOException {
      Preconditions.checkNotNull(this.configuration, "A valid configuration " +
          "is needed");
      Preconditions.checkState(StringUtils.isNotBlank(userName), "A valid " +
          "username is needed");
      Preconditions.checkState(StringUtils.isNotBlank(volumeName), " A valid" +
          " volume name is needed");
      Preconditions.checkNotNull(this.pipelines, "Pipelines cannot be null");
      Preconditions.checkState(this.pipelines.size() > 0, "At least one " +
          "pipeline location is needed for a volume");

      for (int x = 0; x < pipelines.size(); x++) {
        Preconditions.checkNotNull(pipelines.get(x).getData(), "cBlock " +
            "relies on private data on the pipeline, null data found.");
      }

      Preconditions.checkNotNull(clientManager, "Client Manager cannot be " +
          "null");
      Preconditions.checkState(blockSize > 0, " Block size has to be a " +
          "number greater than 0");

      Preconditions.checkState(volumeSize > 0, "Volume Size cannot be less " +
          "than 1");
      Preconditions.checkNotNull(this.flusher, "Flusher cannot be null.");

      CBlockLocalCache cache = new CBlockLocalCache(this.configuration,
          this.volumeName, this.userName, this.pipelines, blockSize,
          volumeSize, flusher);
      cache.setCblockTargetMetrics(this.metrics);
      cache.setClientManager(this.clientManager);

      // TODO : Support user configurable maximum size.
      long cacheSize = computeCacheSize(this.configuration, this.volumeSize,
          this.blockSize);
      cache.setCurrentCacheSize(cacheSize);
      return cache;
    }
  }
}
