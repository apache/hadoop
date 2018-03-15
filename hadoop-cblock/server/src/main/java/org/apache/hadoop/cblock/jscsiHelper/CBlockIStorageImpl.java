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
package org.apache.hadoop.cblock.jscsiHelper;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.cblock.jscsiHelper.cache.CacheModule;
import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import org.apache.hadoop.cblock.jscsiHelper.cache.impl.CBlockLocalCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_TRACE_IO;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_TRACE_IO_DEFAULT;

/**
 * The SCSI Target class for CBlockSCSIServer.
 */
final public class CBlockIStorageImpl implements IStorageModule {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(CBlockIStorageImpl.class);
  private static final Logger TRACER =
      LoggerFactory.getLogger("TraceIO");

  private CacheModule cache;
  private final long volumeSize;
  private final int blockSize;
  private final String userName;
  private final String volumeName;
  private final boolean traceEnabled;
  private final Configuration conf;
  private final ContainerCacheFlusher flusher;
  private List<Pipeline> fullContainerList;

  /**
   * private: constructs a SCSI Target.
   *
   * @param config - config
   * @param userName - Username
   * @param volumeName - Name of the volume
   * @param volumeSize - Size of the volume
   * @param blockSize - Size of the block
   * @param fullContainerList - Ordered list of containers that make up this
   * volume.
   * @param flusher - flusher which is used to flush data from
   *                  level db cache to containers
   * @throws IOException - Throws IOException.
   */
  private CBlockIStorageImpl(Configuration config, String userName,
      String volumeName, long volumeSize, int blockSize,
      List<Pipeline> fullContainerList, ContainerCacheFlusher flusher) {
    this.conf = config;
    this.userName = userName;
    this.volumeName = volumeName;
    this.volumeSize = volumeSize;
    this.blockSize = blockSize;
    this.fullContainerList = new ArrayList<>(fullContainerList);
    this.flusher = flusher;
    this.traceEnabled = conf.getBoolean(DFS_CBLOCK_TRACE_IO,
        DFS_CBLOCK_TRACE_IO_DEFAULT);
  }

  /**
   * private: initialize the cache.
   *
   * @param xceiverClientManager - client manager that is used for creating new
   * connections to containers.
   * @param metrics  - target metrics to maintain metrics for target server
   * @throws IOException - Throws IOException.
   */
  private void initCache(XceiverClientManager xceiverClientManager,
      CBlockTargetMetrics metrics) throws IOException {
    this.cache = CBlockLocalCache.newBuilder()
        .setConfiguration(conf)
        .setVolumeName(this.volumeName)
        .setUserName(this.userName)
        .setPipelines(this.fullContainerList)
        .setClientManager(xceiverClientManager)
        .setBlockSize(blockSize)
        .setVolumeSize(volumeSize)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    this.cache.start();
  }

  /**
   * Gets a new builder for CBlockStorageImpl.
   *
   * @return builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Get Cache.
   *
   * @return - Cache
   */
  public CacheModule getCache() {
    return cache;
  }

  /**
   * Returns block size of this volume.
   *
   * @return int size of block for this volume.
   */
  @Override
  public int getBlockSize() {
    return blockSize;
  }

  /**
   * Checks the index boundary of a block address.
   *
   * @param logicalBlockAddress the index of the first block of data to be read
   * or written
   * @param transferLengthInBlocks the total number of consecutive blocks about
   * to be read or written
   * @return 0 == Success, 1 indicates the LBA address is out of bounds and 2
   * indicates that LBA + transfer size is out of bounds.
   */
  @Override
  public int checkBounds(long logicalBlockAddress, int transferLengthInBlocks) {
    long sizeInBlocks = volumeSize / blockSize;
    int res = 0;
    if (logicalBlockAddress < 0 || logicalBlockAddress >= sizeInBlocks) {
      res = 1;
    }

    if (transferLengthInBlocks < 0 ||
        logicalBlockAddress + transferLengthInBlocks > sizeInBlocks) {
      if (res == 0) {
        res = 2;
      }
    }
    return res;
  }

  /**
   * Number of blocks that make up this volume.
   *
   * @return long - count of blocks.
   */
  @Override
  public long getSizeInBlocks() {
    return volumeSize / blockSize;
  }

  /**
   * Reads the number of bytes that can be read into the bytes buffer from the
   * location indicated.
   *
   * @param bytes the array into which the data will be copied will be filled
   * with data from storage
   * @param storageIndex the position of the first byte to be copied
   * @throws IOException
   */
  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    int startingIdxInBlock = (int) storageIndex % blockSize;
    int idxInBytes = 0;
    if (this.traceEnabled) {
      TRACER.info("Task=ReadStart,length={},location={}",
          bytes.length, storageIndex);
    }
    while (idxInBytes < bytes.length - 1) {
      long blockId = (storageIndex + idxInBytes) / blockSize;
      byte[] dataBytes;

      try {
        LogicalBlock block = this.cache.get(blockId);
        dataBytes = block.getData().array();

        if (this.traceEnabled) {
          TRACER.info("Task=ReadBlock,BlockID={},length={},SHA={}",
              blockId,
              dataBytes.length,
              dataBytes.length > 0 ? DigestUtils.sha256Hex(dataBytes) : null);
        }
      } catch (IOException e) {
        // For an non-existing block cache.get will return a block with zero
        // bytes filled. So any error here is a real error.
        LOGGER.error("getting errors when reading data:" + e);
        throw e;
      }

      int length = blockSize - startingIdxInBlock;
      if (length > bytes.length - idxInBytes) {
        length = bytes.length - idxInBytes;
      }
      if (dataBytes.length >= length) {
        System.arraycopy(dataBytes, startingIdxInBlock, bytes, idxInBytes,
            length);
      }
      startingIdxInBlock = 0;
      idxInBytes += length;
    }
    if (this.traceEnabled) {
      TRACER.info("Task=ReadEnd,length={},location={},SHA={}",
          bytes.length, storageIndex, DigestUtils.sha256Hex(bytes));
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    int startingIdxInBlock = (int) storageIndex % blockSize;
    int idxInBytes = 0;
    if (this.traceEnabled) {
      TRACER.info("Task=WriteStart,length={},location={},SHA={}",
          bytes.length, storageIndex,
          bytes.length > 0 ? DigestUtils.sha256Hex(bytes) : null);
    }

    ByteBuffer dataByte = ByteBuffer.allocate(blockSize);
    while (idxInBytes < bytes.length - 1) {
      long blockId = (storageIndex + idxInBytes) / blockSize;
      int length = blockSize - startingIdxInBlock;
      if (length > bytes.length - idxInBytes) {
        length = bytes.length - idxInBytes;
      }
      System.arraycopy(bytes, idxInBytes, dataByte.array(), startingIdxInBlock,
          length);
      this.cache.put(blockId, dataByte.array());

      if (this.traceEnabled) {
        TRACER.info("Task=WriteBlock,BlockID={},length={},SHA={}",
            blockId, dataByte.array().length,
            dataByte.array().length > 0 ?
                DigestUtils.sha256Hex(dataByte.array()) : null);
      }
      dataByte.clear();
      startingIdxInBlock = 0;
      idxInBytes += length;
    }

    if (this.traceEnabled) {
      TRACER.info("Task=WriteEnd,length={},location={} ",
          bytes.length, storageIndex);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      cache.close();
    } catch (IllegalStateException ise) {
      LOGGER.error("Can not close the storage {}", ise);
      throw ise;
    }
  }

  /**
   * Builder class for CBlocklocalCache.
   */
  public static class Builder {
    private String userName;
    private String volumeName;
    private long volumeSize;
    private int blockSize;
    private List<Pipeline> containerList;
    private Configuration conf;
    private XceiverClientManager clientManager;
    private ContainerCacheFlusher flusher;
    private CBlockTargetMetrics metrics;

    /**
     * Constructs a builder.
     */
    Builder() {

    }

    public Builder setFlusher(ContainerCacheFlusher cacheFlusher) {
      this.flusher = cacheFlusher;
      return this;
    }

    /**
     * set config.
     *
     * @param config - config
     * @return Builder
     */
    public Builder setConf(Configuration config) {
      this.conf = config;
      return this;
    }

    /**
     * set user name.
     *
     * @param cblockUserName - user name
     * @return Builder
     */
    public Builder setUserName(String cblockUserName) {
      this.userName = cblockUserName;
      return this;
    }

    /**
     * set volume name.
     *
     * @param cblockVolumeName -- volume name
     * @return Builder
     */
    public Builder setVolumeName(String cblockVolumeName) {
      this.volumeName = cblockVolumeName;
      return this;
    }

    /**
     * set volume size.
     *
     * @param cblockVolumeSize -- set volume size.
     * @return Builder
     */
    public Builder setVolumeSize(long cblockVolumeSize) {
      this.volumeSize = cblockVolumeSize;
      return this;
    }

    /**
     * set block size.
     *
     * @param cblockBlockSize -- block size
     * @return Builder
     */
    public Builder setBlockSize(int cblockBlockSize) {
      this.blockSize = cblockBlockSize;
      return this;
    }

    /**
     * Set contianer list.
     *
     * @param cblockContainerList - set the pipeline list
     * @return Builder
     */
    public Builder setContainerList(List<Pipeline> cblockContainerList) {
      this.containerList = cblockContainerList;
      return this;
    }

    /**
     * Set client manager.
     *
     * @param xceiverClientManager -- sets the client manager.
     * @return Builder
     */
    public Builder setClientManager(XceiverClientManager xceiverClientManager) {
      this.clientManager = xceiverClientManager;
      return this;
    }

    /**
     * Set Cblock Target Metrics.
     *
     * @param targetMetrics -- sets the cblock target metrics
     * @return Builder
     */
    public Builder setCBlockTargetMetrics(CBlockTargetMetrics targetMetrics) {
      this.metrics = targetMetrics;
      return this;
    }

    /**
     * Builds the CBlockStorageImpl.
     *
     * @return builds the CBlock Scsi Target.
     */
    public CBlockIStorageImpl build() throws IOException {
      if (StringUtils.isBlank(userName)) {
        throw new IllegalArgumentException("User name cannot be null or empty" +
            ".");
      }
      if (StringUtils.isBlank(volumeName)) {
        throw new IllegalArgumentException("Volume name cannot be null or " +
            "empty");
      }

      if (volumeSize < 1) {
        throw new IllegalArgumentException("Volume size cannot be negative or" +
            " zero.");
      }

      if (blockSize < 1) {
        throw new IllegalArgumentException("Block size cannot be negative or " +
            "zero.");
      }

      if (containerList == null || containerList.size() == 0) {
        throw new IllegalArgumentException("Container list cannot be null or " +
            "empty");
      }
      if (clientManager == null) {
        throw new IllegalArgumentException("Client manager cannot be null");
      }
      if (conf == null) {
        throw new IllegalArgumentException("Configuration cannot be null");
      }

      if (flusher == null) {
        throw new IllegalArgumentException("Flusher Cannot be null.");
      }
      CBlockIStorageImpl impl = new CBlockIStorageImpl(this.conf, this.userName,
          this.volumeName, this.volumeSize, this.blockSize, this.containerList,
          this.flusher);
      impl.initCache(this.clientManager, this.metrics);
      return impl;
    }
  }
}
