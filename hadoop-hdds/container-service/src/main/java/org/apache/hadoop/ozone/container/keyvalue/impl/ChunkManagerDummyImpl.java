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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.*;

/**
 * Implementation of ChunkManager built for running performance tests.
 * Chunks are not written to disk, Reads are returned with zero-filled buffers
 */
public class ChunkManagerDummyImpl extends ChunkManagerImpl {
  static final Logger LOG = LoggerFactory.getLogger(
      ChunkManagerDummyImpl.class);

  public ChunkManagerDummyImpl(boolean sync) {
    super(sync);
  }

  /**
   * writes a given chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block
   * @param info - ChunkInfo
   * @param data - data of the chunk
   * @param dispatcherContext - dispatcherContextInfo
   * @throws StorageContainerException
   */
  @Override
  public void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ByteBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException {
    long writeTimeStart = Time.monotonicNow();

    Preconditions.checkNotNull(dispatcherContext);
    DispatcherContext.WriteChunkStage stage = dispatcherContext.getStage();

    Logger log = LoggerFactory.getLogger(ChunkManagerImpl.class);

    try {
      KeyValueContainerData containerData =
          (KeyValueContainerData) container.getContainerData();
      HddsVolume volume = containerData.getVolume();
      VolumeIOStats volumeIOStats = volume.getVolumeIOStats();
      int bufferSize;

      switch (stage) {
      case WRITE_DATA:
        bufferSize = data.capacity();
        if (bufferSize != info.getLen()) {
          String err = String.format("data array does not match the length "
                  + "specified. DataLen: %d Byte Array: %d",
              info.getLen(), bufferSize);
          log.error(err);
          throw new StorageContainerException(err, INVALID_WRITE_SIZE);
        }

        // Increment volumeIO stats here.
        volumeIOStats.incWriteTime(Time.monotonicNow() - writeTimeStart);
        volumeIOStats.incWriteOpCount();
        volumeIOStats.incWriteBytes(info.getLen());
        break;
      case COMMIT_DATA:
        updateContainerWriteStats(container, info, false);
        break;
      case COMBINED:
        updateContainerWriteStats(container, info, false);
        break;
      default:
        throw new IOException("Can not identify write operation.");
      }
    } catch (IOException ex) {
      LOG.error("write data failed. error: {}", ex);
      throw new StorageContainerException("Internal error: ", ex,
          CONTAINER_INTERNAL_ERROR);
    }
  }

  /**
   * return a zero-filled buffer.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block.
   * @param info - ChunkInfo.
   * @param dispatcherContext dispatcher context info.
   * @return byte array
   * TODO: Right now we do not support partial reads and writes of chunks.
   * TODO: Explore if we need to do that for ozone.
   */
  @Override
  public ByteBuffer readChunk(Container container, BlockID blockID,
      ChunkInfo info, DispatcherContext dispatcherContext) {

    long readStartTime = Time.monotonicNow();

    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    ByteBuffer data;
    HddsVolume volume = containerData.getVolume();
    VolumeIOStats volumeIOStats = volume.getVolumeIOStats();

    data = ByteBuffer.allocate((int) info.getLen());

    // Increment volumeIO stats here.
    volumeIOStats.incReadTime(Time.monotonicNow() - readStartTime);
    volumeIOStats.incReadOpCount();
    volumeIOStats.incReadBytes(info.getLen());

    return data;
  }

  /**
   * Delete a given chunk - Do nothing except stats.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block
   * @param info - Chunk Info
   */
  @Override
  public void deleteChunk(Container container, BlockID blockID,
      ChunkInfo info) {
    Preconditions.checkNotNull(blockID, "Block ID cannot be null.");
    KeyValueContainerData containerData =
        (KeyValueContainerData) container.getContainerData();

    if (info.getOffset() == 0) {
      containerData.decrBytesUsed(info.getLen());
    }
  }
}