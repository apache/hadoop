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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;

/**
 * This class is for performing chunk related operations.
 */
public class ChunkManagerImpl implements ChunkManager {
  static final Logger LOG = LoggerFactory.getLogger(ChunkManagerImpl.class);
  private final boolean doSyncWrite;

  public ChunkManagerImpl(boolean sync) {
    doSyncWrite = sync;
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
  public void writeChunk(Container container, BlockID blockID, ChunkInfo info,
      ByteBuffer data, DispatcherContext dispatcherContext)
      throws StorageContainerException {
    Preconditions.checkNotNull(dispatcherContext);
    DispatcherContext.WriteChunkStage stage = dispatcherContext.getStage();
    try {

      KeyValueContainerData containerData = (KeyValueContainerData) container
          .getContainerData();
      HddsVolume volume = containerData.getVolume();
      VolumeIOStats volumeIOStats = volume.getVolumeIOStats();

      File chunkFile = ChunkUtils.getChunkFile(containerData, info);

      boolean isOverwrite = ChunkUtils.validateChunkForOverwrite(
          chunkFile, info);
      File tmpChunkFile = getTmpChunkFile(chunkFile, dispatcherContext);

      LOG.debug(
          "writing chunk:{} chunk stage:{} chunk file:{} tmp chunk file:{}",
          info.getChunkName(), stage, chunkFile, tmpChunkFile);

      switch (stage) {
      case WRITE_DATA:
        if (isOverwrite) {
          // if the actual chunk file already exists here while writing the temp
          // chunk file, then it means the same ozone client request has
          // generated two raft log entries. This can happen either because
          // retryCache expired in Ratis (or log index mismatch/corruption in
          // Ratis). This can be solved by two approaches as of now:
          // 1. Read the complete data in the actual chunk file ,
          //    verify the data integrity and in case it mismatches , either
          // 2. Delete the chunk File and write the chunk again. For now,
          //    let's rewrite the chunk file
          // TODO: once the checksum support for write chunks gets plugged in,
          // the checksum needs to be verified for the actual chunk file and
          // the data to be written here which should be efficient and
          // it matches we can safely return without rewriting.
          LOG.warn("ChunkFile already exists" + chunkFile + ".Deleting it.");
          FileUtil.fullyDelete(chunkFile);
        }
        if (tmpChunkFile.exists()) {
          // If the tmp chunk file already exists it means the raft log got
          // appended, but later on the log entry got truncated in Ratis leaving
          // behind garbage.
          // TODO: once the checksum support for data chunks gets plugged in,
          // instead of rewriting the chunk here, let's compare the checkSums
          LOG.warn(
              "tmpChunkFile already exists" + tmpChunkFile + "Overwriting it.");
        }
        // Initially writes to temporary chunk file.
        ChunkUtils
            .writeData(tmpChunkFile, info, data, volumeIOStats, doSyncWrite);
        // No need to increment container stats here, as still data is not
        // committed here.
        break;
      case COMMIT_DATA:
        // commit the data, means move chunk data from temporary chunk file
        // to actual chunk file.
        if (isOverwrite) {
          // if the actual chunk file already exists , it implies the write
          // chunk transaction in the containerStateMachine is getting
          // reapplied. This can happen when a node restarts.
          // TODO: verify the checkSums for the existing chunkFile and the
          // chunkInfo to be committed here
          LOG.warn("ChunkFile already exists" + chunkFile);
          return;
        }
        // While committing a chunk , just rename the tmp chunk file which has
        // the same term and log index appended as the current transaction
        commitChunk(tmpChunkFile, chunkFile);
        // Increment container stats here, as we commit the data.
        containerData.incrBytesUsed(info.getLen());
        containerData.incrWriteCount();
        containerData.incrWriteBytes(info.getLen());
        break;
      case COMBINED:
        // directly write to the chunk file
        ChunkUtils.writeData(chunkFile, info, data, volumeIOStats, doSyncWrite);
        if (!isOverwrite) {
          containerData.incrBytesUsed(info.getLen());
        }
        containerData.incrWriteCount();
        containerData.incrWriteBytes(info.getLen());
        break;
      default:
        throw new IOException("Can not identify write operation.");
      }
    } catch (StorageContainerException ex) {
      throw ex;
    } catch (NoSuchAlgorithmException ex) {
      LOG.error("write data failed. error: {}", ex);
      throw new StorageContainerException("Internal error: ", ex,
          NO_SUCH_ALGORITHM);
    } catch (ExecutionException  | IOException ex) {
      LOG.error("write data failed. error: {}", ex);
      throw new StorageContainerException("Internal error: ", ex,
          CONTAINER_INTERNAL_ERROR);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("write data failed. error: {}", e);
      throw new StorageContainerException("Internal error: ", e,
          CONTAINER_INTERNAL_ERROR);
    }
  }

  /**
   * reads the data defined by a chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block.
   * @param info - ChunkInfo.
   * @param dispatcherContext dispatcher context info.
   * @return byte array
   * @throws StorageContainerException
   * TODO: Right now we do not support partial reads and writes of chunks.
   * TODO: Explore if we need to do that for ozone.
   */
  public byte[] readChunk(Container container, BlockID blockID, ChunkInfo info,
      DispatcherContext dispatcherContext) throws StorageContainerException {
    try {
      KeyValueContainerData containerData = (KeyValueContainerData) container
          .getContainerData();
      ByteBuffer data;
      HddsVolume volume = containerData.getVolume();
      VolumeIOStats volumeIOStats = volume.getVolumeIOStats();

      // Checking here, which layout version the container is, and reading
      // the chunk file in that format.
      // In version1, we verify checksum if it is available and return data
      // of the chunk file.
      if (containerData.getLayOutVersion() == ChunkLayOutVersion
          .getLatestVersion().getVersion()) {
        File chunkFile = ChunkUtils.getChunkFile(containerData, info);

        // In case the chunk file does not exist but tmp chunk file exist,
        // read from tmp chunk file if readFromTmpFile is set to true
        if (!chunkFile.exists() && dispatcherContext.isReadFromTmpFile()) {
          chunkFile = getTmpChunkFile(chunkFile, dispatcherContext);
        }
        data = ChunkUtils.readData(chunkFile, info, volumeIOStats);
        containerData.incrReadCount();
        long length = chunkFile.length();
        containerData.incrReadBytes(length);
        return data.array();
      }
    } catch (ExecutionException ex) {
      LOG.error("read data failed. error: {}", ex);
      throw new StorageContainerException("Internal error: ",
          ex, CONTAINER_INTERNAL_ERROR);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("read data failed. error: {}", e);
      throw new StorageContainerException("Internal error: ",
          e, CONTAINER_INTERNAL_ERROR);
    }
    return null;
  }

  /**
   * Deletes a given chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block
   * @param info - Chunk Info
   * @throws StorageContainerException
   */
  public void deleteChunk(Container container, BlockID blockID, ChunkInfo info)
      throws StorageContainerException {
    Preconditions.checkNotNull(blockID, "Block ID cannot be null.");
    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    // Checking here, which layout version the container is, and performing
    // deleting chunk operation.
    // In version1, we have only chunk file.
    if (containerData.getLayOutVersion() == ChunkLayOutVersion
        .getLatestVersion().getVersion()) {
      File chunkFile = ChunkUtils.getChunkFile(containerData, info);

      // if the chunk file does not exist, it might have already been deleted.
      // The call might be because of reapply of transactions on datanode
      // restart.
      if (!chunkFile.exists()) {
        LOG.warn("Chunk file doe not exist. chunk info :" + info.toString());
        return;
      }
      if ((info.getOffset() == 0) && (info.getLen() == chunkFile.length())) {
        FileUtil.fullyDelete(chunkFile);
        containerData.decrBytesUsed(chunkFile.length());
      } else {
        LOG.error("Not Supported Operation. Trying to delete a " +
            "chunk that is in shared file. chunk info : " + info.toString());
        throw new StorageContainerException("Not Supported Operation. " +
            "Trying to delete a chunk that is in shared file. chunk info : "
            + info.toString(), UNSUPPORTED_REQUEST);
      }
    }
  }

  /**
   * Shutdown the chunkManager.
   *
   * In the chunkManager we haven't acquired any resources, so nothing to do
   * here.
   */

  public void shutdown() {
    //TODO: need to revisit this during integration of container IO.
  }

  /**
   * Returns the temporary chunkFile path.
   * @param chunkFile chunkFileName
   * @param dispatcherContext dispatcher context info
   * @return temporary chunkFile path
   * @throws StorageContainerException
   */
  private File getTmpChunkFile(File chunkFile,
      DispatcherContext dispatcherContext)  {
    return new File(chunkFile.getParent(),
        chunkFile.getName() +
            OzoneConsts.CONTAINER_CHUNK_NAME_DELIMITER +
            OzoneConsts.CONTAINER_TEMPORARY_CHUNK_PREFIX +
            OzoneConsts.CONTAINER_CHUNK_NAME_DELIMITER +
            dispatcherContext.getTerm() +
            OzoneConsts.CONTAINER_CHUNK_NAME_DELIMITER +
            dispatcherContext.getLogIndex());
  }

  /**
   * Commit the chunk by renaming the temporary chunk file to chunk file.
   * @param tmpChunkFile
   * @param chunkFile
   * @throws IOException
   */
  private void commitChunk(File tmpChunkFile, File chunkFile) throws
      IOException {
    Files.move(tmpChunkFile.toPath(), chunkFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
  }

}
