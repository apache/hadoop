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

package org.apache.hadoop.ozone.container.keyvalue.helpers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.*;

/**
 * Utility methods for chunk operations for KeyValue container.
 */
public final class ChunkUtils {

  private static final Set<Path> LOCKS = ConcurrentHashMap.newKeySet();

  /** Never constructed. **/
  private ChunkUtils() {

  }

  /**
   * Writes the data in chunk Info to the specified location in the chunkfile.
   *
   * @param chunkFile - File to write data to.
   * @param chunkInfo - Data stream to write.
   * @param data - The data buffer.
   * @param volumeIOStats statistics collector
   * @param sync whether to do fsync or not
   */
  public static void writeData(File chunkFile, ChunkInfo chunkInfo,
      ByteBuffer data, VolumeIOStats volumeIOStats, boolean sync)
      throws StorageContainerException, ExecutionException,
      InterruptedException, NoSuchAlgorithmException {
    final int bufferSize = data.remaining();
    Logger log = LoggerFactory.getLogger(ChunkManagerImpl.class);
    if (bufferSize != chunkInfo.getLen()) {
      String err = String.format("data array does not match the length " +
              "specified. DataLen: %d Byte Array: %d",
          chunkInfo.getLen(), bufferSize);
      log.error(err);
      throw new StorageContainerException(err, INVALID_WRITE_SIZE);
    }

    Path path = chunkFile.toPath();
    long startTime = Time.monotonicNow();
    processFileExclusively(path, () -> {
      FileChannel file = null;
      try {
        // skip SYNC and DSYNC to reduce contention on file.lock
        file = FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.SPARSE);

        int size;
        try (FileLock ignored = file.lock()) {
          size = file.write(data, chunkInfo.getOffset());
        }

        // Increment volumeIO stats here.
        volumeIOStats.incWriteTime(Time.monotonicNow() - startTime);
        volumeIOStats.incWriteOpCount();
        volumeIOStats.incWriteBytes(size);
        if (size != bufferSize) {
          log.error("Invalid write size found. Size:{}  Expected: {} ", size,
              bufferSize);
          throw new StorageContainerException("Invalid write size found. " +
              "Size: " + size + " Expected: " + bufferSize, INVALID_WRITE_SIZE);
        }
      } catch (StorageContainerException ex) {
        throw ex;
      } catch (IOException e) {
        throw new StorageContainerException(e, IO_EXCEPTION);
      } finally {
        closeFile(file, sync);
      }

      return null;
    });

    if (log.isDebugEnabled()) {
      log.debug("Write Chunk completed for chunkFile: {}, size {}", chunkFile,
          bufferSize);
    }
  }

  /**
   * Reads data from an existing chunk file.
   *
   * @param chunkFile - file where data lives.
   * @param data - chunk definition.
   * @param volumeIOStats statistics collector
   * @return ByteBuffer
   */
  public static ByteBuffer readData(File chunkFile, ChunkInfo data,
      VolumeIOStats volumeIOStats) throws StorageContainerException {
    Logger log = LoggerFactory.getLogger(ChunkManagerImpl.class);

    if (!chunkFile.exists()) {
      log.error("Unable to find the chunk file. chunk info : {}",
          data.toString());
      throw new StorageContainerException("Unable to find the chunk file. " +
          "chunk info " +
          data.toString(), UNABLE_TO_FIND_CHUNK);
    }

    long offset = data.getOffset();
    long len = data.getLen();
    ByteBuffer buf = ByteBuffer.allocate((int) len);

    Path path = chunkFile.toPath();
    long startTime = Time.monotonicNow();
    return processFileExclusively(path, () -> {
      FileChannel file = null;

      try {
        file = FileChannel.open(path, StandardOpenOption.READ);

        try (FileLock ignored = file.lock(offset, len, true)) {
          file.read(buf, offset);
          buf.flip();
        }

        // Increment volumeIO stats here.
        volumeIOStats.incReadTime(Time.monotonicNow() - startTime);
        volumeIOStats.incReadOpCount();
        volumeIOStats.incReadBytes(len);

        return buf;
      } catch (IOException e) {
        throw new StorageContainerException(e, IO_EXCEPTION);
      } finally {
        if (file != null) {
          IOUtils.closeStream(file);
        }
      }
    });
  }

  /**
   * Validates chunk data and returns a file object to Chunk File that we are
   * expected to write data to.
   *
   * @param chunkFile - chunkFile to write data into.
   * @param info - chunk info.
   * @return true if the chunkFile exists and chunkOffset &lt; chunkFile length,
   *         false otherwise.
   */
  public static boolean validateChunkForOverwrite(File chunkFile,
      ChunkInfo info) {

    Logger log = LoggerFactory.getLogger(ChunkManagerImpl.class);

    if (isOverWriteRequested(chunkFile, info)) {
      if (!isOverWritePermitted(info)) {
        log.warn("Duplicate write chunk request. Chunk overwrite " +
            "without explicit request. {}", info.toString());
      }
      return true;
    }
    return false;
  }

  /**
   * Validates that Path to chunk file exists.
   *
   * @param containerData - Container Data
   * @param info - Chunk info
   * @return - File.
   * @throws StorageContainerException
   */
  public static File getChunkFile(KeyValueContainerData containerData,
                                  ChunkInfo info) throws
      StorageContainerException {

    Preconditions.checkNotNull(containerData, "Container data can't be null");
    Logger log = LoggerFactory.getLogger(ChunkManagerImpl.class);

    String chunksPath = containerData.getChunksPath();
    if (chunksPath == null) {
      log.error("Chunks path is null in the container data");
      throw new StorageContainerException("Unable to get Chunks directory.",
          UNABLE_TO_FIND_DATA_DIR);
    }
    File chunksLoc = new File(chunksPath);
    if (!chunksLoc.exists()) {
      log.error("Chunks path does not exist");
      throw new StorageContainerException("Unable to get Chunks directory.",
          UNABLE_TO_FIND_DATA_DIR);
    }

    return chunksLoc.toPath().resolve(info.getChunkName()).toFile();
  }

  /**
   * Checks if we are getting a request to overwrite an existing range of
   * chunk.
   *
   * @param chunkFile - File
   * @param chunkInfo - Buffer to write
   * @return bool
   */
  public static boolean isOverWriteRequested(File chunkFile, ChunkInfo
      chunkInfo) {

    if (!chunkFile.exists()) {
      return false;
    }

    long offset = chunkInfo.getOffset();
    return offset < chunkFile.length();
  }

  /**
   * Overwrite is permitted if an only if the user explicitly asks for it. We
   * permit this iff the key/value pair contains a flag called
   * [OverWriteRequested, true].
   *
   * @param chunkInfo - Chunk info
   * @return true if the user asks for it.
   */
  public static boolean isOverWritePermitted(ChunkInfo chunkInfo) {
    String overWrite = chunkInfo.getMetadata().get(OzoneConsts.CHUNK_OVERWRITE);
    return (overWrite != null) &&
        (!overWrite.isEmpty()) &&
        (Boolean.valueOf(overWrite));
  }

  /**
   * Returns a CreateContainer Response. This call is used by create and delete
   * containers which have null success responses.
   *
   * @param msg Request
   * @return Response.
   */
  public static ContainerCommandResponseProto getChunkResponseSuccess(
      ContainerCommandRequestProto msg) {
    return ContainerUtils.getSuccessResponse(msg);
  }

  @VisibleForTesting
  static <T, E extends Exception> T processFileExclusively(
      Path path, CheckedSupplier<T, E> op
  ) throws E {
    for (;;) {
      if (LOCKS.add(path)) {
        break;
      }
    }

    try {
      return op.get();
    } finally {
      LOCKS.remove(path);
    }
  }

  private static void closeFile(FileChannel file, boolean sync)
      throws StorageContainerException {
    if (file != null) {
      try {
        if (sync) {
          // ensure data and metadata is persisted
          file.force(true);
        }
        file.close();
      } catch (IOException e) {
        throw new StorageContainerException("Error closing chunk file",
            e, CONTAINER_INTERNAL_ERROR);
      }
    }
  }
}
