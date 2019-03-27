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
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.utils.MetadataStore;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_LEVELDB;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB;

/**
 * Class to run integrity checks on Datanode Containers.
 * Provide infra for Data Scrubbing
 */

public class KeyValueContainerCheck {

  private static final Logger LOG = LoggerFactory.getLogger(Container.class);

  private long containerID;
  private KeyValueContainerData onDiskContainerData; //loaded from fs/disk
  private Configuration checkConfig;

  private String metadataPath;

  public KeyValueContainerCheck(String metadataPath, Configuration conf,
      long containerID) {
    Preconditions.checkArgument(metadataPath != null);

    this.checkConfig = conf;
    this.containerID = containerID;
    this.onDiskContainerData = null;
    this.metadataPath = metadataPath;
  }

  /**
   * fast checks are basic and do not look inside the metadata files.
   * Or into the structures on disk. These checks can be done on Open
   * containers as well without concurrency implications
   * Checks :
   * 1. check directory layout
   * 2. check container file
   *
   * @return void
   */

  public KvCheckError fastCheck() {

    KvCheckError error;
    LOG.trace("Running fast check for container {};", containerID);

    error = loadContainerData();
    if (error != KvCheckError.ERROR_NONE) {
      return error;
    }

    error = checkLayout();
    if (error != KvCheckError.ERROR_NONE) {
      return error;
    }

    error = checkContainerFile();

    return error;
  }

  /**
   * full checks comprise scanning all metadata inside the container.
   * Including the KV database. These checks are intrusive, consume more
   * resources compared to fast checks and should only be done on Closed
   * or Quasi-closed Containers. Concurrency being limited to delete
   * workflows.
   * <p>
   * fullCheck is a superset of fastCheck
   *
   * @return void
   */
  public KvCheckError fullCheck() {
    /**

     */
    KvCheckError error;

    error = fastCheck();
    if (error != KvCheckError.ERROR_NONE) {

      LOG.trace("fastCheck failed, aborting full check for Container {}",
          containerID);
      return error;
    }

    error = checkBlockDB();

    return error;
  }

  /**
   * Check the integrity of the directory structure of the container.
   *
   * @return error code or ERROR_NONE
   */
  private KvCheckError checkLayout() {
    boolean success;
    KvCheckError error = KvCheckError.ERROR_NONE;

    // is metadataPath accessible as a directory?
    try {
      checkDirPath(metadataPath);
    } catch (IOException ie) {
      error = KvCheckError.METADATA_PATH_ACCESS;
      handleCorruption(ie.getMessage(), error, ie);
      return error;
    }

    String chunksPath = onDiskContainerData.getChunksPath();
    // is chunksPath accessible as a directory?
    try {
      checkDirPath(chunksPath);
    } catch (IOException ie) {
      error = KvCheckError.CHUNKS_PATH_ACCESS;
      handleCorruption(ie.getMessage(), error, ie);
      return error;
    }

    return error;
  }

  private void checkDirPath(String path) throws IOException {

    File dirPath = new File(path);
    String errStr = null;
    boolean success = true;

    try {
      if (!dirPath.isDirectory()) {
        success = false;
        errStr = "Not a directory [" + path + "]";
      }
    } catch (SecurityException se) {
      throw new IOException("Security exception checking dir ["
          + path + "]", se);
    } catch (Exception e) {
      throw new IOException("Generic exception checking dir ["
          + path + "]", e);
    }

    try {
      String[] ls = dirPath.list();
      if (ls == null) {
        // null result implies operation failed
        success = false;
        errStr = "null listing for directory [" + path + "]";
      }
    } catch (Exception e) {
      throw new IOException("Exception listing dir [" + path + "]", e);
    }

    if (!success) {
      Preconditions.checkState(errStr != null);
      throw new IOException(errStr);
    }
  }

  private KvCheckError checkContainerFile() {
    /**
     * compare the values in the container file loaded from disk,
     * with the values we are expecting
     */
    KvCheckError error = KvCheckError.ERROR_NONE;
    String dbType;
    Preconditions
        .checkState(onDiskContainerData != null, "Container File not loaded");
    KvCheckAction next;

    try {
      ContainerUtils.verifyChecksum(onDiskContainerData);
    } catch (Exception e) {
      error = KvCheckError.CONTAINERDATA_CKSUM;
      handleCorruption("Container File Checksum mismatch", error, e);
      return error;
    }

    if (onDiskContainerData.getContainerType()
        != ContainerProtos.ContainerType.KeyValueContainer) {
      String errStr = "Bad Container type in Containerdata for " + containerID;
      error = KvCheckError.CONTAINERDATA_TYPE;
      handleCorruption(errStr, error, null);
      return error; // Abort if we do not know the type of Container
    }

    if (onDiskContainerData.getContainerID() != containerID) {
      String errStr =
          "Bad ContainerID field in Containerdata for " + containerID;
      error = KvCheckError.CONTAINERDATA_ID;
      next = handleCorruption(errStr, error, null);
      if (next == KvCheckAction.ABORT) {
        return error;
      } // else continue checking other data elements
    }

    dbType = onDiskContainerData.getContainerDBType();
    if (!dbType.equals(OZONE_METADATA_STORE_IMPL_ROCKSDB) &&
        !dbType.equals(OZONE_METADATA_STORE_IMPL_LEVELDB)) {
      String errStr = "Unknown DBType [" + dbType
          + "] in Container File for  [" + containerID + "]";
      error = KvCheckError.CONTAINERDATA_DBTYPE;
      handleCorruption(errStr, error, null);
      return error;
    }

    KeyValueContainerData kvData = onDiskContainerData;
    if (!metadataPath.toString().equals(kvData.getMetadataPath())) {
      String errStr =
          "Bad metadata path in Containerdata for " + containerID + "Expected ["
              + metadataPath.toString() + "] Got [" + kvData.getMetadataPath()
              + "]";
      error = KvCheckError.CONTAINERDATA_METADATA_PATH;
      next = handleCorruption(errStr, error, null);
      if (next == KvCheckAction.ABORT) {
        return error;
      }
    }

    return error;
  }

  private KvCheckError checkBlockDB() {
    /**
     * Check the integrity of the DB inside each container.
     * In Scope:
     * 1. iterate over each key (Block) and locate the chunks for the block
     * 2. garbage detection : chunks which exist in the filesystem,
     *    but not in the DB. This function is implemented as HDDS-1202
     * Not in scope:
     * 1. chunk checksum verification. this is left to a separate
     * slow chunk scanner
     */
    KvCheckError error;
    Preconditions.checkState(onDiskContainerData != null,
        "invoke loadContainerData prior to calling this function");
    File dbFile;
    File metaDir = new File(metadataPath);

    try {
      dbFile = KeyValueContainerLocationUtil
          .getContainerDBFile(metaDir, containerID);

      if (!dbFile.exists() || !dbFile.canRead()) {

        String dbFileErrorMsg = "Unable to access DB File [" + dbFile.toString()
            + "] for Container [" + containerID + "] metadata path ["
            + metadataPath + "]";
        error = KvCheckError.DB_ACCESS;
        handleCorruption(dbFileErrorMsg, error, null);
        return error;
      }
    } catch (Exception e) {
      String dbFileErrorMessage =
          "Exception when initializing DBFile" + "with metadatapath ["
              + metadataPath + "] for Container [" + containerID
              + "]";
      error = KvCheckError.DB_ACCESS;
      handleCorruption(dbFileErrorMessage, error, e);
      return error;
    }
    onDiskContainerData.setDbFile(dbFile);

    try {
      MetadataStore db = BlockUtils
          .getDB(onDiskContainerData, checkConfig);
      error = iterateBlockDB(db);
    } catch (Exception e) {
      error = KvCheckError.DB_ITERATOR;
      handleCorruption("Block DB Iterator aborted", error, e);
      return error;
    }

    return error;
  }

  private KvCheckError iterateBlockDB(MetadataStore db)
      throws IOException {
    KvCheckError error = KvCheckError.ERROR_NONE;
    Preconditions.checkState(db != null);

    // get "normal" keys from the Block DB
    KeyValueBlockIterator kvIter = new KeyValueBlockIterator(containerID,
        new File(onDiskContainerData.getContainerPath()));

    // ensure there is a chunk file for each key in the DB
    while (kvIter.hasNext()) {
      BlockData block = kvIter.nextBlock();

      List<ContainerProtos.ChunkInfo> chunkInfoList = block.getChunks();
      for (ContainerProtos.ChunkInfo chunk : chunkInfoList) {
        File chunkFile;
        try {
          chunkFile = ChunkUtils
              .getChunkFile(onDiskContainerData,
                  ChunkInfo.getFromProtoBuf(chunk));
        } catch (Exception e) {
          error = KvCheckError.MISSING_CHUNK_FILE;
          handleCorruption("Unable to access chunk path", error, e);
          return error;
        }

        if (!chunkFile.exists()) {
          error = KvCheckError.MISSING_CHUNK_FILE;

          // concurrent mutation in Block DB? lookup the block again.
          byte[] bdata = db.get(
              Longs.toByteArray(block.getBlockID().getLocalID()));
          if (bdata == null) {
            LOG.trace("concurrency with delete, ignoring deleted block");
            error = KvCheckError.ERROR_NONE;
            break; // skip to next block from kvIter
          } else {
            handleCorruption("Missing chunk file", error, null);
            return error;
          }
        }
      }
    }

    return error;
  }

  private KvCheckError loadContainerData() {
    KvCheckError error = KvCheckError.ERROR_NONE;

    File containerFile = KeyValueContainer
        .getContainerFile(metadataPath.toString(), containerID);

    try {
      onDiskContainerData = (KeyValueContainerData) ContainerDataYaml
          .readContainerFile(containerFile);
    } catch (IOException e) {
      error = KvCheckError.FILE_LOAD;
      handleCorruption("Unable to load Container File", error, e);
    }

    return error;
  }

  private KvCheckAction handleCorruption(String reason,
      KvCheckError error, Exception e) {

    // XXX HDDS-1201 need to implement corruption handling/reporting

    String errStr =
        "Corruption detected in container: [" + containerID + "] reason: ["
            + reason + "] error code: [" + error + "]";
    String logMessage = null;

    StackTraceElement[] stackeElems = Thread.currentThread().getStackTrace();
    String caller =
        "Corruption reported from Source File: [" + stackeElems[2].getFileName()
            + "] Line: [" + stackeElems[2].getLineNumber() + "]";

    if (e != null) {
      logMessage = errStr + " exception: [" + e.getMessage() + "]";
      e.printStackTrace();
    } else {
      logMessage = errStr;
    }

    LOG.error(caller);
    LOG.error(logMessage);

    return KvCheckAction.ABORT;
  }

  /**
   * Pre-defined error codes for Container Metadata check.
   */
  public enum KvCheckError {
    ERROR_NONE,
    FILE_LOAD, // unable to load container metafile
    METADATA_PATH_ACCESS, // metadata path is not accessible
    CHUNKS_PATH_ACCESS, // chunks path is not accessible
    CONTAINERDATA_ID, // bad Container-ID stored in Container file
    CONTAINERDATA_METADATA_PATH, // bad metadata path in Container file
    CONTAINERDATA_CHUNKS_PATH, // bad chunks path in Container file
    CONTAINERDATA_CKSUM, // container file checksum mismatch
    CONTAINERDATA_TYPE, // container file incorrect type of Container
    CONTAINERDATA_DBTYPE, // unknown DB Type specified in Container File
    DB_ACCESS, // unable to load Metastore DB
    DB_ITERATOR, // unable to create block iterator for Metastore DB
    MISSING_CHUNK_FILE // chunk file not found
  }

  private enum KvCheckAction {
    CONTINUE, // Continue with remaining checks on the corrupt Container
    ABORT     // Abort checks for the container
  }
}