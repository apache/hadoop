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
   * Run basic integrity checks on container metadata.
   * These checks do not look inside the metadata files.
   * Applicable for OPEN containers.
   *
   * @return true : corruption detected, false : no corruption.
   */
  public boolean fastCheck() {
    boolean corruption = false;
    try {
      basicChecks();

    } catch (IOException e) {
      handleCorruption(e);
      corruption = true;
    }

    return corruption;
  }

  /**
   * Checks :
   * 1. check directory layout
   * 2. check container file
   *
   * @return void
   */

  private void basicChecks() throws IOException {

    LOG.trace("Running basic checks for container {};", containerID);

    loadContainerData();

    checkLayout();
    checkContainerFile();
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
   * @return true : corruption detected, false : no corruption.
   */
  public boolean fullCheck() {
    boolean corruption = false;

    try {
      basicChecks();
      checkBlockDB();

    } catch (IOException e) {
      handleCorruption(e);
      corruption = true;
    }

    return corruption;
  }

  /**
   * Check the integrity of the directory structure of the container.
   */
  private void checkLayout() throws IOException {

    // is metadataPath accessible as a directory?
    checkDirPath(metadataPath);

    // is chunksPath accessible as a directory?
    String chunksPath = onDiskContainerData.getChunksPath();
    checkDirPath(chunksPath);
  }

  private void checkDirPath(String path) throws IOException {

    File dirPath = new File(path);
    String errStr = null;

    try {
      if (!dirPath.isDirectory()) {
        errStr = "Not a directory [" + path + "]";
        throw new IOException(errStr);
      }
    } catch (SecurityException se) {
      throw new IOException("Security exception checking dir ["
          + path + "]", se);
    }

    String[] ls = dirPath.list();
    if (ls == null) {
      // null result implies operation failed
      errStr = "null listing for directory [" + path + "]";
      throw new IOException(errStr);
    }
  }

  private void checkContainerFile() throws IOException {
    /**
     * compare the values in the container file loaded from disk,
     * with the values we are expecting
     */
    String dbType;
    Preconditions
        .checkState(onDiskContainerData != null, "Container File not loaded");

    ContainerUtils.verifyChecksum(onDiskContainerData);

    if (onDiskContainerData.getContainerType()
        != ContainerProtos.ContainerType.KeyValueContainer) {
      String errStr = "Bad Container type in Containerdata for " + containerID;
      throw new IOException(errStr);
    }

    if (onDiskContainerData.getContainerID() != containerID) {
      String errStr =
          "Bad ContainerID field in Containerdata for " + containerID;
      throw new IOException(errStr);
    }

    dbType = onDiskContainerData.getContainerDBType();
    if (!dbType.equals(OZONE_METADATA_STORE_IMPL_ROCKSDB) &&
        !dbType.equals(OZONE_METADATA_STORE_IMPL_LEVELDB)) {
      String errStr = "Unknown DBType [" + dbType
          + "] in Container File for  [" + containerID + "]";
      throw new IOException(errStr);
    }

    KeyValueContainerData kvData = onDiskContainerData;
    if (!metadataPath.toString().equals(kvData.getMetadataPath())) {
      String errStr =
          "Bad metadata path in Containerdata for " + containerID + "Expected ["
              + metadataPath.toString() + "] Got [" + kvData.getMetadataPath()
              + "]";
      throw new IOException(errStr);
    }
  }

  private void checkBlockDB() throws IOException {
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
    Preconditions.checkState(onDiskContainerData != null,
        "invoke loadContainerData prior to calling this function");
    File dbFile;
    File metaDir = new File(metadataPath);

    dbFile = KeyValueContainerLocationUtil
        .getContainerDBFile(metaDir, containerID);

    if (!dbFile.exists() || !dbFile.canRead()) {
      String dbFileErrorMsg = "Unable to access DB File [" + dbFile.toString()
          + "] for Container [" + containerID + "] metadata path ["
          + metadataPath + "]";
      throw new IOException(dbFileErrorMsg);
    }


    onDiskContainerData.setDbFile(dbFile);
    MetadataStore db = BlockUtils
        .getDB(onDiskContainerData, checkConfig);

    iterateBlockDB(db);
  }

  private void iterateBlockDB(MetadataStore db)
      throws IOException {
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
        chunkFile = ChunkUtils.getChunkFile(onDiskContainerData,
            ChunkInfo.getFromProtoBuf(chunk));

        if (!chunkFile.exists()) {
          // concurrent mutation in Block DB? lookup the block again.
          byte[] bdata = db.get(
              Longs.toByteArray(block.getBlockID().getLocalID()));
          if (bdata == null) {
            LOG.trace("concurrency with delete, ignoring deleted block");
            break; // skip to next block from kvIter
          } else {
            String errorStr = "Missing chunk file "
                + chunkFile.getAbsolutePath();
            throw new IOException(errorStr);
          }
        }
      }
    }
  }

  private void loadContainerData() throws IOException {

    File containerFile = KeyValueContainer
        .getContainerFile(metadataPath.toString(), containerID);

    onDiskContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
  }

  private void handleCorruption(IOException e) {
    String errStr =
        "Corruption detected in container: [" + containerID + "] ";
    String logMessage = errStr + "Exception: [" + e.getMessage() + "]";
    LOG.error(logMessage);
  }
}