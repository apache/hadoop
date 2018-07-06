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

import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.*;

/**
 * Class which defines utility methods for KeyValueContainer.
 */

public final class KeyValueContainerUtil {

  /* Never constructed. */
  private KeyValueContainerUtil() {

  }

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueContainerUtil.class);

  /**
   * creates metadata path, chunks path and  metadata DB for the specified
   * container.
   *
   * @param containerMetaDataPath
   * @throws IOException
   */
  public static void createContainerMetaData(File containerMetaDataPath, File
      chunksPath, File dbFile, String containerName, Configuration conf) throws
      IOException {
    Preconditions.checkNotNull(containerMetaDataPath);
    Preconditions.checkNotNull(containerName);
    Preconditions.checkNotNull(conf);

    if (!containerMetaDataPath.mkdirs()) {
      LOG.error("Unable to create directory for metadata storage. Path: {}",
          containerMetaDataPath);
      throw new IOException("Unable to create directory for metadata storage." +
          " Path: " + containerMetaDataPath);
    }
    MetadataStore store = MetadataStoreBuilder.newBuilder().setConf(conf)
        .setCreateIfMissing(true).setDbFile(dbFile).build();

    // we close since the SCM pre-creates containers.
    // we will open and put Db handle into a cache when keys are being created
    // in a container.

    store.close();

    if (!chunksPath.mkdirs()) {
      LOG.error("Unable to create chunks directory Container {}",
          chunksPath);
      //clean up container metadata path and metadata db
      FileUtils.deleteDirectory(containerMetaDataPath);
      FileUtils.deleteDirectory(containerMetaDataPath.getParentFile());
      throw new IOException("Unable to create directory for data storage." +
          " Path: " + chunksPath);
    }
  }

  /**
   * remove Container if it is empty.
   * <p/>
   * There are three things we need to delete.
   * <p/>
   * 1. Container file and metadata file. 2. The Level DB file 3. The path that
   * we created on the data location.
   *
   * @param containerData - Data of the container to remove.
   * @param conf - configuration of the cluster.
   * @param forceDelete - whether this container should be deleted forcibly.
   * @throws IOException
   */
  public static void removeContainer(KeyValueContainerData containerData,
                                     Configuration conf, boolean forceDelete)
      throws IOException {
    Preconditions.checkNotNull(containerData);
    File containerMetaDataPath = new File(containerData
        .getMetadataPath());
    File chunksPath = new File(containerData.getChunksPath());

    // Close the DB connection and remove the DB handler from cache
    KeyUtils.removeDB(containerData, conf);

    // Delete the Container MetaData path.
    FileUtils.deleteDirectory(containerMetaDataPath);

    //Delete the Container Chunks Path.
    FileUtils.deleteDirectory(chunksPath);

    //Delete Container directory
    FileUtils.deleteDirectory(containerMetaDataPath.getParentFile());
  }

  /**
   * Returns a ReadContainer Response.
   *
   * @param request Request
   * @param containerData - data
   * @return Response.
   */
  public static ContainerCommandResponseProto getReadContainerResponse(
      ContainerCommandRequestProto request,
      KeyValueContainerData containerData) {
    Preconditions.checkNotNull(containerData);

    ContainerProtos.ReadContainerResponseProto.Builder response =
        ContainerProtos.ReadContainerResponseProto.newBuilder();
    response.setContainerData(containerData.getProtoBufMessage());

    ContainerCommandResponseProto.Builder builder =
        ContainerUtils.getSuccessResponseBuilder(request);
    builder.setReadContainer(response);
    return builder.build();
  }

  /**
   * Compute checksum of the .container file.
   * @param containerId
   * @param containerFile
   * @throws StorageContainerException
   */
  public static String computeCheckSum(long containerId, File
      containerFile) throws StorageContainerException {
    Preconditions.checkNotNull(containerFile, "containerFile cannot be null");
    MessageDigest sha;
    FileInputStream containerFileStream = null;
    try {
      sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    } catch (NoSuchAlgorithmException e) {
      throw new StorageContainerException("Unable to create Message Digest, " +
          "usually this is a java configuration issue.", NO_SUCH_ALGORITHM);
    }
    try {
      containerFileStream = new FileInputStream(containerFile);
      byte[] byteArray = new byte[1024];
      int bytesCount = 0;
      while ((bytesCount = containerFileStream.read(byteArray)) != -1) {
        sha.update(byteArray, 0, bytesCount);
      }
      String checksum = DigestUtils.sha256Hex(sha.digest());
      return checksum;
    } catch (IOException ex) {
      throw new StorageContainerException("Error during computing checksum: " +
          "for container " + containerId, ex, CONTAINER_CHECKSUM_ERROR);
    } finally {
      IOUtils.closeStream(containerFileStream);
    }
  }

  /**
   * Verify checksum of the container.
   * @param containerId
   * @param checksumFile
   * @param checksum
   * @throws StorageContainerException
   */
  public static void verifyCheckSum(long containerId, File checksumFile,
                                    String checksum)
      throws StorageContainerException {
    try {
      Preconditions.checkNotNull(checksum);
      Preconditions.checkNotNull(checksumFile);
      Path path = Paths.get(checksumFile.getAbsolutePath());
      List<String> fileCheckSum = Files.readAllLines(path);
      Preconditions.checkState(fileCheckSum.size() == 1, "checksum " +
          "should be 32 byte string");
      if (!checksum.equals(fileCheckSum.get(0))) {
        LOG.error("Checksum mismatch for the container {}", containerId);
        throw new StorageContainerException("Checksum mismatch for " +
            "the container " + containerId, CHECKSUM_MISMATCH);
      }
    } catch (StorageContainerException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error("Error during verify checksum for container {}", containerId);
      throw new StorageContainerException("Error during verify checksum" +
          " for container " + containerId, IO_EXCEPTION);
    }
  }

  /**
   * Parse KeyValueContainerData and verify checksum.
   * @param containerData
   * @param containerFile
   * @param checksumFile
   * @param dbFile
   * @param config
   * @throws IOException
   */
  public static void parseKeyValueContainerData(
      KeyValueContainerData containerData, File containerFile, File
      checksumFile, File dbFile, OzoneConfiguration config) throws IOException {

    Preconditions.checkNotNull(containerData, "containerData cannot be null");
    Preconditions.checkNotNull(containerFile, "containerFile cannot be null");
    Preconditions.checkNotNull(checksumFile, "checksumFile cannot be null");
    Preconditions.checkNotNull(dbFile, "dbFile cannot be null");
    Preconditions.checkNotNull(config, "ozone config cannot be null");

    long containerId = containerData.getContainerID();
    String containerName = String.valueOf(containerId);
    File metadataPath = new File(containerData.getMetadataPath());

    Preconditions.checkNotNull(containerName, "container Name cannot be " +
        "null");
    Preconditions.checkNotNull(metadataPath, "metadata path cannot be " +
        "null");

    // Verify Checksum
    String checksum = KeyValueContainerUtil.computeCheckSum(
        containerData.getContainerID(), containerFile);
    KeyValueContainerUtil.verifyCheckSum(containerId, checksumFile, checksum);

    containerData.setDbFile(dbFile);

    MetadataStore metadata = KeyUtils.getDB(containerData, config);
    long bytesUsed = 0;
    List<Map.Entry<byte[], byte[]>> liveKeys = metadata
        .getRangeKVs(null, Integer.MAX_VALUE,
            MetadataKeyFilters.getNormalKeyFilter());
    bytesUsed = liveKeys.parallelStream().mapToLong(e-> {
      KeyData keyData;
      try {
        keyData = KeyUtils.getKeyData(e.getValue());
        return keyData.getSize();
      } catch (IOException ex) {
        return 0L;
      }
    }).sum();
    containerData.setBytesUsed(bytesUsed);
    containerData.setKeyCount(liveKeys.size());
  }

  /**
   * Returns the path where data or chunks live for a given container.
   *
   * @param kvContainerData - KeyValueContainerData
   * @return - Path to the chunks directory
   */
  public static Path getDataDirectory(KeyValueContainerData kvContainerData) {

    String chunksPath = kvContainerData.getChunksPath();
    Preconditions.checkNotNull(chunksPath);

    return Paths.get(chunksPath);
  }

  /**
   * Container metadata directory -- here is where the level DB and
   * .container file lives.
   *
   * @param kvContainerData - KeyValueContainerData
   * @return Path to the metadata directory
   */
  public static Path getMetadataDirectory(
      KeyValueContainerData kvContainerData) {

    String metadataPath = kvContainerData.getMetadataPath();
    Preconditions.checkNotNull(metadataPath);

    return Paths.get(metadataPath);

  }
}
