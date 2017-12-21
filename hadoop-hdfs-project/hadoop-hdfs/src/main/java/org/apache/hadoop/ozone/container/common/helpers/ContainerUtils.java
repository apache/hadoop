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

package org.apache.hadoop.ozone.container.common.helpers;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ContainerManagerImpl;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.INVALID_ARGUMENT;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.UNABLE_TO_FIND_DATA_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_EXTENSION;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_META;

/**
 * A set of helper functions to create proper responses.
 */
public final class ContainerUtils {

  private ContainerUtils() {
    //never constructed.
  }

  /**
   * Returns a CreateContainer Response. This call is used by create and delete
   * containers which have null success responses.
   *
   * @param msg Request
   * @return Response.
   */
  public static ContainerProtos.ContainerCommandResponseProto
      getContainerResponse(ContainerProtos.ContainerCommandRequestProto msg) {
    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        getContainerResponse(msg, ContainerProtos.Result.SUCCESS, "");
    return builder.build();
  }

  /**
   * Returns a ReadContainer Response.
   *
   * @param msg Request
   * @param containerData - data
   * @return Response.
   */
  public static ContainerProtos.ContainerCommandResponseProto
      getReadContainerResponse(ContainerProtos.ContainerCommandRequestProto msg,
      ContainerData containerData) {
    Preconditions.checkNotNull(containerData);

    ContainerProtos.ReadContainerResponseProto.Builder response =
        ContainerProtos.ReadContainerResponseProto.newBuilder();
    response.setContainerData(containerData.getProtoBufMessage());

    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        getContainerResponse(msg, ContainerProtos.Result.SUCCESS, "");
    builder.setReadContainer(response);
    return builder.build();
  }

  /**
   * We found a command type but no associated payload for the command. Hence
   * return malformed Command as response.
   *
   * @param msg - Protobuf message.
   * @param result - result
   * @param message - Error message.
   * @return ContainerCommandResponseProto - MALFORMED_REQUEST.
   */
  public static ContainerProtos.ContainerCommandResponseProto.Builder
      getContainerResponse(ContainerProtos.ContainerCommandRequestProto msg,
      ContainerProtos.Result result, String message) {
    return
        ContainerProtos.ContainerCommandResponseProto.newBuilder()
            .setCmdType(msg.getCmdType())
            .setTraceID(msg.getTraceID())
            .setResult(result)
            .setMessage(message);
  }

  /**
   * Logs the error and returns a response to the caller.
   *
   * @param log - Logger
   * @param ex - Exception
   * @param msg - Request Object
   * @return Response
   */
  public static ContainerProtos.ContainerCommandResponseProto logAndReturnError(
      Logger log, StorageContainerException ex,
      ContainerProtos.ContainerCommandRequestProto msg) {
    log.info("Operation: {} : Trace ID: {} : Message: {} : Result: {}",
        msg.getCmdType().name(), msg.getTraceID(),
        ex.getMessage(), ex.getResult().getValueDescriptor().getName());
    return getContainerResponse(msg, ex.getResult(), ex.getMessage()).build();
  }

  /**
   * Logs the error and returns a response to the caller.
   *
   * @param log - Logger
   * @param ex - Exception
   * @param msg - Request Object
   * @return Response
   */
  public static ContainerProtos.ContainerCommandResponseProto logAndReturnError(
      Logger log, RuntimeException ex,
      ContainerProtos.ContainerCommandRequestProto msg) {
    log.info("Operation: {} : Trace ID: {} : Message: {} ",
        msg.getCmdType().name(), msg.getTraceID(), ex.getMessage());
    return getContainerResponse(msg, INVALID_ARGUMENT, ex.getMessage()).build();
  }

  /**
   * We found a command type but no associated payload for the command. Hence
   * return malformed Command as response.
   *
   * @param msg - Protobuf message.
   * @return ContainerCommandResponseProto - MALFORMED_REQUEST.
   */
  public static ContainerProtos.ContainerCommandResponseProto
      malformedRequest(ContainerProtos.ContainerCommandRequestProto msg) {
    return getContainerResponse(msg, ContainerProtos.Result.MALFORMED_REQUEST,
        "Cmd type does not match the payload.").build();
  }

  /**
   * We found a command type that is not supported yet.
   *
   * @param msg - Protobuf message.
   * @return ContainerCommandResponseProto - MALFORMED_REQUEST.
   */
  public static ContainerProtos.ContainerCommandResponseProto
      unsupportedRequest(ContainerProtos.ContainerCommandRequestProto msg) {
    return getContainerResponse(msg, ContainerProtos.Result.UNSUPPORTED_REQUEST,
        "Server does not support this command yet.").build();
  }

  /**
   * get containerName from a container file.
   *
   * @param containerFile - File
   * @return Name of the container.
   */
  public static String getContainerNameFromFile(File containerFile) {
    Preconditions.checkNotNull(containerFile);
    return Paths.get(containerFile.getParent()).resolve(
        removeExtension(containerFile.getName())).toString();
  }

  /**
   * Verifies that this in indeed a new container.
   *
   * @param containerFile - Container File to verify
   * @param metadataFile - metadata File to verify
   * @throws IOException
   */
  public static void verifyIsNewContainer(File containerFile, File metadataFile)
      throws IOException {
    Logger log = LoggerFactory.getLogger(ContainerManagerImpl.class);
    if (containerFile.exists()) {
      log.error("container already exists on disk. File: {}",
          containerFile.toPath());
      throw new FileAlreadyExistsException("container already exists on " +
          "disk.");
    }

    if (metadataFile.exists()) {
      log.error("metadata found on disk, but missing container. Refusing to" +
          " write this container. File: {} ", metadataFile.toPath());
      throw new FileAlreadyExistsException(("metadata found on disk, but " +
          "missing container. Refusing to write this container."));
    }

    File parentPath = new File(containerFile.getParent());

    if (!parentPath.exists() && !parentPath.mkdirs()) {
      log.error("Unable to create parent path. Path: {}",
          parentPath.toString());
      throw new IOException("Unable to create container directory.");
    }

    if (!containerFile.createNewFile()) {
      log.error("creation of a new container file failed. File: {}",
          containerFile.toPath());
      throw new IOException("creation of a new container file failed.");
    }

    if (!metadataFile.createNewFile()) {
      log.error("creation of the metadata file failed. File: {}",
          metadataFile.toPath());
      throw new IOException("creation of a new container file failed.");
    }
  }

  public static String getContainerDbFileName(String containerName) {
    return containerName + OzoneConsts.DN_CONTAINER_DB;
  }

  /**
   * creates a Metadata DB for the specified container.
   *
   * @param containerPath - Container Path.
   * @throws IOException
   */
  public static Path createMetadata(Path containerPath, String containerName,
      Configuration conf)
      throws IOException {
    Logger log = LoggerFactory.getLogger(ContainerManagerImpl.class);
    Preconditions.checkNotNull(containerPath);
    Path metadataPath = containerPath.resolve(OzoneConsts.CONTAINER_META_PATH);
    if (!metadataPath.toFile().mkdirs()) {
      log.error("Unable to create directory for metadata storage. Path: {}",
          metadataPath);
      throw new IOException("Unable to create directory for metadata storage." +
          " Path: " + metadataPath);
    }
    MetadataStore store = MetadataStoreBuilder.newBuilder()
        .setConf(conf)
        .setCreateIfMissing(true)
        .setDbFile(metadataPath
            .resolve(getContainerDbFileName(containerName)).toFile())
        .build();

    // we close since the SCM pre-creates containers.
    // we will open and put Db handle into a cache when keys are being created
    // in a container.

    store.close();

    Path dataPath = containerPath.resolve(OzoneConsts.CONTAINER_DATA_PATH);
    if (!dataPath.toFile().mkdirs()) {

      // If we failed to create data directory, we cleanup the
      // metadata directory completely. That is, we will delete the
      // whole directory including LevelDB file.
      log.error("Unable to create directory for data storage. cleaning up the" +
              " container path: {} dataPath: {}",
          containerPath, dataPath);
      FileUtils.deleteDirectory(containerPath.toFile());
      throw new IOException("Unable to create directory for data storage." +
          " Path: " + dataPath);
    }
    return metadataPath;
  }

  /**
   * Returns Metadata location.
   *
   * @param containerData - Data
   * @param location - Path
   * @return Path
   */
  public static File getMetadataFile(ContainerData containerData,
      Path location) {
    return location.resolve(containerData
        .getContainerName().concat(CONTAINER_META))
        .toFile();
  }

  /**
   * Returns container file location.
   *
   * @param containerData - Data
   * @param location - Root path
   * @return Path
   */
  public static File getContainerFile(ContainerData containerData,
      Path location) {
    return location.resolve(containerData
        .getContainerName().concat(CONTAINER_EXTENSION))
        .toFile();
  }

  /**
   * Container metadata directory -- here is where the level DB lives.
   *
   * @param cData - cData.
   * @return Path to the parent directory where the DB lives.
   */
  public static Path getMetadataDirectory(ContainerData cData) {
    Path dbPath = Paths.get(cData.getDBPath());
    Preconditions.checkNotNull(dbPath);
    Preconditions.checkState(dbPath.toString().length() > 0);
    return dbPath.getParent();
  }

  /**
   * Returns the path where data or chunks live for a given container.
   *
   * @param cData - cData container
   * @return - Path
   * @throws StorageContainerException
   */
  public static Path getDataDirectory(ContainerData cData)
      throws StorageContainerException {
    Path path = getMetadataDirectory(cData);
    Preconditions.checkNotNull(path);
    Path parentPath = path.getParent();
    if (parentPath == null) {
      throw new StorageContainerException("Unable to get Data directory."
          + path, UNABLE_TO_FIND_DATA_DIR);
    }
    return parentPath.resolve(OzoneConsts.CONTAINER_DATA_PATH);
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
  public static void removeContainer(ContainerData containerData,
      Configuration conf, boolean forceDelete) throws IOException {
    Preconditions.checkNotNull(containerData);
    Path dbPath = Paths.get(containerData.getDBPath());

    MetadataStore db = KeyUtils.getDB(containerData, conf);
    // If the container is not empty and cannot be deleted forcibly,
    // then throw a SCE to stop deleting.
    if(!forceDelete && !db.isEmpty()) {
      throw new StorageContainerException(
          "Container cannot be deleted because it is not empty.",
          ContainerProtos.Result.ERROR_CONTAINER_NOT_EMPTY);
    }
    // Close the DB connection and remove the DB handler from cache
    KeyUtils.removeDB(containerData, conf);

    // Delete the DB File.
    FileUtils.forceDelete(dbPath.toFile());
    dbPath = dbPath.getParent();

    // Delete all Metadata in the Data directories for this containers.
    if (dbPath != null) {
      FileUtils.deleteDirectory(dbPath.toFile());
      dbPath = dbPath.getParent();
    }

    // now delete the container directory, this means that all key data dirs
    // will be removed too.
    if (dbPath != null) {
      FileUtils.deleteDirectory(dbPath.toFile());
    }

    // Delete the container metadata from the metadata locations.
    String rootPath = getContainerNameFromFile(new File(containerData
        .getContainerPath()));
    Path containerPath = Paths.get(rootPath.concat(CONTAINER_EXTENSION));
    Path metaPath = Paths.get(rootPath.concat(CONTAINER_META));

    FileUtils.forceDelete(containerPath.toFile());
    FileUtils.forceDelete(metaPath.toFile());
  }

  /**
   * Write datanode ID protobuf messages to an ID file.
   * The old ID file will be overwritten.
   *
   * @param ids A set of {@link DatanodeID}
   * @param path Local ID file path
   * @throws IOException When read/write error occurs
   */
  private synchronized static void writeDatanodeIDs(List<DatanodeID> ids,
      File path) throws IOException {
    if (path.exists()) {
      if (!path.delete() || !path.createNewFile()) {
        throw new IOException("Unable to overwrite the datanode ID file.");
      }
    } else {
      if(!path.getParentFile().exists() &&
          !path.getParentFile().mkdirs()) {
        throw new IOException("Unable to create datanode ID directories.");
      }
    }
    try (FileOutputStream out = new FileOutputStream(path)) {
      for (DatanodeID id : ids) {
        HdfsProtos.DatanodeIDProto dnId = id.getProtoBufMessage();
        dnId.writeDelimitedTo(out);
      }
    }
  }

  /**
   * Persistent a {@link DatanodeID} to a local file.
   * It reads the IDs first and append a new entry only if the ID is new.
   * This is to avoid on some dirty environment, this file gets too big.
   *
   * @throws IOException when read/write error occurs
   */
  public synchronized static void writeDatanodeIDTo(DatanodeID dnID,
      File path) throws IOException {
    List<DatanodeID> ids = ContainerUtils.readDatanodeIDsFrom(path);
    // Only create or overwrite the file
    // if the ID doesn't exist in the ID file
    for (DatanodeID id : ids) {
      if (id.getProtoBufMessage()
          .equals(dnID.getProtoBufMessage())) {
        return;
      }
    }
    ids.add(dnID);
    writeDatanodeIDs(ids, path);
  }

  /**
   * Read {@link DatanodeID} from a local ID file and return a set of
   * datanode IDs. If the ID file doesn't exist, an empty set is returned.
   *
   * @param path ID file local path
   * @return A set of {@link DatanodeID}
   * @throws IOException If the id file is malformed or other I/O exceptions
   */
  public synchronized static List<DatanodeID> readDatanodeIDsFrom(File path)
      throws IOException {
    List<DatanodeID> ids = new ArrayList<DatanodeID>();
    if (!path.exists()) {
      return ids;
    }
    try(FileInputStream in = new FileInputStream(path)) {
      while(in.available() > 0) {
        try {
          HdfsProtos.DatanodeIDProto id =
              HdfsProtos.DatanodeIDProto.parseDelimitedFrom(in);
          ids.add(DatanodeID.getFromProtoBuf(id));
        } catch (IOException e) {
          throw new IOException("Failed to parse Datanode ID from "
              + path.getAbsolutePath(), e);
        }
      }
    }
    return ids;
  }
}
