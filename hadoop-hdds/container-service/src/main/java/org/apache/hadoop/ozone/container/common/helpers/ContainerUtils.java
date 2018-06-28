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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_EXTENSION;
import org.apache.hadoop.ozone.container.common.impl.ContainerManagerImpl;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
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

import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CLOSED_CONTAINER_IO;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.SUCCESS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.UNABLE_TO_FIND_DATA_DIR;

/**
 * A set of helper functions to create proper responses.
 */
public final class ContainerUtils {

  private ContainerUtils() {
    //never constructed.
  }

  /**
   * Returns a Container Command Response Builder with the specified result
   * and message.
   * @param request requestProto message.
   * @param result result of the command.
   * @param message response message.
   * @return ContainerCommand Response Builder.
   */
  public static ContainerCommandResponseProto.Builder
  getContainerCommandResponse(
      ContainerCommandRequestProto request, Result result, String message) {
    return
        ContainerCommandResponseProto.newBuilder()
            .setCmdType(request.getCmdType())
            .setTraceID(request.getTraceID())
            .setResult(result)
            .setMessage(message);
  }

  /**
   * Returns a Container Command Response Builder. This call is used to build
   * success responses. Calling function can add other fields to the response
   * as required.
   * @param request requestProto message.
   * @return ContainerCommand Response Builder with result as SUCCESS.
   */
  public static ContainerCommandResponseProto.Builder getSuccessResponseBuilder(
      ContainerCommandRequestProto request) {
    return
        ContainerCommandResponseProto.newBuilder()
            .setCmdType(request.getCmdType())
            .setTraceID(request.getTraceID())
            .setResult(Result.SUCCESS);
  }

  /**
   * Returns a Container Command Response. This call is used for creating null
   * success responses.
   * @param request requestProto message.
   * @return ContainerCommand Response with result as SUCCESS.
   */
  public static ContainerCommandResponseProto getSuccessResponse(
      ContainerCommandRequestProto request) {
    ContainerCommandResponseProto.Builder builder =
        getContainerCommandResponse(request, Result.SUCCESS, "");
    return builder.build();
  }

  /**
   * Returns a ReadContainer Response.
   * @param msg requestProto message.
   * @param containerData container data to be returned.
   * @return ReadContainer Response
   */
  public static ContainerProtos.ContainerCommandResponseProto
    getReadContainerResponse(ContainerProtos.ContainerCommandRequestProto msg,
      ContainerData containerData) {
    Preconditions.checkNotNull(containerData);

    ContainerProtos.ReadContainerResponseProto.Builder response =
        ContainerProtos.ReadContainerResponseProto.newBuilder();
    response.setContainerData(containerData.getProtoBufMessage());

    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        getSuccessResponseBuilder(msg);
    builder.setReadContainer(response);
    return builder.build();
  }

  /**
   * We found a command type but no associated payload for the command. Hence
   * return malformed Command as response.
   *
   * @param request - Protobuf message.
   * @return ContainerCommandResponseProto - MALFORMED_REQUEST.
   */
  public static ContainerCommandResponseProto malformedRequest(
      ContainerCommandRequestProto request) {
    return getContainerCommandResponse(request, Result.MALFORMED_REQUEST,
        "Cmd type does not match the payload.").build();
  }

  /**
   * We found a command type that is not supported yet.
   *
   * @param request - Protobuf message.
   * @return ContainerCommandResponseProto - UNSUPPORTED_REQUEST.
   */
  public static ContainerCommandResponseProto unsupportedRequest(
      ContainerCommandRequestProto request) {
    return getContainerCommandResponse(request, Result.UNSUPPORTED_REQUEST,
        "Server does not support this command yet.").build();
  }

  /**
   * Logs the error and returns a response to the caller.
   *
   * @param log - Logger
   * @param ex - Exception
   * @param request - Request Object
   * @return Response
   */
  public static ContainerCommandResponseProto logAndReturnError(
      Logger log, StorageContainerException ex,
      ContainerCommandRequestProto request) {
    log.info("Operation: {} : Trace ID: {} : Message: {} : Result: {}",
        request.getCmdType().name(), request.getTraceID(),
        ex.getMessage(), ex.getResult().getValueDescriptor().getName());
    return getContainerCommandResponse(request, ex.getResult(), ex.getMessage())
        .build();
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

  public static long getContainerIDFromFile(File containerFile) {
    Preconditions.checkNotNull(containerFile);
    String containerID = getContainerNameFromFile(containerFile);
    return Long.parseLong(containerID);
  }

  /**
   * Verifies that this is indeed a new container.
   *
   * @param containerFile - Container File to verify
   * @throws IOException
   */
  public static void verifyIsNewContainer(File containerFile)
      throws IOException {
    Logger log = LoggerFactory.getLogger(ContainerManagerImpl.class);
    if (containerFile.exists()) {
      log.error("container already exists on disk. File: {}",
          containerFile.toPath());
      throw new FileAlreadyExistsException("container already exists on " +
          "disk.");
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
   * Returns container file location.
   *
   * @param containerData - Data
   * @param location - Root path
   * @return Path
   */
  public static File getContainerFile(ContainerData containerData,
      Path location) {
    return location.resolve(Long.toString(containerData
        .getContainerID()).concat(CONTAINER_EXTENSION))
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
          Result.ERROR_CONTAINER_NOT_EMPTY);
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


    FileUtils.forceDelete(containerPath.toFile());

  }

  /**
   * Persistent a {@link DatanodeDetails} to a local file.
   *
   * @throws IOException when read/write error occurs
   */
  public synchronized static void writeDatanodeDetailsTo(
      DatanodeDetails datanodeDetails, File path) throws IOException {
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
      HddsProtos.DatanodeDetailsProto proto =
          datanodeDetails.getProtoBufMessage();
      proto.writeTo(out);
    }
  }

  /**
   * Read {@link DatanodeDetails} from a local ID file.
   *
   * @param path ID file local path
   * @return {@link DatanodeDetails}
   * @throws IOException If the id file is malformed or other I/O exceptions
   */
  public synchronized static DatanodeDetails readDatanodeDetailsFrom(File path)
      throws IOException {
    if (!path.exists()) {
      throw new IOException("Datanode ID file not found.");
    }
    try(FileInputStream in = new FileInputStream(path)) {
      return DatanodeDetails.getFromProtoBuf(
          HddsProtos.DatanodeDetailsProto.parseFrom(in));
    } catch (IOException e) {
      throw new IOException("Failed to parse DatanodeDetails from "
          + path.getAbsolutePath(), e);
    }
  }
}
