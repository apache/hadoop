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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ContainerManagerImpl;
import org.apache.hadoop.ozone.container.common.utils.LevelDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_EXTENSION;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_META;

/**
 * A set of helper functions to create proper responses.
 */
public final class ContainerUtils {

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
   * @param msg           Request
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
   * @param msg     - Protobuf message.
   * @param result  - result
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
   * @param metadataFile  - metadata File to verify
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

  /**
   * creates a Metadata DB for the specified container.
   *
   * @param containerPath - Container Path.
   * @throws IOException
   */
  public static Path createMetadata(Path containerPath) throws IOException {
    Logger log = LoggerFactory.getLogger(ContainerManagerImpl.class);
    Preconditions.checkNotNull(containerPath);
    Path metadataPath = containerPath.resolve(OzoneConsts.CONTAINER_META_PATH);
    if (!metadataPath.toFile().mkdirs()) {
      log.error("Unable to create directory for metadata storage. Path: {}",
          metadataPath);
      throw new IOException("Unable to create directory for metadata storage." +
          " Path: " + metadataPath);
    }
    LevelDBStore store =
        new LevelDBStore(metadataPath.resolve(OzoneConsts.CONTAINER_DB)
            .toFile(), true);

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
   * @param location      - Path
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
   * @param containerData  - Data
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
   * @param cData - cData container
   * @return - Path
   */
  public static Path getDataDirectory(ContainerData cData) throws IOException {
    Path path = getMetadataDirectory(cData);
    Preconditions.checkNotNull(path);
    path = path.getParent();
    if(path == null) {
      throw new IOException("Unable to get Data directory. null path found");
    }
    return path.resolve(OzoneConsts.CONTAINER_DATA_PATH);
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
   * @throws IOException
   */
  public static void removeContainer(ContainerData containerData) throws
      IOException {
    Preconditions.checkNotNull(containerData);

    // TODO : Check if there are any keys. This needs to be done
    // by calling into key layer code, hence this is a TODO for now.

    Path dbPath = Paths.get(containerData.getDBPath());

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

  private ContainerUtils() {
    //never constructed.
  }
}
