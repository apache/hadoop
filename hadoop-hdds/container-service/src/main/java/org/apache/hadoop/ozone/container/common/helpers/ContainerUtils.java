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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import org.yaml.snakeyaml.Yaml;

import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_CHECKSUM_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.ozone.container.common.impl.ContainerData
    .CHARSET_ENCODING;

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
    return ContainerCommandResponseProto.newBuilder()
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
   * @throws FileAlreadyExistsException
   */
  public static void verifyIsNewContainer(File containerFile) throws
      FileAlreadyExistsException {
    Logger log = LoggerFactory.getLogger(ContainerSet.class);
    Preconditions.checkNotNull(containerFile, "containerFile Should not be " +
        "null");
    if (containerFile.getParentFile().exists()) {
      log.error("Container already exists on disk. File: {}", containerFile
          .toPath());
      throw new FileAlreadyExistsException("container already exists on " +
          "disk.");
    }
  }

  public static String getContainerDbFileName(String containerName) {
    return containerName + OzoneConsts.DN_CONTAINER_DB;
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

  /**
   * Verify that the checksum stored in containerData is equal to the
   * computed checksum.
   * @param containerData
   * @throws IOException
   */
  public static void verifyChecksum(ContainerData containerData)
      throws IOException {
    String storedChecksum = containerData.getChecksum();

    Yaml yaml = ContainerDataYaml.getYamlForContainerType(
        containerData.getContainerType());
    containerData.computeAndSetChecksum(yaml);
    String computedChecksum = containerData.getChecksum();

    if (storedChecksum == null || !storedChecksum.equals(computedChecksum)) {
      throw new StorageContainerException("Container checksum error for " +
          "ContainerID: " + containerData.getContainerID() + ". " +
          "\nStored Checksum: " + storedChecksum +
          "\nExpected Checksum: " + computedChecksum,
          CONTAINER_CHECKSUM_ERROR);
    }
  }

  /**
   * Return the SHA-256 chesksum of the containerData.
   * @param containerDataYamlStr ContainerData as a Yaml String
   * @return Checksum of the container data
   * @throws StorageContainerException
   */
  public static String getChecksum(String containerDataYamlStr)
      throws StorageContainerException {
    MessageDigest sha;
    try {
      sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
      sha.update(containerDataYamlStr.getBytes(CHARSET_ENCODING));
      return DigestUtils.sha256Hex(sha.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new StorageContainerException("Unable to create Message Digest, " +
          "usually this is a java configuration issue.", NO_SUCH_ALGORITHM);
    }
  }

  /**
   * Get the .container file from the containerBaseDir.
   * @param containerBaseDir container base directory. The name of this
   *                         directory is same as the containerID
   * @return the .container file
   */
  public static File getContainerFile(File containerBaseDir) {
    // Container file layout is
    // .../<<containerID>>/metadata/<<containerID>>.container
    String containerFilePath = OzoneConsts.CONTAINER_META_PATH + File.separator
        + getContainerID(containerBaseDir) + OzoneConsts.CONTAINER_EXTENSION;
    return new File(containerBaseDir, containerFilePath);
  }

  /**
   * ContainerID can be decoded from the container base directory name.
   */
  public static long getContainerID(File containerBaseDir) {
    return Long.parseLong(containerBaseDir.getName());
  }
}
