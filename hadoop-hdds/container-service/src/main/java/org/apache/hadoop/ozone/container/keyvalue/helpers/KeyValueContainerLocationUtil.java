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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;

import java.io.File;

/**
 * Class which provides utility methods for container locations.
 */
public final class KeyValueContainerLocationUtil {

  /* Never constructed. */
  private KeyValueContainerLocationUtil() {

  }
  /**
   * Returns Container Metadata Location.
   * @param baseDir
   * @param scmId
   * @param containerId
   * @return containerMetadata Path
   */
  public static File getContainerMetaDataPath(String baseDir, String scmId,
                                              long containerId) {
    String containerMetaDataPath = getBaseContainerLocation(baseDir, scmId,
        containerId);
    containerMetaDataPath = containerMetaDataPath + File.separator +
        OzoneConsts.CONTAINER_META_PATH;
    return new File(containerMetaDataPath);
  }


  /**
   * Returns Container Chunks Location.
   * @param baseDir
   * @param scmId
   * @param containerId
   * @return chunksPath
   */
  public static File getChunksLocationPath(String baseDir, String scmId,
                                           long containerId) {
    String chunksPath = getBaseContainerLocation(baseDir, scmId, containerId)
        + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
    return new File(chunksPath);
  }

  /**
   * Returns base directory for specified container.
   * @param baseDir
   * @param scmId
   * @param containerId
   * @return base directory for container.
   */
  private static String getBaseContainerLocation(String baseDir, String scmId,
                                        long containerId) {
    Preconditions.checkNotNull(baseDir, "Base Directory cannot be null");
    Preconditions.checkNotNull(scmId, "scmUuid cannot be null");
    Preconditions.checkState(containerId >= 0,
        "Container Id cannot be negative.");

    String containerSubDirectory = getContainerSubDirectory(containerId);

    String containerMetaDataPath = baseDir  + File.separator + scmId +
        File.separator + Storage.STORAGE_DIR_CURRENT + File.separator +
        containerSubDirectory + File.separator + containerId;

    return containerMetaDataPath;
  }

  /**
   * Returns subdirectory, where this container needs to be placed.
   * @param containerId
   * @return container sub directory
   */
  private static String getContainerSubDirectory(long containerId){
    int directory = (int) ((containerId >> 9) & 0xFF);
    return Storage.CONTAINER_DIR + directory;
  }

  /**
   * Returns containerFile.
   * @param containerMetaDataPath
   * @param containerName
   * @return .container File name
   */
  public static File getContainerFile(File containerMetaDataPath, String
      containerName) {
    Preconditions.checkNotNull(containerMetaDataPath);
    Preconditions.checkNotNull(containerName);
    return new File(containerMetaDataPath, containerName +
        OzoneConsts.CONTAINER_EXTENSION);
  }

  /**
   * Return containerDB File.
   * @param containerMetaDataPath
   * @param containerName
   * @return containerDB File name
   */
  public static File getContainerDBFile(File containerMetaDataPath, String
      containerName) {
    Preconditions.checkNotNull(containerMetaDataPath);
    Preconditions.checkNotNull(containerName);
    return new File(containerMetaDataPath, containerName + OzoneConsts
        .DN_CONTAINER_DB);
  }

  /**
   * Returns container checksum file.
   * @param containerMetaDataPath
   * @param containerName
   * @return container checksum file
   */
  public static File getContainerCheckSumFile(File containerMetaDataPath,
                                              String containerName) {
    Preconditions.checkNotNull(containerMetaDataPath);
    Preconditions.checkNotNull(containerName);
    return new File(containerMetaDataPath, containerName + OzoneConsts
        .CONTAINER_FILE_CHECKSUM_EXTENSION);
  }
}
