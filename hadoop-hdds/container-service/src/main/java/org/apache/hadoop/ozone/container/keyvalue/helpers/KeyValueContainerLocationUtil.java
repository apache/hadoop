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
   * @param hddsVolumeDir base dir of the hdds volume where scm directories
   *                      are stored
   * @param scmId
   * @param containerId
   * @return containerMetadata Path to container metadata location where
   * .container file will be stored.
   */
  public static File getContainerMetaDataPath(String hddsVolumeDir,
                                              String scmId,
                                              long containerId) {
    String containerMetaDataPath =
        getBaseContainerLocation(hddsVolumeDir, scmId,
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
   * @param hddsVolumeDir
   * @param scmId
   * @param containerId
   * @return base directory for container.
   */
  private static String getBaseContainerLocation(String hddsVolumeDir,
                                                 String scmId,
                                                 long containerId) {
    Preconditions.checkNotNull(hddsVolumeDir, "Base Directory cannot be null");
    Preconditions.checkNotNull(scmId, "scmUuid cannot be null");
    Preconditions.checkState(containerId >= 0,
        "Container Id cannot be negative.");

    String containerSubDirectory = getContainerSubDirectory(containerId);

    String containerMetaDataPath = hddsVolumeDir  + File.separator + scmId +
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
   * Return containerDB File.
   */
  public static File getContainerDBFile(File containerMetaDataPath,
      long containerID) {
    return new File(containerMetaDataPath, containerID + OzoneConsts
        .DN_CONTAINER_DB);
  }
}
