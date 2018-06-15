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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;

import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyUtils;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.apache.hadoop.ozone.container.common.impl.KeyValueContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Class which defines utility methods for KeyValueContainer.
 */

public final class KeyValueContainerUtil {

  /* Never constructed. */
  private KeyValueContainerUtil() {

  }

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueContainerUtil.class);


  public static void verifyIsNewContainer(File containerFile) throws
      FileAlreadyExistsException {
    Preconditions.checkNotNull(containerFile, "containerFile Should not be " +
        "null");
    if (containerFile.getParentFile().exists()) {
      LOG.error("container already exists on disk. File: {}", containerFile
          .toPath());
      throw new FileAlreadyExistsException("container already exists on " +
            "disk.");
    }
  }

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

    // Delete the Container MetaData path.
    FileUtils.deleteDirectory(containerMetaDataPath);

    //Delete the Container Chunks Path.
    FileUtils.deleteDirectory(chunksPath);

    //Delete Container directory
    FileUtils.deleteDirectory(containerMetaDataPath.getParentFile());

  }
}
