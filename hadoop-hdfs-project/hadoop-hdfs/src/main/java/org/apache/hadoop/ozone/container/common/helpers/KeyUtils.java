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
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.KeyManagerImpl;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.utils.LevelDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.UNABLE_TO_READ_METADATA_DB;

/**
 * Utils functions to help key functions.
 */
public final class KeyUtils {
  public static final String ENCODING_NAME = "UTF-8";
  public static final Charset ENCODING = Charset.forName(ENCODING_NAME);

  /**
   * Never Constructed.
   */
  private KeyUtils() {
  }

  /**
   * Returns a file handle to LevelDB.
   *
   * @param dbPath - DbPath.
   * @return LevelDB
   */
  public static LevelDBStore getDB(String dbPath) throws IOException {
    Preconditions.checkNotNull(dbPath);
    Preconditions.checkState(!dbPath.isEmpty());
    return new LevelDBStore(new File(dbPath), false);
  }

  /**
   * This function is called with  containerManager ReadLock held.
   *
   * @param container - container.
   * @param cache     - cache
   * @return LevelDB handle.
   * @throws StorageContainerException
   */
  public static LevelDBStore getDB(ContainerData container,
                                   ContainerCache cache)
      throws StorageContainerException {
    Preconditions.checkNotNull(container);
    Preconditions.checkNotNull(cache);
    try {
      LevelDBStore db = cache.getDB(container.getContainerName());
      if (db == null) {
        db = getDB(container.getDBPath());
        cache.putDB(container.getContainerName(), db);
      }
      return db;
    } catch (IOException ex) {
      String message = "Unable to open DB. DB Name: %s, Path: %s. ex: %s"
          .format(container.getContainerName(), container.getDBPath(), ex);
      throw new StorageContainerException(message, UNABLE_TO_READ_METADATA_DB);
    }
  }

  /**
   * Shutdown all DB Handles.
   *
   * @param cache - Cache for DB Handles.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static void shutdownCache(ContainerCache cache)  {
    Logger log = LoggerFactory.getLogger(KeyManagerImpl.class);
    LevelDBStore[] handles = new LevelDBStore[cache.values().size()];
    cache.values().toArray(handles);
    Preconditions.checkState(handles.length == cache.values().size());
    for (LevelDBStore db : handles) {
      try {
        db.close();
      } catch (IOException ex) {
        log.error("error closing db. error {}", ex);
      }
    }
  }

  /**
   * Returns successful keyResponse.
   * @param msg - Request.
   * @return Response.
   */
  public static ContainerProtos.ContainerCommandResponseProto
      getKeyResponse(ContainerProtos.ContainerCommandRequestProto msg) {
    return ContainerUtils.getContainerResponse(msg);
  }


  public static ContainerProtos.ContainerCommandResponseProto
      getKeyDataResponse(ContainerProtos.ContainerCommandRequestProto msg,
      KeyData data) {
    ContainerProtos.GetKeyResponseProto.Builder getKey = ContainerProtos
        .GetKeyResponseProto.newBuilder();
    getKey.setKeyData(data.getProtoBufMessage());
    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        ContainerUtils.getContainerResponse(msg, ContainerProtos.Result
            .SUCCESS, "");
    builder.setGetKey(getKey);
    return  builder.build();
  }

}
