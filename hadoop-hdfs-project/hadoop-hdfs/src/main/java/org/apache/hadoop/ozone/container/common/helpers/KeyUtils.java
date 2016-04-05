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
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.LevelDBStore;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

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
   * @throws IOException
   */
  public static LevelDBStore getDB(ContainerData container,
                                   ContainerCache cache) throws IOException {
    Preconditions.checkNotNull(container);
    Preconditions.checkNotNull(cache);
    LevelDBStore db = cache.getDB(container.getContainerName());
    if (db == null) {
      db = getDB(container.getDBPath());
      cache.putDB(container.getContainerName(), db);
    }
    return db;
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
      getKeyDataResponse(ContainerProtos.ContainerCommandRequestProto msg
                       , KeyData data) {
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
