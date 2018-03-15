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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.utils.MetadataStore;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .Result.UNABLE_TO_READ_METADATA_DB;
import static org.apache.hadoop.hdsl.protocol.proto.ContainerProtos
    .Result.NO_SUCH_KEY;

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
   * Get a DB handler for a given container.
   * If the handler doesn't exist in cache yet, first create one and
   * add into cache. This function is called with containerManager
   * ReadLock held.
   *
   * @param container container.
   * @param conf configuration.
   * @return MetadataStore handle.
   * @throws StorageContainerException
   */
  public static MetadataStore getDB(ContainerData container,
      Configuration conf) throws StorageContainerException {
    Preconditions.checkNotNull(container);
    ContainerCache cache = ContainerCache.getInstance(conf);
    Preconditions.checkNotNull(cache);
    try {
      return cache.getDB(container.getContainerName(), container.getDBPath());
    } catch (IOException ex) {
      String message =
          String.format("Unable to open DB. DB Name: %s, Path: %s. ex: %s",
          container.getContainerName(), container.getDBPath(), ex.getMessage());
      throw new StorageContainerException(message, UNABLE_TO_READ_METADATA_DB);
    }
  }

  /**
   * Remove a DB handler from cache.
   *
   * @param container - Container data.
   * @param conf - Configuration.
   */
  public static void removeDB(ContainerData container,
      Configuration conf) {
    Preconditions.checkNotNull(container);
    ContainerCache cache = ContainerCache.getInstance(conf);
    Preconditions.checkNotNull(cache);
    cache.removeDB(container.getContainerName());
  }
  /**
   * Shutdown all DB Handles.
   *
   * @param cache - Cache for DB Handles.
   */
  @SuppressWarnings("unchecked")
  public static void shutdownCache(ContainerCache cache)  {
    cache.shutdownCache();
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

  /**
   * Parses the key name from a bytes array.
   * @param bytes key name in bytes.
   * @return key name string.
   */
  public static String getKeyName(byte[] bytes) {
    return new String(bytes, ENCODING);
  }

  /**
   * Parses the {@link KeyData} from a bytes array.
   *
   * @param bytes key data in bytes.
   * @return key data.
   * @throws IOException if the bytes array is malformed or invalid.
   */
  public static KeyData getKeyData(byte[] bytes) throws IOException {
    try {
      ContainerProtos.KeyData kd = ContainerProtos.KeyData.parseFrom(bytes);
      KeyData data = KeyData.getFromProtoBuf(kd);
      return data;
    } catch (IOException e) {
      throw new StorageContainerException("Failed to parse key data from the" +
              " bytes array.", NO_SUCH_KEY);
    }
  }
}
