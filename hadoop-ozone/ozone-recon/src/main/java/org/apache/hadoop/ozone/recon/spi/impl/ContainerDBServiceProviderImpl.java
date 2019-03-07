/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.commons.compress.utils.CharsetNames.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.utils.MetaStoreIterator;
import org.apache.hadoop.utils.MetadataStore;

import com.google.common.primitives.Longs;

/**
 * Implementation of the Recon Container DB Service.
 */
@Singleton
public class ContainerDBServiceProviderImpl
    implements ContainerDBServiceProvider {

  private final static String KEY_DELIMITER = "_";

  @Inject
  private MetadataStore containerDBStore;

  /**
   * Concatenate the containerId and Key Prefix using a delimiter and store the
   * count into the container DB store.
   *
   * @param containerKeyPrefix the containerId, key-prefix tuple.
   * @param count              Count of Keys with that prefix.
   * @throws IOException
   */
  @Override
  public void storeContainerKeyMapping(ContainerKeyPrefix containerKeyPrefix,
                                       Integer count)
      throws IOException {
    byte[] containerIdBytes = Longs.toByteArray(containerKeyPrefix
        .getContainerId());
    byte[] keyPrefixBytes = (KEY_DELIMITER + containerKeyPrefix.getKeyPrefix())
        .getBytes(UTF_8);
    byte[] dbKey = ArrayUtils.addAll(containerIdBytes, keyPrefixBytes);
    byte[] dbValue = ByteBuffer.allocate(Integer.BYTES).putInt(count).array();
    containerDBStore.put(dbKey, dbValue);
  }

  /**
   * Put together the key from the passed in object and get the count from
   * the container DB store.
   *
   * @param containerKeyPrefix the containerId, key-prefix tuple.
   * @return count of keys matching the containerId, key-prefix.
   * @throws IOException
   */
  @Override
  public Integer getCountForForContainerKeyPrefix(
      ContainerKeyPrefix containerKeyPrefix) throws IOException {
    byte[] containerIdBytes = Longs.toByteArray(containerKeyPrefix
        .getContainerId());
    byte[] keyPrefixBytes = (KEY_DELIMITER + containerKeyPrefix
        .getKeyPrefix()).getBytes(UTF_8);
    byte[] dbKey = ArrayUtils.addAll(containerIdBytes, keyPrefixBytes);
    byte[] dbValue = containerDBStore.get(dbKey);
    return ByteBuffer.wrap(dbValue).getInt();
  }

  /**
   * Use the DB's prefix seek iterator to start the scan from the given
   * container ID prefix.
   *
   * @param containerId the given containerId.
   * @return Map of (Key-Prefix,Count of Keys).
   */
  @Override
  public Map<String, Integer> getKeyPrefixesForContainer(long containerId) {

    Map<String, Integer> prefixes = new HashMap<>();
    MetaStoreIterator<MetadataStore.KeyValue> containerIterator =
        containerDBStore.iterator();
    byte[] containerIdPrefixBytes = Longs.toByteArray(containerId);
    containerIterator.prefixSeek(containerIdPrefixBytes);
    while (containerIterator.hasNext()) {
      MetadataStore.KeyValue keyValue = containerIterator.next();
      byte[] containerKey = keyValue.getKey();
      long containerIdFromDB = ByteBuffer.wrap(ArrayUtils.subarray
          (containerKey, 0, Long.BYTES)).getLong();

      //The prefix seek only guarantees that the iterator's head will be
      // positioned at the first prefix match. We still have to check the key
      // prefix.
      if (containerIdFromDB == containerId) {
        byte[] keyPrefix = ArrayUtils.subarray(containerKey,
            containerIdPrefixBytes.length + 1,
            containerKey.length);
        prefixes.put(new String(keyPrefix), ByteBuffer.wrap(keyValue.getValue())
            .getInt());
      } else {
        break; //Break when the first mismatch occurs.
      }
    }
    return prefixes;
  }

}