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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.utils.db.Codec;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

/**
 * Codec to encode ContainerKeyPrefix as byte array.
 */
public class ContainerKeyPrefixCodec implements Codec<ContainerKeyPrefix>{

  private final static String KEY_DELIMITER = "_";

  @Override
  public byte[] toPersistedFormat(ContainerKeyPrefix containerKeyPrefix)
      throws IOException {
    Preconditions.checkNotNull(containerKeyPrefix,
            "Null object can't be converted to byte array.");
    byte[] containerIdBytes = Longs.toByteArray(containerKeyPrefix
        .getContainerId());

    //Prefix seek can be done only with containerId. In that case, we can
    // expect the key and version to be undefined.
    if (StringUtils.isNotEmpty(containerKeyPrefix.getKeyPrefix())) {
      byte[] keyPrefixBytes = (KEY_DELIMITER +
          containerKeyPrefix.getKeyPrefix()).getBytes(UTF_8);
      containerIdBytes = ArrayUtils.addAll(containerIdBytes, keyPrefixBytes);
    }

    if (containerKeyPrefix.getKeyVersion() != -1) {
      containerIdBytes = ArrayUtils.addAll(containerIdBytes, KEY_DELIMITER
          .getBytes(UTF_8));
      containerIdBytes = ArrayUtils.addAll(containerIdBytes, Longs.toByteArray(
          containerKeyPrefix.getKeyVersion()));
    }
    return containerIdBytes;
  }

  @Override
  public ContainerKeyPrefix fromPersistedFormat(byte[] rawData)
      throws IOException {

    // First 8 bytes is the containerId.
    long containerIdFromDB = ByteBuffer.wrap(ArrayUtils.subarray(
        rawData, 0, Long.BYTES)).getLong();
    // When reading from byte[], we can always expect to have the containerId,
    // key and version parts in the byte array.
    byte[] keyBytes = ArrayUtils.subarray(rawData,
        Long.BYTES + 1,
        rawData.length - Long.BYTES - 1);
    String keyPrefix = new String(keyBytes, UTF_8);

    // Last 8 bytes is the key version.
    byte[] versionBytes = ArrayUtils.subarray(rawData,
        rawData.length - Long.BYTES,
        rawData.length);
    long version = ByteBuffer.wrap(versionBytes).getLong();
    return new ContainerKeyPrefix(containerIdFromDB, keyPrefix, version);
  }
}
