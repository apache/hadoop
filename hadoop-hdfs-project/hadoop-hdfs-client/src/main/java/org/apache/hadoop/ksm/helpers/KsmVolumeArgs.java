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
package org.apache.hadoop.ksm.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.KeyValue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * A class that encapsulates the KsmVolumeArgs Args.
 */
public final class KsmVolumeArgs {
  private final String adminName;
  private final String ownerName;
  private final String volume;
  private final long quotaInBytes;
  private final Map<String, String> keyValueMap;

  /**
   * Private constructor, constructed via builder.
   * @param adminName  - Administrator's name.
   * @param ownerName  - Volume owner's name
   * @param volume - volume name
   * @param quotaInBytes - Volume Quota in bytes.
   * @param  keyValueMap - keyValue map.
   */
  private KsmVolumeArgs(String adminName, String ownerName, String volume,
                        long quotaInBytes, Map<String, String> keyValueMap) {
    this.adminName = adminName;
    this.ownerName = ownerName;
    this.volume = volume;
    this.quotaInBytes = quotaInBytes;
    this.keyValueMap = keyValueMap;
  }

  /**
   * Returns the Admin Name.
   * @return String.
   */
  public String getAdminName() {
    return adminName;
  }

  /**
   * Returns the owner Name.
   * @return String
   */
  public String getOwnerName() {
    return ownerName;
  }

  /**
   * Returns the volume Name.
   * @return String
   */
  public String getVolume() {
    return volume;
  }

  /**
   * Returns Quota in Bytes.
   * @return long, Quota in bytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  public Map<String, String> getKeyValueMap() {
    return keyValueMap;
  }

  /**
   * Returns new builder class that builds a KsmVolumeArgs.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for KsmVolumeArgs.
   */
  public static class Builder {
    private String adminName;
    private String ownerName;
    private String volume;
    private long quotaInBytes;
    private Map<String, String> keyValueMap;

    /**
     * Constructs a builder.
     */
    Builder() {
      keyValueMap = new HashMap<>();
    }

    public Builder setAdminName(String adminName) {
      this.adminName = adminName;
      return this;
    }

    public Builder setOwnerName(String ownerName) {
      this.ownerName = ownerName;
      return this;
    }

    public Builder setVolume(String volume) {
      this.volume = volume;
      return this;
    }

    public Builder setQuotaInBytes(long quotaInBytes) {
      this.quotaInBytes = quotaInBytes;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      keyValueMap.put(key, value); // overwrite if present.
      return this;
    }

    /**
     * Constructs a CreateVolumeArgument.
     * @return CreateVolumeArgs.
     */
    public KsmVolumeArgs build() {
      Preconditions.checkNotNull(adminName);
      Preconditions.checkNotNull(ownerName);
      Preconditions.checkNotNull(volume);
      return new KsmVolumeArgs(adminName, ownerName, volume, quotaInBytes,
          keyValueMap);
    }
  }

  public VolumeInfo getProtobuf() {
    List<KeyValue> list = new LinkedList<>();
    for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
      list.add(KeyValue.newBuilder().setKey(entry.getKey()).
          setValue(entry.getValue()).build());
    }

    return VolumeInfo.newBuilder()
        .setAdminName(adminName)
        .setOwnerName(ownerName)
        .setVolume(volume)
        .setQuotaInBytes(quotaInBytes)
        .addAllMetadata(list)
        .build();
  }

  public static KsmVolumeArgs getFromProtobuf(VolumeInfo volInfo) {
    return new KsmVolumeArgs(volInfo.getAdminName(), volInfo.getOwnerName(),
        volInfo.getVolume(), volInfo.getQuotaInBytes(),
        volInfo.getMetadataList().stream()
            .collect(Collectors.toMap(KeyValue::getKey,
                KeyValue::getValue)));
  }
}
