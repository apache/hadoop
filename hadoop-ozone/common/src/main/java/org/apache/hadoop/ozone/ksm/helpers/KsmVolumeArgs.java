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
package org.apache.hadoop.ozone.ksm.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.KeyValue;

import java.io.IOException;
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
  private final long creationTime;
  private final long quotaInBytes;
  private final Map<String, String> keyValueMap;
  private final KsmOzoneAclMap aclMap;

  /**
   * Private constructor, constructed via builder.
   * @param adminName  - Administrator's name.
   * @param ownerName  - Volume owner's name
   * @param volume - volume name
   * @param quotaInBytes - Volume Quota in bytes.
   * @param keyValueMap - keyValue map.
   * @param aclMap - User to access rights map.
   * @param creationTime - Volume creation time.
   */
  private KsmVolumeArgs(String adminName, String ownerName, String volume,
                        long quotaInBytes, Map<String, String> keyValueMap,
                        KsmOzoneAclMap aclMap, long creationTime) {
    this.adminName = adminName;
    this.ownerName = ownerName;
    this.volume = volume;
    this.quotaInBytes = quotaInBytes;
    this.keyValueMap = keyValueMap;
    this.aclMap = aclMap;
    this.creationTime = creationTime;
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
   * Returns creation time.
   * @return long
   */
  public long getCreationTime() {
    return creationTime;
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

  public KsmOzoneAclMap getAclMap() {
    return aclMap;
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
    private long creationTime;
    private long quotaInBytes;
    private Map<String, String> keyValueMap;
    private KsmOzoneAclMap aclMap;

    /**
     * Constructs a builder.
     */
    Builder() {
      keyValueMap = new HashMap<>();
      aclMap = new KsmOzoneAclMap();
    }

    public Builder setAdminName(String admin) {
      this.adminName = admin;
      return this;
    }

    public Builder setOwnerName(String owner) {
      this.ownerName = owner;
      return this;
    }

    public Builder setVolume(String volumeName) {
      this.volume = volumeName;
      return this;
    }

    public Builder setCreationTime(long createdOn) {
      this.creationTime = createdOn;
      return this;
    }

    public Builder setQuotaInBytes(long quota) {
      this.quotaInBytes = quota;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      keyValueMap.put(key, value); // overwrite if present.
      return this;
    }

    public Builder addOzoneAcls(OzoneAclInfo acl) throws IOException {
      aclMap.addAcl(acl);
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
          keyValueMap, aclMap, creationTime);
    }
  }

  public VolumeInfo getProtobuf() {
    List<KeyValue> metadataList = new LinkedList<>();
    for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
      metadataList.add(KeyValue.newBuilder().setKey(entry.getKey()).
          setValue(entry.getValue()).build());
    }
    List<OzoneAclInfo> aclList = aclMap.ozoneAclGetProtobuf();

    return VolumeInfo.newBuilder()
        .setAdminName(adminName)
        .setOwnerName(ownerName)
        .setVolume(volume)
        .setQuotaInBytes(quotaInBytes)
        .addAllMetadata(metadataList)
        .addAllVolumeAcls(aclList)
        .setCreationTime(creationTime)
        .build();
  }

  public static KsmVolumeArgs getFromProtobuf(VolumeInfo volInfo) {
    Map<String, String> kvMap = volInfo.getMetadataList().stream()
        .collect(Collectors.toMap(KeyValue::getKey,
            KeyValue::getValue));
    KsmOzoneAclMap aclMap =
        KsmOzoneAclMap.ozoneAclGetFromProtobuf(volInfo.getVolumeAclsList());

    return new KsmVolumeArgs(volInfo.getAdminName(), volInfo.getOwnerName(),
        volInfo.getVolume(), volInfo.getQuotaInBytes(), kvMap, aclMap,
        volInfo.getCreationTime());
  }
}
