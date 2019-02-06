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
package org.apache.hadoop.ozone.om.helpers;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * A class that encapsulates the createVolume Args.
 */
public final class VolumeArgs {
  private final String adminName;
  private final String ownerName;
  private final String volume;
  private final long quotaInBytes;
  private final Map<String, String> extendedAttributes;

  /**
   * Private constructor, constructed via builder.
   *
   * @param adminName - Administrator name.
   * @param ownerName - Volume owner's name
   * @param volume - volume name
   * @param quotaInBytes - Volume Quota in bytes.
   * @param keyValueMap - keyValue map.
   */
  private VolumeArgs(String adminName, String ownerName, String volume,
      long quotaInBytes, Map<String, String> keyValueMap) {
    this.adminName = adminName;
    this.ownerName = ownerName;
    this.volume = volume;
    this.quotaInBytes = quotaInBytes;
    this.extendedAttributes = keyValueMap;
  }

  /**
   * Returns the Admin Name.
   *
   * @return String.
   */
  public String getAdminName() {
    return adminName;
  }

  /**
   * Returns the owner Name.
   *
   * @return String
   */
  public String getOwnerName() {
    return ownerName;
  }

  /**
   * Returns the volume Name.
   *
   * @return String
   */
  public String getVolume() {
    return volume;
  }

  /**
   * Returns Quota in Bytes.
   *
   * @return long, Quota in bytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  public Map<String, String> getExtendedAttributes() {
    return extendedAttributes;
  }

  static class Builder {
    private String adminName;
    private String ownerName;
    private String volume;
    private long quotaInBytes;
    private Map<String, String> extendedAttributes;

    /**
     * Constructs a builder.
     */
    Builder() {
      extendedAttributes = new HashMap<>();
    }

    public void setAdminName(String adminName) {
      this.adminName = adminName;
    }

    public void setOwnerName(String ownerName) {
      this.ownerName = ownerName;
    }

    public void setVolume(String volume) {
      this.volume = volume;
    }

    public void setQuotaInBytes(long quotaInBytes) {
      this.quotaInBytes = quotaInBytes;
    }

    public void addMetadata(String key, String value) {
      extendedAttributes.put(key, value); // overwrite if present.
    }

    /**
     * Constructs a CreateVolumeArgument.
     *
     * @return CreateVolumeArgs.
     */
    public VolumeArgs build() {
      Preconditions.checkNotNull(adminName);
      Preconditions.checkNotNull(ownerName);
      Preconditions.checkNotNull(volume);
      return new VolumeArgs(adminName, ownerName, volume, quotaInBytes,
          extendedAttributes);
    }
  }
}
