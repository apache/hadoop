/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class representing an ozone object.
 */
public final class OzoneObjInfo extends OzoneObj {

  private final String volumeName;
  private final String bucketName;
  private final String keyName;


  private OzoneObjInfo(ResourceType resType, StoreType storeType,
      String volumeName, String bucketName, String keyName) {
    super(resType, storeType);
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
  }

  @Override
  public String getPath() {
    switch (getResourceType()) {
    case VOLUME:
      return getVolumeName();
    case BUCKET:
      return getVolumeName() + OzoneConsts.OZONE_URI_DELIMITER
          + getBucketName();
    case KEY:
      return getVolumeName() + OzoneConsts.OZONE_URI_DELIMITER
          + getBucketName() + OzoneConsts.OZONE_URI_DELIMITER + getKeyName();
    default:
      throw new IllegalArgumentException("Unknown resource " +
        "type" + getResourceType());
    }

  }

  @Override
  public String getVolumeName() {
    return volumeName;
  }

  @Override
  public String getBucketName() {
    return bucketName;
  }

  @Override
  public String getKeyName() {
    return keyName;
  }

  /**
   * Inner builder class.
   */
  public static class Builder {

    private OzoneObj.ResourceType resType;
    private OzoneObj.StoreType storeType;
    private String volumeName;
    private String bucketName;
    private String keyName;

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder setResType(OzoneObj.ResourceType res) {
      this.resType = res;
      return this;
    }

    public Builder setStoreType(OzoneObj.StoreType store) {
      this.storeType = store;
      return this;
    }

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setKeyName(String key) {
      this.keyName = key;
      return this;
    }

    public OzoneObjInfo build() {
      return new OzoneObjInfo(resType, storeType, volumeName, bucketName,
          keyName);
    }
  }

}
