/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.security.acl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class representing an unique ozone object.
 * */
public abstract class OzoneObj implements IOzoneObj {

  private final ResourceType resType;

  private final StoreType storeType;

  OzoneObj(ResourceType resType, StoreType storeType) {

    Preconditions.checkNotNull(resType);
    Preconditions.checkNotNull(storeType);
    this.resType = resType;
    this.storeType = storeType;
  }

  public ResourceType getResourceType() {
    return resType;
  }

  @Override
  public String toString() {
    return "OzoneObj{" +
        "resType=" + resType +
        ", storeType=" + storeType +
        ", path='" + getPath() + '\'' +
        '}';
  }

  public StoreType getStoreType() {
    return storeType;
  }

  public abstract String getVolumeName();

  public abstract String getBucketName();

  public abstract String getKeyName();

  public abstract String getPath();

  /**
   * Ozone Objects supported for ACL.
   */
  public enum ResourceType {
    VOLUME(OzoneConsts.VOLUME),
    BUCKET(OzoneConsts.BUCKET),
    KEY(OzoneConsts.KEY);

    /**
     * String value for this Enum.
     */
    private final String value;

    @Override
    public String toString() {
      return value;
    }

    ResourceType(String resType) {
      value = resType;
    }
  }

  /**
   * Ozone Objects supported for ACL.
   */
  public enum StoreType {
    OZONE(OzoneConsts.OZONE),
    S3(OzoneConsts.S3);

    /**
     * String value for this Enum.
     */
    private final String value;

    @Override
    public String toString() {
      return value;
    }

    StoreType(String objType) {
      value = objType;
    }
  }
}
