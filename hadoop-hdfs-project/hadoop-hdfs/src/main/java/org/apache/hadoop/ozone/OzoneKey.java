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

package org.apache.hadoop.ozone;

import org.apache.hadoop.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ksm.helpers.KsmKeyLocationInfo;

import java.util.List;

/**
 * A class that encapsulates OzoneKey.
 */
public class OzoneKey {

  /**
   * Name of the Volume the Key belongs to.
   */
  private final String volumeName;
  /**
   * Name of the Bucket the Key belongs to.
   */
  private final String bucketName;
  /**
   * Name of the Key.
   */
  private final String keyName;
  /**
   * Size of the data.
   */
  private final long dataSize;

  /**
   * All the locations of this key, in an ordered list.
   */
  private final List<KsmKeyLocationInfo> keyLocations;
  /**
   * Constructs OzoneKey from KsmKeyInfo.
   *
   * @param ksmKeyInfo
   */
  public OzoneKey(KsmKeyInfo ksmKeyInfo) {
    this.volumeName = ksmKeyInfo.getVolumeName();
    this.bucketName = ksmKeyInfo.getBucketName();
    this.keyName = ksmKeyInfo.getKeyName();
    this.dataSize = ksmKeyInfo.getDataSize();
    this.keyLocations = ksmKeyInfo.getKeyLocationList();
  }

  /**
   * Returns Volume Name associated with the Key.
   *
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns Bucket Name associated with the Key.
   *
   * @return bucketName
   */
  public String getBucketName(){
    return bucketName;
  }

  /**
   * Returns the Key Name.
   *
   * @return keyName
   */
  public String getKeyName() {
    return keyName;
  }

  /**
   * Returns the size of the data.
   *
   * @return dataSize
   */
  public long getDataSize() {
    return dataSize;
  }

  /**
   * Retruns the list of the key locations.
   *
   * @return key locations
   */
  public List<KsmKeyLocationInfo> getKeyLocations() {
    return keyLocations;
  }
}
