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

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.client.ReplicationType;

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
  private final String name;
  /**
   * Size of the data.
   */
  private final long dataSize;
  /**
   * Creation time of the key.
   */
  private long creationTime;
  /**
   * Modification time of the key.
   */
  private long modificationTime;

  private ReplicationType replicationType;

  /**
   * Constructs OzoneKey from OmKeyInfo.
   *
   */
  public OzoneKey(String volumeName, String bucketName,
                  String keyName, long size, long creationTime,
                  long modificationTime, ReplicationType type) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.name = keyName;
    this.dataSize = size;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.replicationType = type;
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
  public String getName() {
    return name;
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
   * Returns the creation time of the key.
   *
   * @return creation time
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns the modification time of the key.
   *
   * @return modification time
   */
  public long getModificationTime() {
    return modificationTime;
  }

  /**
   * Returns the replication type of the key.
   *
   * @return replicationType
   */
  public ReplicationType getReplicationType() {
    return replicationType;
  }
}
