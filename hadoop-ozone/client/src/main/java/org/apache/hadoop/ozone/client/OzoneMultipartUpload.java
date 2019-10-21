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

import java.time.Instant;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;

/**
 * Information about one initialized upload.
 */
public class OzoneMultipartUpload {

  private String volumeName;

  private String bucketName;

  private String keyName;

  private String uploadId;

  private Instant creationTime;

  private ReplicationType replicationType;

  private ReplicationFactor replicationFactor;

  public OzoneMultipartUpload(String volumeName, String bucketName,
      String keyName, String uploadId, Instant creationTime,
      ReplicationType replicationType,
      ReplicationFactor replicationFactor) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.uploadId = uploadId;
    this.creationTime = creationTime;
    this.replicationType = replicationType;
    this.replicationFactor = replicationFactor;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public String getUploadId() {
    return uploadId;
  }

  public Instant getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(Instant creationTime) {
    this.creationTime = creationTime;
  }

  public ReplicationType getReplicationType() {
    return replicationType;
  }

  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
  }
}
