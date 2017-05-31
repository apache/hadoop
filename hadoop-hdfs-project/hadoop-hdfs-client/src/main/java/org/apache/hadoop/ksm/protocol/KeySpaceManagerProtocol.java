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
package org.apache.hadoop.ksm.protocol;

import org.apache.hadoop.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ksm.helpers.KsmVolumeArgs;
import java.io.IOException;
import java.util.List;

/**
 * Protocol to talk to KSM.
 */
public interface KeySpaceManagerProtocol {

  /**
   * Creates a volume.
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  void createVolume(KsmVolumeArgs args) throws IOException;

  /**
   * Changes the owner of a volume.
   * @param volume  - Name of the volume.
   * @param owner - Name of the owner.
   * @throws IOException
   */
  void setOwner(String volume, String owner) throws IOException;

  /**
   * Changes the Quota on a volume.
   * @param volume - Name of the volume.
   * @param quota - Quota in bytes.
   * @throws IOException
   */
  void setQuota(String volume, long quota) throws IOException;

  /**
   * Checks if the specified user can access this volume.
   * @param volume - volume
   * @param userName - user name
   * @throws IOException
   */
  void checkVolumeAccess(String volume, String userName) throws IOException;

  /**
   * Gets the volume information.
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  KsmVolumeArgs getVolumeInfo(String volume) throws IOException;

  /**
   * Deletes an existing empty volume.
   * @param volume - Name of the volume.
   * @throws IOException
   */
  void deleteVolume(String volume) throws IOException;

  /**
   * Lists volume owned by a specific user.
   * @param userName - user name
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  List<KsmVolumeArgs> listVolumeByUser(String userName, String prefix, String
      prevKey, long maxKeys) throws IOException;

  /**
   * Lists volume all volumes in the cluster.
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  List<KsmVolumeArgs> listAllVolumes(String prefix, String
      prevKey, long maxKeys) throws IOException;

  /**
   * Creates a bucket.
   * @param bucketInfo - BucketInfo to create Bucket.
   * @throws IOException
   */
  void createBucket(KsmBucketInfo bucketInfo) throws IOException;

  /**
   * Gets the bucket information.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @return KsmBucketInfo or exception is thrown.
   * @throws IOException
   */
  KsmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException;

  /**
   * Sets bucket property from args.
   * @param args - BucketArgs.
   * @throws IOException
   */
  void setBucketProperty(KsmBucketArgs args) throws IOException;

  /**
   * Allocate a block to a container, the block is returned to the client.
   *
   * @param args the args of the key.
   * @return KsmKeyInfo isntacne that client uses to talk to container.
   * @throws IOException
   */
  KsmKeyInfo allocateKey(KsmKeyArgs args) throws IOException;

  /**
   * Look up for the container of an existing key.
   *
   * @param args the args of the key.
   * @return KsmKeyInfo isntacne that client uses to talk to container.
   * @throws IOException
   */
  KsmKeyInfo lookupKey(KsmKeyArgs args) throws IOException;

}
