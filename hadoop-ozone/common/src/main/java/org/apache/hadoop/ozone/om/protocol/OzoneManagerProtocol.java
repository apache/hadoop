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
package org.apache.hadoop.ozone.om.protocol;

import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo;
import java.io.IOException;
import java.util.List;

/**
 * Protocol to talk to OM.
 */
public interface OzoneManagerProtocol {

  /**
   * Creates a volume.
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  void createVolume(OmVolumeArgs args) throws IOException;

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
   * @param userAcl - user acls which needs to be checked for access
   * @return true if the user has required access for the volume,
   *         false otherwise
   * @throws IOException
   */
  boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException;

  /**
   * Gets the volume information.
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  OmVolumeArgs getVolumeInfo(String volume) throws IOException;

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
  List<OmVolumeArgs> listVolumeByUser(String userName, String prefix, String
      prevKey, int maxKeys) throws IOException;

  /**
   * Lists volume all volumes in the cluster.
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  List<OmVolumeArgs> listAllVolumes(String prefix, String
      prevKey, int maxKeys) throws IOException;

  /**
   * Creates a bucket.
   * @param bucketInfo - BucketInfo to create Bucket.
   * @throws IOException
   */
  void createBucket(OmBucketInfo bucketInfo) throws IOException;

  /**
   * Gets the bucket information.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @return OmBucketInfo or exception is thrown.
   * @throws IOException
   */
  OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException;

  /**
   * Sets bucket property from args.
   * @param args - BucketArgs.
   * @throws IOException
   */
  void setBucketProperty(OmBucketArgs args) throws IOException;

  /**
   * Open the given key and return an open key session.
   *
   * @param args the args of the key.
   * @return OpenKeySession instance that client uses to talk to container.
   * @throws IOException
   */
  OpenKeySession openKey(OmKeyArgs args) throws IOException;

  /**
   * Commit a key. This will make the change from the client visible. The client
   * is identified by the clientID.
   *
   * @param args the key to commit
   * @param clientID the client identification
   * @throws IOException
   */
  void commitKey(OmKeyArgs args, long clientID) throws IOException;

  /**
   * Allocate a new block, it is assumed that the client is having an open key
   * session going on. This block will be appended to this open key session.
   *
   * @param args the key to append
   * @param clientID the client identification
   * @return an allocated block
   * @throws IOException
   */
  OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID)
      throws IOException;

  /**
   * Look up for the container of an existing key.
   *
   * @param args the args of the key.
   * @return OmKeyInfo instance that client uses to talk to container.
   * @throws IOException
   */
  OmKeyInfo lookupKey(OmKeyArgs args) throws IOException;

  /**
   * Rename an existing key within a bucket.
   * @param args the args of the key.
   * @param toKeyName New name to be used for the Key
   * @throws IOException
   */
  void renameKey(OmKeyArgs args, String toKeyName) throws IOException;

  /**
   * Deletes an existing key.
   *
   * @param args the args of the key.
   * @throws IOException
   */
  void deleteKey(OmKeyArgs args) throws IOException;

  /**
   * Deletes an existing empty bucket from volume.
   * @param volume - Name of the volume.
   * @param bucket - Name of the bucket.
   * @throws IOException
   */
  void deleteBucket(String volume, String bucket) throws IOException;

  /**
   * Returns a list of buckets represented by {@link OmBucketInfo}
   * in the given volume. Argument volumeName is required, others
   * are optional.
   *
   * @param volumeName
   *   the name of the volume.
   * @param startBucketName
   *   the start bucket name, only the buckets whose name is
   *   after this value will be included in the result.
   * @param bucketPrefix
   *   bucket name prefix, only the buckets whose name has
   *   this prefix will be included in the result.
   * @param maxNumOfBuckets
   *   the maximum number of buckets to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listBuckets(String volumeName,
      String startBucketName, String bucketPrefix, int maxNumOfBuckets)
      throws IOException;

  /**
   * Returns a list of keys represented by {@link OmKeyInfo}
   * in the given bucket. Argument volumeName, bucketName is required,
   * others are optional.
   *
   * @param volumeName
   *   the name of the volume.
   * @param bucketName
   *   the name of the bucket.
   * @param startKeyName
   *   the start key name, only the keys whose name is
   *   after this value will be included in the result.
   * @param keyPrefix
   *   key name prefix, only the keys whose name has
   *   this prefix will be included in the result.
   * @param maxKeys
   *   the maximum number of keys to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  List<OmKeyInfo> listKeys(String volumeName,
      String bucketName, String startKeyName, String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * Returns list of Ozone services with its configuration details.
   *
   * @return list of Ozone services
   * @throws IOException
   */
  List<ServiceInfo> getServiceList() throws IOException;
}
