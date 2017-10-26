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

package org.apache.hadoop.ozone.client.protocol;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneQuota;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.ReplicationFactor;
import org.apache.hadoop.ozone.client.ReplicationType;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

import java.io.IOException;
import java.util.List;

/**
 * An implementer of this interface is capable of connecting to Ozone Cluster
 * and perform client operations. The protocol used for communication is
 * determined by the implementation class specified by
 * property <code>ozone.client.protocol</code>. The build-in implementation
 * includes: {@link org.apache.hadoop.ozone.client.rpc.RpcClient} for RPC and
 * {@link  org.apache.hadoop.ozone.client.rest.RestClient} for REST.
 */
public interface ClientProtocol {

  /**
   * Creates a new Volume.
   * @param volumeName Name of the Volume
   * @throws IOException
   */
  void createVolume(String volumeName)
      throws IOException;

  /**
   * Creates a new Volume with properties set in VolumeArgs.
   * @param volumeName Name of the Volume
   * @param args Properties to be set for the Volume
   * @throws IOException
   */
  void createVolume(String volumeName, VolumeArgs args)
      throws IOException;

  /**
   * Sets the owner of volume.
   * @param volumeName Name of the Volume
   * @param owner to be set for the Volume
   * @throws IOException
   */
  void setVolumeOwner(String volumeName, String owner) throws IOException;

  /**
   * Set Volume Quota.
   * @param volumeName Name of the Volume
   * @param quota Quota to be set for the Volume
   * @throws IOException
   */
  void setVolumeQuota(String volumeName, OzoneQuota quota)
      throws IOException;

  /**
   * Returns {@link OzoneVolume}.
   * @param volumeName Name of the Volume
   * @return {@link OzoneVolume}
   * @throws IOException
   * */
  OzoneVolume getVolumeDetails(String volumeName)
      throws IOException;

  /**
   * Checks if a Volume exists and the user with a role specified has access
   * to the Volume.
   * @param volumeName Name of the Volume
   * @param acl requested acls which needs to be checked for access
   * @return Boolean - True if the user with a role can access the volume.
   * This is possible for owners of the volume and admin users
   * @throws IOException
   */
  boolean checkVolumeAccess(String volumeName, OzoneAcl acl)
      throws IOException;

  /**
   * Deletes an empty Volume.
   * @param volumeName Name of the Volume
   * @throws IOException
   */
  void deleteVolume(String volumeName) throws IOException;

  /**
   * Lists all volumes in the cluster that matches the volumePrefix,
   * size of the returned list depends on maxListResult. If volume prefix
   * is null, returns all the volumes. The caller has to make multiple calls
   * to read all volumes.
   *
   * @param volumePrefix Volume prefix to match
   * @param prevVolume Starting point of the list, this volume is excluded
   * @param maxListResult Max number of volumes to return.
   * @return {@code List<OzoneVolume>}
   * @throws IOException
   */
  List<OzoneVolume> listVolumes(String volumePrefix, String prevVolume,
                                int maxListResult)
      throws IOException;

  /**
   * Lists all volumes in the cluster that are owned by the specified
   * user and matches the volumePrefix, size of the returned list depends on
   * maxListResult. If the user is null, return volumes owned by current user.
   * If volume prefix is null, returns all the volumes. The caller has to make
   * multiple calls to read all volumes.
   *
   * @param user User Name
   * @param volumePrefix Volume prefix to match
   * @param prevVolume Starting point of the list, this volume is excluded
   * @param maxListResult Max number of volumes to return.
   * @return {@code List<OzoneVolume>}
   * @throws IOException
   */
  List<OzoneVolume> listVolumes(String user, String volumePrefix,
                                    String prevVolume, int maxListResult)
      throws IOException;

  /**
   * Creates a new Bucket in the Volume.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @throws IOException
   */
  void createBucket(String volumeName, String bucketName)
      throws IOException;

  /**
   * Creates a new Bucket in the Volume, with properties set in BucketArgs.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param bucketArgs Bucket Arguments
   * @throws IOException
   */
  void createBucket(String volumeName, String bucketName,
                    BucketArgs bucketArgs)
      throws IOException;

  /**
   * Adds ACLs to the Bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param addAcls ACLs to be added
   * @throws IOException
   */
  void addBucketAcls(String volumeName, String bucketName,
                     List<OzoneAcl> addAcls)
      throws IOException;

  /**
   * Removes ACLs from a Bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param removeAcls ACLs to be removed
   * @throws IOException
   */
  void removeBucketAcls(String volumeName, String bucketName,
                        List<OzoneAcl> removeAcls)
      throws IOException;


  /**
   * Enables or disables Bucket Versioning.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param versioning True to enable Versioning, False to disable.
   * @throws IOException
   */
  void setBucketVersioning(String volumeName, String bucketName,
                           Boolean versioning)
      throws IOException;

  /**
   * Sets the Storage Class of a Bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param storageType StorageType to be set
   * @throws IOException
   */
  void setBucketStorageType(String volumeName, String bucketName,
                            StorageType storageType)
      throws IOException;

  /**
   * Deletes a bucket if it is empty.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @throws IOException
   */
  void deleteBucket(String volumeName, String bucketName)
      throws IOException;

  /**
   * True if the bucket exists and user has read access
   * to the bucket else throws Exception.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @throws IOException
   */
  void checkBucketAccess(String volumeName, String bucketName)
      throws IOException;

  /**
   * Returns {@link OzoneBucket}.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @return {@link OzoneBucket}
   * @throws IOException
   */
  OzoneBucket getBucketDetails(String volumeName, String bucketName)
      throws IOException;

  /**
   * Returns the List of Buckets in the Volume that matches the bucketPrefix,
   * size of the returned list depends on maxListResult. The caller has to make
   * multiple calls to read all volumes.
   * @param volumeName Name of the Volume
   * @param bucketPrefix Bucket prefix to match
   * @param prevBucket Starting point of the list, this bucket is excluded
   * @param maxListResult Max number of buckets to return.
   * @return {@code List<OzoneBucket>}
   * @throws IOException
   */
  List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix,
                                String prevBucket, int maxListResult)
      throws IOException;

  /**
   * Writes a key in an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @return {@link OzoneOutputStream}
   *
   */
  OzoneOutputStream createKey(String volumeName, String bucketName,
                              String keyName, long size, ReplicationType type,
                              ReplicationFactor factor)
      throws IOException;

  /**
   * Reads a key from an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @return {@link OzoneInputStream}
   * @throws IOException
   */
  OzoneInputStream getKey(String volumeName, String bucketName, String keyName)
      throws IOException;


  /**
   * Deletes an existing key.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @throws IOException
   */
  void deleteKey(String volumeName, String bucketName, String keyName)
      throws IOException;


  /**
   * Returns list of Keys in {Volume/Bucket} that matches the keyPrefix,
   * size of the returned list depends on maxListResult. The caller has
   * to make multiple calls to read all keys.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyPrefix Bucket prefix to match
   * @param prevKey Starting point of the list, this key is excluded
   * @param maxListResult Max number of buckets to return.
   * @return {@code List<OzoneKey>}
   * @throws IOException
   */
  List<OzoneKey> listKeys(String volumeName, String bucketName,
                          String keyPrefix, String prevKey, int maxListResult)
      throws IOException;


  /**
   * Get OzoneKey.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Key name
   * @return {@link OzoneKey}
   * @throws IOException
   */
  OzoneKey getKeyDetails(String volumeName, String bucketName,
                         String keyName)
      throws IOException;

  /**
   * Close and release the resources.
   */
  void close() throws IOException;

}
