/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.OzoneConsts.Versioning;
import org.apache.hadoop.ozone.io.OzoneInputStream;
import org.apache.hadoop.ozone.io.OzoneOutputStream;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * OzoneClient can connect to a Ozone Cluster and
 * perform basic operations.
 */
public interface OzoneClient {

  /**
   * Creates a new Volume.
   *
   * @param volumeName Name of the Volume
   *
   * @throws IOException
   */
  void createVolume(String volumeName)
      throws IOException;

  /**
   * Creates a new Volume, with owner set.
   *
   * @param volumeName Name of the Volume
   * @param owner Owner to be set for Volume
   *
   * @throws IOException
   */
  void createVolume(String volumeName, String owner)
      throws IOException;

  /**
   * Creates a new Volume, with owner and quota set.
   *
   * @param volumeName Name of the Volume
   * @param owner Owner to be set for Volume
   * @param acls ACLs to be added to the Volume
   *
   * @throws IOException
   */
  void createVolume(String volumeName, String owner,
                    OzoneAcl... acls)
      throws IOException;

  /**
   * Creates a new Volume, with owner and quota set.
   *
   * @param volumeName Name of the Volume
   * @param owner Owner to be set for Volume
   * @param quota Volume Quota
   *
   * @throws IOException
   */
  void createVolume(String volumeName, String owner,
                    long quota)
      throws IOException;

  /**
   * Creates a new Volume, with owner and quota set.
   *
   * @param volumeName Name of the Volume
   * @param owner Owner to be set for Volume
   * @param quota Volume Quota
   * @param acls ACLs to be added to the Volume
   *
   * @throws IOException
   */
  void createVolume(String volumeName, String owner,
                    long quota, OzoneAcl... acls)
      throws IOException;

  /**
   * Sets the owner of the volume.
   *
   * @param volumeName Name of the Volume
   * @param owner to be set for the Volume
   *
   * @throws IOException
   */
  void setVolumeOwner(String volumeName, String owner) throws IOException;

  /**
   * Set Volume Quota.
   *
   * @param volumeName Name of the Volume
   * @param quota Quota to be set for the Volume
   *
   * @throws IOException
   */
  void setVolumeQuota(String volumeName, long quota)
      throws IOException;

  /**
   * Returns {@link OzoneVolume}.
   *
   * @param volumeName Name of the Volume
   *
   * @return KsmVolumeArgs
   *
   * @throws OzoneVolume
   * */
  OzoneVolume getVolumeDetails(String volumeName)
      throws IOException;

  /**
   * Checks if a Volume exists and the user with a role specified has access
   * to the Volume.
   *
   * @param volumeName Name of the Volume
   * @param acl requested acls which needs to be checked for access
   *
   * @return Boolean - True if the user with a role can access the volume.
   * This is possible for owners of the volume and admin users
   *
   * @throws IOException
   */
  boolean checkVolumeAccess(String volumeName, OzoneAcl acl)
      throws IOException;

  /**
   * Deletes an Empty Volume.
   *
   * @param volumeName Name of the Volume
   *
   * @throws IOException
   */
  void deleteVolume(String volumeName) throws IOException;

  /**
   * Returns the List of Volumes owned by current user.
   *
   * @param volumePrefix Volume prefix to match
   *
   * @return KsmVolumeArgs Iterator
   *
   * @throws IOException
   */
  Iterator<OzoneVolume> listVolumes(String volumePrefix)
      throws IOException;

  /**
   * Returns the List of Volumes owned by the specific user.
   *
   * @param volumePrefix Volume prefix to match
   * @param user User Name
   *
   * @return KsmVolumeArgs Iterator
   *
   * @throws IOException
   */
  Iterator<OzoneVolume> listVolumes(String volumePrefix, String user)
      throws IOException;

  /**
   * Creates a new Bucket in the Volume.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   *
   * @throws IOException
   */
  void createBucket(String volumeName, String bucketName)
      throws IOException;

  /**
   * Creates a new Bucket in the Volume, with versioning set.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param versioning Bucket versioning
   *
   * @throws IOException
   */
  void createBucket(String volumeName, String bucketName,
                    Versioning versioning)
      throws IOException;

  /**
   * Creates a new Bucket in the Volume, with storage type set.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param storageType StorageType for the Bucket
   *
   * @throws IOException
   */
  void createBucket(String volumeName, String bucketName,
                    StorageType storageType)
      throws IOException;

  /**
   * Creates a new Bucket in the Volume, with ACLs set.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param acls OzoneAcls for the Bucket
   *
   * @throws IOException
   */
  void createBucket(String volumeName, String bucketName,
                           OzoneAcl... acls)
      throws IOException;


  /**
   * Creates a new Bucket in the Volume, with versioning
   * storage type and ACLs set.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param storageType StorageType for the Bucket
   *
   * @throws IOException
   */
  void createBucket(String volumeName, String bucketName,
                           OzoneConsts.Versioning versioning,
                           StorageType storageType, OzoneAcl... acls)
      throws IOException;

  /**
   * Adds or Removes ACLs from a Bucket.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   *
   * @throws IOException
   */
  void addBucketAcls(String volumeName, String bucketName,
                     List<OzoneAcl> addAcls)
      throws IOException;

  /**
   * Adds or Removes ACLs from a Bucket.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   *
   * @throws IOException
   */
  void removeBucketAcls(String volumeName, String bucketName,
                        List<OzoneAcl> removeAcls)
      throws IOException;


  /**
   * Enables or disables Bucket Versioning.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   *
   * @throws IOException
   */
  void setBucketVersioning(String volumeName, String bucketName,
                           Versioning versioning)
      throws IOException;

  /**
   * Sets the Storage Class of a Bucket.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   *
   * @throws IOException
   */
  void setBucketStorageType(String volumeName, String bucketName,
                            StorageType storageType)
      throws IOException;

  /**
   * Deletes a bucket if it is empty.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   *
   * @throws IOException
   */
  void deleteBucket(String volumeName, String bucketName)
      throws IOException;

  /**
   * true if the bucket exists and user has read access
   * to the bucket else throws Exception.
   *
   * @param volumeName Name of the Volume
   *
   * @throws IOException
   */
  void checkBucketAccess(String volumeName, String bucketName)
      throws IOException;

    /**
     * Returns {@link OzoneBucket}.
     *
     * @param volumeName Name of the Volume
     * @param bucketName Name of the Bucket
     *
     * @return OzoneBucket
     *
     * @throws IOException
     */
  OzoneBucket getBucketDetails(String volumeName, String bucketName)
        throws IOException;

  /**
   * Returns the List of Buckets in the Volume.
   *
   * @param volumeName Name of the Volume
   * @param bucketPrefix Bucket prefix to match
   *
   * @return KsmVolumeArgs Iterator
   *
   * @throws IOException
   */
  Iterator<OzoneBucket> listBuckets(String volumeName, String bucketPrefix)
      throws IOException;

  /**
   * Writes a key in an existing bucket.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param size Size of the data
   *
   * @return OutputStream
   *
   */
  OzoneOutputStream createKey(String volumeName, String bucketName,
                              String keyName, long size)
      throws IOException;

  /**
   * Reads a key from an existing bucket.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   *
   * @return LengthInputStream
   *
   * @throws IOException
   */
  OzoneInputStream getKey(String volumeName, String bucketName, String keyName)
      throws IOException;


  /**
   * Deletes an existing key.
   *
   * @param volumeName Name of the Volume
   *
   * @throws IOException
   */
  void deleteKey(String volumeName, String bucketName, String keyName)
      throws IOException;


  /**
   * Returns list of {@link OzoneKey} in Volume/Bucket.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   *
   * @return OzoneKey
   *
   * @throws IOException
   */
  List<OzoneKey> listKeys(String volumeName, String bucketName,
                            String keyPrefix)
      throws IOException;


  /**
   * Get OzoneKey.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Key name
   *
   * @return OzoneKey
   *
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
