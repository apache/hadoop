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

package org.apache.hadoop.ozone.web.interfaces;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Storage handler Interface is the Interface between
 * REST protocol and file system.
 *
 * We will have two default implementations of this interface.
 * One for the local file system that is handy while testing
 * and another which will point to the HDFS backend.
 */
@InterfaceAudience.Private
public interface StorageHandler extends Closeable{

  /**
   * Creates a Storage Volume.
   *
   * @param args - Volume Name
   *
   * @throws IOException
   * @throws OzoneException
   */
  void createVolume(VolumeArgs args) throws IOException, OzoneException;


  /**
   * setVolumeOwner - sets the owner of the volume.
   *
   * @param args owner info is present in the args
   *
   * @throws IOException
   * @throws OzoneException
   */
  void setVolumeOwner(VolumeArgs args) throws IOException, OzoneException;


  /**
   * Set Volume Quota.
   *
   * @param args - Has Quota info
   * @param remove - true if the request is to remove the quota
   *
   * @throws IOException
   * @throws OzoneException
   */
  void setVolumeQuota(VolumeArgs args, boolean remove)
      throws IOException, OzoneException;

  /**
   * Checks if a Volume exists and the user with a role specified has access
   * to the Volume.
   *
   * @param volume - Volume Name whose access permissions needs to be checked
   * @param acl - requested acls which needs to be checked for access
   *
   * @return - Boolean - True if the user with a role can access the volume.
   * This is possible for owners of the volume and admin users
   *
   * @throws IOException
   * @throws OzoneException
   */
  boolean checkVolumeAccess(String volume, OzoneAcl acl)
      throws IOException, OzoneException;


  /**
   * Returns the List of Volumes owned by the specific user.
   *
   * @param args - ListArgs
   *
   * @return - List of Volumes
   *
   * @throws IOException
   * @throws OzoneException
   */
  ListVolumes listVolumes(ListArgs args) throws IOException, OzoneException;

  /**
   * Deletes an Empty Volume.
   *
   * @param args - Volume Args
   *
   * @throws IOException
   * @throws OzoneException
   */
  void deleteVolume(VolumeArgs args) throws IOException, OzoneException;


  /**
   * Returns Info about the specified Volume.
   *
   * @param args - Volume Args
   *
   * @return VolumeInfo
   *
   * @throws IOException
   * @throws OzoneException
   */
  VolumeInfo getVolumeInfo(VolumeArgs args) throws IOException, OzoneException;

  /**
   * Creates a Bucket in specified Volume.
   *
   * @param args BucketArgs- BucketName, UserName and Acls
   *
   * @throws IOException
   */
  void createBucket(BucketArgs args) throws IOException, OzoneException;

  /**
   * Adds or Removes ACLs from a Bucket.
   *
   * @param args - BucketArgs
   *
   * @throws IOException
   */
  void setBucketAcls(BucketArgs args) throws IOException, OzoneException;

  /**
   * Enables or disables Bucket Versioning.
   *
   * @param args - BucketArgs
   *
   * @throws IOException
   */
  void setBucketVersioning(BucketArgs args) throws IOException, OzoneException;

  /**
   * Sets the Storage Class of a Bucket.
   *
   * @param args - BucketArgs
   *
   * @throws IOException
   */
  void setBucketStorageClass(BucketArgs args)
      throws IOException, OzoneException;

  /**
   * Deletes a bucket if it is empty.
   *
   * @param args Bucket args structure
   *
   * @throws IOException
   */
  void deleteBucket(BucketArgs args) throws IOException, OzoneException;

  /**
   * true if the bucket exists and user has read access
   * to the bucket else throws Exception.
   *
   * @param args Bucket args structure
   *
   * @throws IOException
   */
  void checkBucketAccess(BucketArgs args) throws IOException, OzoneException;


  /**
   * Returns all Buckets of a specified Volume.
   *
   * @param listArgs -- List Args.
   *
   * @return ListAllBuckets
   *
   * @throws OzoneException
   */
  ListBuckets listBuckets(ListArgs listArgs) throws
      IOException, OzoneException;


  /**
   * Returns Bucket's Metadata as a String.
   *
   * @param args Bucket args structure
   *
   * @return Info about the bucket
   *
   * @throws IOException
   */
  BucketInfo getBucketInfo(BucketArgs args) throws IOException, OzoneException;

  /**
   * Writes a key in an existing bucket.
   *
   * @param args KeyArgs
   *
   * @return InputStream
   *
   * @throws OzoneException
   */
  OutputStream newKeyWriter(KeyArgs args)
      throws IOException, OzoneException;


  /**
   * Tells the file system that the object has been written out
   * completely and it can do any house keeping operation that needs
   * to be done.
   *
   * @param args Key Args
   *
   * @param stream
   * @throws IOException
   */
  void commitKey(KeyArgs args, OutputStream stream)
      throws IOException, OzoneException;


  /**
   * Reads a key from an existing bucket.
   *
   * @param args KeyArgs
   *
   * @return LengthInputStream
   *
   * @throws IOException
   */
  LengthInputStream newKeyReader(KeyArgs args)
      throws IOException, OzoneException;


  /**
   * Deletes an existing key.
   *
   * @param args KeyArgs
   *
   * @throws OzoneException
   */
  void deleteKey(KeyArgs args) throws IOException, OzoneException;

  /**
   * Renames an existing key within a bucket.
   *
   * @param args KeyArgs
   * @param toKeyName New name to be used for the key
   * @throws OzoneException
   */
  void renameKey(KeyArgs args, String toKeyName)
      throws IOException, OzoneException;

  /**
   * Returns a list of Key.
   *
   * @param args KeyArgs
   *
   * @return BucketList
   *
   * @throws IOException
   */
  ListKeys listKeys(ListArgs args) throws IOException, OzoneException;

  /**
   * Get information of the specified Key.
   *
   * @param args Key Args
   *
   * @return KeyInfo
   *
   * @throws IOException
   * @throws OzoneException
   */
  KeyInfo getKeyInfo(KeyArgs args) throws IOException, OzoneException;

  /**
   * Get detail information of the specified Key.
   *
   * @param args Key Args
   *
   * @return KeyInfo
   *
   * @throws IOException
   * @throws OzoneException
   */
  KeyInfo getKeyInfoDetails(KeyArgs args) throws IOException, OzoneException;

  /**
   * Closes all the opened resources.
   */
  @Override
  void close();
}
