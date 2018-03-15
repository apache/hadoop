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

package org.apache.hadoop.ozone.web.localstorage;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;

import java.io.IOException;
import java.io.OutputStream;

/**
 * PLEASE NOTE : This file is a dummy backend for test purposes and prototyping
 * effort only. It does not handle any Object semantics correctly, neither does
 * it take care of security.
 */
@InterfaceAudience.Private
public class LocalStorageHandler implements StorageHandler {
  private final Configuration conf;

  /**
   * Constructs LocalStorageHandler.
   *
   * @param conf ozone conf.
   */
  public LocalStorageHandler(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Creates Storage Volume.
   *
   * @param args - volumeArgs
   * @throws IOException
   */
  @Override
  public void createVolume(VolumeArgs args) throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.createVolume(args);

  }

  /**
   * setVolumeOwner - sets the owner of the volume.
   *
   * @param args volumeArgs
   * @throws IOException
   */
  @Override
  public void setVolumeOwner(VolumeArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.setVolumeProperty(args, OzoneMetadataManager.VolumeProperty.OWNER);
  }

  /**
   * Set Volume Quota Info.
   *
   * @param args   - volumeArgs
   * @param remove - true if the request is to remove the quota
   * @throws IOException
   */
  @Override
  public void setVolumeQuota(VolumeArgs args, boolean remove)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);

    if (remove) {
      OzoneQuota quota = new OzoneQuota();
      args.setQuota(quota);
    }
    oz.setVolumeProperty(args, OzoneMetadataManager.VolumeProperty.QUOTA);
  }

  /**
   * Checks if a Volume exists and the user specified has access to the volume.
   *
   * @param volume - Volume Name
   * @param acl - Ozone acl which needs to be compared for access
   * @return - Boolean - True if the user can modify the volume. This is
   * possible for owners of the volume and admin users
   * @throws IOException
   */
  @Override
  public boolean checkVolumeAccess(String volume, OzoneAcl acl)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    return oz.checkVolumeAccess(volume, acl);
  }

  /**
   * Returns Info about the specified Volume.
   *
   * @param args - volumeArgs
   * @return VolumeInfo
   * @throws IOException
   */
  @Override
  public VolumeInfo getVolumeInfo(VolumeArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    return oz.getVolumeInfo(args);
  }

  /**
   * Deletes an Empty Volume.
   *
   * @param args - Volume Args
   * @throws IOException
   */
  @Override
  public void deleteVolume(VolumeArgs args) throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.deleteVolume(args);

  }

  /**
   * Returns the List of Volumes owned by the specific user.
   *
   * @param args - ListArgs
   * @return - List of Volumes
   * @throws IOException
   */
  @Override
  public ListVolumes listVolumes(ListArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    return oz.listVolumes(args);
  }

  /**
   * true if the bucket exists and user has read access to the bucket else
   * throws Exception.
   *
   * @param args Bucket args structure
   * @throws IOException
   */
  @Override
  public void checkBucketAccess(BucketArgs args)
      throws IOException, OzoneException {

  }

  /**
   * Creates a Bucket in specified Volume.
   *
   * @param args BucketArgs- BucketName, UserName and Acls
   * @throws IOException
   */
  @Override
  public void createBucket(BucketArgs args) throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.createBucket(args);
  }

  /**
   * Adds or Removes ACLs from a Bucket.
   *
   * @param args - BucketArgs
   * @throws IOException
   */
  @Override
  public void setBucketAcls(BucketArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.setBucketProperty(args, OzoneMetadataManager.BucketProperty.ACLS);
  }

  /**
   * Enables or disables Bucket Versioning.
   *
   * @param args - BucketArgs
   * @throws IOException
   */
  @Override
  public void setBucketVersioning(BucketArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.setBucketProperty(args, OzoneMetadataManager.BucketProperty.VERSIONING);

  }

  /**
   * Sets the Storage Class of a Bucket.
   *
   * @param args - BucketArgs
   * @throws IOException
   */
  @Override
  public void setBucketStorageClass(BucketArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.setBucketProperty(args, OzoneMetadataManager.BucketProperty.STORAGETYPE);

  }

  /**
   * Deletes a bucket if it is empty.
   *
   * @param args Bucket args structure
   * @throws IOException
   */
  @Override
  public void deleteBucket(BucketArgs args) throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.deleteBucket(args);
  }

  /**
   * Returns all Buckets of a specified Volume.
   *
   * @param args --User Args
   * @return ListAllBuckets
   * @throws OzoneException
   */
  @Override
  public ListBuckets listBuckets(ListArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    return oz.listBuckets(args);
  }

  /**
   * Returns Bucket's Metadata as a String.
   *
   * @param args Bucket args structure
   * @return Info about the bucket
   * @throws IOException
   */
  @Override
  public BucketInfo getBucketInfo(BucketArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    return oz.getBucketInfo(args);
  }

  /**
   * Writes a key in an existing bucket.
   *
   * @param args KeyArgs
   * @return InputStream
   * @throws OzoneException
   */
  @Override
  public OutputStream newKeyWriter(KeyArgs args) throws IOException,
      OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    return oz.createKey(args);
  }

  /**
   * Tells the file system that the object has been written out completely and
   * it can do any house keeping operation that needs to be done.
   *
   * @param args   Key Args
   * @param stream
   * @throws IOException
   */
  @Override
  public void commitKey(KeyArgs args, OutputStream stream) throws
      IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.commitKey(args, stream);

  }

  /**
   * Reads a key from an existing bucket.
   *
   * @param args KeyArgs
   * @return LengthInputStream
   * @throws IOException
   */
  @Override
  public LengthInputStream newKeyReader(KeyArgs args) throws IOException,
      OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    return oz.newKeyReader(args);
  }

  /**
   * Deletes an existing key.
   *
   * @param args KeyArgs
   * @throws OzoneException
   */
  @Override
  public void deleteKey(KeyArgs args) throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    oz.deleteKey(args);
  }

  /**
   * Returns a list of Key.
   *
   * @param args KeyArgs
   * @return BucketList
   * @throws IOException
   */
  @Override
  public ListKeys listKeys(ListArgs args) throws IOException, OzoneException {
    OzoneMetadataManager oz =
        OzoneMetadataManager.getOzoneMetadataManager(conf);
    return oz.listKeys(args);

  }

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
  @Override
  public KeyInfo getKeyInfo(KeyArgs args) throws IOException, OzoneException {
    OzoneMetadataManager oz = OzoneMetadataManager
        .getOzoneMetadataManager(conf);
    return oz.getKeyInfo(args);
  }

  @Override
  public void close() {
    //No resource to close, do nothing.
  }

}
