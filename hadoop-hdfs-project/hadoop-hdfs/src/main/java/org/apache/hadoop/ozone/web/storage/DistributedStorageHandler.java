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

package org.apache.hadoop.ozone.web.storage;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link StorageHandler} implementation that distributes object storage
 * across the nodes of an HDFS cluster.
 */
public final class DistributedStorageHandler implements StorageHandler {

  @Override
  public void createVolume(VolumeArgs args) throws
      IOException, OzoneException {

  }

  @Override
  public void setVolumeOwner(VolumeArgs args) throws
      IOException, OzoneException {

  }

  @Override
  public void setVolumeQuota(VolumeArgs args, boolean remove)
      throws IOException, OzoneException {

  }

  @Override
  public boolean checkVolumeAccess(VolumeArgs args)
      throws IOException, OzoneException {
    return false;
  }

  @Override
  public ListVolumes listVolumes(UserArgs args)
      throws IOException, OzoneException {
    return null;
  }

  @Override
  public void deleteVolume(VolumeArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public VolumeInfo getVolumeInfo(VolumeArgs args)
      throws IOException, OzoneException {
    return null;
  }

  @Override
  public void createBucket(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public void setBucketAcls(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public void setBucketVersioning(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public void setBucketStorageClass(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public void deleteBucket(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public void checkBucketAccess(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public ListBuckets listBuckets(VolumeArgs args)
      throws IOException, OzoneException {
    return null;
  }

  @Override
  public BucketInfo getBucketInfo(BucketArgs args)
      throws IOException, OzoneException {
    return null;
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
    return null;
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
    return null;
  }

  /**
   * Deletes an existing key.
   *
   * @param args KeyArgs
   * @throws OzoneException
   */
  @Override
  public void deleteKey(KeyArgs args) throws IOException, OzoneException {

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
    return null;
  }
}
