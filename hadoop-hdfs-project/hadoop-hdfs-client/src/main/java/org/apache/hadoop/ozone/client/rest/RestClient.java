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

package org.apache.hadoop.ozone.client.rest;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneQuota;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.ReplicationFactor;
import org.apache.hadoop.ozone.client.ReplicationType;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;

import java.io.IOException;
import java.util.List;

/**
 * Ozone Client REST protocol implementation. It uses REST protocol to
 * connect to Ozone Handler that executes client calls
 */
public class RestClient implements ClientProtocol {

   /**
    * Creates RestClient instance with the given configuration.
    * @param conf Configuration
    * @throws IOException
    */
  public RestClient(Configuration conf)
      throws IOException {
    Preconditions.checkNotNull(conf);
  }

  @Override
  public void createVolume(String volumeName) throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void createVolume(
      String volumeName, org.apache.hadoop.ozone.client.VolumeArgs args)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setVolumeOwner(String volumeName, String owner)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setVolumeQuota(String volumeName, OzoneQuota quota)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneVolume getVolumeDetails(String volumeName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public boolean checkVolumeAccess(String volumeName, OzoneAcl acl)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteVolume(String volumeName) throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public List<OzoneVolume> listVolumes(String volumePrefix, String prevKey,
                                       int maxListResult)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public List<OzoneVolume> listVolumes(String user, String volumePrefix,
                                       String prevKey, int maxListResult)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void createBucket(String volumeName, String bucketName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void createBucket(
      String volumeName, String bucketName, BucketArgs bucketArgs)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void addBucketAcls(
      String volumeName, String bucketName, List<OzoneAcl> addAcls)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void removeBucketAcls(
      String volumeName, String bucketName, List<OzoneAcl> removeAcls)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setBucketVersioning(
      String volumeName, String bucketName, Boolean versioning)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setBucketStorageType(
      String volumeName, String bucketName, StorageType storageType)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteBucket(String volumeName, String bucketName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void checkBucketAccess(String volumeName, String bucketName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneBucket getBucketDetails(String volumeName, String bucketName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix,
                                       String prevBucket, int maxListResult)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  /**
   * Writes a key in an existing bucket.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @param type
   * @param factor @return {@link OzoneOutputStream}
   */
  @Override
  public OzoneOutputStream createKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationType type, ReplicationFactor factor)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneInputStream getKey(
      String volumeName, String bucketName, String keyName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteKey(String volumeName, String bucketName, String keyName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public List<OzoneKey> listKeys(String volumeName, String bucketName,
                                 String keyPrefix, String prevKey,
                                 int maxListResult)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneKey getKeyDetails(
      String volumeName, String bucketName, String keyName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void close() throws IOException {
  }
}
