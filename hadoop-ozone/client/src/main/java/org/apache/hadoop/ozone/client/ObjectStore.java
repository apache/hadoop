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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * ObjectStore class is responsible for the client operations that can be
 * performed on Ozone Object Store.
 */
public class ObjectStore {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  private final ClientProtocol proxy;

  /**
   * Cache size to be used for listVolume calls.
   */
  private int listCacheSize;

  /**
   * Creates an instance of ObjectStore.
   * @param conf Configuration object.
   * @param proxy ClientProtocol proxy.
   */
  public ObjectStore(Configuration conf, ClientProtocol proxy) {
    this.proxy = proxy;
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
  }

  @VisibleForTesting
  protected ObjectStore() {
    proxy = null;
  }

  /**
   * Creates the volume with default values.
   * @param volumeName Name of the volume to be created.
   * @throws IOException
   */
  public void createVolume(String volumeName) throws IOException {
    proxy.createVolume(volumeName);
  }

  /**
   * Creates the volume.
   * @param volumeName Name of the volume to be created.
   * @param volumeArgs Volume properties.
   * @throws IOException
   */
  public void createVolume(String volumeName, VolumeArgs volumeArgs)
      throws IOException {
    proxy.createVolume(volumeName, volumeArgs);
  }

  /**
   * Creates an S3 bucket inside Ozone manager and creates the mapping needed
   * to access via both S3 and Ozone.
   * @param userName - S3 user name.
   * @param s3BucketName - S3 bucket Name.
   * @throws IOException - On failure, throws an exception like Bucket exists.
   */
  public void createS3Bucket(String userName, String s3BucketName) throws
      IOException {
    proxy.createS3Bucket(userName, s3BucketName);
  }

  /**
   * Deletes an s3 bucket and removes mapping of Ozone volume/bucket.
   * @param bucketName - S3 Bucket Name.
   * @throws  IOException in case the bucket cannot be deleted.
   */
  public void deleteS3Bucket(String bucketName) throws IOException {
    proxy.deleteS3Bucket(bucketName);
  }

  /**
   * Returns the Ozone Namespace for the S3Bucket. It will return the
   * OzoneVolume/OzoneBucketName.
   * @param s3BucketName  - S3 Bucket Name.
   * @return String - The Ozone canonical name for this s3 bucket. This
   * string is useful for mounting an OzoneFS.
   * @throws IOException - Error is throw if the s3bucket does not exist.
   */
  public String getOzoneBucketMapping(String s3BucketName) throws IOException {
    return proxy.getOzoneBucketMapping(s3BucketName);
  }

  /**
   * Returns the corresponding Ozone volume given an S3 Bucket.
   * @param s3BucketName - S3Bucket Name.
   * @return String - Ozone Volume name.
   * @throws IOException - Throws if the s3Bucket does not exist.
   */
  public String getOzoneVolumeName(String s3BucketName) throws IOException {
    String mapping = getOzoneBucketMapping(s3BucketName);
    return mapping.split("/")[0];

  }

  /**
   * Returns the corresponding Ozone bucket name for the given S3 bucket.
   * @param s3BucketName - S3Bucket Name.
   * @return String - Ozone bucket Name.
   * @throws IOException - Throws if the s3bucket does not exist.
   */
  public String getOzoneBucketName(String s3BucketName) throws IOException {
    String mapping = getOzoneBucketMapping(s3BucketName);
    return mapping.split("/")[1];
  }


  /**
   * Returns the volume information.
   * @param volumeName Name of the volume.
   * @return OzoneVolume
   * @throws IOException
   */
  public OzoneVolume getVolume(String volumeName) throws IOException {
    OzoneVolume volume = proxy.getVolumeDetails(volumeName);
    return volume;
  }


  /**
   * Returns Iterator to iterate over all the volumes in object store.
   * The result can be restricted using volume prefix, will return all
   * volumes if volume prefix is null.
   *
   * @param volumePrefix Volume prefix to match
   * @return {@code Iterator<OzoneVolume>}
   */
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix)
      throws IOException {
    return listVolumes(volumePrefix, null);
  }

  /**
   * Returns Iterator to iterate over all the volumes after prevVolume in object
   * store. If prevVolume is null it iterates from the first volume.
   * The result can be restricted using volume prefix, will return all
   * volumes if volume prefix is null.
   *
   * @param volumePrefix Volume prefix to match
   * @param prevVolume Volumes will be listed after this volume name
   * @return {@code Iterator<OzoneVolume>}
   */
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix,
      String prevVolume) throws IOException {
    return new VolumeIterator(null, volumePrefix, prevVolume);
  }

  /**
   * Returns Iterator to iterate over the list of volumes after prevVolume owned
   * by a specific user. The result can be restricted using volume prefix, will
   * return all volumes if volume prefix is null. If user is not null, returns
   * the volume of current user.
   *
   * @param user User Name
   * @param volumePrefix Volume prefix to match
   * @param prevVolume Volumes will be listed after this volume name
   * @return {@code Iterator<OzoneVolume>}
   */
  public Iterator<? extends OzoneVolume> listVolumesByUser(String user,
      String volumePrefix, String prevVolume)
      throws IOException {
    if(Strings.isNullOrEmpty(user)) {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    }
    return new VolumeIterator(user, volumePrefix, prevVolume);
  }

  /**
   * Deletes the volume.
   * @param volumeName Name of the volume.
   * @throws IOException
   */
  public void deleteVolume(String volumeName) throws IOException {
    proxy.deleteVolume(volumeName);
  }

  /**
   * An Iterator to iterate over {@link OzoneVolume} list.
   */
  private class VolumeIterator implements Iterator<OzoneVolume> {

    private String user = null;
    private String volPrefix = null;

    private Iterator<OzoneVolume> currentIterator;
    private OzoneVolume currentValue;

    /**
     * Creates an Iterator to iterate over all volumes after
     * prevVolume of the user. If prevVolume is null it iterates from the
     * first volume. The returned volumes match volume prefix.
     * @param user user name
     * @param volPrefix volume prefix to match
     */
    VolumeIterator(String user, String volPrefix, String prevVolume) {
      this.user = user;
      this.volPrefix = volPrefix;
      this.currentValue = null;
      this.currentIterator = getNextListOfVolumes(prevVolume).iterator();
    }

    @Override
    public boolean hasNext() {
      if(!currentIterator.hasNext()) {
        currentIterator = getNextListOfVolumes(
            currentValue != null ? currentValue.getName() : null)
            .iterator();
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzoneVolume next() {
      if(hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Returns the next set of volume list using proxy.
     * @param prevVolume previous volume, this will be excluded from the result
     * @return {@code List<OzoneVolume>}
     */
    private List<OzoneVolume> getNextListOfVolumes(String prevVolume) {
      try {
        //if user is null, we do list of all volumes.
        if(user != null) {
          return proxy.listVolumes(user, volPrefix, prevVolume, listCacheSize);
        }
        return proxy.listVolumes(volPrefix, prevVolume, listCacheSize);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
