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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.hadoop.security.token.Token;

/**
 * ObjectStore class is responsible for the client operations that can be
 * performed on Ozone Object Store.
 */
public class ObjectStore {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  // TODO: remove rest api and client
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
    this.proxy = TracingUtil.createProxy(proxy, ClientProtocol.class, conf);
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
  }

  @VisibleForTesting
  protected ObjectStore() {
    proxy = null;
  }

  @VisibleForTesting
  public ClientProtocol getClientProxy() {
    return proxy;
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
  @SuppressWarnings("StringSplitter")
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
  @SuppressWarnings("StringSplitter")
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

  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    return proxy.getS3Secret(kerberosID);
  }

  /**
   * Returns Iterator to iterate over all buckets for a user.
   * The result can be restricted using bucket prefix, will return all
   * buckets if bucket prefix is null.
   *
   * @param userName user name
   * @param bucketPrefix Bucket prefix to match
   * @return {@code Iterator<OzoneBucket>}
   */
  public Iterator<? extends OzoneBucket> listS3Buckets(String userName,
                                                       String bucketPrefix) {
    return listS3Buckets(userName, bucketPrefix, null);
  }

  /**
   * Returns Iterator to iterate over all buckets after prevBucket for a
   * specific user. If prevBucket is null it returns an iterator to iterate over
   * all the buckets of a user. The result can be restricted using bucket
   * prefix, will return all buckets if bucket prefix is null.
   *
   * @param userName user name
   * @param bucketPrefix Bucket prefix to match
   * @param prevBucket Buckets are listed after this bucket
   * @return {@code Iterator<OzoneBucket>}
   */
  public Iterator<? extends OzoneBucket> listS3Buckets(String userName,
                                                       String bucketPrefix,
                                                       String prevBucket) {
    return new S3BucketIterator(userName, bucketPrefix, prevBucket);
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

  public KeyProvider getKeyProvider() throws IOException {
    return proxy.getKeyProvider();
  }

  public URI getKeyProviderUri() throws IOException {
    return proxy.getKeyProviderUri();
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

  /**
   * An Iterator to iterate over {@link OzoneBucket} list.
   */
  public class S3BucketIterator implements Iterator<OzoneBucket> {

    private String bucketPrefix = null;
    private String userName;

    private Iterator<OzoneBucket> currentIterator;
    private OzoneBucket currentValue;


    /**
     * Creates an Iterator to iterate over all buckets after prevBucket for
     * a user. If prevBucket is null it returns an iterator which list all
     * the buckets of the user.
     * The returned buckets match bucket prefix.
     * @param user
     * @param bucketPrefix
     * @param prevBucket
     */
    public S3BucketIterator(String user, String bucketPrefix, String
        prevBucket) {
      Objects.requireNonNull(user);
      this.userName = user;
      this.bucketPrefix = bucketPrefix;
      this.currentValue = null;
      this.currentIterator = getNextListOfS3Buckets(prevBucket).iterator();
    }

    @Override
    public boolean hasNext() {
      if(!currentIterator.hasNext()) {
        currentIterator = getNextListOfS3Buckets(
            currentValue != null ? currentValue.getName() : null)
            .iterator();
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzoneBucket next() {
      if(hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Gets the next set of bucket list using proxy.
     * @param prevBucket
     * @return {@code List<OzoneVolume>}
     */
    private List<OzoneBucket> getNextListOfS3Buckets(String prevBucket) {
      try {
        return proxy.listS3Buckets(userName, bucketPrefix, prevBucket,
            listCacheSize);
      } catch (OMException e) {
        if (e.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
          return new ArrayList<>();
        } else {
          throw new RuntimeException(e);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Get a valid Delegation Token.
   *
   * @param renewer the designated renewer for the token
   * @return Token<OzoneDelegationTokenSelector>
   * @throws IOException
   */
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return proxy.getDelegationToken(renewer);
  }

  /**
   * Renew an existing delegation token.
   *
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   */
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    return proxy.renewDelegationToken(token);
  }

  /**
   * Cancel an existing delegation token.
   *
   * @param token delegation token
   * @throws IOException
   */
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    proxy.cancelDelegationToken(token);
  }

  /**
   * @return canonical service name of ozone delegation token.
   */
  public String getCanonicalServiceName() {
    return proxy.getCanonicalServiceName();
  }

}
