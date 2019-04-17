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
package org.apache.hadoop.ozone.s3.endpoint;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.header.AuthenticationHeaderParser;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic helpers for all the REST endpoints.
 */
public class EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(EndpointBase.class);

  @Inject
  private OzoneClient client;

  @Inject
  private AuthenticationHeaderParser authenticationHeaderParser;

  protected OzoneBucket getBucket(String volumeName, String bucketName)
      throws IOException {
    return getVolume(volumeName).getBucket(bucketName);
  }

  protected OzoneBucket getBucket(OzoneVolume volume, String bucketName)
      throws OS3Exception, IOException {
    OzoneBucket bucket;
    try {
      bucket = volume.getBucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName);
      } else {
        throw ex;
      }
    }
    return bucket;
  }

  protected OzoneBucket getBucket(String bucketName)
      throws OS3Exception, IOException {
    OzoneBucket bucket;
    try {
      OzoneVolume volume = getVolume(getOzoneVolumeName(bucketName));
      bucket = volume.getBucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND
          || ex.getResult() == ResultCodes.S3_BUCKET_NOT_FOUND) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName);
      } else {
        throw ex;
      }
    }
    return bucket;
  }

  protected OzoneVolume getVolume(String volumeName) throws IOException {
    OzoneVolume volume = null;
    try {
      volume = client.getObjectStore().getVolume(volumeName);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
        throw new NotFoundException("Volume " + volumeName + " is not found");
      } else {
        throw ex;
      }
    }
    return volume;
  }

  /**
   * Create an S3Bucket, and also it creates mapping needed to access via
   * ozone and S3.
   * @param userName
   * @param bucketName
   * @return location of the S3Bucket.
   * @throws IOException
   */
  protected String createS3Bucket(String userName, String bucketName) throws
      IOException {
    try {
      client.getObjectStore().createS3Bucket(userName, bucketName);
    } catch (OMException ex) {
      if (ex.getResult() != ResultCodes.S3_BUCKET_ALREADY_EXISTS) {
        // S3 does not return error for bucket already exists, it just
        // returns the location.
        throw ex;
      }
    }

    // Not required to call as bucketname is same, but calling now in future
    // if mapping changes we get right location.
    String location = client.getObjectStore().getOzoneBucketName(
        bucketName);
    return "/"+location;
  }

  /**
   * Deletes an s3 bucket and removes mapping of Ozone volume/bucket.
   * @param s3BucketName - S3 Bucket Name.
   * @throws  IOException in case the bucket cannot be deleted.
   */
  public void deleteS3Bucket(String s3BucketName)
      throws IOException {
    client.getObjectStore().deleteS3Bucket(s3BucketName);
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
    return client.getObjectStore().getOzoneBucketMapping(s3BucketName);
  }

  /**
   * Returns the corresponding Ozone volume given an S3 Bucket.
   * @param s3BucketName - S3Bucket Name.
   * @return String - Ozone Volume name.
   * @throws IOException - Throws if the s3Bucket does not exist.
   */
  public String getOzoneVolumeName(String s3BucketName) throws IOException {
    return client.getObjectStore().getOzoneVolumeName(s3BucketName);
  }

  /**
   * Returns the corresponding Ozone bucket name for the given S3 bucket.
   * @param s3BucketName - S3Bucket Name.
   * @return String - Ozone bucket Name.
   * @throws IOException - Throws if the s3bucket does not exist.
   */
  public String getOzoneBucketName(String s3BucketName) throws IOException {
    return client.getObjectStore().getOzoneBucketName(s3BucketName);
  }

  /**
   * Returns Iterator to iterate over all buckets for a specific user.
   * The result can be restricted using bucket prefix, will return all
   * buckets if bucket prefix is null.
   *
   * @param userName
   * @param prefix
   * @return {@code Iterator<OzoneBucket>}
   */
  public Iterator<? extends OzoneBucket> listS3Buckets(String userName,
                                                       String prefix)  {
    return client.getObjectStore().listS3Buckets(userName, prefix);
  }

  /**
   * Returns Iterator to iterate over all buckets after prevBucket for a
   * specific user. If prevBucket is null it returns an iterator to iterate
   * over all buckets for this user. The result can be restricted using
   * bucket prefix, will return all buckets if bucket prefix is null.
   *
   * @param prefix Bucket prefix to match
   * @param previousBucket Buckets are listed after this bucket
   * @return {@code Iterator<OzoneBucket>}
   */
  public Iterator<? extends OzoneBucket> listS3Buckets(String userName,
                                                       String prefix,
                                                       String previousBucket)  {
    return client.getObjectStore().listS3Buckets(userName, prefix,
        previousBucket);
  }

  public AuthenticationHeaderParser getAuthenticationHeaderParser() {
    return authenticationHeaderParser;
  }

  @VisibleForTesting
  public void setAuthenticationHeaderParser(AuthenticationHeaderParser parser) {
    this.authenticationHeaderParser = parser;
  }

  @VisibleForTesting
  public void setClient(OzoneClient ozoneClient) {
    this.client = ozoneClient;
  }
}
