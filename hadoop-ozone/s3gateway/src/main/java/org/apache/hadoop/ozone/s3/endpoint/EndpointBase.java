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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV2;
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV4;

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

  protected OzoneBucket getBucket(String volumeName, String bucketName)
      throws IOException {
    return getVolume(volumeName).getBucket(bucketName);
  }

  protected OzoneBucket getBucket(OzoneVolume volume, String bucketName)
      throws OS3Exception, IOException {
    OzoneBucket bucket;
    try {
      bucket = volume.getBucket(bucketName);
    } catch (IOException ex) {
      LOG.error("Error occurred is {}", ex);
      if (ex.getMessage().contains("NOT_FOUND")) {
        OS3Exception oex =
            S3ErrorTable.newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName);
        throw oex;
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
    } catch (IOException ex) {
      LOG.error("Error occurred is {}", ex);
      if (ex.getMessage().contains("NOT_FOUND")) {
        OS3Exception oex =
            S3ErrorTable.newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName);
        throw oex;
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
    } catch (Exception ex) {
      if (ex.getMessage().contains("NOT_FOUND")) {
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
    } catch (IOException ex) {
      LOG.error("createS3Bucket error:", ex);
      if (!ex.getMessage().contains("ALREADY_EXISTS")) {
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
   * Retrieve the username based on the authorization header.
   *
   * @param httpHeaders
   * @return Identified username
   * @throws OS3Exception
   */
  public String parseUsername(
      @Context HttpHeaders httpHeaders) throws OS3Exception {
    String auth = httpHeaders.getHeaderString("Authorization");
    LOG.info("Auth header string {}", auth);

    if (auth == null) {
      throw S3ErrorTable
          .newError(S3ErrorTable.MALFORMED_HEADER, auth);
    }

    String userName;
    if (auth.startsWith("AWS4")) {
      LOG.info("V4 Header {}", auth);
      AuthorizationHeaderV4 authorizationHeader = new AuthorizationHeaderV4(
          auth);
      userName = authorizationHeader.getAccessKeyID().toLowerCase();
    } else {
      LOG.info("V2 Header {}", auth);
      AuthorizationHeaderV2 authorizationHeader = new AuthorizationHeaderV2(
          auth);
      userName = authorizationHeader.getAccessKeyID().toLowerCase();
    }
    return userName;
  }

  @VisibleForTesting
  public void setClient(OzoneClient ozoneClient) {
    this.client = ozoneClient;
  }

}
