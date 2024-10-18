/*
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

package org.apache.hadoop.fs.s3a.api;

import javax.annotation.Nullable;
import java.util.List;

import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;

/**
 * Factory for S3 request objects.
 *
 * This is where the owner FS's {@code prepareRequest()}
 * callback is invoked to mark up a request for this span.
 *
 * All AWS request objects MUST BE created through this, in
 * the active audit span.
 * The standard implementation provides a callback for the S3AFS or
 * tests to be invoked to prepare each request.
 * Such callbacks SHOULD NOT raise exceptions other
 * than argument validation exceptions.
 * as there are no guarantees how they are processed.
 * That is: no guarantees of retry or translation.
 */
@InterfaceStability.Unstable
@InterfaceAudience.LimitedPrivate("testing/diagnostics")
public interface RequestFactory {

  /**
   * Set the encryption secrets for all subsequent requests.
   * @param secrets encryption secrets.
   */
  void setEncryptionSecrets(EncryptionSecrets secrets);

  /**
   * Get the canned ACL of this FS.
   * @return an ACL, if any
   */
  String getCannedACL();

  /**
   * Get the encryption algorithm of this endpoint.
   * @return the encryption algorithm.
   */
  S3AEncryptionMethods getServerSideEncryptionAlgorithm();

  /**
   * Get the content encoding (e.g. gzip) or return null if none.
   * @return content encoding
   */
  String getContentEncoding();

  /**
   * Get the object storage class, return null if none.
   * @return storage class
   */
  StorageClass getStorageClass();

  /**
   * Create a copy request builder.
   * This includes the work of copying the relevant parts
   * of the metadata from the source
   * @param srcKey source
   * @param dstKey destination
   * @param srcom source object metadata.
   * @return the request builder
   */
  CopyObjectRequest.Builder newCopyObjectRequestBuilder(String srcKey,
      String dstKey,
      HeadObjectResponse srcom);


  /**
   * Create a {@link PutObjectRequest} request builder.
   * The metadata is assumed to have been configured with the size of the
   * operation.
   * @param key key of object
   * @param options options for the request
   * @param length length of object to be uploaded
   * @param isDirectoryMarker true if object to be uploaded is a directory marker
   * @return the request builder
   */
  PutObjectRequest.Builder newPutObjectRequestBuilder(String key,
      PutObjectOptions options,
      long length,
      boolean isDirectoryMarker);

  /**
   * Create a {@link PutObjectRequest} request for creating
   * an empty directory.
   *
   * @param directory destination directory.
   * @return request builder for a zero byte upload.
   */
  PutObjectRequest.Builder newDirectoryMarkerRequest(String directory);

  /**
   * List all multipart uploads under a prefix.
   * @param prefix prefix to list under
   * @return the request builder.
   */
  ListMultipartUploadsRequest.Builder newListMultipartUploadsRequestBuilder(
      @Nullable String prefix);

  /**
   * Abort a multipart upload.
   * @param destKey destination object key
   * @param uploadId ID of initiated upload
   * @return the request builder.
   */
  AbortMultipartUploadRequest.Builder newAbortMultipartUploadRequestBuilder(
      String destKey,
      String uploadId);

  /**
   * Start a multipart upload.
   * @param destKey destination object key
   * @param options options for the request
   * @return the request builder.
   * @throws PathIOException if multipart uploads are disabled
   */
  CreateMultipartUploadRequest.Builder newMultipartUploadRequestBuilder(
      String destKey,
      @Nullable PutObjectOptions options) throws PathIOException;

  /**
   * Complete a multipart upload.
   * @param destKey destination object key
   * @param uploadId ID of initiated upload
   * @param partETags ordered list of etags
   * @return the request builder.
   */
  CompleteMultipartUploadRequest.Builder newCompleteMultipartUploadRequestBuilder(
      String destKey,
      String uploadId,
      List<CompletedPart> partETags);

  /**
   * Create a HEAD object request builder.
   * @param key key, may have trailing /
   * @return the request builder.
   */
  HeadObjectRequest.Builder newHeadObjectRequestBuilder(String key);

  /**
   * Create a HEAD bucket request builder.
   * @param bucket bucket to get metadata for
   * @return the request builder.
   */
  HeadBucketRequest.Builder newHeadBucketRequestBuilder(String bucket);


  /**
   * Create a GET request builder.
   * @param key object key
   * @return the request builder.
   */
  GetObjectRequest.Builder newGetObjectRequestBuilder(String key);

  /**
   * Create and initialize a part request builder of a multipart upload.
   *
   * @param destKey      destination key of ongoing operation
   * @param uploadId     ID of ongoing upload
   * @param partNumber   current part number of the upload
   * @param isLastPart   isLastPart is this the last part?
   * @param size         amount of data
   * @return the request builder.
   * @throws PathIOException if the part number is out of range.
   */
  UploadPartRequest.Builder newUploadPartRequestBuilder(
      String destKey,
      String uploadId,
      int partNumber,
      boolean isLastPart,
      long size) throws PathIOException;

  /**
   * Create the (legacy) V1 list request builder.
   * @param key key to list under
   * @param delimiter delimiter for keys
   * @param maxKeys maximum number in a list page.
   * @return the request builder.
   */
  ListObjectsRequest.Builder newListObjectsV1RequestBuilder(String key,
      String delimiter,
      int maxKeys);

  /**
   * Create a V2 list request builder.
   * This will be recycled for any subsequent requests.
   * @param key key to list under
   * @param delimiter delimiter for keys
   * @param maxKeys maximum number in a list page.
   * @return the request builder.
   */
  ListObjectsV2Request.Builder newListObjectsV2RequestBuilder(String key,
      String delimiter,
      int maxKeys);

  /**
   * Create a request builder to delete a single object.
   * @param key object to delete
   * @return the request builder.
   */
  DeleteObjectRequest.Builder newDeleteObjectRequestBuilder(String key);

  /**
   * Create a request builder to delete objects in bulk.
   * @param keysToDelete list of keys to delete.
   * @return the request builder.
   */
  DeleteObjectsRequest.Builder newBulkDeleteRequestBuilder(
          List<ObjectIdentifier> keysToDelete);

}
