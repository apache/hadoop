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
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;

/**
 * Factory for S3 objects.
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
  CannedAccessControlList getCannedACL();

  /**
   * Create the AWS SDK structure used to configure SSE,
   * if the encryption secrets contain the information/settings for this.
   * @return an optional set of KMS Key settings
   */
  Optional<SSEAwsKeyManagementParams> generateSSEAwsKeyParams();

  /**
   * Create the SSE-C structure for the AWS SDK, if the encryption secrets
   * contain the information/settings for this.
   * This will contain a secret extracted from the bucket/configuration.
   * @return an optional customer key.
   */
  Optional<SSECustomerKey> generateSSECustomerKey();

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
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   *
   * @param length length of data to set in header; Ignored if negative
   * @return a new metadata instance
   */
  ObjectMetadata newObjectMetadata(long length);

  /**
   * Create a copy request.
   * This includes the work of copying the relevant parts
   * of the metadata from the source
   * @param srcKey source
   * @param dstKey destination
   * @param srcom source object metadata.
   * @return the request
   */
  CopyObjectRequest newCopyObjectRequest(String srcKey,
      String dstKey,
      ObjectMetadata srcom);

  /**
   * Create a putObject request.
   * Adds the ACL and metadata
   * @param key key of object
   * @param metadata metadata header
   * @param options options for the request
   * @param srcfile source file
   * @return the request
   */
  PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, PutObjectOptions options, File srcfile);

  /**
   * Create a {@link PutObjectRequest} request.
   * The metadata is assumed to have been configured with the size of the
   * operation.
   * @param key key of object
   * @param metadata metadata header
   * @param options options for the request
   * @param inputStream source data.
   * @return the request
   */
  PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata,
      PutObjectOptions options,
      InputStream inputStream);

  /**
   * Create a {@link PutObjectRequest} request for creating
   * an empty directory.
   *
   * @param directory destination directory.
   * @return request for a zero byte upload.
   */
  PutObjectRequest newDirectoryMarkerRequest(String directory);

  /**
   * List all multipart uploads under a prefix.
   * @param prefix prefix to list under
   * @return the request.
   */
  ListMultipartUploadsRequest newListMultipartUploadsRequest(
      @Nullable String prefix);

  /**
   * Abort a multipart upload.
   * @param destKey destination object key
   * @param uploadId ID of initiated upload
   * @return the request.
   */
  AbortMultipartUploadRequest newAbortMultipartUploadRequest(
      String destKey,
      String uploadId);

  /**
   * Start a multipart upload.
   * @param destKey destination object key
   * @param options options for the request
   * @return the request.
   * @throws PathIOException if multipart uploads are disabled
   */
  InitiateMultipartUploadRequest newMultipartUploadRequest(
      String destKey,
      @Nullable PutObjectOptions options) throws PathIOException;

  /**
   * Complete a multipart upload.
   * @param destKey destination object key
   * @param uploadId ID of initiated upload
   * @param partETags ordered list of etags
   * @return the request.
   */
  CompleteMultipartUploadRequest newCompleteMultipartUploadRequest(
      String destKey,
      String uploadId,
      List<PartETag> partETags);

  /**
   * Create a HEAD request.
   * @param key key, may have trailing /
   * @return the request.
   */
  GetObjectMetadataRequest newGetObjectMetadataRequest(String key);

  /**
   * Create a GET request.
   * @param key object key
   * @return the request.
   */
  GetObjectRequest newGetObjectRequest(String key);

  /**
   * Create and initialize a part request of a multipart upload.
   * Exactly one of: {@code uploadStream} or {@code sourceFile}
   * must be specified.
   * A subset of the file may be posted, by providing the starting point
   * in {@code offset} and a length of block in {@code size} equal to
   * or less than the remaining bytes.
   * @param destKey destination key of ongoing operation
   * @param uploadId ID of ongoing upload
   * @param partNumber current part number of the upload
   * @param size amount of data
   * @param uploadStream source of data to upload
   * @param sourceFile optional source file.
   * @param offset offset in file to start reading.
   * @return the request.
   * @throws PathIOException if the part number is out of range.
   */
  UploadPartRequest newUploadPartRequest(
      String destKey,
      String uploadId,
      int partNumber,
      long size,
      InputStream uploadStream,
      File sourceFile,
      long offset) throws PathIOException;

  /**
   * Create a S3 Select request for the destination object.
   * This does not build the query.
   * @param key object key
   * @return the request
   */
  SelectObjectContentRequest newSelectRequest(String key);

  /**
   * Create the (legacy) V1 list request.
   * @param key key to list under
   * @param delimiter delimiter for keys
   * @param maxKeys maximum number in a list page.
   * @return the request
   */
  ListObjectsRequest newListObjectsV1Request(String key,
      String delimiter,
      int maxKeys);

  /**
   * Create the next V1 page list request, following
   * on from the previous response.
   * @param prev previous response
   * @return the request
   */

  ListNextBatchOfObjectsRequest newListNextBatchOfObjectsRequest(
      ObjectListing prev);

  /**
   * Create a V2 list request.
   * This will be recycled for any subsequent requests.
   * @param key key to list under
   * @param delimiter delimiter for keys
   * @param maxKeys maximum number in a list page.
   * @return the request
   */
  ListObjectsV2Request newListObjectsV2Request(String key,
      String delimiter,
      int maxKeys);

  /**
   * Create a request to delete a single object.
   * @param key object to delete
   * @return the request
   */
  DeleteObjectRequest newDeleteObjectRequest(String key);

  /**
   * Bulk delete request.
   * @param keysToDelete list of keys to delete.
   * @return the request
   */
  DeleteObjectsRequest newBulkDeleteRequest(
          List<DeleteObjectsRequest.KeyVersion> keysToDelete);

}
