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

package org.apache.hadoop.fs.s3a.impl;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;

public interface RequestFactory {

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
   * Sets server side encryption parameters to the part upload
   * request when encryption is enabled.
   * @param request upload part request
   */
  void setOptionalUploadPartRequestParameters(
      UploadPartRequest request);

  /**
   * Sets server side encryption parameters to the part upload
   * request when encryption is enabled.
   * @param request upload part request
   */
  void setOptionalGetObjectMetadataParameters(
      GetObjectMetadataRequest request);

  /**
   * Set the optional parameters when initiating the request (encryption,
   * headers, storage, etc).
   * @param request request to patch.
   */
  void setOptionalMultipartUploadRequestParameters(
      InitiateMultipartUploadRequest request);

  void setOptionalObjectMetadata(ObjectMetadata metadata);

  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   *
   * @param length length of data to set in header; Ignored if negative
   * @return a new metadata instance
   */
  ObjectMetadata newObjectMetadata(long length);

  CopyObjectRequest newCopyObjectRequest(String srcKey,
      String dstKey);

  /**
   * Create a putObject request.
   * Adds the ACL and metadata
   * @param key key of object
   * @param metadata metadata header
   * @param srcfile source file
   * @return the request
   */
  PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, File srcfile);

  /**
   * Create a {@link PutObjectRequest} request.
   * The metadata is assumed to have been configured with the size of the
   * operation.
   * @param key key of object
   * @param metadata metadata header
   * @param inputStream source data.
   * @return the request
   */
  PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata,
      InputStream inputStream);

  ListMultipartUploadsRequest newListMultipartUploadsRequest(String prefix);

  AbortMultipartUploadRequest newAbortMultipartUploadRequest(String destKey,
      String uploadId);

  InitiateMultipartUploadRequest newMultipartUploadRequest(String destKey);

  CompleteMultipartUploadRequest newCompleteMultipartUploadRequest(String destKey,
      String uploadId,
      List<PartETag> partETags);

  GetObjectMetadataRequest newGetObjectMetadataRequest(String key);

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
   */
  UploadPartRequest newUploadPartRequest(
      String destKey,
      String uploadId,
      int partNumber,
      int size,
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

  ListObjectsRequest newListObjectsRequest();

  ListObjectsV2Request newListObjectsV2Request();
}
