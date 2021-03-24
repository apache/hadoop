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
import java.util.ArrayList;
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
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecretOperations;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DEFAULT_UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

/**
 * The standard implementation of the request factory.
 * This creates AWS SDK request classes for the specific bucket,
 * with standard options/headers set.
 * It is also where preflight checking of parameters can take place.
 *
 * All creation of AWS S3 requests MUST be through this class so that
 * common options (encryption etc.) can be added here,
 * and so that any chained transformation of requests can be applied.
 */
public class RequestFactoryImpl implements RequestFactory {

  public static final Logger LOG = LoggerFactory.getLogger(
      RequestFactoryImpl.class);

  /**
   * Target bucket.
   */
  private final String bucket;

  /**
   * Encryption secrets.
   */
  private final EncryptionSecrets encryptionSecrets;

  /**
   * ACL For new objects.
   */
  private final CannedAccessControlList cannedACL;

  private final long multipartPartCountLimit;

  /** Requester Pays. TODO: Wire up. */
  private final boolean requesterPays;


  protected RequestFactoryImpl(
      final String bucket,
      final EncryptionSecrets encryptionSecrets,
      final CannedAccessControlList cannedACL,
      final long multipartPartCountLimit,
      final boolean requesterPays) {
    this.encryptionSecrets = encryptionSecrets;
    this.bucket = bucket;
    this.cannedACL = cannedACL;
    this.multipartPartCountLimit = multipartPartCountLimit;
    this.requesterPays = requesterPays;
  }

  /**
   * Get the canned ACL of this FS.
   * @return an ACL, if any
   */
  @Override
  public CannedAccessControlList getCannedACL() {
    return cannedACL;
  }

  /**
   * Get the target bucket.
   * @return the bucket.
   */
  protected String getBucket() {
    return bucket;
  }

  /**
   * Create the AWS SDK structure used to configure SSE,
   * if the encryption secrets contain the information/settings for this.
   * @return an optional set of KMS Key settings
   */
  @Override
  public Optional<SSEAwsKeyManagementParams> generateSSEAwsKeyParams() {
    return EncryptionSecretOperations.createSSEAwsKeyManagementParams(
        encryptionSecrets);
  }

  /**
   * Create the SSE-C structure for the AWS SDK, if the encryption secrets
   * contain the information/settings for this.
   * This will contain a secret extracted from the bucket/configuration.
   * @return an optional customer key.
   */
  @Override
  public Optional<SSECustomerKey> generateSSECustomerKey() {
    return EncryptionSecretOperations.createSSECustomerKey(
        encryptionSecrets);
  }

  /**
   * Get the encryption algorithm of this endpoint.
   * @return the encryption algorithm.
   */
  @Override
  public S3AEncryptionMethods getServerSideEncryptionAlgorithm() {
    return encryptionSecrets.getEncryptionMethod();
  }

  /**
   * Sets server side encryption parameters to the part upload
   * request when encryption is enabled.
   * @param request upload part request
   */
  @Override
  public void setOptionalUploadPartRequestParameters(
      UploadPartRequest request) {
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
  }

  /**
   * Sets server side encryption parameters to the part upload
   * request when encryption is enabled.
   * @param request upload part request
   */
  @Override
  public void setOptionalGetObjectMetadataParameters(
      GetObjectMetadataRequest request) {
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
  }

  /**
   * Set the optional parameters when initiating the request (encryption,
   * headers, storage, etc).
   * @param request request to patch.
   */
  @Override
  public void setOptionalMultipartUploadRequestParameters(
      InitiateMultipartUploadRequest request) {
    generateSSEAwsKeyParams().ifPresent(request::setSSEAwsKeyManagementParams);
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
  }

  private void setOptionalPutRequestParameters(PutObjectRequest request) {
    generateSSEAwsKeyParams().ifPresent(request::setSSEAwsKeyManagementParams);
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
  }

  @Override
  public void setOptionalObjectMetadata(ObjectMetadata metadata) {
    final S3AEncryptionMethods algorithm
        = getServerSideEncryptionAlgorithm();
    if (S3AEncryptionMethods.SSE_S3 == algorithm) {
      metadata.setSSEAlgorithm(algorithm.getMethod());
    }
  }


  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   *
   * @param length length of data to set in header; Ignored if negative
   * @return a new metadata instance
   */
  @Override
  public ObjectMetadata newObjectMetadata(long length) {
    final ObjectMetadata om = new ObjectMetadata();
    setOptionalObjectMetadata(om);
    if (length >= 0) {
      om.setContentLength(length);
    }
    return om;
  }

  @Override
  public CopyObjectRequest newCopyObjectRequest(String srcKey,
      String dstKey) {
    CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(getBucket(), srcKey, getBucket(), dstKey);

    copyObjectRequest.setCannedAccessControlList(cannedACL);
    return copyObjectRequest;
  }

  /**
   * Create a putObject request.
   * Adds the ACL and metadata
   * @param key key of object
   * @param metadata metadata header
   * @param srcfile source file
   * @return the request
   */
  @Override
  public PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, File srcfile) {
    Preconditions.checkNotNull(srcfile);
    PutObjectRequest putObjectRequest = new PutObjectRequest(getBucket(), key,
        srcfile);
    setOptionalPutRequestParameters(putObjectRequest);
    putObjectRequest.setCannedAcl(cannedACL);
    putObjectRequest.setMetadata(metadata);
    return putObjectRequest;
  }

  /**
   * Create a {@link PutObjectRequest} request.
   * The metadata is assumed to have been configured with the size of the
   * operation.
   * @param key key of object
   * @param metadata metadata header
   * @param inputStream source data.
   * @return the request
   */
  @Override
  public PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata,
      InputStream inputStream) {
    Preconditions.checkNotNull(inputStream);
    Preconditions.checkArgument(isNotEmpty(key), "Null/empty key");
    PutObjectRequest putObjectRequest = new PutObjectRequest(getBucket(), key,
        inputStream, metadata);
    setOptionalPutRequestParameters(putObjectRequest);
    putObjectRequest.setCannedAcl(cannedACL);
    return putObjectRequest;
  }

  @Override
  public ListMultipartUploadsRequest newListMultipartUploadsRequest(String prefix) {
    ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(
        getBucket());
    if (!prefix.isEmpty()) {
      if (!prefix.endsWith("/")) {
        prefix = prefix + "/";
      }
      request.setPrefix(prefix);
    }
    return request;
  }

  @Override
  public AbortMultipartUploadRequest newAbortMultipartUploadRequest(String destKey,
      String uploadId) {
    return new AbortMultipartUploadRequest(getBucket(),
        destKey,
        uploadId);
  }

  @Override
  public InitiateMultipartUploadRequest newMultipartUploadRequest(String destKey) {
    final InitiateMultipartUploadRequest initiateMPURequest =
        new InitiateMultipartUploadRequest(getBucket(),
            destKey,
            newObjectMetadata(-1));
    initiateMPURequest.setCannedACL(getCannedACL());
    setOptionalMultipartUploadRequestParameters(initiateMPURequest);
    return initiateMPURequest;
  }

  @Override
  public CompleteMultipartUploadRequest newCompleteMultipartUploadRequest(String destKey,
      String uploadId,
      List<PartETag> partETags) {
    // a copy of the list is required, so that the AWS SDK doesn't
    // attempt to sort an unmodifiable list.
    return new CompleteMultipartUploadRequest(bucket,
        destKey, uploadId, new ArrayList<>(partETags));

  }

  @Override
  public GetObjectMetadataRequest newGetObjectMetadataRequest(String key) {
    GetObjectMetadataRequest request =
        new GetObjectMetadataRequest(getBucket(), key);
    //SSE-C requires to be filled in if enabled for object metadata
    setOptionalGetObjectMetadataParameters(request);
    return request;
  }

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
  @Override
  public UploadPartRequest newUploadPartRequest(
      String destKey,
      String uploadId,
      int partNumber,
      int size,
      InputStream uploadStream,
      File sourceFile,
      long offset) throws PathIOException {
    checkNotNull(uploadId);
    // exactly one source must be set; xor verifies this
    checkArgument((uploadStream != null) ^ (sourceFile != null),
        "Data source");
    checkArgument(size >= 0, "Invalid partition size %s", size);
    checkArgument(partNumber > 0,
        "partNumber must be between 1 and %s inclusive, but is %s",
        DEFAULT_UPLOAD_PART_COUNT_LIMIT, partNumber);

    LOG.debug("Creating part upload request for {} #{} size {}",
        uploadId, partNumber, size);
    final String pathErrorMsg = "Number of parts in multipart upload exceeded."
        + " Current part count = %s, Part count limit = %s ";
    if (partNumber > multipartPartCountLimit) {
      throw new PathIOException(destKey,
          String.format(pathErrorMsg, partNumber, multipartPartCountLimit));
    }
    UploadPartRequest request = new UploadPartRequest()
        .withBucketName(getBucket())
        .withKey(destKey)
        .withUploadId(uploadId)
        .withPartNumber(partNumber)
        .withPartSize(size);
    if (uploadStream != null) {
      // there's an upload stream. Bind to it.
      request.setInputStream(uploadStream);
    } else {
      checkArgument(sourceFile.exists(),
          "Source file does not exist: %s", sourceFile);
      checkArgument(offset >= 0, "Invalid offset %s", offset);
      long length = sourceFile.length();
      checkArgument(offset == 0 || offset < length,
          "Offset %s beyond length of file %s", offset, length);
      request.setFile(sourceFile);
      request.setFileOffset(offset);
    }
    return request;
  }


  /**
   * Create a S3 Select request for the destination object.
   * This does not build the query.
   * @param key object key
   * @return the request
   */
  @Override
  public SelectObjectContentRequest newSelectRequest(String key) {
    SelectObjectContentRequest request = new SelectObjectContentRequest();
    request.setBucketName(bucket);
    request.setKey(key);
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
    return request;
  }

  @Override
  public ListObjectsRequest newListObjectsRequest() {
    return new ListObjectsRequest().withBucketName(bucket);
  }

  @Override
  public ListObjectsV2Request newListObjectsV2Request() {
    return new ListObjectsV2Request().withBucketName(bucket);
  }

  /**
   * Create a builder.
   * @return new builder.
   */
  public static RequestFactoryBuilder builder() {
    return new RequestFactoryBuilder();
  }


  /**
   * Builder.
   */
  public static final class RequestFactoryBuilder {


    /**
     * Target bucket.
     */
    private String bucket;

    /**
     * Encryption secrets.
     */
    private EncryptionSecrets encryptionSecrets = new EncryptionSecrets();

    /**
     * ACL For new objects.
     */
    private CannedAccessControlList cannedACL = null;

    /** Requester Pays. TODO: Wire up. */
    private boolean requesterPays = false;

    private long multipartPartCountLimit = DEFAULT_UPLOAD_PART_COUNT_LIMIT;

    private RequestFactoryBuilder() {
    }

    public RequestFactoryBuilder withBucket(final String _bucket) {
      bucket = _bucket;
      return this;
    }

    public RequestFactoryBuilder withEncryptionSecrets(
        final EncryptionSecrets _encryptionSecrets) {
      encryptionSecrets = _encryptionSecrets;
      return this;
    }

    public RequestFactoryBuilder withCannedACL(
        final CannedAccessControlList _cannedACL) {
      cannedACL = _cannedACL;
      return this;
    }

    public RequestFactoryBuilder withRequesterPays(final boolean _requesterPays) {
      requesterPays = _requesterPays;
      return this;
    }

    public RequestFactoryBuilder withMultipartPartCountLimit(
        final long _multipartPartCountLimit) {
      multipartPartCountLimit = _multipartPartCountLimit;
      return this;
    }

    public RequestFactory build() {
      return new RequestFactoryImpl(bucket, encryptionSecrets, cannedACL,
          multipartPartCountLimit, requesterPays);
    }
    
    
  }
}
