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

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.utils.Md5Utils;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecretOperations;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DEFAULT_UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.Preconditions.checkNotNull;
import static software.amazon.awssdk.services.s3.model.StorageClass.UNKNOWN_TO_SDK_VERSION;

/**
 * The standard implementation of the request factory.
 * This creates AWS SDK request classes for the specific bucket,
 * with standard options/headers set.
 * It is also where custom setting parameters can take place.
 *
 * All creation of AWS S3 requests MUST be through this class so that
 * common options (encryption etc.) can be added here,
 * and so that any chained transformation of requests can be applied.
 *
 * This is where audit span information is added to the requests,
 * until it is done in the AWS SDK itself.
 *
 * All created request builders will be passed to
 * {@link PrepareRequest#prepareRequest(SdkRequest.Builder)} before
 * being returned to the caller.
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
  private EncryptionSecrets encryptionSecrets;

  /**
   * ACL For new objects.
   */
  private final String cannedACL;

  /**
   * Max number of multipart entries allowed in a large
   * upload. Tunable for testing only.
   */
  private final long multipartPartCountLimit;

  /**
   * Callback to prepare requests.
   */
  private final PrepareRequest requestPreparer;

  /**
   * Content encoding (null for none).
   */
  private final String contentEncoding;

  /**
   * Storage class.
   */
  private final StorageClass storageClass;

  /**
   * Is multipart upload enabled.
   */
  private final boolean isMultipartUploadEnabled;

  /**
   * Constructor.
   * @param builder builder with all the configuration.
   */
  protected RequestFactoryImpl(
      final RequestFactoryBuilder builder) {
    this.bucket = builder.bucket;
    this.cannedACL = builder.cannedACL;
    this.encryptionSecrets = builder.encryptionSecrets;
    this.multipartPartCountLimit = builder.multipartPartCountLimit;
    this.requestPreparer = builder.requestPreparer;
    this.contentEncoding = builder.contentEncoding;
    this.storageClass = builder.storageClass;
    this.isMultipartUploadEnabled = builder.isMultipartUploadEnabled;
  }

  /**
   * Preflight preparation of AWS request.
   * @param <T> web service request builder
   * @return prepared builder.
   */
  @Retries.OnceRaw
  private <T extends SdkRequest.Builder> T prepareRequest(T t) {
    if (requestPreparer != null) {
      requestPreparer.prepareRequest(t);
    }
    return t;
  }

  /**
   * Get the canned ACL of this FS.
   * @return an ACL, if any
   */
  @Override
  public String getCannedACL() {
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
   * Get the encryption algorithm of this endpoint.
   * @return the encryption algorithm.
   */
  @Override
  public S3AEncryptionMethods getServerSideEncryptionAlgorithm() {
    return encryptionSecrets.getEncryptionMethod();
  }

  /**
   * Get the content encoding (e.g. gzip) or return null if none.
   * @return content encoding
   */
  @Override
  public String getContentEncoding() {
    return contentEncoding;
  }

  /**
   * Get the object storage class, return null if none.
   * @return storage class
   */
  @Override
  public StorageClass getStorageClass() {
    return storageClass;
  }

  /**
   * Sets server side encryption parameters to the part upload
   * request when encryption is enabled.
   * @param builder upload part request builder
   */
  protected void uploadPartEncryptionParameters(
      UploadPartRequest.Builder builder) {
    // need to set key to get objects encrypted with SSE_C
    EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets).ifPresent(base64customerKey -> {
      builder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(base64customerKey)
          .sseCustomerKeyMD5(Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
    });
  }

  private CopyObjectRequest.Builder buildCopyObjectRequest() {

    CopyObjectRequest.Builder copyObjectRequestBuilder = CopyObjectRequest.builder();

    if (contentEncoding != null) {
      copyObjectRequestBuilder.contentEncoding(contentEncoding);
    }

    return copyObjectRequestBuilder;
  }

  @Override
  public CopyObjectRequest.Builder newCopyObjectRequestBuilder(String srcKey,
      String dstKey,
      HeadObjectResponse srcom) {

    CopyObjectRequest.Builder copyObjectRequestBuilder = buildCopyObjectRequest();

    Map<String, String> dstom = new HashMap<>();
    HeaderProcessing.cloneObjectMetadata(srcom, dstom, copyObjectRequestBuilder);
    copyEncryptionParameters(srcom, copyObjectRequestBuilder);

    copyObjectRequestBuilder
        .metadata(dstom)
        .metadataDirective(MetadataDirective.REPLACE)
        .acl(cannedACL);

    if (srcom.storageClass() != null && srcom.storageClass() != UNKNOWN_TO_SDK_VERSION) {
      copyObjectRequestBuilder.storageClass(srcom.storageClass());
    }

    copyObjectRequestBuilder.destinationBucket(getBucket())
        .destinationKey(dstKey).sourceBucket(getBucket()).sourceKey(srcKey);

    return prepareRequest(copyObjectRequestBuilder);
  }

  /**
   * Propagate encryption parameters from source file if set else use the
   * current filesystem encryption settings.
   * @param copyObjectRequestBuilder copy object request builder.
   * @param srcom source object metadata.
   */
  protected void copyEncryptionParameters(HeadObjectResponse srcom,
      CopyObjectRequest.Builder copyObjectRequestBuilder) {

    final S3AEncryptionMethods algorithm = getServerSideEncryptionAlgorithm();

    String sourceKMSId = srcom.ssekmsKeyId();
    if (isNotEmpty(sourceKMSId)) {
      // source KMS ID is propagated
      LOG.debug("Propagating SSE-KMS settings from source {}",
          sourceKMSId);
      copyObjectRequestBuilder.ssekmsKeyId(sourceKMSId);
      return;
    }

    if (S3AEncryptionMethods.SSE_S3 == algorithm) {
      copyObjectRequestBuilder.serverSideEncryption(algorithm.getMethod());
    } else if (S3AEncryptionMethods.SSE_KMS == algorithm) {
      copyObjectRequestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
      // Set the KMS key if present, else S3 uses AWS managed key.
      EncryptionSecretOperations.getSSEAwsKMSKey(encryptionSecrets)
          .ifPresent(kmsKey -> copyObjectRequestBuilder.ssekmsKeyId(kmsKey));
    } else if (S3AEncryptionMethods.SSE_C == algorithm) {
      EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets)
          .ifPresent(base64customerKey -> {
            copyObjectRequestBuilder.copySourceSSECustomerAlgorithm(
                    ServerSideEncryption.AES256.name()).copySourceSSECustomerKey(base64customerKey)
                .copySourceSSECustomerKeyMD5(
                    Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)))
                .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
                .sseCustomerKey(base64customerKey).sseCustomerKeyMD5(
                    Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
          });
    }
  }
  /**
   * Create a putObject request.
   * Adds the ACL, storage class and metadata
   * @param key key of object
   * @param options options for the request, including headers
   * @param length length of object to be uploaded
   * @param isDirectoryMarker true if object to be uploaded is a directory marker
   * @return the request builder
   */
  @Override
  public PutObjectRequest.Builder newPutObjectRequestBuilder(String key,
      final PutObjectOptions options,
      long length,
      boolean isDirectoryMarker) {

    Preconditions.checkArgument(isNotEmpty(key), "Null/empty key");

    PutObjectRequest.Builder putObjectRequestBuilder =
        buildPutObjectRequest(length, isDirectoryMarker);
    putObjectRequestBuilder.bucket(getBucket()).key(key);

    if (options != null) {
      putObjectRequestBuilder.metadata(options.getHeaders());
    }

    putEncryptionParameters(putObjectRequestBuilder);

    if (storageClass != null) {
      putObjectRequestBuilder.storageClass(storageClass);
    }

    return prepareRequest(putObjectRequestBuilder);
  }

  private PutObjectRequest.Builder buildPutObjectRequest(long length, boolean isDirectoryMarker) {

    PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder();

    putObjectRequestBuilder.acl(cannedACL);

    if (length >= 0) {
      putObjectRequestBuilder.contentLength(length);
    }

    if (contentEncoding != null && !isDirectoryMarker) {
      putObjectRequestBuilder.contentEncoding(contentEncoding);
    }

    return putObjectRequestBuilder;
  }

  private void putEncryptionParameters(PutObjectRequest.Builder putObjectRequestBuilder) {
    final S3AEncryptionMethods algorithm
        = getServerSideEncryptionAlgorithm();

    if (S3AEncryptionMethods.SSE_S3 == algorithm) {
      putObjectRequestBuilder.serverSideEncryption(algorithm.getMethod());
    } else if (S3AEncryptionMethods.SSE_KMS == algorithm) {
      putObjectRequestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
      // Set the KMS key if present, else S3 uses AWS managed key.
      EncryptionSecretOperations.getSSEAwsKMSKey(encryptionSecrets)
          .ifPresent(kmsKey -> putObjectRequestBuilder.ssekmsKeyId(kmsKey));
    } else if (S3AEncryptionMethods.SSE_C == algorithm) {
      EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets)
          .ifPresent(base64customerKey -> {
            putObjectRequestBuilder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
                .sseCustomerKey(base64customerKey).sseCustomerKeyMD5(
                    Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
          });
    }
  }

  @Override
  public PutObjectRequest.Builder newDirectoryMarkerRequest(String directory) {
    String key = directory.endsWith("/")
        ? directory
        : (directory + "/");

    // preparation happens in here
    PutObjectRequest.Builder putObjectRequestBuilder = buildPutObjectRequest(0L, true);

    putObjectRequestBuilder.bucket(getBucket()).key(key)
        .contentType(HeaderProcessing.CONTENT_TYPE_X_DIRECTORY);

    putEncryptionParameters(putObjectRequestBuilder);

    return prepareRequest(putObjectRequestBuilder);
  }

  @Override
  public ListMultipartUploadsRequest.Builder
      newListMultipartUploadsRequestBuilder(String prefix) {

    ListMultipartUploadsRequest.Builder requestBuilder = ListMultipartUploadsRequest.builder();

    requestBuilder.bucket(getBucket());
    if (prefix != null) {
      requestBuilder.prefix(prefix);
    }
    return prepareRequest(requestBuilder);
  }

  @Override
  public AbortMultipartUploadRequest.Builder newAbortMultipartUploadRequestBuilder(
      String destKey,
      String uploadId) {
    AbortMultipartUploadRequest.Builder requestBuilder =
        AbortMultipartUploadRequest.builder().bucket(getBucket()).key(destKey).uploadId(uploadId);

    return prepareRequest(requestBuilder);
  }

  private void multipartUploadEncryptionParameters(
      CreateMultipartUploadRequest.Builder mpuRequestBuilder) {
    final S3AEncryptionMethods algorithm = getServerSideEncryptionAlgorithm();

    if (S3AEncryptionMethods.SSE_S3 == algorithm) {
      mpuRequestBuilder.serverSideEncryption(algorithm.getMethod());
    } else if (S3AEncryptionMethods.SSE_KMS == algorithm) {
      mpuRequestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
      // Set the KMS key if present, else S3 uses AWS managed key.
      EncryptionSecretOperations.getSSEAwsKMSKey(encryptionSecrets)
          .ifPresent(kmsKey -> mpuRequestBuilder.ssekmsKeyId(kmsKey));
    } else if (S3AEncryptionMethods.SSE_C == algorithm) {
      EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets)
          .ifPresent(base64customerKey -> {
            mpuRequestBuilder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
                .sseCustomerKey(base64customerKey).sseCustomerKeyMD5(
                    Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
          });
    }
  }

  @Override
  public CreateMultipartUploadRequest.Builder newMultipartUploadRequestBuilder(
      final String destKey,
      @Nullable final PutObjectOptions options) throws PathIOException {
    if (!isMultipartUploadEnabled) {
      throw new PathIOException(destKey, "Multipart uploads are disabled.");
    }

    CreateMultipartUploadRequest.Builder requestBuilder = CreateMultipartUploadRequest.builder();

    if (contentEncoding != null) {
      requestBuilder.contentEncoding(contentEncoding);
    }

    if (options != null) {
      requestBuilder.metadata(options.getHeaders());
    }

    requestBuilder.bucket(getBucket()).key(destKey).acl(cannedACL);

    multipartUploadEncryptionParameters(requestBuilder);

    if (storageClass != null) {
      requestBuilder.storageClass(storageClass);
    }

    return prepareRequest(requestBuilder);
  }

  @Override
  public CompleteMultipartUploadRequest.Builder newCompleteMultipartUploadRequestBuilder(
      String destKey,
      String uploadId,
      List<CompletedPart> partETags) {
    // a copy of the list is required, so that the AWS SDK doesn't
    // attempt to sort an unmodifiable list.
    CompleteMultipartUploadRequest.Builder requestBuilder =
        CompleteMultipartUploadRequest.builder().bucket(bucket).key(destKey).uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(partETags).build());

    return prepareRequest(requestBuilder);
  }

  @Override
  public HeadObjectRequest.Builder newHeadObjectRequestBuilder(String key) {

    HeadObjectRequest.Builder headObjectRequestBuilder =
        HeadObjectRequest.builder().bucket(getBucket()).key(key);

    // need to set key to get metadata for objects encrypted with SSE_C
    EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets).ifPresent(base64customerKey -> {
      headObjectRequestBuilder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(base64customerKey)
          .sseCustomerKeyMD5(Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
    });

    return prepareRequest(headObjectRequestBuilder);
  }

  @Override
  public HeadBucketRequest.Builder newHeadBucketRequestBuilder(String bucketName) {

    HeadBucketRequest.Builder headBucketRequestBuilder =
        HeadBucketRequest.builder().bucket(bucketName);

    return prepareRequest(headBucketRequestBuilder);
  }

  @Override
  public GetObjectRequest.Builder newGetObjectRequestBuilder(String key) {
    GetObjectRequest.Builder builder = GetObjectRequest.builder()
        .bucket(bucket)
        .key(key);

    // need to set key to get objects encrypted with SSE_C
    EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets).ifPresent(base64customerKey -> {
      builder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(base64customerKey)
          .sseCustomerKeyMD5(Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
    });

    return prepareRequest(builder);
  }

  @Override
  public UploadPartRequest.Builder newUploadPartRequestBuilder(
      String destKey,
      String uploadId,
      int partNumber,
      long size) throws PathIOException {
    checkNotNull(uploadId);
    checkArgument(size >= 0, "Invalid partition size %s", size);
    checkArgument(partNumber > 0,
        "partNumber must be between 1 and %s inclusive, but is %s",
        multipartPartCountLimit, partNumber);

    LOG.debug("Creating part upload request for {} #{} size {}",
        uploadId, partNumber, size);
    final String pathErrorMsg = "Number of parts in multipart upload exceeded."
        + " Current part count = %s, Part count limit = %s ";
    if (partNumber > multipartPartCountLimit) {
      throw new PathIOException(destKey,
          String.format(pathErrorMsg, partNumber, multipartPartCountLimit));
    }
    UploadPartRequest.Builder builder = UploadPartRequest.builder()
        .bucket(getBucket())
        .key(destKey)
        .uploadId(uploadId)
        .partNumber(partNumber)
        .contentLength(size);
    uploadPartEncryptionParameters(builder);
    return prepareRequest(builder);
  }

  @Override
  public SelectObjectContentRequest.Builder newSelectRequestBuilder(String key) {
    SelectObjectContentRequest.Builder requestBuilder =
        SelectObjectContentRequest.builder().bucket(bucket).key(key);

    EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets).ifPresent(base64customerKey -> {
      requestBuilder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(base64customerKey)
          .sseCustomerKeyMD5(Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
    });

    return prepareRequest(requestBuilder);
  }

  @Override
  public ListObjectsRequest.Builder newListObjectsV1RequestBuilder(
      final String key,
      final String delimiter,
      final int maxKeys) {

    ListObjectsRequest.Builder requestBuilder =
        ListObjectsRequest.builder().bucket(bucket).maxKeys(maxKeys).prefix(key);

    if (delimiter != null) {
      requestBuilder.delimiter(delimiter);
    }

    return prepareRequest(requestBuilder);
  }

  @Override
  public ListObjectsV2Request.Builder newListObjectsV2RequestBuilder(
      final String key,
      final String delimiter,
      final int maxKeys) {

    final ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
        .bucket(bucket)
        .maxKeys(maxKeys)
        .prefix(key);

    if (delimiter != null) {
      requestBuilder.delimiter(delimiter);
    }

    return prepareRequest(requestBuilder);
  }

  @Override
  public DeleteObjectRequest.Builder newDeleteObjectRequestBuilder(String key) {
    return prepareRequest(DeleteObjectRequest.builder().bucket(bucket).key(key));
  }

  @Override
  public DeleteObjectsRequest.Builder newBulkDeleteRequestBuilder(
          List<ObjectIdentifier> keysToDelete) {
    return prepareRequest(DeleteObjectsRequest
        .builder()
        .bucket(bucket)
        .delete(d -> d.objects(keysToDelete).quiet(true)));
  }

  @Override
  public void setEncryptionSecrets(final EncryptionSecrets secrets) {
    encryptionSecrets = secrets;
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
    private String cannedACL = null;

    /** Content Encoding. */
    private String contentEncoding;

    /**
     * Storage class.
     */
    private StorageClass storageClass;

    /**
     * Multipart limit.
     */
    private long multipartPartCountLimit = DEFAULT_UPLOAD_PART_COUNT_LIMIT;

    /**
     * Callback to prepare requests.
     */
    private PrepareRequest requestPreparer;

    /**
     * Is Multipart Enabled on the path.
     */
    private boolean isMultipartUploadEnabled = true;

    private RequestFactoryBuilder() {
    }

    /**
     * Build the request factory.
     * @return the factory
     */
    public RequestFactory build() {
      return new RequestFactoryImpl(this);
    }

    /**
     * Content encoding.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withContentEncoding(final String value) {
      contentEncoding = value;
      return this;
    }

    /**
     * Storage class.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withStorageClass(final StorageClass value) {
      storageClass = value;
      return this;
    }

    /**
     * Target bucket.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withBucket(final String value) {
      bucket = value;
      return this;
    }

    /**
     * Encryption secrets.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withEncryptionSecrets(
        final EncryptionSecrets value) {
      encryptionSecrets = value;
      return this;
    }

    /**
     * ACL For new objects.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withCannedACL(
        final String value) {
      cannedACL = value;
      return this;
    }

    /**
     * Multipart limit.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withMultipartPartCountLimit(
        final long value) {
      multipartPartCountLimit = value;
      return this;
    }

    /**
     * Callback to prepare requests.
     *
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withRequestPreparer(
        final PrepareRequest value) {
      this.requestPreparer = value;
      return this;
    }

    /**
     * Multipart upload enabled.
     *
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withMultipartUploadEnabled(
        final boolean value) {
      this.isMultipartUploadEnabled = value;
      return this;
    }
  }

  /**
   * This is a callback for anything to "prepare" every request
   * after creation. The S3AFileSystem's Audit Manager is expected
   * to be wired up via this call so can audit/prepare requests
   * after their creation.
   */
  @FunctionalInterface
  public interface PrepareRequest {

    /**
     * Post-creation preparation of AWS request.
     * @param t request builder
     */
    @Retries.OnceRaw
    void prepareRequest(SdkRequest.Builder t);
  }
}
