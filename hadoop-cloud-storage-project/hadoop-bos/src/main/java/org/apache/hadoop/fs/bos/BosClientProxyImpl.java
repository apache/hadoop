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

package org.apache.hadoop.fs.bos;

import com.baidubce.BceServiceException;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.auth.DefaultBceSessionCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.AbortMultipartUploadRequest;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.CompleteMultipartUploadRequest;
import com.baidubce.services.bos.model.CompleteMultipartUploadResponse;
import com.baidubce.services.bos.model.CopyObjectRequest;
import com.baidubce.services.bos.model.CopyObjectResponse;
import com.baidubce.services.bos.model.DeleteDirectoryRequest;
import com.baidubce.services.bos.model.DeleteDirectoryResponse;
import com.baidubce.services.bos.model.DeleteMultipleObjectsRequest;
import com.baidubce.services.bos.model.DeleteObjectRequest;
import com.baidubce.services.bos.model.GetObjectMetadataRequest;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.HeadBucketRequest;
import com.baidubce.services.bos.model.HeadBucketResponse;
import com.baidubce.services.bos.model.InitiateMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadResponse;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.services.bos.model.PutObjectRequest;
import com.baidubce.services.bos.model.PutObjectResponse;
import com.baidubce.services.bos.model.RenameObjectRequest;
import com.baidubce.services.bos.model.RenameObjectResponse;
import com.baidubce.services.bos.model.UploadPartCopyRequest;
import com.baidubce.services.bos.model.UploadPartCopyResponse;
import com.baidubce.services.bos.model.UploadPartRequest;
import com.baidubce.services.bos.model.UploadPartResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.bos.credentials.BceCredentialsProvider;
import org.apache.hadoop.fs.bos.credentials.HadoopCredentialsProvider;
import org.apache.hadoop.fs.bos.exceptions.BandwidthLimitException;
import org.apache.hadoop.fs.bos.exceptions.BosException;
import org.apache.hadoop.fs.bos.exceptions.BosHotObjectException;
import org.apache.hadoop.fs.bos.exceptions.BosServerException;
import org.apache.hadoop.fs.bos.exceptions.SessionTokenExpireException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Default implementation of {@link BosClientProxy} that
 * delegates operations to the BOS SDK {@link BosClient}.
 */
public class BosClientProxyImpl implements BosClientProxy {

  /**
   * Constructs a BosClientProxyImpl.
   * Call {@link #init(URI, Configuration)} to initialize.
   */
  public BosClientProxyImpl() {
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(BosClientProxyImpl.class);

  private volatile BosClient bosClient = null;
  private volatile BosClient preBosClient = null;
  private Configuration conf = null;
  private URI uri = null;
  private static final int BOS_UPLOAD_CONFLICT_CODE = 412;
  private static final int BOS_REQUEST_LIMIT_CODE = 429;
  private static final int BOS_NO_SUCH_KEY_CODE = 404;

  private String user = null;
  private static final int STREAM_BUFFER_DEFAULT_SIZE =
      64 * 1024;
  private BceCredentialsProvider provider;

  /** {@inheritDoc} */
  @Override
  public void init(URI uri, Configuration conf) {
    this.conf = conf;
    this.uri = uri;

    BosClientConfiguration config =
        new BosClientConfiguration();
    updateUserForInit();
    provider =
        BceCredentialsProvider
            .getBceCredentialsProviderImpl(conf);
    if (provider instanceof HadoopCredentialsProvider) {
      provider.setConf(this.conf);
    }
    DefaultBceSessionCredentials token =
        provider.getCredentials(uri, user);

    if (token == null
        || StringUtils.isEmpty(token.getAccessKeyId())
        || StringUtils.isEmpty(token.getSecretKey())) {
      throw new IllegalArgumentException(
          "user accessKey and secretAccessKey"
              + " should not be null");
    }
    if (token.getSessionToken() == null
        || token.getSessionToken().trim().isEmpty()) {
      config.setCredentials(
          new DefaultBceCredentials(
              token.getAccessKeyId(),
              token.getSecretKey()
          ));
    } else {
      config.setCredentials(
          new DefaultBceSessionCredentials(
              token.getAccessKeyId(),
              token.getSecretKey(),
              token.getSessionToken()
          ));
    }
    // support ConsistencyView
    config.setConsistencyView(
        BosClientConfiguration.STRONG_CONSISTENCY_VIEW);
    config.setEnableHttpAsyncPut(false);

    String endPoint = conf.get(
        "fs.bos.bucket." + uri.getHost() + ".endpoint",
        null);
    if (endPoint == null) {
      endPoint = conf.get(BaiduBosConstants.BOS_ENDPOINT);
    }
    config.setEndpoint(endPoint);
    int maxConnections = conf.getInt(
        BaiduBosConstants.BOS_MAX_CONNECTIONS, 1000);
    config.setMaxConnections(maxConnections);
    String userAgent =
        BaiduBosConstants.BOS_FILESYSTEM_USER_AGENT
            + "/"
            + BosClientConfiguration.DEFAULT_USER_AGENT;
    config.setUserAgent(userAgent);
    config.withStreamBufferSize(conf.getInt(
        BaiduBosConstants.BOS_STREAM_BUFFER_SIZE,
        STREAM_BUFFER_DEFAULT_SIZE));
    bosClient = new BosClient(config);
  }

  /** {@inheritDoc} */
  @Override
  public PutObjectResponse putObject(
      String bucketName, String key,
      InputStream input, ObjectMetadata metadata)
      throws IOException {
    PutObjectResponse response = null;
    try {
      PutObjectRequest request =
          new PutObjectRequest(
              bucketName, key, input, metadata);
      response = bosClient.putObject(request);
    } catch (BceServiceException e) {
      handleBosServiceException(e);
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public void putEmptyObject(
      String bucketName, String key,
      ObjectMetadata objectMeta) throws IOException {
    InputStream in = null;
    try {
      in = new ByteArrayInputStream(new byte[0]);
      objectMeta.setContentType("binary/octet-stream");
      objectMeta.setContentLength(0);
      PutObjectRequest request =
          new PutObjectRequest(
              bucketName, key, in, objectMeta);
      bosClient.putObject(request);
    } catch (BceServiceException e) {
      if (BOS_UPLOAD_CONFLICT_CODE == e.getStatusCode()
          || BOS_REQUEST_LIMIT_CODE
              == e.getStatusCode()) {
        // status code 412 or 429 means another thread
        // is uploading the same object
        LOG.debug(
            "another thread uploading same bos object");
        try {
          bosClient.getObjectMetadata(bucketName, key);
        } catch (BceServiceException ex) {
          if (BOS_NO_SUCH_KEY_CODE
              == ex.getStatusCode()) {
            LOG.error("concurrentUpload bos failed");
            handleBosServiceException(key, ex);
          }
        }
      } else {
        handleBosServiceException(e);
      }
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException ex) {
        // ignore close exception
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public ObjectMetadata getObjectMetadata(
      String bucketName, String key) throws IOException {
    ObjectMetadata response = null;
    try {
      GetObjectMetadataRequest request =
          new GetObjectMetadataRequest(bucketName, key);
      response = bosClient.getObjectMetadata(request);
    } catch (BceServiceException e) {
      handleBosServiceException(key, e);
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public BosObject getObject(GetObjectRequest request)
      throws IOException {
    BosObject response = null;
    try {
      response = bosClient.getObject(request);
    } catch (BceServiceException e) {
      handleBosServiceException(request.getKey(), e);
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public ListObjectsResponse listObjects(
      ListObjectsRequest request) throws IOException {
    ListObjectsResponse response = null;
    request.setNeedExtMeta(true);
    try {
      response = bosClient.listObjects(request);
    } catch (BceServiceException e) {
      handleBosServiceException(e);
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public UploadPartResponse uploadPart(
      UploadPartRequest request) throws IOException {
    UploadPartResponse response = null;
    try {
      long now = System.currentTimeMillis();
      response = bosClient.uploadPart(request);
      LOG.debug("upload use time : {} ms",
          System.currentTimeMillis() - now);
    } catch (BceServiceException e) {
      handleBosServiceException(e);
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public void deleteObject(String bucketName, String key)
      throws IOException {
    try {
      DeleteObjectRequest request =
          new DeleteObjectRequest(bucketName, key);
      bosClient.deleteObject(request);
    } catch (BceServiceException e) {
      if (BOS_NO_SUCH_KEY_CODE == e.getStatusCode()) {
        // if key does not exist, treat as success
        LOG.warn(
            "Deleting key: {} from bucket: {}"
                + " but key is not exist",
            key, bucketName);
      } else {
        handleBosServiceException(e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteMultipleObjects(
      DeleteMultipleObjectsRequest request)
      throws IOException {
    try {
      bosClient.deleteMultipleObjects(request);
    } catch (BceServiceException e) {
      // BOS_NO_SUCH_KEY_CODE in response.getErrors,
      // don't handle
      handleBosServiceException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public DeleteDirectoryResponse deleteDirectory(
      String bucketName, String key,
      boolean isDeleteRecursive, String marker)
      throws IOException {
    DeleteDirectoryResponse response = null;
    try {
      DeleteDirectoryRequest request =
          new DeleteDirectoryRequest(
              bucketName, key,
              isDeleteRecursive, marker);
      response = bosClient.deleteDirectory(request);
    } catch (BceServiceException e) {
      if (BOS_NO_SUCH_KEY_CODE == e.getStatusCode()) {
        // if key does not exist, treat as success
        LOG.warn(
            "Deleting Directory: {} from bucket: {}"
                + " but Directory is not exist",
            key, bucketName);
      } else {
        handleBosServiceException(e);
      }
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public CopyObjectResponse copyObject(
      String sourceBucketName, String sourceKey,
      String destinationBucketName,
      String destinationKey) throws IOException {
    CopyObjectRequest request = new CopyObjectRequest(
        sourceBucketName, sourceKey,
        destinationBucketName, destinationKey);
    CopyObjectResponse response = null;
    try {
      response = bosClient.copyObject(request);
    } catch (BceServiceException e) {
      handleBosServiceException(sourceKey, e);
    }
    return response;
  }

  /** {@inheritDoc} */
  public CopyObjectResponse copyObject(
      CopyObjectRequest request) throws IOException {
    CopyObjectResponse response = null;
    try {
      response = bosClient.copyObject(request);
    } catch (BceServiceException e) {
      handleBosServiceException(
          request.getSourceKey(), e);
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public RenameObjectResponse renameObject(
      String bucketName, String sourceKey,
      String destinationKey) throws IOException {
    RenameObjectResponse response = null;
    try {
      RenameObjectRequest request =
          new RenameObjectRequest(
              bucketName, sourceKey, destinationKey);
      response = bosClient.renameObject(request);
    } catch (BceServiceException e) {
      if (BOS_NO_SUCH_KEY_CODE == e.getStatusCode()) {
        try {
          bosClient.getObjectMetadata(
              bucketName, destinationKey);
        } catch (BceServiceException e2) {
          handleBosServiceException(sourceKey, e);
        }
      } else {
        handleBosServiceException(sourceKey, e);
      }
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public CompleteMultipartUploadResponse
      completeMultipartUpload(
          CompleteMultipartUploadRequest request)
      throws IOException {
    CompleteMultipartUploadResponse response = null;
    try {
      response =
          bosClient.completeMultipartUpload(request);
    } catch (BceServiceException e) {
      if (BOS_NO_SUCH_KEY_CODE == e.getStatusCode()
          && e.getErrorCode() != null
          && e.getErrorCode().trim()
              .equals("NoSuchUpload")) {
        LOG.warn("The upload ID might be invalid, or"
            + " the multipart upload might have been"
            + " aborted or completed.");
        throw new IOException(
            "NoSuchUpload: upload ID is invalid or"
                + " the upload has been aborted", e);
      } else {
        handleBosServiceException(e);
      }

    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public InitiateMultipartUploadResponse
      initiateMultipartUpload(
          InitiateMultipartUploadRequest request)
      throws IOException {
    InitiateMultipartUploadResponse response = null;
    try {
      response =
          bosClient.initiateMultipartUpload(request);
    } catch (BceServiceException e) {
      handleBosServiceException(e);
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public void abortMultipartUpload(
      String bucketName, String key, String uploadId)
      throws IOException {
    try {
      AbortMultipartUploadRequest request =
          new AbortMultipartUploadRequest(
              bucketName, key, uploadId);
      bosClient.abortMultipartUpload(request);
    } catch (BceServiceException e) {
      handleBosServiceException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    if (bosClient != null) {
      bosClient.shutdown();
      bosClient = null;
    }
  }

  /**
   * Handles a BOS service exception with key context. If the
   * error is a 404, throws {@link FileNotFoundException}.
   *
   * @param key the object key involved in the operation
   * @param e   the BOS service exception
   * @throws IOException wrapping the service exception
   */
  private void handleBosServiceException(
      String key, BceServiceException e)
      throws IOException {
    if (BOS_NO_SUCH_KEY_CODE == e.getStatusCode()) {
      throw new FileNotFoundException(
          "Key '" + key + "' does not exist in BOS");
    } else {
      handleBosServiceException(e);
    }
  }

  /**
   * Handles a BOS service exception, translating it into the
   * appropriate IOException subclass based on status code and
   * error code.
   *
   * @param e the BOS service exception
   * @throws IOException wrapping the service exception
   */
  private void handleBosServiceException(
      BceServiceException e) throws IOException {
    // process all 400 status, because as token expired
    // bos server return "Error Code=null" when request
    // object use a http head method
    if (400 == e.getStatusCode()
        && (e.getErrorCode() == null
            || "null".equals(e.getErrorCode())
            || "InvalidSessionToken".equals(
                e.getErrorCode().trim()))) {
      LOG.error(
          "sts session expired or invalid,"
              + " need to update client");
      throw new SessionTokenExpireException(e);
    } else if (400 == e.getStatusCode()) {
      LOG.error("request unknown error : {}",
          e.getCause());
      throw new SessionTokenExpireException(e);
    } else if (BOS_REQUEST_LIMIT_CODE
        == e.getStatusCode()
        && (e.getErrorCode() == null
            || e.getErrorCode().trim()
                .equals("null"))) {
      throw new BandwidthLimitException(
          new IOException(
              "trigger bos rate limit"
                  + " for too many requests !!!"));
    } else if (BOS_REQUEST_LIMIT_CODE
        == e.getStatusCode()
        && e.getErrorCode().trim()
            .equals("RequestRateLimitExceeded")) {
      throw new BosHotObjectException(
          new IOException(
              "trigger bos object rate limit !!!"));
    } else if (BOS_REQUEST_LIMIT_CODE
        == e.getStatusCode()) {
      throw new BosHotObjectException(
          new IOException(
              "status code 429 !!!" + e.getCause()));
    } else if (e.getCause() instanceof IOException) {
      throw (IOException) e.getCause();
    } else {
      LOG.debug(
          "BOS Error code: {}; BOS Error message: {}",
          e.getErrorCode(), e.getErrorMessage());
      if (5 == (e.getStatusCode() / 100)) {
        throw new BosServerException(e);
      }
      throw new BosException(e);
    }
  }

  /** {@inheritDoc} */
  public boolean isHierarchyBucket(String bucketName)
      throws IOException {
    HeadBucketResponse response = null;
    try {
      HeadBucketRequest request =
          new HeadBucketRequest(bucketName);
      response = this.bosClient.headBucket(request);
    } catch (BceServiceException e) {
      handleBosServiceException(e);
    }

    if (response != null
        && response.getMetadata() != null) {
      return "namespace".equals(
          response.getMetadata().getBucketType());
    }

    return false;
  }

  /**
   * Initializes the current user name during client
   * initialization.
   */
  private void updateUserForInit() {
    try {
      UserGroupInformation ugi =
          UserGroupInformation.getCurrentUser();
      user = ugi.getShortUserName();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  public UploadPartCopyResponse uploadPartCopy(
      UploadPartCopyRequest request) throws IOException {
    UploadPartCopyResponse response = null;
    try {
      response = bosClient.uploadPartCopy(request);
    } catch (BceServiceException e) {
      handleBosServiceException(e);
    }
    return response;
  }

}
