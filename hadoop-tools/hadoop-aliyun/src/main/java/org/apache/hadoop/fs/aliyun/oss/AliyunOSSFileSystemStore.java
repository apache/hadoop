/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CannedAccessControlList;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * Core implementation of Aliyun OSS Filesystem for Hadoop.
 * Provides the bridging logic between Hadoop's abstract filesystem and
 * Aliyun OSS.
 */
public class AliyunOSSFileSystemStore {
  public static final Logger LOG =
      LoggerFactory.getLogger(AliyunOSSFileSystemStore.class);
  private FileSystem.Statistics statistics;
  private OSSClient ossClient;
  private String bucketName;
  private long uploadPartSize;
  private long multipartThreshold;
  private long partSize;
  private int maxKeys;
  private String serverSideEncryptionAlgorithm;

  public void initialize(URI uri, Configuration conf,
                         FileSystem.Statistics stat) throws IOException {
    statistics = stat;
    ClientConfiguration clientConf = new ClientConfiguration();
    clientConf.setMaxConnections(conf.getInt(MAXIMUM_CONNECTIONS_KEY,
        MAXIMUM_CONNECTIONS_DEFAULT));
    boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS_KEY,
        SECURE_CONNECTIONS_DEFAULT);
    clientConf.setProtocol(secureConnections ? Protocol.HTTPS : Protocol.HTTP);
    clientConf.setMaxErrorRetry(conf.getInt(MAX_ERROR_RETRIES_KEY,
        MAX_ERROR_RETRIES_DEFAULT));
    clientConf.setConnectionTimeout(conf.getInt(ESTABLISH_TIMEOUT_KEY,
        ESTABLISH_TIMEOUT_DEFAULT));
    clientConf.setSocketTimeout(conf.getInt(SOCKET_TIMEOUT_KEY,
        SOCKET_TIMEOUT_DEFAULT));

    String proxyHost = conf.getTrimmed(PROXY_HOST_KEY, "");
    int proxyPort = conf.getInt(PROXY_PORT_KEY, -1);
    if (StringUtils.isNotEmpty(proxyHost)) {
      clientConf.setProxyHost(proxyHost);
      if (proxyPort >= 0) {
        clientConf.setProxyPort(proxyPort);
      } else {
        if (secureConnections) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          clientConf.setProxyPort(443);
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          clientConf.setProxyPort(80);
        }
      }
      String proxyUsername = conf.getTrimmed(PROXY_USERNAME_KEY);
      String proxyPassword = conf.getTrimmed(PROXY_PASSWORD_KEY);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME_KEY + " or " +
            PROXY_PASSWORD_KEY + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      clientConf.setProxyUsername(proxyUsername);
      clientConf.setProxyPassword(proxyPassword);
      clientConf.setProxyDomain(conf.getTrimmed(PROXY_DOMAIN_KEY));
      clientConf.setProxyWorkstation(conf.getTrimmed(PROXY_WORKSTATION_KEY));
    } else if (proxyPort >= 0) {
      String msg = "Proxy error: " + PROXY_PORT_KEY + " set without " +
          PROXY_HOST_KEY;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    String endPoint = conf.getTrimmed(ENDPOINT_KEY, "");
    CredentialsProvider provider =
        AliyunOSSUtils.getCredentialsProvider(conf);
    ossClient = new OSSClient(endPoint, provider, clientConf);
    uploadPartSize = conf.getLong(MULTIPART_UPLOAD_SIZE_KEY,
        MULTIPART_UPLOAD_SIZE_DEFAULT);
    multipartThreshold = conf.getLong(MIN_MULTIPART_UPLOAD_THRESHOLD_KEY,
        MIN_MULTIPART_UPLOAD_THRESHOLD_DEFAULT);
    partSize = conf.getLong(MULTIPART_UPLOAD_SIZE_KEY,
        MULTIPART_UPLOAD_SIZE_DEFAULT);
    if (partSize < MIN_MULTIPART_UPLOAD_PART_SIZE) {
      partSize = MIN_MULTIPART_UPLOAD_PART_SIZE;
    }
    serverSideEncryptionAlgorithm =
        conf.get(SERVER_SIDE_ENCRYPTION_ALGORITHM_KEY, "");

    if (uploadPartSize < 5 * 1024 * 1024) {
      LOG.warn(MULTIPART_UPLOAD_SIZE_KEY + " must be at least 5 MB");
      uploadPartSize = 5 * 1024 * 1024;
    }

    if (multipartThreshold < 5 * 1024 * 1024) {
      LOG.warn(MIN_MULTIPART_UPLOAD_THRESHOLD_KEY + " must be at least 5 MB");
      multipartThreshold = 5 * 1024 * 1024;
    }

    if (multipartThreshold > 1024 * 1024 * 1024) {
      LOG.warn(MIN_MULTIPART_UPLOAD_THRESHOLD_KEY + " must be less than 1 GB");
      multipartThreshold = 1024 * 1024 * 1024;
    }

    String cannedACLName = conf.get(CANNED_ACL_KEY, CANNED_ACL_DEFAULT);
    if (StringUtils.isNotEmpty(cannedACLName)) {
      CannedAccessControlList cannedACL =
          CannedAccessControlList.valueOf(cannedACLName);
      ossClient.setBucketAcl(bucketName, cannedACL);
    }

    maxKeys = conf.getInt(MAX_PAGING_KEYS_KEY, MAX_PAGING_KEYS_DEFAULT);
    bucketName = uri.getHost();
  }

  /**
   * Delete an object, and update write operation statistics.
   *
   * @param key key to blob to delete.
   */
  public void deleteObject(String key) {
    ossClient.deleteObject(bucketName, key);
    statistics.incrementWriteOps(1);
  }

  /**
   * Delete a list of keys, and update write operation statistics.
   *
   * @param keysToDelete collection of keys to delete.
   */
  public void deleteObjects(List<String> keysToDelete) {
    if (CollectionUtils.isNotEmpty(keysToDelete)) {
      DeleteObjectsRequest deleteRequest =
          new DeleteObjectsRequest(bucketName);
      deleteRequest.setKeys(keysToDelete);
      ossClient.deleteObjects(deleteRequest);
      statistics.incrementWriteOps(keysToDelete.size());
    }
  }

  /**
   * Delete a directory from Aliyun OSS.
   *
   * @param key directory key to delete.
   */
  public void deleteDirs(String key) {
    key = AliyunOSSUtils.maybeAddTrailingSlash(key);
    ListObjectsRequest listRequest = new ListObjectsRequest(bucketName);
    listRequest.setPrefix(key);
    listRequest.setDelimiter(null);
    listRequest.setMaxKeys(maxKeys);

    while (true) {
      ObjectListing objects = ossClient.listObjects(listRequest);
      statistics.incrementReadOps(1);
      List<String> keysToDelete = new ArrayList<String>();
      for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
        keysToDelete.add(objectSummary.getKey());
      }
      deleteObjects(keysToDelete);
      if (objects.isTruncated()) {
        listRequest.setMarker(objects.getNextMarker());
      } else {
        break;
      }
    }
  }

  /**
   * Return metadata of a given object key.
   *
   * @param key object key.
   * @return return null if key does not exist.
   */
  public ObjectMetadata getObjectMetadata(String key) {
    try {
      return ossClient.getObjectMetadata(bucketName, key);
    } catch (OSSException osse) {
      return null;
    } finally {
      statistics.incrementReadOps(1);
    }
  }

  /**
   * Upload an empty file as an OSS object, using single upload.
   *
   * @param key object key.
   * @throws IOException if failed to upload object.
   */
  public void storeEmptyFile(String key) throws IOException {
    ObjectMetadata dirMeta = new ObjectMetadata();
    byte[] buffer = new byte[0];
    ByteArrayInputStream in = new ByteArrayInputStream(buffer);
    dirMeta.setContentLength(0);
    try {
      ossClient.putObject(bucketName, key, in, dirMeta);
    } finally {
      in.close();
    }
  }

  /**
   * Copy an object from source key to destination key.
   *
   * @param srcKey source key.
   * @param dstKey destination key.
   * @return true if file is successfully copied.
   */
  public boolean copyFile(String srcKey, String dstKey) {
    ObjectMetadata objectMeta =
        ossClient.getObjectMetadata(bucketName, srcKey);
    long contentLength = objectMeta.getContentLength();
    if (contentLength <= multipartThreshold) {
      return singleCopy(srcKey, dstKey);
    } else {
      return multipartCopy(srcKey, contentLength, dstKey);
    }
  }

  /**
   * Use single copy to copy an OSS object.
   * (The caller should make sure srcPath is a file and dstPath is valid)
   *
   * @param srcKey source key.
   * @param dstKey destination key.
   * @return true if object is successfully copied.
   */
  private boolean singleCopy(String srcKey, String dstKey) {
    CopyObjectResult copyResult =
        ossClient.copyObject(bucketName, srcKey, bucketName, dstKey);
    LOG.debug(copyResult.getETag());
    return true;
  }

  /**
   * Use multipart copy to copy an OSS object.
   * (The caller should make sure srcPath is a file and dstPath is valid)
   *
   * @param srcKey source key.
   * @param contentLength data size of the object to copy.
   * @param dstKey destination key.
   * @return true if success, or false if upload is aborted.
   */
  private boolean multipartCopy(String srcKey, long contentLength,
      String dstKey) {
    long realPartSize =
        AliyunOSSUtils.calculatePartSize(contentLength, uploadPartSize);
    int partNum = (int) (contentLength / realPartSize);
    if (contentLength % realPartSize != 0) {
      partNum++;
    }
    InitiateMultipartUploadRequest initiateMultipartUploadRequest =
        new InitiateMultipartUploadRequest(bucketName, dstKey);
    ObjectMetadata meta = new ObjectMetadata();
    if (StringUtils.isNotEmpty(serverSideEncryptionAlgorithm)) {
      meta.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    initiateMultipartUploadRequest.setObjectMetadata(meta);
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
    String uploadId = initiateMultipartUploadResult.getUploadId();
    List<PartETag> partETags = new ArrayList<PartETag>();
    try {
      for (int i = 0; i < partNum; i++) {
        long skipBytes = realPartSize * i;
        long size = (realPartSize < contentLength - skipBytes) ?
            realPartSize : contentLength - skipBytes;
        UploadPartCopyRequest partCopyRequest = new UploadPartCopyRequest();
        partCopyRequest.setSourceBucketName(bucketName);
        partCopyRequest.setSourceKey(srcKey);
        partCopyRequest.setBucketName(bucketName);
        partCopyRequest.setKey(dstKey);
        partCopyRequest.setUploadId(uploadId);
        partCopyRequest.setPartSize(size);
        partCopyRequest.setBeginIndex(skipBytes);
        partCopyRequest.setPartNumber(i + 1);
        UploadPartCopyResult partCopyResult =
            ossClient.uploadPartCopy(partCopyRequest);
        statistics.incrementWriteOps(1);
        partETags.add(partCopyResult.getPartETag());
      }
      CompleteMultipartUploadRequest completeMultipartUploadRequest =
          new CompleteMultipartUploadRequest(bucketName, dstKey,
              uploadId, partETags);
      CompleteMultipartUploadResult completeMultipartUploadResult =
          ossClient.completeMultipartUpload(completeMultipartUploadRequest);
      LOG.debug(completeMultipartUploadResult.getETag());
      return true;
    } catch (OSSException | ClientException e) {
      AbortMultipartUploadRequest abortMultipartUploadRequest =
          new AbortMultipartUploadRequest(bucketName, dstKey, uploadId);
      ossClient.abortMultipartUpload(abortMultipartUploadRequest);
      return false;
    }
  }

  /**
   * Upload a file as an OSS object, using single upload.
   *
   * @param key object key.
   * @param file local file to upload.
   * @throws IOException if failed to upload object.
   */
  public void uploadObject(String key, File file) throws IOException {
    File object = file.getAbsoluteFile();
    FileInputStream fis = new FileInputStream(object);
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(object.length());
    if (StringUtils.isNotEmpty(serverSideEncryptionAlgorithm)) {
      meta.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    try {
      PutObjectResult result = ossClient.putObject(bucketName, key, fis, meta);
      LOG.debug(result.getETag());
      statistics.incrementWriteOps(1);
    } finally {
      fis.close();
    }
  }

  /**
   * Upload a file as an OSS object, using multipart upload.
   *
   * @param key object key.
   * @param file local file to upload.
   * @throws IOException if failed to upload object.
   */
  public void multipartUploadObject(String key, File file) throws IOException {
    File object = file.getAbsoluteFile();
    long dataLen = object.length();
    long realPartSize = AliyunOSSUtils.calculatePartSize(dataLen, partSize);
    int partNum = (int) (dataLen / realPartSize);
    if (dataLen % realPartSize != 0) {
      partNum += 1;
    }

    InitiateMultipartUploadRequest initiateMultipartUploadRequest =
        new InitiateMultipartUploadRequest(bucketName, key);
    ObjectMetadata meta = new ObjectMetadata();
    if (StringUtils.isNotEmpty(serverSideEncryptionAlgorithm)) {
      meta.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    initiateMultipartUploadRequest.setObjectMetadata(meta);
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
    List<PartETag> partETags = new ArrayList<PartETag>();
    String uploadId = initiateMultipartUploadResult.getUploadId();

    try {
      for (int i = 0; i < partNum; i++) {
        // TODO: Optimize this, avoid opening the object multiple times
        FileInputStream fis = new FileInputStream(object);
        try {
          long skipBytes = realPartSize * i;
          AliyunOSSUtils.skipFully(fis, skipBytes);
          long size = (realPartSize < dataLen - skipBytes) ?
              realPartSize : dataLen - skipBytes;
          UploadPartRequest uploadPartRequest = new UploadPartRequest();
          uploadPartRequest.setBucketName(bucketName);
          uploadPartRequest.setKey(key);
          uploadPartRequest.setUploadId(uploadId);
          uploadPartRequest.setInputStream(fis);
          uploadPartRequest.setPartSize(size);
          uploadPartRequest.setPartNumber(i + 1);
          UploadPartResult uploadPartResult =
              ossClient.uploadPart(uploadPartRequest);
          statistics.incrementWriteOps(1);
          partETags.add(uploadPartResult.getPartETag());
        } finally {
          fis.close();
        }
      }
      CompleteMultipartUploadRequest completeMultipartUploadRequest =
          new CompleteMultipartUploadRequest(bucketName, key,
              uploadId, partETags);
      CompleteMultipartUploadResult completeMultipartUploadResult =
          ossClient.completeMultipartUpload(completeMultipartUploadRequest);
      LOG.debug(completeMultipartUploadResult.getETag());
    } catch (OSSException | ClientException e) {
      AbortMultipartUploadRequest abortMultipartUploadRequest =
          new AbortMultipartUploadRequest(bucketName, key, uploadId);
      ossClient.abortMultipartUpload(abortMultipartUploadRequest);
    }
  }

  /**
   * list objects.
   *
   * @param prefix prefix.
   * @param maxListingLength max no. of entries
   * @param marker last key in any previous search.
   * @param recursive whether to list directory recursively.
   * @return a list of matches.
   */
  public ObjectListing listObjects(String prefix, int maxListingLength,
                                   String marker, boolean recursive) {
    String delimiter = recursive ? null : "/";
    prefix = AliyunOSSUtils.maybeAddTrailingSlash(prefix);
    ListObjectsRequest listRequest = new ListObjectsRequest(bucketName);
    listRequest.setPrefix(prefix);
    listRequest.setDelimiter(delimiter);
    listRequest.setMaxKeys(maxListingLength);
    listRequest.setMarker(marker);

    ObjectListing listing = ossClient.listObjects(listRequest);
    statistics.incrementReadOps(1);
    return listing;
  }

  /**
   * Retrieve a part of an object.
   *
   * @param key the object name that is being retrieved from the Aliyun OSS.
   * @param byteStart start position.
   * @param byteEnd end position.
   * @return This method returns null if the key is not found.
   */
  public InputStream retrieve(String key, long byteStart, long byteEnd) {
    try {
      GetObjectRequest request = new GetObjectRequest(bucketName, key);
      request.setRange(byteStart, byteEnd);
      return ossClient.getObject(request).getObjectContent();
    } catch (OSSException | ClientException e) {
      return null;
    }
  }

  /**
   * Close OSS client properly.
   */
  public void close() {
    if (ossClient != null) {
      ossClient.shutdown();
      ossClient = null;
    }
  }

  /**
   * Clean up all objects matching the prefix.
   *
   * @param prefix Aliyun OSS object prefix.
   */
  public void purge(String prefix) {
    String key;
    try {
      ObjectListing objects = listObjects(prefix, maxKeys, null, true);
      for (OSSObjectSummary object : objects.getObjectSummaries()) {
        key = object.getKey();
        ossClient.deleteObject(bucketName, key);
      }

      for (String dir: objects.getCommonPrefixes()) {
        deleteDirs(dir);
      }
    } catch (OSSException | ClientException e) {
      LOG.error("Failed to purge " + prefix);
    }
  }
}
