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
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * Core implementation of Aliyun OSS Filesystem for Hadoop.
 * Provides the bridging logic between Hadoop's abstract filesystem and
 * Aliyun OSS.
 */
public class AliyunOSSFileSystemStore {
  public static final Logger LOG =
      LoggerFactory.getLogger(AliyunOSSFileSystemStore.class);
  private String username;
  private FileSystem.Statistics statistics;
  private OSSClient ossClient;
  private String bucketName;
  private long uploadPartSize;
  private int maxKeys;
  private String serverSideEncryptionAlgorithm;
  private boolean useListV1;

  public void initialize(URI uri, Configuration conf, String user,
                         FileSystem.Statistics stat) throws IOException {
    this.username = user;
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
    clientConf.setUserAgent(
        conf.get(USER_AGENT_PREFIX, USER_AGENT_PREFIX_DEFAULT) + ", Hadoop/"
            + VersionInfo.getVersion());

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
    if (StringUtils.isEmpty(endPoint)) {
      throw new IllegalArgumentException("Aliyun OSS endpoint should not be " +
          "null or empty. Please set proper endpoint with 'fs.oss.endpoint'.");
    }
    CredentialsProvider provider =
        AliyunOSSUtils.getCredentialsProvider(uri, conf);
    ossClient = new OSSClient(endPoint, provider, clientConf);
    uploadPartSize = AliyunOSSUtils.getMultipartSizeProperty(conf,
        MULTIPART_UPLOAD_PART_SIZE_KEY, MULTIPART_UPLOAD_PART_SIZE_DEFAULT);

    serverSideEncryptionAlgorithm =
        conf.get(SERVER_SIDE_ENCRYPTION_ALGORITHM_KEY, "");

    bucketName = uri.getHost();

    String cannedACLName = conf.get(CANNED_ACL_KEY, CANNED_ACL_DEFAULT);
    if (StringUtils.isNotEmpty(cannedACLName)) {
      CannedAccessControlList cannedACL =
          CannedAccessControlList.valueOf(cannedACLName);
      ossClient.setBucketAcl(bucketName, cannedACL);
      statistics.incrementWriteOps(1);
    }

    maxKeys = conf.getInt(MAX_PAGING_KEYS_KEY, MAX_PAGING_KEYS_DEFAULT);
    int listVersion = conf.getInt(LIST_VERSION, DEFAULT_LIST_VERSION);
    if (listVersion < 1 || listVersion > 2) {
      LOG.warn("Configured fs.oss.list.version {} is invalid, forcing " +
          "version 2", listVersion);
    }
    useListV1 = (listVersion == 1);
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
   * @throws IOException if failed to delete objects.
   */
  public void deleteObjects(List<String> keysToDelete) throws IOException {
    if (CollectionUtils.isEmpty(keysToDelete)) {
      LOG.warn("Keys to delete is empty.");
      return;
    }

    int retry = 10;
    int tries = 0;
    while (CollectionUtils.isNotEmpty(keysToDelete)) {
      DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucketName);
      deleteRequest.setKeys(keysToDelete);
      // There are two modes to do batch delete:
      // 1. verbose mode: A list of all deleted objects is returned.
      // 2. quiet mode: No message body is returned.
      // Here, we choose the verbose mode to do batch delete.
      deleteRequest.setQuiet(false);
      DeleteObjectsResult result = ossClient.deleteObjects(deleteRequest);
      statistics.incrementWriteOps(1);
      final List<String> deletedObjects = result.getDeletedObjects();
      keysToDelete = keysToDelete.stream().filter(item -> !deletedObjects.contains(item))
          .collect(Collectors.toList());
      tries++;
      if (tries == retry) {
        break;
      }
    }

    if (tries == retry && CollectionUtils.isNotEmpty(keysToDelete)) {
      // Most of time, it is impossible to try 10 times, expect the
      // Aliyun OSS service problems.
      throw new IOException("Failed to delete Aliyun OSS objects for " + tries + " times.");
    }
  }

  /**
   * Delete a directory from Aliyun OSS.
   *
   * @param key directory key to delete.
   * @throws IOException if failed to delete directory.
   */
  public void deleteDirs(String key) throws IOException {
    OSSListRequest listRequest = createListObjectsRequest(key,
        maxKeys, null, null, true);
    while (true) {
      OSSListResult objects = listObjects(listRequest);
      statistics.incrementReadOps(1);
      List<String> keysToDelete = new ArrayList<String>();
      for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
        keysToDelete.add(objectSummary.getKey());
      }
      deleteObjects(keysToDelete);
      if (objects.isTruncated()) {
        if (objects.isV1()) {
          listRequest.getV1().setMarker(objects.getV1().getNextMarker());
        } else {
          listRequest.getV2().setContinuationToken(
              objects.getV2().getNextContinuationToken());
        }
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
      GenericRequest request = new GenericRequest(bucketName, key);
      request.setLogEnabled(false);
      ObjectMetadata objectMeta = ossClient.getObjectMetadata(request);
      statistics.incrementReadOps(1);
      return objectMeta;
    } catch (OSSException osse) {
      LOG.debug("Exception thrown when get object meta: "
              + key + ", exception: " + osse);
      return null;
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
      statistics.incrementWriteOps(1);
    } finally {
      in.close();
    }
  }

  /**
   * Copy an object from source key to destination key.
   *
   * @param srcKey source key.
   * @param srcLen source file length.
   * @param dstKey destination key.
   * @return true if file is successfully copied.
   */
  public boolean copyFile(String srcKey, long srcLen, String dstKey) {
    try {
      //1, try single copy first
      return singleCopy(srcKey, dstKey);
    } catch (Exception e) {
      //2, if failed(shallow copy not supported), then multi part copy
      LOG.debug("Exception thrown when copy file: " + srcKey
          + ", exception: " + e + ", use multipartCopy instead");
      return multipartCopy(srcKey, srcLen, dstKey);
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
    statistics.incrementWriteOps(1);
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
        statistics.incrementBytesWritten(size);
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
   * Upload an input stream as an OSS object, using single upload.
   * @param key object key.
   * @param in input stream to upload.
   * @param size size of the input stream.
   * @throws IOException if failed to upload object.
   */
  public void uploadObject(String key, InputStream in, long size)
      throws IOException {
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(size);

    if (StringUtils.isNotEmpty(serverSideEncryptionAlgorithm)) {
      meta.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }

    PutObjectResult result = ossClient.putObject(bucketName, key, in, meta);
    LOG.debug(result.getETag());
    statistics.incrementWriteOps(1);
  }

  /**
   * list objects.
   *
   * @param listRequest list request.
   * @return a list of matches.
   */
  public OSSListResult listObjects(OSSListRequest listRequest) {
    OSSListResult listResult;
    if (listRequest.isV1()) {
      listResult = OSSListResult.v1(
          ossClient.listObjects(listRequest.getV1()));
    } else {
      listResult = OSSListResult.v2(
          ossClient.listObjectsV2(listRequest.getV2()));
    }
    statistics.incrementReadOps(1);
    return listResult;
  }

  /**
   * continue to list objects depends on previous list result.
   *
   * @param listRequest list request.
   * @param preListResult previous list result.
   * @return a list of matches.
   */
  public OSSListResult continueListObjects(OSSListRequest listRequest,
      OSSListResult preListResult) {
    OSSListResult listResult;
    if (listRequest.isV1()) {
      listRequest.getV1().setMarker(preListResult.getV1().getNextMarker());
      listResult = OSSListResult.v1(
          ossClient.listObjects(listRequest.getV1()));
    } else {
      listRequest.getV2().setContinuationToken(
          preListResult.getV2().getNextContinuationToken());
      listResult = OSSListResult.v2(
          ossClient.listObjectsV2(listRequest.getV2()));
    }
    statistics.incrementReadOps(1);
    return listResult;
  }

  /**
   * create list objects request.
   *
   * @param prefix prefix.
   * @param maxListingLength max no. of entries
   * @param marker last key in any previous search.
   * @param continuationToken list from a specific point.
   * @param recursive whether to list directory recursively.
   * @return a list of matches.
   */
  protected OSSListRequest createListObjectsRequest(String prefix,
      int maxListingLength, String marker,
      String continuationToken, boolean recursive) {
    String delimiter = recursive ? null : "/";
    prefix = AliyunOSSUtils.maybeAddTrailingSlash(prefix);
    if (useListV1) {
      ListObjectsRequest listRequest = new ListObjectsRequest(bucketName);
      listRequest.setPrefix(prefix);
      listRequest.setDelimiter(delimiter);
      listRequest.setMaxKeys(maxListingLength);
      listRequest.setMarker(marker);
      return OSSListRequest.v1(listRequest);
    } else {
      ListObjectsV2Request listV2Request = new ListObjectsV2Request(bucketName);
      listV2Request.setPrefix(prefix);
      listV2Request.setDelimiter(delimiter);
      listV2Request.setMaxKeys(maxListingLength);
      listV2Request.setContinuationToken(continuationToken);
      return OSSListRequest.v2(listV2Request);
    }
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
      InputStream in = ossClient.getObject(request).getObjectContent();
      statistics.incrementReadOps(1);
      return in;
    } catch (OSSException | ClientException e) {
      LOG.error("Exception thrown when store retrieves key: "
              + key + ", exception: " + e);
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
   * @throws IOException if failed to clean up objects.
   */
  public void purge(String prefix) throws IOException {
    deleteDirs(prefix);
  }

  public RemoteIterator<LocatedFileStatus> singleStatusRemoteIterator(
      final FileStatus fileStatus, final BlockLocation[] locations) {
    return new RemoteIterator<LocatedFileStatus>() {
      private boolean hasNext = true;
      @Override
      public boolean hasNext() throws IOException {
        return fileStatus != null && hasNext;
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (hasNext()) {
          LocatedFileStatus s = new LocatedFileStatus(fileStatus,
              fileStatus.isFile() ? locations : null);
          hasNext = false;
          return s;
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public RemoteIterator<LocatedFileStatus> createLocatedFileStatusIterator(
      final String prefix, final int maxListingLength, FileSystem fs,
      PathFilter filter, FileStatusAcceptor acceptor, boolean recursive) {
    return new RemoteIterator<LocatedFileStatus>() {
      private boolean firstListing = true;
      private boolean meetEnd = false;
      private ListIterator<FileStatus> batchIterator;
      private OSSListRequest listRequest = null;

      @Override
      public boolean hasNext() throws IOException {
        if (firstListing) {
          requestNextBatch();
          firstListing = false;
        }
        return batchIterator.hasNext() || requestNextBatch();
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (hasNext()) {
          FileStatus status = batchIterator.next();
          BlockLocation[] locations = fs.getFileBlockLocations(status,
              0, status.getLen());
          return new LocatedFileStatus(
              status, status.isFile() ? locations : null);
        } else {
          throw new NoSuchElementException();
        }
      }

      private boolean requestNextBatch() {
        while (!meetEnd) {
          if (continueListStatus()) {
            return true;
          }
        }

        return false;
      }

      private boolean continueListStatus() {
        if (meetEnd) {
          return false;
        }
        if (listRequest == null) {
          listRequest = createListObjectsRequest(prefix,
              maxListingLength, null, null, recursive);
        }
        OSSListResult listing = listObjects(listRequest);
        List<FileStatus> stats = new ArrayList<>(
            listing.getObjectSummaries().size() +
            listing.getCommonPrefixes().size());
        for (OSSObjectSummary summary : listing.getObjectSummaries()) {
          String key = summary.getKey();
          Path path = fs.makeQualified(new Path("/" + key));
          if (filter.accept(path) && acceptor.accept(path, summary)) {
            FileStatus status = new OSSFileStatus(summary.getSize(),
                key.endsWith("/"), 1, fs.getDefaultBlockSize(path),
                summary.getLastModified().getTime(), path, username);
            stats.add(status);
          }
        }

        for (String commonPrefix : listing.getCommonPrefixes()) {
          Path path = fs.makeQualified(new Path("/" + commonPrefix));
          if (filter.accept(path) && acceptor.accept(path, commonPrefix)) {
            FileStatus status = new OSSFileStatus(0, true, 1, 0, 0,
                path, username);
            stats.add(status);
          }
        }

        batchIterator = stats.listIterator();
        if (listing.isTruncated()) {
          if (listing.isV1()) {
            listRequest.getV1().setMarker(listing.getV1().getNextMarker());
          } else {
            listRequest.getV2().setContinuationToken(
                listing.getV2().getNextContinuationToken());
          }
        } else {
          meetEnd = true;
        }
        statistics.incrementReadOps(1);
        return batchIterator.hasNext();
      }
    };
  }

  public PartETag uploadPart(OSSDataBlocks.BlockUploadData partData,
      long size, String key, String uploadId, int idx) throws IOException {
    if (partData.hasFile()) {
      return uploadPart(partData.getFile(), key, uploadId, idx);
    } else {
      return uploadPart(partData.getUploadStream(), size, key, uploadId, idx);
    }
  }

  public PartETag uploadPart(File file, String key, String uploadId, int idx)
      throws IOException {
    InputStream in = new FileInputStream(file);
    try {
      return uploadPart(in, file.length(), key, uploadId, idx);
    } finally {
      in.close();
    }
  }

  public PartETag uploadPart(InputStream in, long size, String key,
      String uploadId, int idx) throws IOException {
    Exception caught = null;
    int tries = 3;
    while (tries > 0) {
      try {
        UploadPartRequest uploadRequest = new UploadPartRequest();
        uploadRequest.setBucketName(bucketName);
        uploadRequest.setKey(key);
        uploadRequest.setUploadId(uploadId);
        uploadRequest.setInputStream(in);
        uploadRequest.setPartSize(size);
        uploadRequest.setPartNumber(idx);
        UploadPartResult uploadResult = ossClient.uploadPart(uploadRequest);
        statistics.incrementWriteOps(1);
        return uploadResult.getPartETag();
      } catch (Exception e) {
        LOG.debug("Failed to upload " + key + ", part " + idx +
            "try again.", e);
        caught = e;
      }
      tries--;
    }

    assert (caught != null);
    throw new IOException("Failed to upload " + key + ", part " + idx +
        " for 3 times.", caught);
  }

  /**
   * Initiate multipart upload.
   * @param key object key.
   * @return upload id.
   */
  public String getUploadId(String key) {
    InitiateMultipartUploadRequest initiateMultipartUploadRequest =
        new InitiateMultipartUploadRequest(bucketName, key);
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
    return initiateMultipartUploadResult.getUploadId();
  }

  /**
   * Complete the specific multipart upload.
   * @param key object key.
   * @param uploadId upload id of this multipart upload.
   * @param partETags part etags need to be completed.
   * @return CompleteMultipartUploadResult.
   */
  public CompleteMultipartUploadResult completeMultipartUpload(String key,
      String uploadId, List<PartETag> partETags) {
    Collections.sort(partETags, new PartNumberAscendComparator());
    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        new CompleteMultipartUploadRequest(bucketName, key, uploadId,
            partETags);
    return ossClient.completeMultipartUpload(completeMultipartUploadRequest);
  }

  /**
   * Abort the specific multipart upload.
   * @param key object key.
   * @param uploadId upload id of this multipart upload.
   */
  public void abortMultipartUpload(String key, String uploadId) {
    AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(
        bucketName, key, uploadId);
    ossClient.abortMultipartUpload(request);
  }

  private static class PartNumberAscendComparator
      implements Comparator<PartETag>, Serializable {
    @Override
    public int compare(PartETag o1, PartETag o2) {
      if (o1.getPartNumber() > o2.getPartNumber()) {
        return 1;
      } else {
        return -1;
      }
    }
  }
}
