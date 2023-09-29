/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.endpoint.SuffixEndpointBuilder;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.AbortMultipartUploadRequest;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.CompleteMultipartUploadRequest;
import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.DeleteObjectRequest;
import com.qcloud.cos.model.GetObjectMetadataRequest;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.InitiateMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadResult;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PutObjectResult;
import com.qcloud.cos.model.UploadPartRequest;
import com.qcloud.cos.model.UploadPartResult;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.utils.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.auth.COSCredentialsProviderList;
import org.apache.hadoop.util.VersionInfo;
import org.apache.http.HttpStatus;

/**
 * The class actually performs access operation to the COS blob store.
 * It provides the bridging logic for the Hadoop's abstract filesystem and COS.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class CosNativeFileSystemStore implements NativeFileSystemStore {
  private COSClient cosClient;
  private String bucketName;
  private int maxRetryTimes;

  public static final Logger LOG =
      LoggerFactory.getLogger(CosNativeFileSystemStore.class);

  /**
   * Initialize the client to access COS blob storage.
   *
   * @param conf Hadoop configuration with COS configuration options.
   * @throws IOException Initialize the COS client failed,
   *                     caused by incorrect options.
   */
  private void initCOSClient(URI uri, Configuration conf) throws IOException {
    COSCredentialsProviderList credentialProviderList =
        CosNUtils.createCosCredentialsProviderSet(uri, conf);
    String region = conf.get(CosNConfigKeys.COSN_REGION_KEY);
    String endpointSuffix = conf.get(
        CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY);
    if (null == region && null == endpointSuffix) {
      String exceptionMsg = String.format("config %s and %s at least one",
          CosNConfigKeys.COSN_REGION_KEY,
          CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY);
      throw new IOException(exceptionMsg);
    }

    COSCredentials cosCred;
    cosCred = new BasicCOSCredentials(
        credentialProviderList.getCredentials().getCOSAccessKeyId(),
        credentialProviderList.getCredentials().getCOSSecretKey());

    boolean useHttps = conf.getBoolean(CosNConfigKeys.COSN_USE_HTTPS_KEY,
        CosNConfigKeys.DEFAULT_USE_HTTPS);

    ClientConfig config;
    if (null == region) {
      config = new ClientConfig(new Region(""));
      config.setEndpointBuilder(new SuffixEndpointBuilder(endpointSuffix));
    } else {
      config = new ClientConfig(new Region(region));
    }
    if (useHttps) {
      config.setHttpProtocol(HttpProtocol.https);
    }

    config.setUserAgent(conf.get(CosNConfigKeys.USER_AGENT,
        CosNConfigKeys.DEFAULT_USER_AGENT) + " For " + " Hadoop "
        + VersionInfo.getVersion());

    this.maxRetryTimes = conf.getInt(CosNConfigKeys.COSN_MAX_RETRIES_KEY,
        CosNConfigKeys.DEFAULT_MAX_RETRIES);

    config.setMaxConnectionsCount(
        conf.getInt(CosNConfigKeys.MAX_CONNECTION_NUM,
            CosNConfigKeys.DEFAULT_MAX_CONNECTION_NUM));

    this.cosClient = new COSClient(cosCred, config);
  }

  /**
   * Initialize the CosNativeFileSystemStore object, including
   * its COS client and default COS bucket.
   *
   * @param uri  The URI of the COS bucket accessed by default.
   * @param conf Hadoop configuration with COS configuration options.
   * @throws IOException Initialize the COS client failed.
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    try {
      initCOSClient(uri, conf);
      this.bucketName = uri.getHost();
    } catch (Exception e) {
      handleException(e, "");
    }
  }

  /**
   * Store a file into COS from the specified input stream, which would be
   * retried until the success or maximum number.
   *
   * @param key         COS object key.
   * @param inputStream Input stream to be uploaded into COS.
   * @param md5Hash     MD5 value of the content to be uploaded.
   * @param length      Length of uploaded content.
   * @throws IOException Upload the file failed.
   */
  private void storeFileWithRetry(String key, InputStream inputStream,
      byte[] md5Hash, long length) throws IOException {
    try {
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentMD5(Base64.encodeAsString(md5Hash));
      objectMetadata.setContentLength(length);
      PutObjectRequest putObjectRequest =
          new PutObjectRequest(bucketName, key, inputStream, objectMetadata);

      PutObjectResult putObjectResult =
          (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
      LOG.debug("Store file successfully. COS key: [{}], ETag: [{}].",
          key, putObjectResult.getETag());
    } catch (Exception e) {
      String errMsg = String.format("Store file failed. COS key: [%s], "
          + "exception: [%s]", key, e.toString());
      LOG.error(errMsg);
      handleException(new Exception(errMsg), key);
    }
  }

  /**
   * Store a local file into COS.
   *
   * @param key     COS object key.
   * @param file    The local file to be uploaded.
   * @param md5Hash The MD5 value of the file to be uploaded.
   * @throws IOException Upload the file failed.
   */
  @Override
  public void storeFile(String key, File file, byte[] md5Hash)
      throws IOException {
    LOG.info("Store file from local path: [{}]. file length: [{}] COS key: " +
        "[{}]", file.getCanonicalPath(), file.length(), key);
    storeFileWithRetry(key, new BufferedInputStream(new FileInputStream(file)),
        md5Hash, file.length());
  }

  /**
   * Store a file into COS from the specified input stream.
   *
   * @param key           COS object key.
   * @param inputStream   The Input stream to be uploaded.
   * @param md5Hash       The MD5 value of the content to be uploaded.
   * @param contentLength Length of uploaded content.
   * @throws IOException Upload the file failed.
   */
  @Override
  public void storeFile(
      String key,
      InputStream inputStream,
      byte[] md5Hash,
      long contentLength) throws IOException {
    LOG.info("Store file from input stream. COS key: [{}], "
        + "length: [{}].", key, contentLength);
    storeFileWithRetry(key, inputStream, md5Hash, contentLength);
  }

  // For cos, storeEmptyFile means creating a directory
  @Override
  public void storeEmptyFile(String key) throws IOException {
    if (!key.endsWith(CosNFileSystem.PATH_DELIMITER)) {
      key = key + CosNFileSystem.PATH_DELIMITER;
    }

    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(0);
    InputStream input = new ByteArrayInputStream(new byte[0]);
    PutObjectRequest putObjectRequest =
        new PutObjectRequest(bucketName, key, input, objectMetadata);
    try {
      PutObjectResult putObjectResult =
          (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
      LOG.debug("Store empty file successfully. COS key: [{}], ETag: [{}].",
          key, putObjectResult.getETag());
    } catch (Exception e) {
      String errMsg = String.format("Store empty file failed. "
          + "COS key: [%s], exception: [%s]", key, e.toString());
      LOG.error(errMsg);
      handleException(new Exception(errMsg), key);
    }
  }

  public PartETag uploadPart(File file, String key, String uploadId,
      int partNum) throws IOException {
    InputStream inputStream = new FileInputStream(file);
    try {
      return uploadPart(inputStream, key, uploadId, partNum, file.length());
    } finally {
      inputStream.close();
    }
  }

  @Override
  public PartETag uploadPart(InputStream inputStream, String key,
      String uploadId, int partNum, long partSize) throws IOException {
    UploadPartRequest uploadPartRequest = new UploadPartRequest();
    uploadPartRequest.setBucketName(this.bucketName);
    uploadPartRequest.setUploadId(uploadId);
    uploadPartRequest.setInputStream(inputStream);
    uploadPartRequest.setPartNumber(partNum);
    uploadPartRequest.setPartSize(partSize);
    uploadPartRequest.setKey(key);

    try {
      UploadPartResult uploadPartResult =
          (UploadPartResult) callCOSClientWithRetry(uploadPartRequest);
      return uploadPartResult.getPartETag();
    } catch (Exception e) {
      String errMsg = String.format("Current thread: [%d], COS key: [%s], "
              + "upload id: [%s], part num: [%d], exception: [%s]",
          Thread.currentThread().getId(), key, uploadId, partNum, e.toString());
      handleException(new Exception(errMsg), key);
    }

    return null;
  }

  public void abortMultipartUpload(String key, String uploadId) {
    LOG.info("Abort the multipart upload. COS key: [{}], upload id: [{}].",
        key, uploadId);
    AbortMultipartUploadRequest abortMultipartUploadRequest =
        new AbortMultipartUploadRequest(bucketName, key, uploadId);
    cosClient.abortMultipartUpload(abortMultipartUploadRequest);
  }

  /**
   * Initialize a multipart upload and return the upload id.
   *
   * @param key The COS object key initialized to multipart upload.
   * @return The multipart upload id.
   */
  public String getUploadId(String key) {
    if (null == key || key.length() == 0) {
      return "";
    }

    LOG.info("Initiate a multipart upload. bucket: [{}], COS key: [{}].",
        bucketName, key);
    InitiateMultipartUploadRequest initiateMultipartUploadRequest =
        new InitiateMultipartUploadRequest(bucketName, key);
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        cosClient.initiateMultipartUpload(initiateMultipartUploadRequest);
    return initiateMultipartUploadResult.getUploadId();
  }

  /**
   * Finish a multipart upload process, which will merge all parts uploaded.
   *
   * @param key          The COS object key to be finished.
   * @param uploadId     The upload id of the multipart upload to be finished.
   * @param partETagList The etag list of the part that has been uploaded.
   * @return The result object of completing the multipart upload process.
   */
  public CompleteMultipartUploadResult completeMultipartUpload(
      String key, String uploadId, List<PartETag> partETagList) {
    Collections.sort(partETagList, new Comparator<PartETag>() {
      @Override
      public int compare(PartETag o1, PartETag o2) {
        return o1.getPartNumber() - o2.getPartNumber();
      }
    });
    LOG.info("Complete the multipart upload. bucket: [{}], COS key: [{}], "
        + "upload id: [{}].", bucketName, key, uploadId);
    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        new CompleteMultipartUploadRequest(
            bucketName, key, uploadId, partETagList);
    return cosClient.completeMultipartUpload(completeMultipartUploadRequest);
  }

  private FileMetadata queryObjectMetadata(String key) throws IOException {
    GetObjectMetadataRequest getObjectMetadataRequest =
        new GetObjectMetadataRequest(bucketName, key);
    try {
      ObjectMetadata objectMetadata =
          (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);
      long mtime = 0;
      if (objectMetadata.getLastModified() != null) {
        mtime = objectMetadata.getLastModified().getTime();
      }
      long fileSize = objectMetadata.getContentLength();
      FileMetadata fileMetadata = new FileMetadata(key, fileSize, mtime,
          !key.endsWith(CosNFileSystem.PATH_DELIMITER));
      LOG.debug("Retrieve file metadata. COS key: [{}], ETag: [{}], "
              + "length: [{}].", key, objectMetadata.getETag(),
          objectMetadata.getContentLength());
      return fileMetadata;
    } catch (CosServiceException e) {
      if (e.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
        String errorMsg = String.format("Retrieve file metadata file failed. "
            + "COS key: [%s], CosServiceException: [%s].", key, e.toString());
        LOG.error(errorMsg);
        handleException(new Exception(errorMsg), key);
      }
    }
    return null;
  }

  @Override
  public FileMetadata retrieveMetadata(String key) throws IOException {
    if (key.endsWith(CosNFileSystem.PATH_DELIMITER)) {
      key = key.substring(0, key.length() - 1);
    }

    if (!key.isEmpty()) {
      FileMetadata fileMetadata = queryObjectMetadata(key);
      if (fileMetadata != null) {
        return fileMetadata;
      }
    }

    // If the key is a directory.
    key = key + CosNFileSystem.PATH_DELIMITER;
    return queryObjectMetadata(key);
  }

  /**
   * Download a COS object and return the input stream associated with it.
   *
   * @param key The object key that is being retrieved from the COS bucket
   * @return This method returns null if the key is not found
   * @throws IOException if failed to download.
   */
  @Override
  public InputStream retrieve(String key) throws IOException {
    LOG.debug("Retrieve object key: [{}].", key);
    GetObjectRequest getObjectRequest =
        new GetObjectRequest(this.bucketName, key);
    try {
      COSObject cosObject =
          (COSObject) callCOSClientWithRetry(getObjectRequest);
      return cosObject.getObjectContent();
    } catch (Exception e) {
      String errMsg = String.format("Retrieving key: [%s] occurs "
          + "an exception: [%s].", key, e.toString());
      LOG.error("Retrieving COS key: [{}] occurs an exception: [{}].", key, e);
      handleException(new Exception(errMsg), key);
    }
    // never will get here
    return null;
  }

  /**
   * Retrieved a part of a COS object, which is specified the start position.
   *
   * @param key            The object key that is being retrieved from
   *                       the COS bucket.
   * @param byteRangeStart The start position of the part to be retrieved in
   *                       the object.
   * @return The input stream associated with the retrieved object.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public InputStream retrieve(String key, long byteRangeStart)
      throws IOException {
    try {
      LOG.debug("Retrieve COS key:[{}]. range start:[{}].",
          key, byteRangeStart);
      long fileSize = getFileLength(key);
      long byteRangeEnd = fileSize - 1;
      GetObjectRequest getObjectRequest =
          new GetObjectRequest(this.bucketName, key);
      if (byteRangeEnd >= byteRangeStart) {
        getObjectRequest.setRange(byteRangeStart, fileSize - 1);
      }
      COSObject cosObject =
          (COSObject) callCOSClientWithRetry(getObjectRequest);
      return cosObject.getObjectContent();
    } catch (Exception e) {
      String errMsg =
          String.format("Retrieving COS key: [%s] occurs an exception. " +
                  "byte range start: [%s], exception: [%s].",
              key, byteRangeStart, e.toString());
      LOG.error(errMsg);
      handleException(new Exception(errMsg), key);
    }

    // never will get here
    return null;
  }

  /**
   * Download a part of a COS object, which is specified the start and
   * end position.
   *
   * @param key            The object key that is being downloaded
   * @param byteRangeStart The start position of the part to be retrieved in
   *                       the object.
   * @param byteRangeEnd   The end position of the part to be retrieved in
   *                       the object.
   * @return The input stream associated with the retrieved objects.
   * @throws IOException If failed to retrieve.
   */
  @Override
  public InputStream retrieveBlock(String key, long byteRangeStart,
      long byteRangeEnd) throws IOException {
    try {
      GetObjectRequest request = new GetObjectRequest(this.bucketName, key);
      request.setRange(byteRangeStart, byteRangeEnd);
      COSObject cosObject = (COSObject) this.callCOSClientWithRetry(request);
      return cosObject.getObjectContent();
    } catch (CosServiceException e) {
      String errMsg =
          String.format("Retrieving key [%s] with byteRangeStart [%d] occurs " +
                  "an CosServiceException: [%s].",
              key, byteRangeStart, e.toString());
      LOG.error(errMsg);
      handleException(new Exception(errMsg), key);
      return null;
    } catch (CosClientException e) {
      String errMsg =
          String.format("Retrieving key [%s] with byteRangeStart [%d] "
                  + "occurs an exception: [%s].",
              key, byteRangeStart, e.toString());
      LOG.error("Retrieving COS key: [{}] with byteRangeStart: [{}] " +
          "occurs an exception: [{}].", key, byteRangeStart, e);
      handleException(new Exception(errMsg), key);
    }

    return null;
  }

  @Override
  public PartialListing list(String prefix, int maxListingLength)
      throws IOException {
    return list(prefix, maxListingLength, null, false);
  }

  @Override
  public PartialListing list(String prefix, int maxListingLength,
      String priorLastKey, boolean recurse) throws IOException {
    return list(prefix, recurse ? null : CosNFileSystem.PATH_DELIMITER,
        maxListingLength, priorLastKey);
  }

  /**
   * List the metadata for all objects that
   * the object key has the specified prefix.
   *
   * @param prefix           The prefix to be listed.
   * @param delimiter        The delimiter is a sign, the same paths between
   *                         are listed.
   * @param maxListingLength The maximum number of listed entries.
   * @param priorLastKey     The last key in any previous search.
   * @return A metadata list on the match.
   * @throws IOException If list objects failed.
   */
  private PartialListing list(String prefix, String delimiter,
      int maxListingLength, String priorLastKey) throws IOException {
    LOG.debug("List objects. prefix: [{}], delimiter: [{}], " +
            "maxListLength: [{}], priorLastKey: [{}].",
        prefix, delimiter, maxListingLength, priorLastKey);

    if (!prefix.startsWith(CosNFileSystem.PATH_DELIMITER)) {
      prefix += CosNFileSystem.PATH_DELIMITER;
    }
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
    listObjectsRequest.setBucketName(bucketName);
    listObjectsRequest.setPrefix(prefix);
    listObjectsRequest.setDelimiter(delimiter);
    listObjectsRequest.setMarker(priorLastKey);
    listObjectsRequest.setMaxKeys(maxListingLength);
    ObjectListing objectListing = null;
    try {
      objectListing =
          (ObjectListing) callCOSClientWithRetry(listObjectsRequest);
    } catch (Exception e) {
      String errMsg = String.format("prefix: [%s], delimiter: [%s], "
              + "maxListingLength: [%d], priorLastKey: [%s]. "
              + "List objects occur an exception: [%s].", prefix,
          (delimiter == null) ? "" : delimiter, maxListingLength, priorLastKey,
          e.toString());
      LOG.error(errMsg);
      handleException(new Exception(errMsg), prefix);
    }
    ArrayList<FileMetadata> fileMetadataArray = new ArrayList<>();
    ArrayList<FileMetadata> commonPrefixArray = new ArrayList<>();

    if (null == objectListing) {
      String errMsg = String.format("List the prefix: [%s] failed. " +
              "delimiter: [%s], max listing length:" +
              " [%s], prior last key: [%s]",
          prefix, delimiter, maxListingLength, priorLastKey);
      handleException(new Exception(errMsg), prefix);
    }

    List<COSObjectSummary> summaries = objectListing.getObjectSummaries();
    for (COSObjectSummary cosObjectSummary : summaries) {
      String filePath = cosObjectSummary.getKey();
      if (!filePath.startsWith(CosNFileSystem.PATH_DELIMITER)) {
        filePath = CosNFileSystem.PATH_DELIMITER + filePath;
      }
      if (filePath.equals(prefix)) {
        continue;
      }
      long mtime = 0;
      if (cosObjectSummary.getLastModified() != null) {
        mtime = cosObjectSummary.getLastModified().getTime();
      }
      long fileLen = cosObjectSummary.getSize();
      fileMetadataArray.add(
          new FileMetadata(filePath, fileLen, mtime, true));
    }
    List<String> commonPrefixes = objectListing.getCommonPrefixes();
    for (String commonPrefix : commonPrefixes) {
      if (!commonPrefix.startsWith(CosNFileSystem.PATH_DELIMITER)) {
        commonPrefix = CosNFileSystem.PATH_DELIMITER + commonPrefix;
      }
      commonPrefixArray.add(
          new FileMetadata(commonPrefix, 0, 0, false));
    }

    FileMetadata[] fileMetadata = new FileMetadata[fileMetadataArray.size()];
    for (int i = 0; i < fileMetadataArray.size(); ++i) {
      fileMetadata[i] = fileMetadataArray.get(i);
    }
    FileMetadata[] commonPrefixMetaData =
        new FileMetadata[commonPrefixArray.size()];
    for (int i = 0; i < commonPrefixArray.size(); ++i) {
      commonPrefixMetaData[i] = commonPrefixArray.get(i);
    }
    // when truncated is false, it means that listing is finished.
    if (!objectListing.isTruncated()) {
      return new PartialListing(
          null, fileMetadata, commonPrefixMetaData);
    } else {
      return new PartialListing(
          objectListing.getNextMarker(), fileMetadata, commonPrefixMetaData);
    }
  }

  @Override
  public void delete(String key) throws IOException {
    LOG.debug("Delete object key: [{}] from bucket: {}.", key, this.bucketName);
    try {
      DeleteObjectRequest deleteObjectRequest =
          new DeleteObjectRequest(bucketName, key);
      callCOSClientWithRetry(deleteObjectRequest);
    } catch (Exception e) {
      String errMsg =
          String.format("Delete key: [%s] occurs an exception: [%s].",
              key, e.toString());
      LOG.error(errMsg);
      handleException(new Exception(errMsg), key);
    }
  }

  public void rename(String srcKey, String dstKey) throws IOException {
    LOG.debug("Rename source key: [{}] to dest key: [{}].", srcKey, dstKey);
    try {
      CopyObjectRequest copyObjectRequest =
          new CopyObjectRequest(bucketName, srcKey, bucketName, dstKey);
      callCOSClientWithRetry(copyObjectRequest);
      DeleteObjectRequest deleteObjectRequest =
          new DeleteObjectRequest(bucketName, srcKey);
      callCOSClientWithRetry(deleteObjectRequest);
    } catch (Exception e) {
      String errMsg = String.format("Rename object unsuccessfully. "
              + "source cos key: [%s], dest COS " +
              "key: [%s], exception: [%s]",
          srcKey,
          dstKey, e.toString());
      LOG.error(errMsg);
      handleException(new Exception(errMsg), srcKey);
    }
  }

  @Override
  public void copy(String srcKey, String dstKey) throws IOException {
    LOG.debug("Copy source key: [{}] to dest key: [{}].", srcKey, dstKey);
    try {
      CopyObjectRequest copyObjectRequest =
          new CopyObjectRequest(bucketName, srcKey, bucketName, dstKey);
      callCOSClientWithRetry(copyObjectRequest);
    } catch (Exception e) {
      String errMsg = String.format("Copy object unsuccessfully. "
              + "source COS key: %s, dest COS key: " +
              "%s, exception: %s",
          srcKey,
          dstKey, e.toString());
      LOG.error(errMsg);
      handleException(new Exception(errMsg), srcKey);
    }
  }

  @Override
  public void purge(String prefix) throws IOException {
    throw new IOException("purge not supported");
  }

  @Override
  public void dump() throws IOException {
    throw new IOException("dump not supported");
  }

  // process Exception and print detail
  private void handleException(Exception e, String key) throws IOException {
    String cosPath = CosNFileSystem.SCHEME + "://" + bucketName + key;
    String exceptInfo = String.format("%s : %s", cosPath, e.toString());
    throw new IOException(exceptInfo);
  }

  @Override
  public long getFileLength(String key) throws IOException {
    LOG.debug("Get file length. COS key: {}", key);
    GetObjectMetadataRequest getObjectMetadataRequest =
        new GetObjectMetadataRequest(bucketName, key);
    try {
      ObjectMetadata objectMetadata =
          (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);
      return objectMetadata.getContentLength();
    } catch (Exception e) {
      String errMsg = String.format("Getting file length occurs an exception." +
              "COS key: %s, exception: %s", key,
          e.toString());
      LOG.error(errMsg);
      handleException(new Exception(errMsg), key);
      return 0; // never will get here
    }
  }

  private <X> Object callCOSClientWithRetry(X request)
      throws CosServiceException, IOException {
    String sdkMethod = "";
    int retryIndex = 1;
    while (true) {
      try {
        if (request instanceof PutObjectRequest) {
          sdkMethod = "putObject";
          return this.cosClient.putObject((PutObjectRequest) request);
        } else if (request instanceof UploadPartRequest) {
          sdkMethod = "uploadPart";
          if (((UploadPartRequest) request).getInputStream()
              instanceof ByteBufferInputStream) {
            ((UploadPartRequest) request).getInputStream()
                .mark((int) ((UploadPartRequest) request).getPartSize());
          }
          return this.cosClient.uploadPart((UploadPartRequest) request);
        } else if (request instanceof GetObjectMetadataRequest) {
          sdkMethod = "queryObjectMeta";
          return this.cosClient.getObjectMetadata(
              (GetObjectMetadataRequest) request);
        } else if (request instanceof DeleteObjectRequest) {
          sdkMethod = "deleteObject";
          this.cosClient.deleteObject((DeleteObjectRequest) request);
          return new Object();
        } else if (request instanceof CopyObjectRequest) {
          sdkMethod = "copyFile";
          return this.cosClient.copyObject((CopyObjectRequest) request);
        } else if (request instanceof GetObjectRequest) {
          sdkMethod = "getObject";
          return this.cosClient.getObject((GetObjectRequest) request);
        } else if (request instanceof ListObjectsRequest) {
          sdkMethod = "listObjects";
          return this.cosClient.listObjects((ListObjectsRequest) request);
        } else {
          throw new IOException("no such method");
        }
      } catch (CosServiceException cse) {
        String errMsg = String.format("Call cos sdk failed, "
                + "retryIndex: [%d / %d], "
                + "call method: %s, exception: %s",
            retryIndex, this.maxRetryTimes, sdkMethod, cse.toString());
        int statusCode = cse.getStatusCode();
        // Retry all server errors
        if (statusCode / 100 == 5) {
          if (retryIndex <= this.maxRetryTimes) {
            LOG.info(errMsg);
            long sleepLeast = retryIndex * 300L;
            long sleepBound = retryIndex * 500L;
            try {
              if (request instanceof UploadPartRequest) {
                if (((UploadPartRequest) request).getInputStream()
                    instanceof ByteBufferInputStream) {
                  ((UploadPartRequest) request).getInputStream().reset();
                }
              }
              Thread.sleep(
                  ThreadLocalRandom.current().nextLong(sleepLeast, sleepBound));
              ++retryIndex;
            } catch (InterruptedException e) {
              throw new IOException(e.toString());
            }
          } else {
            LOG.error(errMsg);
            throw new IOException(errMsg);
          }
        } else {
          throw cse;
        }
      } catch (Exception e) {
        String errMsg = String.format("Call cos sdk failed, "
            + "call method: %s, exception: %s", sdkMethod, e.toString());
        LOG.error(errMsg);
        throw new IOException(errMsg);
      }
    }
  }

  @Override
  public void close() {
    if (null != this.cosClient) {
      this.cosClient.shutdown();
    }
  }
}
