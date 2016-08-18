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

package org.apache.hadoop.fs.aliyun.oss;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.UserInfo;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.util.Progressable;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CannedAccessControlList;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;

/**
 * Implementation of {@link FileSystem} for <a href="https://oss.aliyun.com">
 * Aliyun OSS</a>, used to access OSS blob system in a filesystem style.
 */
public class AliyunOSSFileSystem extends FileSystem {

  private URI uri;
  private Path workingDir;
  private OSSClient ossClient;
  private String bucketName;
  private long uploadPartSize;
  private long multipartThreshold;
  private int maxKeys;
  private String serverSideEncryptionAlgorithm;

  @Override
  public FSDataOutputStream append(Path path, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Append is not supported!");
  }

  @Override
  public void close() throws IOException {
    try {
      if (ossClient != null) {
        ossClient.shutdown();
      }
    } finally {
      super.close();
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    String key = pathToKey(path);

    if (!overwrite && exists(path)) {
      throw new FileAlreadyExistsException(path + " already exists");
    }

    return new FSDataOutputStream(new AliyunOSSOutputStream(getConf(),
        ossClient, bucketName, key, progress, statistics,
        serverSideEncryptionAlgorithm), (Statistics)(null));
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    FileStatus status;
    try {
      status = getFileStatus(path);
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't delete " + path + ": Does not exist!");
      }
      return false;
    }

    String key = pathToKey(status.getPath());
    if (status.isDirectory()) {
      if (!key.endsWith("/")) {
        key += "/";
      }
      if (!recursive) {
        FileStatus[] statuses = listStatus(status.getPath());
        // Check whether it is an empty directory or not
        if (statuses.length > 0) {
          throw new IOException("Cannot remove directory" + path +
              ": It is not empty!");
        } else {
          // Delete empty directory without '-r'
          ossClient.deleteObject(bucketName, key);
          statistics.incrementWriteOps(1);
        }
      } else {
        ListObjectsRequest listRequest = new ListObjectsRequest(bucketName);
        listRequest.setPrefix(key);
        listRequest.setMaxKeys(maxKeys);

        while (true) {
          ObjectListing objects = ossClient.listObjects(listRequest);
          statistics.incrementReadOps(1);
          List<String> keysToDelete = new ArrayList<String>();
          for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
            keysToDelete.add(objectSummary.getKey());
          }
          DeleteObjectsRequest deleteRequest =
              new DeleteObjectsRequest(bucketName);
          deleteRequest.setKeys(keysToDelete);
          ossClient.deleteObjects(deleteRequest);
          statistics.incrementWriteOps(1);
          if (objects.isTruncated()) {
            listRequest.setMarker(objects.getNextMarker());
          } else {
            break;
          }
        }
      }
    } else {
      ossClient.deleteObject(bucketName, key);
      statistics.incrementWriteOps(1);
    }
    //TODO: optimize logic here
    try {
      Path pPath = status.getPath().getParent();
      FileStatus pStatus = getFileStatus(pPath);
      if (pStatus.isDirectory()) {
        return true;
      } else {
        throw new IOException("Path " + pPath +
            " is assumed to be a directory!");
      }
    } catch (FileNotFoundException fnfe) {
      // Make sure the parent directory exists
      return mkdir(bucketName, pathToKey(status.getPath().getParent()));
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    Path qualifiedPath = path.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);

    // Root always exists
    if (key.length() == 0) {
      return new FileStatus(0, true, 1, 0, 0, qualifiedPath);
    }

    ObjectMetadata meta = getObjectMetadata(key);
    // If key not found and key does not end with "/"
    if (meta == null && !key.endsWith("/")) {
      // Case: dir + "/"
      key += "/";
      meta = getObjectMetadata(key);
    }
    if (meta == null) {
      // Case: dir + "/" + file
      ListObjectsRequest listRequest = new ListObjectsRequest(bucketName);
      listRequest.setPrefix(key);
      listRequest.setDelimiter("/");
      listRequest.setMaxKeys(1);

      ObjectListing listing = ossClient.listObjects(listRequest);
      statistics.incrementReadOps(1);
      if (!listing.getObjectSummaries().isEmpty() ||
          !listing.getCommonPrefixes().isEmpty()) {
        return new FileStatus(0, true, 1, 0, 0, qualifiedPath);
      } else {
        throw new FileNotFoundException(path + ": No such file or directory!");
      }
    } else if (objectRepresentsDirectory(key, meta.getContentLength())) {
      return new FileStatus(0, true, 1, 0, 0, qualifiedPath);
    } else {
      return new FileStatus(meta.getContentLength(), false, 1,
          getDefaultBlockSize(path), meta.getLastModified().getTime(),
          qualifiedPath);
    }
  }

  /**
   * Return object metadata given object key.
   *
   * @param key object key
   * @return return null if key does not exist
   */
  private ObjectMetadata getObjectMetadata(String key) {
    try {
      return ossClient.getObjectMetadata(bucketName, key);
    } catch (OSSException osse) {
      return null;
    } finally {
      statistics.incrementReadOps(1);
    }
  }

  @Override
  public String getScheme() {
    return "oss";
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Deprecated
  public long getDefaultBlockSize() {
    return getConf().getLong(FS_OSS_BLOCK_SIZE_KEY, FS_OSS_BLOCK_SIZE_DEFAULT);
  }

  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }

  /**
   * Initialize new FileSystem.
   *
   * @param name the uri of the file system, including host, port, etc.
   *
   * @param conf configuration of the file system
   * @throws IOException IO problems
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);

    uri = java.net.URI.create(name.getScheme() + "://" + name.getAuthority());
    workingDir =
        new Path("/user",
            System.getProperty("user.name")).makeQualified(uri, null);

    bucketName = name.getHost();

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
    if (!proxyHost.isEmpty()) {
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
    ossClient =
        new OSSClient(endPoint, getCredentialsProvider(name, conf), clientConf);

    maxKeys = conf.getInt(MAX_PAGING_KEYS_KEY, MAX_PAGING_KEYS_DEFAULT);
    uploadPartSize = conf.getLong(MULTIPART_UPLOAD_SIZE_KEY,
        MULTIPART_UPLOAD_SIZE_DEFAULT);
    multipartThreshold = conf.getLong(MIN_MULTIPART_UPLOAD_THRESHOLD_KEY,
        MIN_MULTIPART_UPLOAD_THRESHOLD_DEFAULT);

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
    if (!cannedACLName.isEmpty()) {
      CannedAccessControlList cannedACL =
          CannedAccessControlList.valueOf(cannedACLName);
      ossClient.setBucketAcl(bucketName, cannedACL);
    }

    serverSideEncryptionAlgorithm =
        conf.get(SERVER_SIDE_ENCRYPTION_ALGORITHM_KEY, "");

    setConf(conf);
  }

  /**
   * Create the default credential provider, or load in one explicitly
   * identified in the configuration.
   * @param name the uri of the file system
   * @param conf configuration
   * @return a credential provider
   * @throws IOException on any problem. Class construction issues may be
   * nested inside the IOE.
   */
  private CredentialsProvider getCredentialsProvider(URI name,
      Configuration conf) throws IOException {
    CredentialsProvider credentials;

    String className = conf.getTrimmed(ALIYUN_OSS_CREDENTIALS_PROVIDER_KEY);
    if (StringUtils.isEmpty(className)) {
      Configuration newConf =
          ProviderUtils.excludeIncompatibleCredentialProviders(conf,
              AliyunOSSFileSystem.class);
      String accessKey =
          AliyunOSSUtils.getPassword(newConf, ACCESS_KEY,
              UserInfo.EMPTY.getUser());
      String secretKey =
          AliyunOSSUtils.getPassword(newConf, SECRET_KEY,
              UserInfo.EMPTY.getPassword());
      credentials =
          new DefaultCredentialProvider(
              new DefaultCredentials(accessKey, secretKey));

    } else {
      try {
        LOG.debug("Credential provider class is:" + className);
        Class<?> credClass = Class.forName(className);
        try {
          credentials =
              (CredentialsProvider)credClass.getDeclaredConstructor(
                  URI.class, Configuration.class).newInstance(this.uri, conf);
        } catch (NoSuchMethodException | SecurityException e) {
          credentials =
              (CredentialsProvider)credClass.getDeclaredConstructor()
              .newInstance();
        }
      } catch (ClassNotFoundException e) {
        throw new IOException(className + " not found.", e);
      } catch (NoSuchMethodException | SecurityException e) {
        throw new IOException(String.format("%s constructor exception.  A " +
            "class specified in %s must provide an accessible constructor " +
            "accepting URI and Configuration, or an accessible default " +
            "constructor.", className, ALIYUN_OSS_CREDENTIALS_PROVIDER_KEY), e);
      } catch (ReflectiveOperationException | IllegalArgumentException e) {
        throw new IOException(className + " instantiation exception.", e);
      }
    }

    return credentials;
  }

  /**
   * Check if OSS object represents a directory.
   *
   * @param name object key
   * @param size object content length
   * @return true if object represents a directory
   */
  private boolean objectRepresentsDirectory(final String name,
      final long size) {
    return !name.isEmpty() && name.endsWith("/") && size == 0L;
  }

  /**
   * Turns a path (relative or otherwise) into an OSS key.
   *
   * @param path the path of the file
   * @return the key of the object that represent the file
   */
  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  private Path keyToPath(String key) {
    return new Path("/" + key);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    String key = pathToKey(path);
    if (LOG.isDebugEnabled()) {
      LOG.debug("List status for path: " + path);
    }

    final List<FileStatus> result = new ArrayList<FileStatus>();
    final FileStatus fileStatus = getFileStatus(path);

    if (fileStatus.isDirectory()) {
      if (!key.endsWith("/")) {
        key = key + "/";
      }

      ListObjectsRequest listObjectsRequest =
          new ListObjectsRequest(bucketName);
      listObjectsRequest.setPrefix(key);
      listObjectsRequest.setDelimiter("/");
      listObjectsRequest.setMaxKeys(maxKeys);

      if (LOG.isDebugEnabled()) {
        LOG.debug("listStatus: doing listObjects for directory " + key);
      }

      while (true) {
        ObjectListing objects = ossClient.listObjects(listObjectsRequest);
        statistics.incrementReadOps(1);
        for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
          Path keyPath = keyToPath(objectSummary.getKey())
              .makeQualified(uri, workingDir);
          if (keyPath.equals(path)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Ignoring: " + keyPath);
            }
            continue;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding: fi: " + keyPath);
            }
            result.add(new FileStatus(objectSummary.getSize(), false, 1,
                getDefaultBlockSize(keyPath),
                objectSummary.getLastModified().getTime(), keyPath));
          }
        }

        for (String prefix : objects.getCommonPrefixes()) {
          Path keyPath = keyToPath(prefix).makeQualified(uri, workingDir);
          if (keyPath.equals(path)) {
            continue;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding: rd: " + keyPath);
            }
            result.add(new FileStatus(0, true, 1, 0, 0, keyPath));
          }
        }

        if (objects.isTruncated()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("listStatus: list truncated - getting next batch");
          }
          listObjectsRequest.setMarker(objects.getNextMarker());
          statistics.incrementReadOps(1);
        } else {
          break;
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding: rd (not a dir): " + path);
      }
      result.add(fileStatus);
    }

    return result.toArray(new FileStatus[result.size()]);
  }

  /**
   * Used to create an empty file that represents an empty directory.
   *
   * @param bucketName the bucket this directory belongs to
   * @param objectName directory path
   * @return true if directory successfully created
   * @throws IOException
   */
  private boolean mkdir(final String bucket, final String objectName)
      throws IOException {
    String dirName = objectName;
    ObjectMetadata dirMeta = new ObjectMetadata();
    byte[] buffer = new byte[0];
    ByteArrayInputStream in = new ByteArrayInputStream(buffer);
    dirMeta.setContentLength(0);
    if (!objectName.endsWith("/")) {
      dirName += "/";
    }
    try {
      ossClient.putObject(bucket, dirName, in, dirMeta);
      return true;
    } finally {
      in.close();
    }
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission)
      throws IOException {
    try {
      FileStatus fileStatus = getFileStatus(path);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + path);
      }
    } catch (FileNotFoundException e) {
      validatePath(path);
      String key = pathToKey(path);
      return mkdir(bucketName, key);
    }
  }

  /**
   * Check whether the path is a valid path.
   *
   * @param path the path to be checked
   * @throws IOException
   */
  private void validatePath(Path path) throws IOException {
    Path fPart = path.getParent();
    do {
      try {
        FileStatus fileStatus = getFileStatus(fPart);
        if (fileStatus.isDirectory()) {
          // If path exists and a directory, exit
          break;
        } else {
          throw new FileAlreadyExistsException(String.format(
              "Can't make directory for path '%s', it is a file.", fPart));
        }
      } catch (FileNotFoundException fnfe) {
      }
      fPart = fPart.getParent();
    } while (fPart != null);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    final FileStatus fileStatus = getFileStatus(path);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open " + path +
          " because it is a directory");
    }

    return new FSDataInputStream(new AliyunOSSInputStream(getConf(), ossClient,
        bucketName, pathToKey(path), fileStatus.getLen(), statistics));
  }

  @Override
  public boolean rename(Path srcPath, Path dstPath) throws IOException {
    if (srcPath.isRoot()) {
      // Cannot rename root of file system
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot rename the root of a filesystem");
      }
      return false;
    }
    Path parent = dstPath.getParent();
    while (parent != null && !srcPath.equals(parent)) {
      parent = parent.getParent();
    }
    if (parent != null) {
      return false;
    }
    FileStatus srcStatus = getFileStatus(srcPath);
    FileStatus dstStatus;
    try {
      dstStatus = getFileStatus(dstPath);
    } catch (FileNotFoundException fnde) {
      dstStatus = null;
    }
    if (dstStatus == null) {
      // If dst doesn't exist, check whether dst dir exists or not
      dstStatus = getFileStatus(dstPath.getParent());
      if (!dstStatus.isDirectory()) {
        throw new IOException(String.format(
            "Failed to rename %s to %s, %s is a file", srcPath, dstPath,
            dstPath.getParent()));
      }
    } else {
      if (srcStatus.getPath().equals(dstStatus.getPath())) {
        return !srcStatus.isDirectory();
      } else if (dstStatus.isDirectory()) {
        // If dst is a directory
        dstPath = new Path(dstPath, srcPath.getName());
        FileStatus[] statuses;
        try {
          statuses = listStatus(dstPath);
        } catch (FileNotFoundException fnde) {
          statuses = null;
        }
        if (statuses != null && statuses.length > 0) {
          // If dst exists and not a directory / not empty
          throw new FileAlreadyExistsException(String.format(
              "Failed to rename %s to %s, file already exists or not empty!",
              srcPath, dstPath));
        }
      } else {
        // If dst is not a directory
        throw new FileAlreadyExistsException(String.format(
            "Failed to rename %s to %s, file already exists!", srcPath,
            dstPath));
      }
    }
    if (srcStatus.isDirectory()) {
      copyDirectory(srcPath, dstPath);
    } else {
      copyFile(srcPath, dstPath);
    }
    if (srcPath.equals(dstPath)) {
      return true;
    } else {
      return delete(srcPath, true);
    }
  }

  /**
   * Copy file from source path to destination path.
   * (the caller should make sure srcPath is a file and dstPath is valid.)
   *
   * @param srcPath source path
   * @param dstPath destination path
   * @return true if successfully copied
   */
  private boolean copyFile(Path srcPath, Path dstPath) {
    String srcKey = pathToKey(srcPath);
    String dstKey = pathToKey(dstPath);
    return copyFile(srcKey, dstKey);
  }

  /**
   * Copy an object from source key to destination key.
   *
   * @param srcKey source key
   * @param dstKey destination key
   * @return true if successfully copied
   */
  private boolean copyFile(String srcKey, String dstKey) {
    ObjectMetadata objectMeta =
        ossClient.getObjectMetadata(bucketName, srcKey);
    long dataLen = objectMeta.getContentLength();
    if (dataLen <= multipartThreshold) {
      return singleCopy(srcKey, dstKey);
    } else {
      return multipartCopy(srcKey, dataLen, dstKey);
    }
  }

  /**
   * Use single copy to copy an oss object.
   *
   * @param srcKey source key
   * @param dstKey destination key
   * @return true if successfully copied
   * (the caller should make sure srcPath is a file and dstPath is valid)
   */
  private boolean singleCopy(String srcKey, String dstKey) {
    CopyObjectResult copyResult =
        ossClient.copyObject(bucketName, srcKey, bucketName, dstKey);
    LOG.debug(copyResult.getETag());
    return true;
  }

  /**
   * Use multipart copy to copy an oss object.
   * (the caller should make sure srcPath is a file and dstPath is valid)
   *
   * @param srcKey source key
   * @param dataLen data size of the object to copy
   * @param dstKey destination key
   * @return true if successfully copied, or false if upload is aborted
   */
  private boolean multipartCopy(String srcKey, long dataLen, String dstKey) {
    int partNum = (int)(dataLen / uploadPartSize);
    if (dataLen % uploadPartSize != 0) {
      partNum++;
    }
    InitiateMultipartUploadRequest initiateMultipartUploadRequest =
        new InitiateMultipartUploadRequest(bucketName, dstKey);
    ObjectMetadata meta = new ObjectMetadata();
    if (!serverSideEncryptionAlgorithm.isEmpty()) {
      meta.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    initiateMultipartUploadRequest.setObjectMetadata(meta);
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
    String uploadId = initiateMultipartUploadResult.getUploadId();
    List<PartETag> partETags = new ArrayList<PartETag>();
    try {
      for (int i = 0; i < partNum; i++) {
        long skipBytes = uploadPartSize * i;
        long size = (uploadPartSize < dataLen - skipBytes) ?
            uploadPartSize : dataLen - skipBytes;
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
   * Copy a directory from source path to destination path.
   * (the caller should make sure srcPath is a directory, and dstPath is valid)
   *
   * @param srcPath source path
   * @param dstPath destination path
   * @return true if successfully copied
   */
  private boolean copyDirectory(Path srcPath, Path dstPath) {
    String srcKey = pathToKey(srcPath);
    String dstKey = pathToKey(dstPath);

    if (!srcKey.endsWith("/")) {
      srcKey = srcKey + "/";
    }
    if (!dstKey.endsWith("/")) {
      dstKey = dstKey + "/";
    }

    if (dstKey.startsWith(srcKey)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot rename a directory to a subdirectory of self");
      }
      return false;
    }

    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
    listObjectsRequest.setPrefix(srcKey);
    listObjectsRequest.setMaxKeys(maxKeys);

    ObjectListing objects = ossClient.listObjects(listObjectsRequest);
    statistics.incrementReadOps(1);
    // Copy files from src folder to dst
    while (true) {
      for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
        String newKey =
            dstKey.concat(objectSummary.getKey().substring(srcKey.length()));
        copyFile(objectSummary.getKey(), newKey);
      }
      if (objects.isTruncated()) {
        listObjectsRequest.setMarker(objects.getNextMarker());
        statistics.incrementReadOps(1);
      } else {
        break;
      }
    }
    return true;
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    this.workingDir = dir;
  }

}
