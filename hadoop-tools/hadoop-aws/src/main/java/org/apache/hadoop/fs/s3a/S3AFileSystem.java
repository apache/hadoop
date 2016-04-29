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

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProviderChain;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.event.ProgressEvent;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.s3a.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3AFileSystem extends FileSystem {
  /**
   * Default blocksize as used in blocksize and FS status queries
   */
  public static final int DEFAULT_BLOCKSIZE = 32 * 1024 * 1024;
  private URI uri;
  private Path workingDir;
  private AmazonS3Client s3;
  private String bucket;
  private int maxKeys;
  private long partSize;
  private boolean enableMultiObjectsDelete;
  private TransferManager transfers;
  private ExecutorService threadPoolExecutor;
  private long multiPartThreshold;
  public static final Logger LOG = LoggerFactory.getLogger(S3AFileSystem.class);
  private CannedAccessControlList cannedACL;
  private String serverSideEncryptionAlgorithm;

  // The maximum number of entries that can be deleted in any call to s3
  private static final int MAX_ENTRIES_TO_DELETE = 1000;

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);

    uri = URI.create(name.getScheme() + "://" + name.getAuthority());
    workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(this.uri,
        this.getWorkingDirectory());

    AWSAccessKeys creds = getAWSAccessKeys(name, conf);

    AWSCredentialsProviderChain credentials = new AWSCredentialsProviderChain(
        new BasicAWSCredentialsProvider(
            creds.getAccessKey(), creds.getAccessSecret()),
        new InstanceProfileCredentialsProvider(),
        new AnonymousAWSCredentialsProvider()
    );

    bucket = name.getHost();

    ClientConfiguration awsConf = new ClientConfiguration();
    awsConf.setMaxConnections(conf.getInt(MAXIMUM_CONNECTIONS,
      DEFAULT_MAXIMUM_CONNECTIONS));
    boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS,
        DEFAULT_SECURE_CONNECTIONS);
    awsConf.setProtocol(secureConnections ?  Protocol.HTTPS : Protocol.HTTP);
    awsConf.setMaxErrorRetry(conf.getInt(MAX_ERROR_RETRIES,
      DEFAULT_MAX_ERROR_RETRIES));
    awsConf.setConnectionTimeout(conf.getInt(ESTABLISH_TIMEOUT,
        DEFAULT_ESTABLISH_TIMEOUT));
    awsConf.setSocketTimeout(conf.getInt(SOCKET_TIMEOUT,
      DEFAULT_SOCKET_TIMEOUT));
    String signerOverride = conf.getTrimmed(SIGNING_ALGORITHM, "");
    if(!signerOverride.isEmpty()) {
      awsConf.setSignerOverride(signerOverride);
    }

    initProxySupport(conf, awsConf, secureConnections);

    initAmazonS3Client(conf, credentials, awsConf);

    maxKeys = conf.getInt(MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS);
    partSize = conf.getLong(MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
    multiPartThreshold = conf.getLong(MIN_MULTIPART_THRESHOLD,
      DEFAULT_MIN_MULTIPART_THRESHOLD);
    enableMultiObjectsDelete = conf.getBoolean(ENABLE_MULTI_DELETE, true);

    if (partSize < 5 * 1024 * 1024) {
      LOG.error(MULTIPART_SIZE + " must be at least 5 MB");
      partSize = 5 * 1024 * 1024;
    }

    if (multiPartThreshold < 5 * 1024 * 1024) {
      LOG.error(MIN_MULTIPART_THRESHOLD + " must be at least 5 MB");
      multiPartThreshold = 5 * 1024 * 1024;
    }

    int maxThreads = conf.getInt(MAX_THREADS, DEFAULT_MAX_THREADS);
    if (maxThreads < 2) {
      LOG.warn(MAX_THREADS + " must be at least 2: forcing to 2.");
      maxThreads = 2;
    }
    int totalTasks = conf.getInt(MAX_TOTAL_TASKS, DEFAULT_MAX_TOTAL_TASKS);
    if (totalTasks < 1) {
      LOG.warn(MAX_TOTAL_TASKS + "must be at least 1: forcing to 1.");
      totalTasks = 1;
    }
    long keepAliveTime = conf.getLong(KEEPALIVE_TIME, DEFAULT_KEEPALIVE_TIME);
    threadPoolExecutor = new BlockingThreadPoolExecutorService(maxThreads,
        maxThreads + totalTasks, keepAliveTime, TimeUnit.SECONDS,
        "s3a-transfer-shared");

    initTransferManager();

    initCannedAcls(conf);

    if (!s3.doesBucketExist(bucket)) {
      throw new IOException("Bucket " + bucket + " does not exist");
    }

    initMultipartUploads(conf);

    serverSideEncryptionAlgorithm = conf.get(SERVER_SIDE_ENCRYPTION_ALGORITHM);

    setConf(conf);
  }

  void initProxySupport(Configuration conf, ClientConfiguration awsConf,
      boolean secureConnections) throws IllegalArgumentException,
      IllegalArgumentException {
    String proxyHost = conf.getTrimmed(PROXY_HOST, "");
    int proxyPort = conf.getInt(PROXY_PORT, -1);
    if (!proxyHost.isEmpty()) {
      awsConf.setProxyHost(proxyHost);
      if (proxyPort >= 0) {
        awsConf.setProxyPort(proxyPort);
      } else {
        if (secureConnections) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          awsConf.setProxyPort(443);
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          awsConf.setProxyPort(80);
        }
      }
      String proxyUsername = conf.getTrimmed(PROXY_USERNAME);
      String proxyPassword = conf.getTrimmed(PROXY_PASSWORD);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME + " or " +
            PROXY_PASSWORD + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      awsConf.setProxyUsername(proxyUsername);
      awsConf.setProxyPassword(proxyPassword);
      awsConf.setProxyDomain(conf.getTrimmed(PROXY_DOMAIN));
      awsConf.setProxyWorkstation(conf.getTrimmed(PROXY_WORKSTATION));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using proxy server {}:{} as user {} with password {} on " +
                "domain {} as workstation {}", awsConf.getProxyHost(),
            awsConf.getProxyPort(), String.valueOf(awsConf.getProxyUsername()),
            awsConf.getProxyPassword(), awsConf.getProxyDomain(),
            awsConf.getProxyWorkstation());
      }
    } else if (proxyPort >= 0) {
      String msg = "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  private void initAmazonS3Client(Configuration conf,
      AWSCredentialsProviderChain credentials, ClientConfiguration awsConf)
      throws IllegalArgumentException {
    s3 = new AmazonS3Client(credentials, awsConf);
    String endPoint = conf.getTrimmed(ENDPOINT,"");
    if (!endPoint.isEmpty()) {
      try {
        s3.setEndpoint(endPoint);
      } catch (IllegalArgumentException e) {
        String msg = "Incorrect endpoint: "  + e.getMessage();
        LOG.error(msg);
        throw new IllegalArgumentException(msg, e);
      }
    }
    enablePathStyleAccessIfRequired(conf);
  }

  private void enablePathStyleAccessIfRequired(Configuration conf) {
    final boolean pathStyleAccess = conf.getBoolean(PATH_STYLE_ACCESS, false);
    if (pathStyleAccess) {
      LOG.debug("Enabling path style access!");
      s3.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    }
  }

  private void initTransferManager() {
    TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
    transferConfiguration.setMinimumUploadPartSize(partSize);
    transferConfiguration.setMultipartUploadThreshold(multiPartThreshold);
    transferConfiguration.setMultipartCopyPartSize(partSize);
    transferConfiguration.setMultipartCopyThreshold(multiPartThreshold);

    transfers = new TransferManager(s3, threadPoolExecutor);
    transfers.setConfiguration(transferConfiguration);
  }

  private void initCannedAcls(Configuration conf) {
    String cannedACLName = conf.get(CANNED_ACL, DEFAULT_CANNED_ACL);
    if (!cannedACLName.isEmpty()) {
      cannedACL = CannedAccessControlList.valueOf(cannedACLName);
    } else {
      cannedACL = null;
    }
  }

  private void initMultipartUploads(Configuration conf) {
    boolean purgeExistingMultipart = conf.getBoolean(PURGE_EXISTING_MULTIPART,
      DEFAULT_PURGE_EXISTING_MULTIPART);
    long purgeExistingMultipartAge = conf.getLong(PURGE_EXISTING_MULTIPART_AGE,
      DEFAULT_PURGE_EXISTING_MULTIPART_AGE);

    if (purgeExistingMultipart) {
      Date purgeBefore = new Date(new Date().getTime() - purgeExistingMultipartAge*1000);

      transfers.abortMultipartUploads(bucket, purgeBefore);
    }
  }

  /**
   * Return the access key and secret for S3 API use.
   * Credentials may exist in configuration, within credential providers
   * or indicated in the UserInfo of the name URI param.
   * @param name the URI for which we need the access keys.
   * @param conf the Configuration object to interogate for keys.
   * @return AWSAccessKeys
   */
  AWSAccessKeys getAWSAccessKeys(URI name, Configuration conf)
      throws IOException {
    String accessKey = null;
    String secretKey = null;
    String userInfo = name.getUserInfo();
    if (userInfo != null) {
      int index = userInfo.indexOf(':');
      if (index != -1) {
        accessKey = userInfo.substring(0, index);
        secretKey = userInfo.substring(index + 1);
      } else {
        accessKey = userInfo;
      }
    }
    Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
          conf, S3AFileSystem.class);
    if (accessKey == null) {
      try {
        final char[] key = c.getPassword(ACCESS_KEY);
        if (key != null) {
          accessKey = (new String(key)).trim();
        }
      } catch(IOException ioe) {
        throw new IOException("Cannot find AWS access key.", ioe);
      }
    }
    if (secretKey == null) {
      try {
        final char[] pass = c.getPassword(SECRET_KEY);
        if (pass != null) {
          secretKey = (new String(pass)).trim();
        }
      } catch(IOException ioe) {
        throw new IOException("Cannot find AWS secret key.", ioe);
      }
    }
    return new AWSAccessKeys(accessKey, secretKey);
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return "s3a"
   */
  @Override
  public String getScheme() {
    return "s3a";
  }

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   */
  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public int getDefaultPort() {
    return Constants.S3A_DEFAULT_PORT;
  }

  /**
   * Returns the S3 client used by this filesystem.
   * @return AmazonS3Client
   */
  @VisibleForTesting
  AmazonS3Client getAmazonS3Client() {
    return s3;
  }

  public S3AFileSystem() {
    super();
  }

  /* Turns a path (relative or otherwise) into an S3 key
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

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening '{}' for reading.", f);
    }
    final FileStatus fileStatus = getFileStatus(f);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open " + f + " because it is a directory");
    }

    return new FSDataInputStream(new S3AInputStream(bucket, pathToKey(f),
      fileStatus.getLen(), s3, statistics));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission the permission to set.
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize the requested block size.
   * @param progress the progress reporter.
   * @throws IOException in the event of IO related errors.
   * @see #setPermission(Path, FsPermission)
   */
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    String key = pathToKey(f);

    if (!overwrite && exists(f)) {
      throw new FileAlreadyExistsException(f + " already exists");
    }
    if (getConf().getBoolean(FAST_UPLOAD, DEFAULT_FAST_UPLOAD)) {
      return new FSDataOutputStream(new S3AFastOutputStream(s3, this, bucket,
          key, progress, statistics, cannedACL,
          serverSideEncryptionAlgorithm, partSize, multiPartThreshold,
          threadPoolExecutor), statistics);
    }
    // We pass null to FSDataOutputStream so it won't count writes that are being buffered to a file
    return new FSDataOutputStream(new S3AOutputStream(getConf(), transfers, this,
      bucket, key, progress, cannedACL, statistics,
      serverSideEncryptionAlgorithm), null);
  }

  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException indicating that append is not supported.
   */
  public FSDataOutputStream append(Path f, int bufferSize,
    Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }


  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   *
   * Warning: S3 does not support renames. This method does a copy which can
   * take S3 some time to execute with large files and directories. Since
   * there is no Progressable passed in, this can time out jobs.
   *
   * Note: This implementation differs with other S3 drivers. Specifically:
   *       Fails if src is a file and dst is a directory.
   *       Fails if src is a directory and dst is a file.
   *       Fails if the parent of dst does not exist or is a file.
   *       Fails if dst is a directory that is not empty.
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on failure
   * @return true if rename is successful
   */
  public boolean rename(Path src, Path dst) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Rename path {} to {}", src, dst);
    }

    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);

    if (srcKey.isEmpty() || dstKey.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("rename: src or dst are empty");
      }
      return false;
    }

    S3AFileStatus srcStatus;
    try {
      srcStatus = getFileStatus(src);
    } catch (FileNotFoundException e) {
      LOG.error("rename: src not found {}", src);
      return false;
    }

    if (srcKey.equals(dstKey)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("rename: src and dst refer to the same file or directory");
      }
      return srcStatus.isFile();
    }

    S3AFileStatus dstStatus = null;
    try {
      dstStatus = getFileStatus(dst);

      if (srcStatus.isDirectory() && dstStatus.isFile()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("rename: src is a directory and dst is a file");
        }
        return false;
      }

      if (dstStatus.isDirectory() && !dstStatus.isEmptyDirectory()) {
        return false;
      }
    } catch (FileNotFoundException e) {
      // Parent must exist
      Path parent = dst.getParent();
      if (!pathToKey(parent).isEmpty()) {
        try {
          S3AFileStatus dstParentStatus = getFileStatus(dst.getParent());
          if (!dstParentStatus.isDirectory()) {
            return false;
          }
        } catch (FileNotFoundException e2) {
          return false;
        }
      }
    }

    // Ok! Time to start
    if (srcStatus.isFile()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("rename: renaming file " + src + " to " + dst);
      }
      if (dstStatus != null && dstStatus.isDirectory()) {
        String newDstKey = dstKey;
        if (!newDstKey.endsWith("/")) {
          newDstKey = newDstKey + "/";
        }
        String filename =
            srcKey.substring(pathToKey(src.getParent()).length()+1);
        newDstKey = newDstKey + filename;
        copyFile(srcKey, newDstKey);
      } else {
        copyFile(srcKey, dstKey);
      }
      delete(src, false);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("rename: renaming directory " + src + " to " + dst);
      }

      // This is a directory to directory copy
      if (!dstKey.endsWith("/")) {
        dstKey = dstKey + "/";
      }

      if (!srcKey.endsWith("/")) {
        srcKey = srcKey + "/";
      }

      //Verify dest is not a child of the source directory
      if (dstKey.startsWith(srcKey)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("cannot rename a directory to a subdirectory of self");
        }
        return false;
      }

      List<DeleteObjectsRequest.KeyVersion> keysToDelete =
        new ArrayList<>();
      if (dstStatus != null && dstStatus.isEmptyDirectory()) {
        // delete unnecessary fake directory.
        keysToDelete.add(new DeleteObjectsRequest.KeyVersion(dstKey));
      }

      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(srcKey);
      request.setMaxKeys(maxKeys);

      ObjectListing objects = s3.listObjects(request);
      statistics.incrementReadOps(1);

      while (true) {
        for (S3ObjectSummary summary : objects.getObjectSummaries()) {
          keysToDelete.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
          String newDstKey = dstKey + summary.getKey().substring(srcKey.length());
          copyFile(summary.getKey(), newDstKey);

          if (keysToDelete.size() == MAX_ENTRIES_TO_DELETE) {
            removeKeys(keysToDelete, true);
          }
        }

        if (objects.isTruncated()) {
          objects = s3.listNextBatchOfObjects(objects);
          statistics.incrementReadOps(1);
        } else {
          if (!keysToDelete.isEmpty()) {
            removeKeys(keysToDelete, false);
          }
          break;
        }
      }
    }

    if (src.getParent() != dst.getParent()) {
      deleteUnnecessaryFakeDirectories(dst.getParent());
      createFakeDirectoryIfNecessary(src.getParent());
    }
    return true;
  }

  /**
   * A helper method to delete a list of keys on a s3-backend.
   *
   * @param keysToDelete collection of keys to delete on the s3-backend
   * @param clearKeys clears the keysToDelete-list after processing the list
   *            when set to true
   */
  private void removeKeys(List<DeleteObjectsRequest.KeyVersion> keysToDelete,
          boolean clearKeys) {
    if (enableMultiObjectsDelete) {
      DeleteObjectsRequest deleteRequest
          = new DeleteObjectsRequest(bucket).withKeys(keysToDelete);
      s3.deleteObjects(deleteRequest);
      statistics.incrementWriteOps(1);
    } else {
      int writeops = 0;

      for (DeleteObjectsRequest.KeyVersion keyVersion : keysToDelete) {
        s3.deleteObject(
            new DeleteObjectRequest(bucket, keyVersion.getKey()));
        writeops++;
      }

      statistics.incrementWriteOps(writeops);
    }
    if (clearKeys) {
      keysToDelete.clear();
    }
  }

  /** Delete a file.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return  true if delete is successful else false.
   * @throws IOException due to inability to delete a directory or file.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Delete path " + f + " - recursive " + recursive);
    }
    S3AFileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't delete " + f + " - does not exist");
      }
      return false;
    }

    String key = pathToKey(f);

    if (status.isDirectory()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("delete: Path is a directory");
      }

      if (!recursive && !status.isEmptyDirectory()) {
        throw new IOException("Path is a folder: " + f +
                              " and it is not an empty directory");
      }

      if (!key.endsWith("/")) {
        key = key + "/";
      }

      if (key.equals("/")) {
        LOG.info("s3a cannot delete the root directory");
        return false;
      }

      if (status.isEmptyDirectory()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Deleting fake empty directory");
        }
        s3.deleteObject(bucket, key);
        statistics.incrementWriteOps(1);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Getting objects for directory prefix " + key + " to delete");
        }

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(key);
        // Hopefully not setting a delimiter will cause this to find everything
        //request.setDelimiter("/");
        request.setMaxKeys(maxKeys);

        List<DeleteObjectsRequest.KeyVersion> keys =
          new ArrayList<>();
        ObjectListing objects = s3.listObjects(request);
        statistics.incrementReadOps(1);
        while (true) {
          for (S3ObjectSummary summary : objects.getObjectSummaries()) {
            keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got object to delete " + summary.getKey());
            }

            if (keys.size() == MAX_ENTRIES_TO_DELETE) {
              removeKeys(keys, true);
            }
          }

          if (objects.isTruncated()) {
            objects = s3.listNextBatchOfObjects(objects);
            statistics.incrementReadOps(1);
          } else {
            if (!keys.isEmpty()) {
              removeKeys(keys, false);
            }
            break;
          }
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("delete: Path is a file");
      }
      s3.deleteObject(bucket, key);
      statistics.incrementWriteOps(1);
    }

    createFakeDirectoryIfNecessary(f.getParent());

    return true;
  }

  private void createFakeDirectoryIfNecessary(Path f) throws IOException {
    String key = pathToKey(f);
    if (!key.isEmpty() && !exists(f)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating new fake directory at " + f);
      }
      createFakeDirectory(bucket, key);
    }
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    String key = pathToKey(f);
    if (LOG.isDebugEnabled()) {
      LOG.debug("List status for path: " + f);
    }

    final List<FileStatus> result = new ArrayList<FileStatus>();
    final FileStatus fileStatus =  getFileStatus(f);

    if (fileStatus.isDirectory()) {
      if (!key.isEmpty()) {
        key = key + "/";
      }

      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(key);
      request.setDelimiter("/");
      request.setMaxKeys(maxKeys);

      if (LOG.isDebugEnabled()) {
        LOG.debug("listStatus: doing listObjects for directory " + key);
      }

      ObjectListing objects = s3.listObjects(request);
      statistics.incrementReadOps(1);

      Path fQualified = f.makeQualified(uri, workingDir);

      while (true) {
        for (S3ObjectSummary summary : objects.getObjectSummaries()) {
          Path keyPath = keyToPath(summary.getKey()).makeQualified(uri, workingDir);
          // Skip over keys that are ourselves and old S3N _$folder$ files
          if (keyPath.equals(fQualified) ||
              summary.getKey().endsWith(S3N_FOLDER_SUFFIX)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Ignoring: " + keyPath);
            }
            continue;
          }

          if (objectRepresentsDirectory(summary.getKey(), summary.getSize())) {
            result.add(new S3AFileStatus(true, true, keyPath));
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding: fd: " + keyPath);
            }
          } else {
            result.add(new S3AFileStatus(summary.getSize(),
                dateToLong(summary.getLastModified()), keyPath,
                getDefaultBlockSize(fQualified)));
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding: fi: " + keyPath);
            }
          }
        }

        for (String prefix : objects.getCommonPrefixes()) {
          Path keyPath = keyToPath(prefix).makeQualified(uri, workingDir);
          if (keyPath.equals(f)) {
            continue;
          }
          result.add(new S3AFileStatus(true, false, keyPath));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding: rd: " + keyPath);
          }
        }

        if (objects.isTruncated()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("listStatus: list truncated - getting next batch");
          }

          objects = s3.listNextBatchOfObjects(objects);
          statistics.incrementReadOps(1);
        } else {
          break;
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding: rd (not a dir): " + f);
      }
      result.add(fileStatus);
    }

    return result.toArray(new FileStatus[result.size()]);
  }



  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   *
   * @param new_dir the current working directory.
   */
  public void setWorkingDirectory(Path new_dir) {
    workingDir = new_dir;
  }

  /**
   * Get the current working directory for the given file system
   * @return the directory pathname
   */
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has the semantics of Unix 'mkdir -p'.
   * Existence of the directory hierarchy is not an error.
   * @param f path to create
   * @param permission to apply to f
   */
  // TODO: If we have created an empty file at /foo/bar and we then call
  // mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Making directory: " + f);
    }


    try {
      FileStatus fileStatus = getFileStatus(f);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + f);
      }
    } catch (FileNotFoundException e) {
      Path fPart = f;
      do {
        try {
          FileStatus fileStatus = getFileStatus(fPart);
          if (fileStatus.isFile()) {
            throw new FileAlreadyExistsException(String.format(
                "Can't make directory for path '%s' since it is a file.",
                fPart));
          }
        } catch (FileNotFoundException fnfe) {
        }
        fPart = fPart.getParent();
      } while (fPart != null);

      String key = pathToKey(f);
      createFakeDirectory(bucket, key);
      return true;
    }
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws java.io.FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public S3AFileStatus getFileStatus(Path f) throws IOException {
    String key = pathToKey(f);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting path status for " + f + " (" + key + ")");
    }


    if (!key.isEmpty()) {
      try {
        ObjectMetadata meta = s3.getObjectMetadata(bucket, key);
        statistics.incrementReadOps(1);

        if (objectRepresentsDirectory(key, meta.getContentLength())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found exact file: fake directory");
          }
          return new S3AFileStatus(true, true,
              f.makeQualified(uri, workingDir));
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found exact file: normal file");
          }
          return new S3AFileStatus(meta.getContentLength(),
              dateToLong(meta.getLastModified()),
              f.makeQualified(uri, workingDir),
              getDefaultBlockSize(f.makeQualified(uri, workingDir)));
        }
      } catch (AmazonServiceException e) {
        if (e.getStatusCode() != 404) {
          printAmazonServiceException(e);
          throw e;
        }
      } catch (AmazonClientException e) {
        printAmazonClientException(e);
        throw e;
      }

      // Necessary?
      if (!key.endsWith("/")) {
        try {
          String newKey = key + "/";
          ObjectMetadata meta = s3.getObjectMetadata(bucket, newKey);
          statistics.incrementReadOps(1);

          if (objectRepresentsDirectory(newKey, meta.getContentLength())) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Found file (with /): fake directory");
            }
            return new S3AFileStatus(true, true, f.makeQualified(uri, workingDir));
          } else {
            LOG.warn("Found file (with /): real file? should not happen: {}", key);

            return new S3AFileStatus(meta.getContentLength(),
                dateToLong(meta.getLastModified()),
                f.makeQualified(uri, workingDir),
                getDefaultBlockSize(f.makeQualified(uri, workingDir)));
          }
        } catch (AmazonServiceException e) {
          if (e.getStatusCode() != 404) {
            printAmazonServiceException(e);
            throw e;
          }
        } catch (AmazonClientException e) {
          printAmazonClientException(e);
          throw e;
        }
      }
    }

    try {
      if (!key.isEmpty() && !key.endsWith("/")) {
        key = key + "/";
      }
      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(key);
      request.setDelimiter("/");
      request.setMaxKeys(1);

      ObjectListing objects = s3.listObjects(request);
      statistics.incrementReadOps(1);

      if (!objects.getCommonPrefixes().isEmpty()
          || objects.getObjectSummaries().size() > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found path as directory (with /): " +
              objects.getCommonPrefixes().size() + "/" +
              objects.getObjectSummaries().size());

          for (S3ObjectSummary summary : objects.getObjectSummaries()) {
            LOG.debug("Summary: " + summary.getKey() + " " + summary.getSize());
          }
          for (String prefix : objects.getCommonPrefixes()) {
            LOG.debug("Prefix: " + prefix);
          }
        }

        return new S3AFileStatus(true, false,
            f.makeQualified(uri, workingDir));
      } else if (key.isEmpty()) {
        LOG.debug("Found root directory");
        return new S3AFileStatus(true, true, f.makeQualified(uri, workingDir));
      }
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() != 404) {
        printAmazonServiceException(e);
        throw e;
      }
    } catch (AmazonClientException e) {
      printAmazonClientException(e);
      throw e;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Not Found: " + f);
    }
    throw new FileNotFoundException("No such file or directory: " + f);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   *
   * This version doesn't need to create a temporary file to calculate the md5.
   * Sadly this doesn't seem to be used by the shell cp :(
   *
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
    Path dst) throws IOException {
    String key = pathToKey(dst);

    if (!overwrite && exists(dst)) {
      throw new IOException(dst + " already exists");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Copying local file from " + src + " to " + dst);
    }

    // Since we have a local file, we don't need to stream into a temporary file
    LocalFileSystem local = getLocal(getConf());
    File srcfile = local.pathToFile(src);

    final ObjectMetadata om = new ObjectMetadata();
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setSSEAlgorithm(serverSideEncryptionAlgorithm);
    }
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, srcfile);
    putObjectRequest.setCannedAcl(cannedACL);
    putObjectRequest.setMetadata(om);

    ProgressListener progressListener = new ProgressListener() {
      public void progressChanged(ProgressEvent progressEvent) {
        switch (progressEvent.getEventType()) {
          case TRANSFER_PART_COMPLETED_EVENT:
            statistics.incrementWriteOps(1);
            break;
          default:
            break;
        }
      }
    };

    Upload up = transfers.upload(putObjectRequest);
    up.addProgressListener(progressListener);
    try {
      up.waitForUploadResult();
      statistics.incrementWriteOps(1);
    } catch (InterruptedException e) {
      throw new IOException("Got interrupted, cancelling");
    }

    // This will delete unnecessary fake parent directories
    finishedWrite(key);

    if (delSrc) {
      local.delete(src, false);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      if (transfers != null) {
        transfers.shutdownNow(true);
        transfers = null;
      }
    }
  }

  /**
  * Override getCononicalServiceName because we don't support token in S3A
  */
  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }

  private void copyFile(String srcKey, String dstKey) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("copyFile " + srcKey + " -> " + dstKey);
    }

    ObjectMetadata srcom = s3.getObjectMetadata(bucket, srcKey);
    ObjectMetadata dstom = cloneObjectMetadata(srcom);
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      dstom.setSSEAlgorithm(serverSideEncryptionAlgorithm);
    }
    CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
    copyObjectRequest.setCannedAccessControlList(cannedACL);
    copyObjectRequest.setNewObjectMetadata(dstom);

    ProgressListener progressListener = new ProgressListener() {
      public void progressChanged(ProgressEvent progressEvent) {
        switch (progressEvent.getEventType()) {
          case TRANSFER_PART_COMPLETED_EVENT:
            statistics.incrementWriteOps(1);
            break;
          default:
            break;
        }
      }
    };

    Copy copy = transfers.copy(copyObjectRequest);
    copy.addProgressListener(progressListener);
    try {
      copy.waitForCopyResult();
      statistics.incrementWriteOps(1);
    } catch (InterruptedException e) {
      throw new IOException("Got interrupted, cancelling");
    }
  }

  private boolean objectRepresentsDirectory(final String name, final long size) {
    return !name.isEmpty() && name.charAt(name.length() - 1) == '/' && size == 0L;
  }

  // Handles null Dates that can be returned by AWS
  private static long dateToLong(final Date date) {
    if (date == null) {
      return 0L;
    }

    return date.getTime();
  }

  public void finishedWrite(String key) throws IOException {
    deleteUnnecessaryFakeDirectories(keyToPath(key).getParent());
  }

  private void deleteUnnecessaryFakeDirectories(Path f) throws IOException {
    while (true) {
      try {
        String key = pathToKey(f);
        if (key.isEmpty()) {
          break;
        }

        S3AFileStatus status = getFileStatus(f);

        if (status.isDirectory() && status.isEmptyDirectory()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting fake directory " + key + "/");
          }
          s3.deleteObject(bucket, key + "/");
          statistics.incrementWriteOps(1);
        }
      } catch (FileNotFoundException | AmazonServiceException e) {
      }

      if (f.isRoot()) {
        break;
      }

      f = f.getParent();
    }
  }


  private void createFakeDirectory(final String bucketName, final String objectName)
      throws AmazonClientException, AmazonServiceException {
    if (!objectName.endsWith("/")) {
      createEmptyObject(bucketName, objectName + "/");
    } else {
      createEmptyObject(bucketName, objectName);
    }
  }

  // Used to create an empty file that represents an empty directory
  private void createEmptyObject(final String bucketName, final String objectName)
      throws AmazonClientException, AmazonServiceException {
    final InputStream im = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    final ObjectMetadata om = new ObjectMetadata();
    om.setContentLength(0L);
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setSSEAlgorithm(serverSideEncryptionAlgorithm);
    }
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, im, om);
    putObjectRequest.setCannedAcl(cannedACL);
    s3.putObject(putObjectRequest);
    statistics.incrementWriteOps(1);
  }

  /**
   * Creates a copy of the passed {@link ObjectMetadata}.
   * Does so without using the {@link ObjectMetadata#clone()} method,
   * to avoid copying unnecessary headers.
   * @param source the {@link ObjectMetadata} to copy
   * @return a copy of {@link ObjectMetadata} with only relevant attributes
   */
  private ObjectMetadata cloneObjectMetadata(ObjectMetadata source) {
    // This approach may be too brittle, especially if
    // in future there are new attributes added to ObjectMetadata
    // that we do not explicitly call to set here
    ObjectMetadata ret = new ObjectMetadata();

    // Non null attributes
    ret.setContentLength(source.getContentLength());

    // Possibly null attributes
    // Allowing nulls to pass breaks it during later use
    if (source.getCacheControl() != null) {
      ret.setCacheControl(source.getCacheControl());
    }
    if (source.getContentDisposition() != null) {
      ret.setContentDisposition(source.getContentDisposition());
    }
    if (source.getContentEncoding() != null) {
      ret.setContentEncoding(source.getContentEncoding());
    }
    if (source.getContentMD5() != null) {
      ret.setContentMD5(source.getContentMD5());
    }
    if (source.getContentType() != null) {
      ret.setContentType(source.getContentType());
    }
    if (source.getExpirationTime() != null) {
      ret.setExpirationTime(source.getExpirationTime());
    }
    if (source.getExpirationTimeRuleId() != null) {
      ret.setExpirationTimeRuleId(source.getExpirationTimeRuleId());
    }
    if (source.getHttpExpiresDate() != null) {
      ret.setHttpExpiresDate(source.getHttpExpiresDate());
    }
    if (source.getLastModified() != null) {
      ret.setLastModified(source.getLastModified());
    }
    if (source.getOngoingRestore() != null) {
      ret.setOngoingRestore(source.getOngoingRestore());
    }
    if (source.getRestoreExpirationTime() != null) {
      ret.setRestoreExpirationTime(source.getRestoreExpirationTime());
    }
    if (source.getSSEAlgorithm() != null) {
      ret.setSSEAlgorithm(source.getSSEAlgorithm());
    }
    if (source.getSSECustomerAlgorithm() != null) {
      ret.setSSECustomerAlgorithm(source.getSSECustomerAlgorithm());
    }
    if (source.getSSECustomerKeyMd5() != null) {
      ret.setSSECustomerKeyMd5(source.getSSECustomerKeyMd5());
    }

    for (Map.Entry<String, String> e : source.getUserMetadata().entrySet()) {
      ret.addUserMetadata(e.getKey(), e.getValue());
    }
    return ret;
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize i/o time.
   * @deprecated use {@link #getDefaultBlockSize(Path)} instead
   */
  @Deprecated
  public long getDefaultBlockSize() {
    // default to 32MB: large enough to minimize the impact of seeks
    return getConf().getLong(FS_S3A_BLOCK_SIZE, DEFAULT_BLOCKSIZE);
  }

  private void printAmazonServiceException(AmazonServiceException ase) {
    LOG.info("Caught an AmazonServiceException, which means your request made it " +
        "to Amazon S3, but was rejected with an error response for some reason.");
    LOG.info("Error Message: " + ase.getMessage());
    LOG.info("HTTP Status Code: " + ase.getStatusCode());
    LOG.info("AWS Error Code: " + ase.getErrorCode());
    LOG.info("Error Type: " + ase.getErrorType());
    LOG.info("Request ID: " + ase.getRequestId());
    LOG.info("Class Name: " + ase.getClass().getName());
  }

  private void printAmazonClientException(AmazonClientException ace) {
    LOG.info("Caught an AmazonClientException, which means the client encountered " +
        "a serious internal problem while trying to communicate with S3, " +
        "such as not being able to access the network.");
    LOG.info("Error Message: {}" + ace, ace);
  }

  /**
   * This is a simple encapsulation of the
   * S3 access key and secret.
   */
  static class AWSAccessKeys {
    private String accessKey = null;
    private String accessSecret = null;

    /**
     * Constructor.
     * @param key - AWS access key
     * @param secret - AWS secret key
     */
    public AWSAccessKeys(String key, String secret) {
      accessKey = key;
      accessSecret = secret;
    }

    /**
     * Return the AWS access key.
     * @return key
     */
    public String getAccessKey() {
      return accessKey;
    }

    /**
     * Return the AWS secret key.
     * @return secret
     */
    public String getAccessSecret() {
      return accessSecret;
    }
  }
}
