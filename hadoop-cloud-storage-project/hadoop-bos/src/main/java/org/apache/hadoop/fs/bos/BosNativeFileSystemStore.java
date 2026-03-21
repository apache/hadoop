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

import com.baidubce.BceClientException;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.BosPrefixInfo;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.bos.exceptions.BandwidthLimitException;
import org.apache.hadoop.fs.bos.exceptions.BosHotObjectException;
import org.apache.hadoop.fs.bos.exceptions.BosServerException;
import org.apache.hadoop.fs.bos.exceptions.SessionTokenExpireException;
import org.apache.hadoop.fs.bos.utils.BosCRC32CCheckSum;
import org.apache.hadoop.io.retry.DefaultFailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for BOS native file system store
 * implementations. Provides common operations such as object
 * retrieval, listing, deletion, copy, and rename. Concrete
 * subclasses handle hierarchy vs. non-hierarchy bucket
 * differences.
 */
public abstract class BosNativeFileSystemStore {

  /** Logger instance using the runtime class name. */
  public final Logger log =
      LoggerFactory.getLogger(this.getClass().getName());

  /** The BOS client proxy for service operations. */
  protected final BosClientProxy bosClientProxy;

  /** The name of the BOS bucket. */
  protected final String bucketName;

  /** The Hadoop configuration. */
  protected final Configuration conf;

  /** Thread pool for bounded parallel operations. */
  protected ExecutorService boundedThreadPool;

  private String envUserName;
  private String envGroupName;

  /**
   * Constructs a BosNativeFileSystemStore.
   *
   * @param bosClientProxy the BOS client proxy
   * @param bucketName     the name of the bucket
   * @param conf           the Hadoop configuration
   */
  public BosNativeFileSystemStore(
      BosClientProxy bosClientProxy,
      String bucketName, Configuration conf) {
    this.bosClientProxy = bosClientProxy;
    this.bucketName = bucketName;
    this.conf = conf;
  }

  /**
   * Creates a default BOS client proxy with retry policies.
   *
   * @param conf the Hadoop configuration
   * @return a retry-wrapped BOS client proxy
   */
  private static BosClientProxy
      createDefaultBosClientProxy(Configuration conf) {
    BosClientProxy bosClientProxy =
        new BosClientProxyImpl();
    RetryPolicy basePolicy =
        RetryPolicies
            .retryUpToMaximumCountWithProportionalSleep(
                conf.getInt(
                    BaiduBosConstants.BOS_MAX_RETRIES,
                    3),
                conf.getLong(
                    BaiduBosConstants
                        .BOS_SLEEP_MILL_SECONDS,
                    200),
                TimeUnit.MILLISECONDS);

    BosRandomRetryPolicy bosHotObjectRandomRetryPolicy =
        new BosRandomRetryPolicy(
            conf.getInt(
                BaiduBosConstants.BOS_MAX_RETRIES, 3),
            2000,
            8000,
            TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>, RetryPolicy>
        exceptionToPolicyMap = new HashMap<>();

    exceptionToPolicyMap.put(
        IOException.class, basePolicy);
    exceptionToPolicyMap.put(
        SessionTokenExpireException.class, basePolicy);
    exceptionToPolicyMap.put(
        BceClientException.class, basePolicy);
    exceptionToPolicyMap.put(
        BosServerException.class, basePolicy);
    exceptionToPolicyMap.put(
        BandwidthLimitException.class,
        bosHotObjectRandomRetryPolicy);
    exceptionToPolicyMap.put(
        BosHotObjectException.class,
        bosHotObjectRandomRetryPolicy);
    RetryPolicy methodPolicy =
        RetryPolicies.retryByException(
            RetryPolicies.TRY_ONCE_THEN_FAIL,
            exceptionToPolicyMap);

    Map<String, RetryPolicy> methodNameToPolicyMap =
        new HashMap<>();

    methodNameToPolicyMap.put(
        "putObject", methodPolicy);
    methodNameToPolicyMap.put(
        "putEmptyObject", methodPolicy);
    methodNameToPolicyMap.put(
        "getObjectMetadata", methodPolicy);
    methodNameToPolicyMap.put(
        "getObject", methodPolicy);
    methodNameToPolicyMap.put(
        "listObjects", methodPolicy);
    methodNameToPolicyMap.put(
        "uploadPart", methodPolicy);
    methodNameToPolicyMap.put(
        "deleteObject", methodPolicy);
    methodNameToPolicyMap.put(
        "deleteDirectory", methodPolicy);
    methodNameToPolicyMap.put(
        "deleteMultipleObjects", methodPolicy);
    methodNameToPolicyMap.put(
        "copyObject", methodPolicy);
    methodNameToPolicyMap.put(
        "renameObject", methodPolicy);
    methodNameToPolicyMap.put(
        "completeMultipartUpload", methodPolicy);
    methodNameToPolicyMap.put(
        "initiateMultipartUpload", methodPolicy);
    methodNameToPolicyMap.put(
        "isHierarchyBucket", methodPolicy);

    return (BosClientProxy) RetryProxy.create(
        BosClientProxy.class,
        new DefaultFailoverProxyProvider<>(
            BosClientProxy.class, bosClientProxy),
        methodNameToPolicyMap,
        new BosTryOnceThenFail());
  }

  /**
   * Initializes this store, including setting up thread
   * pools.
   *
   * @throws IOException if initialization fails
   */
  public void initialize() throws IOException {
    initThreadPools();
  }

  /**
   * Initializes the bounded thread pool used for parallel
   * operations.
   */
  private void initThreadPools() {
    int activeTasks = conf.getInt(
        BaiduBosConstants.BOS_THREADS_MAX_NUM,
        BaiduBosConstants.BOS_THREADS_MAX_NUM_DEFAULT);
    int waitingTasks = conf.getInt(
        BaiduBosConstants.BOS_THREADS_MAX_NUM,
        BaiduBosConstants.BOS_THREADS_MAX_NUM_DEFAULT)
        * 2;
    boundedThreadPool =
        BlockingThreadPoolExecutorService.newInstance(
            activeTasks,
            waitingTasks,
            60, TimeUnit.SECONDS,
            this.bucketName + "-pool");
  }

  /**
   * Creates a BosNativeFileSystemStore for the given URI.
   * Determines whether the bucket is hierarchy-enabled and
   * returns the appropriate implementation.
   *
   * @param uri  the URI of the BOS filesystem
   * @param conf the Hadoop configuration
   * @return a new store instance
   * @throws IOException if creation fails
   */
  public static BosNativeFileSystemStore createStore(
      URI uri, Configuration conf) throws IOException {
    BosClientProxy bosClientProxy =
        createDefaultBosClientProxy(conf);
    bosClientProxy.init(uri, conf);
    String bucketName = uri.getHost();
    boolean isHierarchy;
    String hierarchyConf = conf.get(
        BaiduBosConstants.BOS_BUCKET_HIERARCHY, "null");
    if (hierarchyConf.equals("true")) {
      isHierarchy = true;
    } else if (hierarchyConf.equals("false")) {
      isHierarchy = false;
    } else {
      isHierarchy =
          bosClientProxy.isHierarchyBucket(bucketName);
    }
    BosNativeFileSystemStore store = null;
    if (isHierarchy) {
      store = new HierarchyBosNativeFileSystemStore(
          bosClientProxy, bucketName, conf);
    } else {
      store = new NonHierarchyBosNativeFileSystemStore(
          bosClientProxy, bucketName, conf);
    }
    store.initialize();
    return store;
  }

  /**
   * Closes the BOS client and releases resources.
   */
  public synchronized void close() {
    try {
      if (boundedThreadPool != null) {
        boundedThreadPool.shutdownNow();
      }
    } catch (Exception e) {
      log.error("thread pool shutdown fail:", e);
    }
    try {
      if (bosClientProxy != null) {
        bosClientProxy.close();
      }
    } catch (Exception e) {
      log.error("bos client close fail:", e);
    }
  }

  /**
   * Stores an empty file (zero-length object) in BOS using
   * the environment user and group names.
   *
   * @param key the object key
   * @throws IOException if the operation fails
   */
  public void storeEmptyFile(String key)
      throws IOException {
    storeEmptyFile(
        key, this.getEnvUserName(),
        this.getEnvGroupName());
  }

  /**
   * Stores an empty file (zero-length object) in BOS with
   * specified user and group names.
   *
   * @param key       the object key
   * @param userName  the user name metadata
   * @param groupName the group name metadata
   * @throws IOException if the operation fails
   */
  public void storeEmptyFile(
      String key, String userName, String groupName)
      throws IOException {
    ObjectMetadata objectMeta = new ObjectMetadata();
    objectMeta.addUserMetadata(
        BaiduBosConstants.BOS_FILE_PATH_USER_KEY,
        userName);
    objectMeta.addUserMetadata(
        BaiduBosConstants.BOS_FILE_PATH_GROUP_KEY,
        groupName);
    bosClientProxy.putEmptyObject(
        bucketName, key, objectMeta);
  }

  /**
   * Creates an output stream for writing a file to BOS.
   *
   * @param key          the object key
   * @param hadoopConf   the Hadoop configuration
   * @return an output stream for writing the file
   */
  public OutputStream createFile(
      String key, Configuration hadoopConf) {
    return new BosOutputStream(
        this, this.bucketName, key, hadoopConf);
  }

  /**
   * Gets the file checksum for the specified key.
   *
   * @param key the object key
   * @return the file checksum, or null if unavailable
   * @throws IOException if the key does not exist or an I/O
   *                     error occurs
   */
  public FileChecksum getFileChecksum(String key)
      throws IOException {
    try {
      ObjectMetadata metadata =
          bosClientProxy.getObjectMetadata(
              bucketName, key);
      if (metadata.getxBceCrc32c() == null) {
        return null;
      }
      return new BosCRC32CCheckSum(
          metadata.getxBceCrc32c());
    } catch (FileNotFoundException e) {
      throw new FileNotFoundException(
          key + ": No such file or directory.");
    }
  }

  /**
   * Checks whether the given key represents a directory.
   *
   * @param key the object key
   * @return true if the key is a directory, false if it is a
   *         file or does not exist
   * @throws IOException if an I/O error occurs
   */
  public abstract boolean isDirectory(String key)
      throws IOException;

  /**
   * Retrieves the metadata for the specified key.
   *
   * @param key the object key
   * @return the file metadata
   * @throws IOException if the key is not found or an I/O
   *                     error occurs
   */
  public abstract FileMetadata retrieveMetadata(
      String key) throws IOException;

  /**
   * Retrieves a range of bytes from the object at the given
   * key.
   *
   * @param key the object key
   * @param byteRangeStart the start of the byte range
   * @param byteRangeEnd the end of the byte range
   * @param meta the file metadata for verification
   * @return an input stream for reading the range, or null
   *         if the key is not found
   * @throws IOException if an I/O error occurs
   */
  public InputStream retrieve(
      String key, long byteRangeStart,
      long byteRangeEnd, FileMetadata meta)
      throws IOException {
    log.debug(
        "Getting key: {} from bucket: {} with"
            + " byteRangeStart: {} and"
            + " byteRangeEnd: {}.",
        key, bucketName, byteRangeStart, byteRangeEnd);
    if (meta == null || !meta.getKey().equals(key)) {
      throw new IOException(
          "FileMetadata not match key:" + key);
    }
    GetObjectRequest request =
        new GetObjectRequest(bucketName, key);
    // Due to the InvalidRange exception of bos, we
    // shouldn't set range for empty file
    if (meta.getLength() != 0) {
      request.setRange(byteRangeStart, byteRangeEnd);
    }
    BosObject object =
        bosClientProxy.getObject(request);
    return object.getObjectContent();
  }

  /**
   * Converts a prefix to a directory-style key by appending
   * a trailing delimiter if not already present.
   *
   * @param prefix the prefix to convert
   * @return the directory-style key
   */
  public String prefixToDir(String prefix) {
    // !prefix.isEmpty() must not be removed since ""
    // is root dir.
    if (prefix != null && !prefix.isEmpty()
        && !prefix.endsWith(
            BaiduBosFileSystem.PATH_DELIMITER)) {
      prefix += BaiduBosFileSystem.PATH_DELIMITER;
    }

    return prefix;
  }

  /**
   * Lists objects in BOS with the default path delimiter.
   *
   * @param prefix           the prefix to filter by
   * @param maxListingLength the maximum number of results
   * @param priorLastKey     the marker for pagination
   * @return the partial listing result
   * @throws IOException if an I/O error occurs
   */
  public PartialListing list(
      String prefix, int maxListingLength,
      String priorLastKey) throws IOException {
    return list(
        prefix, BaiduBosFileSystem.PATH_DELIMITER,
        maxListingLength, priorLastKey);
  }

  /**
   * Lists objects in BOS with a specified delimiter.
   *
   * @param prefix           the prefix to filter by
   * @param maxListingLength the maximum number of results
   * @param priorLastKey     the marker for pagination
   * @param delimiter        the delimiter for grouping
   * @return the partial listing result
   * @throws IOException if an I/O error occurs
   */
  public PartialListing list(
      String prefix, int maxListingLength,
      String priorLastKey, String delimiter)
      throws IOException {
    if (this.isHierarchy()) {
      delimiter = BaiduBosFileSystem.PATH_DELIMITER;
    }
    return list(
        prefix, delimiter,
        maxListingLength, priorLastKey);
  }

  /**
   * Returns whether this store is backed by a hierarchy
   * (namespace) bucket.
   *
   * @return true if the bucket is a hierarchy bucket
   */
  protected abstract boolean isHierarchy();

  /**
   * Lists objects in BOS.
   *
   * @param prefix           the prefix to filter by
   * @param delimiter        the delimiter for grouping
   * @param maxListingLength the maximum number of results
   * @param priorLastKey     the marker for pagination
   * @return the partial listing result, or null if the list
   *         could not be populated
   * @throws IOException if an I/O error occurs
   */
  private PartialListing list(
      String prefix, String delimiter,
      int maxListingLength, String priorLastKey)
      throws IOException {
    ListObjectsRequest request =
        new ListObjectsRequest(bucketName);
    request.setPrefix(prefixToDir(prefix));
    request.setMarker(priorLastKey);
    if (delimiter != null) {
      request.setDelimiter(delimiter);
    }
    request.setMaxKeys(maxListingLength);

    ListObjectsResponse objects =
        bosClientProxy.listObjects(request);

    List<FileMetadata> fileMetadata =
        new LinkedList<FileMetadata>();
    for (int i = 0;
        i < objects.getContents().size(); i++) {
      BosObjectSummary object =
          objects.getContents().get(i);
      if (object.getKey() != null
          && !object.getKey().isEmpty()) {
        long lastMod =
            object.getLastModified() != null
                ? object.getLastModified().getTime()
                : 0L;
        String userMeta =
            object.getUserMeta() == null
                ? null
                : object.getUserMeta().get(
                    BaiduBosConstants
                        .BOS_FILE_PATH_USER_KEY);
        String groupMeta =
            object.getUserMeta() == null
                ? null
                : object.getUserMeta().get(
                    BaiduBosConstants
                        .BOS_FILE_PATH_GROUP_KEY);
        fileMetadata.add(new FileMetadata(
            object.getKey(),
            object.getSize(),
            lastMod,
            false,
            userMeta,
            groupMeta,
            null,
            false
        ));
      }
    }

    List<FileMetadata> commonPrefix =
        new LinkedList<FileMetadata>();
    if (objects.getCommonPrefixesWithExtMeta() != null) {
      for (int i = 0;
          i < objects.getCommonPrefixesWithExtMeta()
              .size();
          i++) {
        BosPrefixInfo bosPrefixInfo =
            objects.getCommonPrefixesWithExtMeta()
                .get(i);
        long lastMod =
            bosPrefixInfo.getLastModified() == null
                ? 0L
                : bosPrefixInfo.getLastModified()
                    .getTime();
        String userMeta =
            bosPrefixInfo.getUserMeta() == null
                ? null
                : bosPrefixInfo.getUserMeta().get(
                    BaiduBosConstants
                        .BOS_FILE_PATH_USER_KEY);
        String groupMeta =
            bosPrefixInfo.getUserMeta() == null
                ? null
                : bosPrefixInfo.getUserMeta().get(
                    BaiduBosConstants
                        .BOS_FILE_PATH_GROUP_KEY);
        commonPrefix.add(new FileMetadata(
            bosPrefixInfo.getPrefix(),
            0L,
            lastMod,
            true,
            userMeta,
            groupMeta,
            null,
            false
        ));
      }
    }

    return new PartialListing(
        objects.getNextMarker(),
        fileMetadata.toArray(
            new FileMetadata[fileMetadata.size()]),
        commonPrefix.toArray(
            new FileMetadata[commonPrefix.size()]));
  }

  /**
   * Deletes the directory at the given key.
   *
   * @param key       the directory key
   * @param recursive whether to delete recursively
   * @throws IOException if the deletion fails
   */
  public abstract void deleteDirs(
      String key, boolean recursive) throws IOException;

  /**
   * Deletes the object at the given key.
   *
   * @param key the object key
   * @throws IOException if the deletion fails
   */
  public void delete(String key) throws IOException {
    log.debug(
        "Deleting key: {} from bucket {}",
        key, bucketName);
    bosClientProxy.deleteObject(bucketName, key);
  }

  /**
   * Copies an object from source key to destination key
   * within the same bucket.
   *
   * @param srcKey the source object key
   * @param dstKey the destination object key
   * @throws IOException if the copy fails
   */
  public void copy(String srcKey, String dstKey)
      throws IOException {
    log.debug(
        "Copying srcKey: {} to dstKey: {}"
            + " in bucket: {}",
        srcKey, dstKey, bucketName);
    bosClientProxy.copyObject(
        bucketName, srcKey, bucketName, dstKey);
  }

  /**
   * Renames an object from source key to destination key.
   *
   * @param srcKey the source object key
   * @param dstKey the destination object key
   * @param isFile whether the source key is a file
   * @throws IOException if the rename fails
   */
  public abstract void rename(
      String srcKey, String dstKey,
      boolean isFile) throws IOException;

  /**
   * Returns the BOS client proxy.
   *
   * @return the BOS client proxy
   */
  protected BosClientProxy getBosClientProxy() {
    return bosClientProxy;
  }

  /**
   * Returns the bucket name.
   *
   * @return the bucket name
   */
  protected String getBucketName() {
    return bucketName;
  }

  /**
   * Returns the Hadoop configuration.
   *
   * @return the configuration
   */
  protected Configuration getConf() {
    return conf;
  }

  /**
   * Returns the bounded thread pool.
   *
   * @return the executor service
   */
  protected ExecutorService getBoundedThreadPool() {
    return boundedThreadPool;
  }

  /**
   * Returns the current user name from the environment.
   * Falls back to the system property if UGI lookup fails.
   *
   * @return the user name
   */
  public String getEnvUserName() {
    if (this.envUserName != null) {
      return this.envUserName;
    }
    try {
      this.envUserName =
          UserGroupInformation.getCurrentUser()
              .getShortUserName();
    } catch (IOException ex) {
      log.error(
          "get user fail,fallback to system"
              + " property user.name");
      this.envUserName =
          System.getProperty("user.name");
    }
    return this.envUserName;
  }

  /**
   * Returns the current group name from the environment.
   * Falls back to the user name if group lookup fails.
   *
   * @return the group name
   */
  public String getEnvGroupName() {
    if (this.envGroupName != null) {
      return this.envGroupName;
    }
    try {
      String[] groupNames =
          UserGroupInformation.getCurrentUser()
              .getGroupNames();
      if (groupNames.length != 0) {
        this.envGroupName = groupNames[0];
      } else {
        this.envGroupName = getEnvUserName();
      }
    } catch (IOException e) {
      log.warn(
          "get current user failed!"
              + " use user.name prop {}",
          e.getMessage());
      this.envGroupName =
          System.getProperty("user.name");
    }
    return this.envGroupName;
  }
}
