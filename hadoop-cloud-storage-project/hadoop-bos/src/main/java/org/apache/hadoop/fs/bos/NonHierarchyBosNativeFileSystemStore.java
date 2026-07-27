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

import com.baidubce.services.bos.model.CompleteMultipartUploadRequest;
import com.baidubce.services.bos.model.DeleteMultipleObjectsRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadResponse;
import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.services.bos.model.PartETag;
import com.baidubce.services.bos.model.UploadPartCopyRequest;
import com.baidubce.services.bos.model.UploadPartCopyResponse;
import org.apache.hadoop.conf.Configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Store implementation for non-hierarchy (flat) BOS buckets.
 * Directories are simulated using key prefixes and delimiter
 * conventions.
 */
public class NonHierarchyBosNativeFileSystemStore
    extends BosNativeFileSystemStore {

  private final int multipartBlockSize;
  private final long copyLargeFileThreshold;

  /**
   * Constructs a store for a non-hierarchy bucket.
   *
   * @param bosClientProxy the BOS client proxy
   * @param bucketName     the name of the bucket
   * @param conf           the Hadoop configuration
   */
  public NonHierarchyBosNativeFileSystemStore(
      BosClientProxy bosClientProxy,
      String bucketName, Configuration conf) {
    super(bosClientProxy, bucketName, conf);
    this.multipartBlockSize = conf.getInt(
        BaiduBosConstants.BOS_MULTI_UPLOAD_BLOCK_SIZE,
        BaiduBosConstants
            .BOS_MULTI_UPLOAD_BLOCK_SIZE_DEFAULT);
    this.copyLargeFileThreshold = conf.getLong(
        BaiduBosConstants
            .BOS_COPY_LARGE_FILE_THRESHOLD,
        BaiduBosConstants
            .BOS_COPY_LARGE_FILE_THRESHOLD_DEFAULT);
  }

  /**
   * {@inheritDoc}
   *
   * Checks whether the key is a directory by looking for
   * the directory marker or listing sub-objects.
   */
  @Override
  public boolean isDirectory(String key)
      throws IOException {
    try {
      key = prefixToDir(key);
      bosClientProxy.getObjectMetadata(bucketName, key);
      return true;
    } catch (FileNotFoundException ex) {
      PartialListing listing =
          list(key, 1, null, null);
      if (listing != null) {
        if (listing.getFiles().length > 0) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * Retrieves metadata for the given key. If the key is not
   * found as-is, tries appending a trailing delimiter. If
   * sub-objects exist, rebuilds the directory marker.
   */
  @Override
  public FileMetadata retrieveMetadata(String key)
      throws IOException {
    boolean isFolder = false;
    try {
      ObjectMetadata meta =
          bosClientProxy.getObjectMetadata(
              bucketName, key);
      if (key.endsWith(
          BaiduBosFileSystem.PATH_DELIMITER)) {
        isFolder = true;
      }
      long lastMod =
          meta.getLastModified() != null
              ? meta.getLastModified().getTime() : 0L;
      return new FileMetadata(
          key, meta.getContentLength(),
          lastMod, isFolder, meta);
    } catch (FileNotFoundException e) {
      try {
        key = prefixToDir(key);
        ObjectMetadata meta =
            bosClientProxy.getObjectMetadata(
                bucketName, key);
        isFolder = true;
        long lastMod =
            meta.getLastModified() != null
                ? meta.getLastModified().getTime()
                : 0L;
        return new FileMetadata(
            key, meta.getContentLength(),
            lastMod, isFolder, meta);
      } catch (FileNotFoundException ex) {
        PartialListing listing =
            list(key, 1, null, null);
        if (listing != null) {
          if (listing.getFiles().length > 0) {
            // rebuild dir marker with trailing slash to ensure it is
            // treated as a directory, not a zero-byte file
            String dirKey = prefixToDir(key);
            this.storeEmptyFile(dirKey);
            return new FileMetadata(
                dirKey, 0, 0, true);
          }
        }
        throw new FileNotFoundException(
            key + ": No such file or directory.");
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  protected boolean isHierarchy() {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * Deletes a directory and optionally its contents by
   * listing and batch-deleting objects with the given prefix.
   */
  @Override
  public void deleteDirs(
      String key, boolean recursive) throws IOException {
    log.debug(
        "Deleting dirs key:{}, bucket: {},"
            + " recursive:{}",
        key, bucketName, recursive);
    String listKey = key;
    ArrayList<String> keys = new ArrayList<String>();
    String priorLastKey = null;

    do {
      keys.clear();
      PartialListing listing =
          list(listKey, 1000, priorLastKey, null);
      if (listing != null) {
        DeleteMultipleObjectsRequest deleteRequest =
            (new DeleteMultipleObjectsRequest())
                .withBucketName(bucketName);
        for (FileMetadata fileMetadata
            : listing.getFiles()) {
          keys.add(fileMetadata.getKey());
        }
        if (!recursive
            && (keys.size() >= 2
                || (keys.size() == 1
                    && !keys.get(0).equals(
                        prefixToDir(key))))) {
          throw new IOException(
              key + " is a directory.");
        }
        if (!keys.isEmpty()) {
          deleteRequest.setObjectKeys(keys);
          bosClientProxy.deleteMultipleObjects(
              deleteRequest);
        }
        priorLastKey = listing.getPriorLastKey();
      }
    } while (priorLastKey != null
        && !priorLastKey.isEmpty());
  }

  /**
   * {@inheritDoc}
   *
   * Renames an object. For files, delegates to
   * {@code renameObject}. For directories, collects rename
   * tasks and executes them in parallel.
   */
  @Override
  public void rename(
      String srcKey, String dstKey,
      final boolean isFile) throws IOException {
    log.debug(
        "Renaming srcKey: {} to dstKey: {}"
            + " in bucket: {}",
        srcKey, dstKey, bucketName);
    try {
      if (isFile) {
        bosClientProxy.renameObject(
            bucketName, srcKey, dstKey);
      } else {
        this.storeEmptyFile(
            this.prefixToDir(srcKey));
        List<RenameTask> tasks =
            collectRenameTasks(srcKey, dstKey);
        runRenameTask(tasks);
      }
    } catch (FileNotFoundException e) {
      log.debug(
          "Renaming srcKey: {} to dstKey: {}"
              + " in bucket: {} failed,"
              + " FileNotFoundException",
          srcKey, dstKey, bucketName);
      throw e;
    }
  }

  /**
   * Collects rename tasks for all objects under the source
   * directory prefix.
   *
   * @param src the source directory key
   * @param des the destination directory key
   * @return the list of rename tasks
   * @throws IOException if listing fails
   */
  private List<RenameTask> collectRenameTasks(
      String src, String des) throws IOException {
    List<RenameTask> tasks = new ArrayList<>();
    String priorLastKey = null;
    String srcKey = this.prefixToDir(src);
    String dstKey = this.prefixToDir(des);
    do {
      PartialListing listing = this.list(
          prefixToDir(srcKey),
          1000, priorLastKey, null);
      for (FileMetadata file : listing.getFiles()) {
        String newKey = dstKey
            + file.getKey().substring(
                srcKey.length());
        tasks.add(new RenameTask(
            this, file.getKey(), newKey));
      }
      priorLastKey = listing.getPriorLastKey();
    } while (priorLastKey != null);
    return tasks;
  }

  /**
   * Runs rename tasks in parallel using the bounded thread
   * pool.
   *
   * @param tasks the list of rename tasks
   * @throws IOException if any rename operation fails
   */
  private void runRenameTask(List<RenameTask> tasks)
      throws IOException {
    List<Future<Void>> futures = new ArrayList<>();
    // Submit rename tasks for parallel execution
    Future<Void> curFuture = null;
    try {
      for (RenameTask task : tasks) {
        futures.add(
            this.boundedThreadPool.submit(task));
      }

      for (Future<Void> future : futures) {
        curFuture = future;
        curFuture.get();
      }
    } catch (InterruptedException
        | ExecutionException e) {
      for (Future<Void> f : futures) {
        f.cancel(true);
      }
      throw new IOException(e);
    }
  }

  /**
   * Copies a large file using multipart copy.
   *
   * @param srcKey   the source object key
   * @param dstKey   the destination object key
   * @param fileSize the size of the file in bytes
   * @throws IOException if the copy fails
   */
  private void copyLargeFile(
      String srcKey, String dstKey, long fileSize)
      throws IOException {
    String copyUploadId =
        initMultipartUpload(dstKey);
    long leftSize = fileSize;
    long skipBytes = 0;
    int partNumber = 1;
    List<PartETag> partETags =
        new ArrayList<PartETag>();
    long partSize = multipartBlockSize;

    while (leftSize > 0) {
      if (leftSize < partSize) {
        partSize = leftSize;
      }
      UploadPartCopyRequest copyRequest =
          new UploadPartCopyRequest();
      copyRequest.setBucketName(bucketName);
      copyRequest.setKey(dstKey);
      copyRequest.setSourceBucketName(bucketName);
      copyRequest.setSourceKey(srcKey);
      copyRequest.setUploadId(copyUploadId);
      copyRequest.setPartSize(partSize);
      copyRequest.setOffSet(skipBytes);
      copyRequest.setPartNumber(partNumber);
      UploadPartCopyResponse copyResponse =
          bosClientProxy.uploadPartCopy(copyRequest);
      // Save the returned PartETag to the list
      PartETag partETag = new PartETag(
          partNumber, copyResponse.getETag());
      partETags.add(partETag);
      leftSize -= partSize;
      skipBytes += partSize;
      partNumber += 1;
    }

    completeMultipartUpload(
        bucketName, dstKey, copyUploadId, partETags);
  }

  /**
   * Initiates a multipart upload for the given key.
   *
   * @param key the object key
   * @return the upload ID
   * @throws IOException if the initiation fails
   */
  private String initMultipartUpload(String key)
      throws IOException {
    InitiateMultipartUploadRequest request =
        new InitiateMultipartUploadRequest(
            bucketName, key);
    InitiateMultipartUploadResponse result =
        bosClientProxy.initiateMultipartUpload(
            request);
    return result.getUploadId();
  }

  /**
   * Completes a multipart upload by assembling the parts.
   *
   * @param bucket   the bucket name
   * @param objectKey the object key
   * @param mpUploadId the upload ID
   * @param partETags the list of part ETags
   * @throws IOException if the completion fails
   */
  private void completeMultipartUpload(
      String bucket, String objectKey,
      String mpUploadId, List<PartETag> partETags)
      throws IOException {
    Collections.sort(partETags,
        new Comparator<PartETag>() {
          public int compare(
              PartETag arg1, PartETag arg2) {
            PartETag part1 = arg1;
            PartETag part2 = arg2;
            return part1.getPartNumber()
                - part2.getPartNumber();
          }
        });
    CompleteMultipartUploadRequest request =
        new CompleteMultipartUploadRequest(
            bucket, objectKey, mpUploadId, partETags);
    bosClientProxy.completeMultipartUpload(request);
  }

  /**
   * A callable task that renames a single object in BOS.
   */
  private static class RenameTask
      implements Callable<Void> {

    private final String srcKey;
    private final String dstKey;
    private final BosNativeFileSystemStore store;

    /**
     * Constructs a RenameTask.
     *
     * @param store  the file system store
     * @param srcKey the source object key
     * @param dstKey the destination object key
     */
    RenameTask(
        BosNativeFileSystemStore store,
        String srcKey, String dstKey) {
      this.store = store;
      this.srcKey = srcKey;
      this.dstKey = dstKey;
    }

    /**
     * Executes the rename operation.
     *
     * @return null
     * @throws IOException if the rename fails
     */
    public Void call() throws IOException {
      store.rename(srcKey, dstKey, true);
      return null;
    }

    /**
     * Returns the source key.
     *
     * @return the source key
     */
    String getSrcKey() {
      return srcKey;
    }

    /**
     * Returns the destination key.
     *
     * @return the destination key
     */
    String getDstKey() {
      return dstKey;
    }
  }
}
