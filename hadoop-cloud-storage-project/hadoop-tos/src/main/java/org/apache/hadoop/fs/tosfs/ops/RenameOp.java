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

package org.apache.hadoop.fs.tosfs.ops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.common.Tasks;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.MultipartUpload;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RenameOp {
  private static final Logger LOG = LoggerFactory.getLogger(RenameOp.class);
  private static final int RENAME_RETRY_TIMES = 3;

  private final Configuration conf;
  private final ObjectStorage storage;
  private final ExecutorService renamePool;
  // Whether enable object storage atomic rename object capability.
  private final boolean renameObjectEnabled;

  public RenameOp(Configuration conf, ObjectStorage storage, ExecutorService taskThreadPool) {
    this.conf = conf;
    this.storage = storage;
    this.renamePool = taskThreadPool;
    this.renameObjectEnabled =
        conf.getBoolean(ConfKeys.FS_OBJECT_RENAME_ENABLED.key(storage.scheme()),
            ConfKeys.FS_OBJECT_RENAME_ENABLED_DEFAULT);
  }

  public void renameDir(Path src, Path dst) {
    String srcKey = ObjectUtils.pathToKey(src, true);
    String dstKey = ObjectUtils.pathToKey(dst, true);
    renameDir(srcKey, dstKey);
  }

  public void renameFile(Path src, Path dst, long length) {
    String srcKey = ObjectUtils.pathToKey(src, false);
    String dstKey = ObjectUtils.pathToKey(dst, false);
    renameFile(srcKey, dstKey, length);
  }

  /**
   * Renames each object after listing all objects with given src key via renaming semantic if
   * object storage supports atomic rename semantic, otherwise renaming all objects via
   * copy & delete.
   *
   * @param srcKey the source dir key, ending with slash.
   * @param dstKey the destination parent dir key, ending with slash.
   */
  private void renameDir(String srcKey, String dstKey) {
    Iterable<ObjectInfo> objs = storage.listAll(srcKey, "");
    if (renameObjectEnabled) {
      Tasks.foreach(objs)
          .executeWith(renamePool)
          .throwFailureWhenFinished()
          .retry(RENAME_RETRY_TIMES)
          .revertWith(sourceInfo -> {
            String newDstKey = dstKey + sourceInfo.key().substring(srcKey.length());
            String newSrcKey = sourceInfo.key();
            LOG.debug("Try to rollback dest key {} to source key {}", newDstKey, newSrcKey);

            storage.rename(newDstKey, newSrcKey);
          })
          .run(sourceInfo -> {
            String newDstKey = dstKey + sourceInfo.key().substring(srcKey.length());
            String newSrcKey = sourceInfo.key();
            LOG.debug("Try to rename src key {} to dest key {}", newSrcKey, newDstKey);

            storage.rename(newSrcKey, newDstKey);
          });
    } else {
      Tasks.foreach(objs)
          .executeWith(renamePool)
          .throwFailureWhenFinished()
          .retry(RENAME_RETRY_TIMES)
          .revertWith(sourceInfo -> {
            String newDstKey = dstKey + sourceInfo.key().substring(srcKey.length());
            storage.delete(newDstKey);
          })
          .run(sourceInfo -> {
            String newDstKey = dstKey + sourceInfo.key().substring(srcKey.length());
            LOG.debug("Try to rename src key {} to dest key {}", sourceInfo.key(), newDstKey);

            try {
              if (ObjectInfo.isDir(newDstKey)) {
                mkdir(newDstKey);
              } else {
                copyFile(sourceInfo.key(), newDstKey, sourceInfo.size());
              }
            } catch (IOException e) {
              throw new UncheckedIOException(
                  String.format("Failed to copy source file %s to dest file %s", sourceInfo.key(),
                      newDstKey), e);
            }
          });

      // Delete all the source keys, since we've already copied them into destination keys.
      storage.deleteAll(srcKey);
    }
  }

  private void renameFile(String srcKey, String dstKey, long fileSize) {
    if (renameObjectEnabled) {
      storage.rename(srcKey, dstKey);
    } else {
      Tasks.foreach(0)
          .throwFailureWhenFinished()
          .retry(RENAME_RETRY_TIMES)
          .revertWith(obj -> storage.delete(dstKey))
          .run(obj -> {
            try {
              copyFile(srcKey, dstKey, fileSize);
            } catch (IOException e) {
              throw new UncheckedIOException(
                  String.format("Failed to copy source file %s to dest file %s", srcKey, dstKey),
                  e);
            }
          });

      Tasks.foreach(0)
          .throwFailureWhenFinished()
          .retry(RENAME_RETRY_TIMES)
          .run(obj -> storage.delete(srcKey));
    }
  }

  private void copyFile(String srcKey, String dstKey, long srcSize) throws IOException {
    long byteSizePerPart = conf.getLong(ConfKeys.FS_MULTIPART_SIZE.key(storage.scheme()),
        ConfKeys.FS_MULTIPART_SIZE_DEFAULT);
    long multiPartCopyThreshold =
        conf.getLong(ConfKeys.FS_MULTIPART_COPY_THRESHOLD.key(storage.scheme()),
            ConfKeys.FS_MULTIPART_COPY_THRESHOLD_DEFAULT);
    if (srcSize > multiPartCopyThreshold) {
      uploadPartCopy(srcKey, srcSize, dstKey, byteSizePerPart);
    } else {
      storage.copy(srcKey, dstKey);
    }
  }

  private void uploadPartCopy(String srcKey, long srcSize, String dstKey, long byteSizePerPart) {
    final MultipartUpload multipartUpload = storage.createMultipartUpload(dstKey);
    try {
      Preconditions.checkState(byteSizePerPart >= multipartUpload.minPartSize(),
          "Configured upload part size %s must be greater than or equals to the minimal part"
              + " size %s, please check configure key %s.", byteSizePerPart,
          multipartUpload.minPartSize(), ConfKeys.FS_MULTIPART_SIZE.key(storage.scheme()));

      AtomicInteger partNumGetter = new AtomicInteger(0);
      List<CompletableFuture<Part>> results = Lists.newArrayList();
      for (long start = 0, end; start < srcSize; start += byteSizePerPart) {
        end = Math.min(start + byteSizePerPart, srcSize) - 1;
        Preconditions.checkArgument(end >= 0, "Invalid copy range start: %s, end: %s", start, end);
        // Submit upload part copy task to the thread pool.
        CompletableFuture<Part> result = asyncUploadPartCopy(srcKey, multipartUpload,
            partNumGetter.incrementAndGet(), start, end);
        results.add(result);
      }

      // Waiting for all the upload parts to be finished.
      List<Part> parts = results.stream()
          .map(CompletableFuture::join)
          .sorted(Comparator.comparing(Part::num))
          .collect(Collectors.toList());

      finishUpload(multipartUpload.key(), multipartUpload.uploadId(), parts);
    } catch (Exception e) {
      LOG.error("Encountering error when upload part copy", e);
      CommonUtils.runQuietly(
          () -> storage.abortMultipartUpload(multipartUpload.key(), multipartUpload.uploadId()));
      throw e;
    }
  }

  protected void finishUpload(String key, String uploadId, List<Part> uploadParts) {
    storage.completeUpload(key, uploadId, uploadParts);
  }

  private CompletableFuture<Part> asyncUploadPartCopy(
      String srcKey, MultipartUpload multipartUpload, int partNum,
      long copyRangeStart, long copyRangeEnd) {
    return CompletableFuture.supplyAsync(
        () -> storage.uploadPartCopy(srcKey, multipartUpload.key(), multipartUpload.uploadId(),
            partNum, copyRangeStart, copyRangeEnd), renamePool).whenComplete((part, err) -> {
      if (err != null) {
        LOG.error("Failed to upload part copy, src key: {}, multipartUpload: {}, partNum: {},"
                + " copy range start: {}, copy range end: {}", srcKey, multipartUpload, partNum,
            copyRangeStart, copyRangeEnd, err);
      }
    });
  }

  private void mkdir(String key) {
    storage.put(key, new byte[0]);
  }
}
