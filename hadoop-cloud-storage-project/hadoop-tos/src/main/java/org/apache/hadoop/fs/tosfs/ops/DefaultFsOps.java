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
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.tosfs.RawFileStatus;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.object.request.ListObjectsRequest;
import org.apache.hadoop.fs.tosfs.object.response.ListObjectsResponse;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.Iterables;
import org.apache.hadoop.util.Lists;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.hadoop.fs.tosfs.object.ObjectUtils.SLASH;

/**
 * Provides rename, delete, list capabilities for general purpose bucket.
 */
public class DefaultFsOps implements FsOps {
  private final ObjectStorage storage;
  private final ExecutorService taskThreadPool;
  private final Function<ObjectInfo, RawFileStatus> objMapper;
  private final RenameOp renameOp;
  private final boolean asyncCreateParentDir;

  public DefaultFsOps(
      ObjectStorage storage,
      Configuration conf,
      ExecutorService taskThreadPool,
      Function<ObjectInfo, RawFileStatus> objMapper) {
    this.storage = storage;
    this.taskThreadPool = taskThreadPool;
    this.objMapper = objMapper;
    this.renameOp = new RenameOp(conf, storage, taskThreadPool);
    this.asyncCreateParentDir =
        conf.getBoolean(ConfKeys.FS_ASYNC_CREATE_MISSED_PARENT.key(storage.scheme()),
            ConfKeys.FS_ASYNC_CREATE_MISSED_PARENT_DEFAULT);
  }

  @Override
  public void renameFile(Path src, Path dst, long length) {
    renameOp.renameFile(src, dst, length);
    mkdirIfNecessary(src.getParent(), asyncCreateParentDir);
  }

  @Override
  public void renameDir(Path src, Path dst) {
    renameOp.renameDir(src, dst);
    mkdirIfNecessary(src.getParent(), asyncCreateParentDir);
  }

  @Override
  public void deleteFile(Path file) {
    storage.delete(ObjectUtils.pathToKey(file));
    mkdirIfNecessary(file.getParent(), asyncCreateParentDir);
  }

  @Override
  public void deleteDir(Path dir, boolean recursive) throws IOException {
    String dirKey = ObjectUtils.pathToKey(dir, true);
    if (recursive) {
      storage.deleteAll(dirKey);
    } else {
      if (isEmptyDirectory(dir)) {
        storage.delete(dirKey);
      } else {
        throw new PathIsNotEmptyDirectoryException(dir.toString());
      }
    }
  }

  @Override
  public Iterable<RawFileStatus> listDir(Path dir, boolean recursive,
      Predicate<String> postFilter) {
    String key = ObjectUtils.pathToKey(dir, true);
    String delimiter = recursive ? null : SLASH;

    ListObjectsRequest req = ListObjectsRequest.builder()
        .prefix(key)
        .startAfter(key)
        .delimiter(delimiter)
        .build();
    return Iterables.transform(asObjectInfo(storage.list(req), postFilter), objMapper);
  }

  @Override
  public boolean isEmptyDirectory(Path dir) {
    String key = ObjectUtils.pathToKey(dir, true);
    ListObjectsRequest req = ListObjectsRequest.builder()
        .prefix(key)
        .startAfter(key)
        .delimiter(SLASH)
        .maxKeys(1)
        .build();
    return !asObjectInfo(storage.list(req), s -> true).iterator().hasNext();
  }

  @Override
  public void mkdirs(Path dir) {
    if (dir.isRoot()) {
      return;
    }
    String key = ObjectUtils.pathToKey(dir, true);
    storage.put(key, new byte[0]);

    // Create parent dir if missed.
    Path parentPath = dir.getParent();
    String parentKey = ObjectUtils.pathToKey(parentPath, true);
    while (!parentPath.isRoot() && storage.head(parentKey) == null) {
      storage.put(parentKey, new byte[0]);
      parentPath = parentPath.getParent();
      parentKey = ObjectUtils.pathToKey(parentPath, true);
    }
  }

  private void mkdirIfNecessary(Path path, boolean async) {
    if (path != null) {
      CommonUtils.runQuietly(() -> {
        Future<?> future = taskThreadPool.submit(() -> {
          String key = ObjectUtils.pathToKey(path, true);
          if (!key.isEmpty() && storage.head(key) == null) {
            mkdirs(ObjectUtils.keyToPath(key));
          }
        });

        if (!async) {
          future.get();
        }
      });
    }
  }

  /**
   * Convert ListObjectResponse iterable to FileStatus iterable,
   * using file status acceptor to filter the expected objects and common prefixes.
   *
   * @param listResponses the iterable of ListObjectsResponse
   * @param filter        the file status acceptor
   * @return the iterable of TosFileStatus
   */
  private Iterable<ObjectInfo> asObjectInfo(Iterable<ListObjectsResponse> listResponses,
      Predicate<String> filter) {
    Iterable<List<ObjectInfo>> results = Iterables.transform(listResponses, listResp -> {
      List<ObjectInfo> objs = Lists.newArrayList();

      // Add object files.
      objs.addAll(listResp.objects());

      // Add object directories.
      for (String prefix : listResp.commonPrefixes()) {
        objs.add(new ObjectInfo(prefix, 0, new Date(), Constants.MAGIC_CHECKSUM, true));
      }

      return objs;
    });

    return Iterables.filter(Iterables.concat(results), o -> filter.test(o.key()));
  }
}
