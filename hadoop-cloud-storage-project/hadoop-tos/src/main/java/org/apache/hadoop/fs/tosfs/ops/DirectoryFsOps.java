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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.tosfs.RawFileStatus;
import org.apache.hadoop.fs.tosfs.object.DirectoryStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.util.Iterables;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Provides rename, delete, list capabilities for directory bucket.
 */
public class DirectoryFsOps implements FsOps {
  private final DirectoryStorage storage;
  private final Function<ObjectInfo, RawFileStatus> objMapper;

  public DirectoryFsOps(DirectoryStorage storage, Function<ObjectInfo, RawFileStatus> objMapper) {
    this.storage = storage;
    this.objMapper = objMapper;
  }

  @Override
  public void renameFile(Path src, Path dst, long length) {
    innerRename(src, dst, false);
  }

  @Override
  public void renameDir(Path src, Path dst) {
    innerRename(src, dst, true);
  }

  private void innerRename(Path src, Path dst, boolean isDir) {
    // Need to ensure the dest parent exist before rename file in directory bucket.
    String dstParentKey = ObjectUtils.pathToKey(dst.getParent(), true);
    if (!dstParentKey.isEmpty() && storage.head(dstParentKey) == null) {
      mkdirs(dst.getParent());
    }

    String srcKey = ObjectUtils.pathToKey(src, isDir);
    String dstKey = ObjectUtils.pathToKey(dst, isDir);
    storage.rename(srcKey, dstKey);
  }

  @Override
  public void deleteFile(Path file) {
    storage.delete(ObjectUtils.pathToKey(file));
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
    Iterable<ObjectInfo> objs =
        Iterables.filter(storage.listDir(key, recursive), obj -> postFilter.test(obj.key()));
    return Iterables.transform(objs, objMapper);
  }

  @Override
  public boolean isEmptyDirectory(Path dir) {
    return storage.isEmptyDir(ObjectUtils.pathToKey(dir, true));
  }

  @Override
  public void mkdirs(Path dir) {
    if (dir.isRoot()) {
      return;
    }
    String key = ObjectUtils.pathToKey(dir, true);
    // Directory bucket will create missed parent dirs automatically.
    storage.put(key, new byte[0]);
  }
}
