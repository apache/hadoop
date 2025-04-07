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

package org.apache.hadoop.fs.tosfs.object;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.util.Range;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;

import java.util.List;

public final class ObjectUtils {
  public static final String SLASH = "/";

  private ObjectUtils() {
  }

  public static Path keyToPath(String key) {
    return new Path(SLASH + key);
  }

  public static String path(String key) {
    return key.startsWith(SLASH) ? key : SLASH + key;
  }

  public static String pathToKey(Path p) {
    return pathToKey(p, false);
  }

  public static String pathToKey(Path p, Boolean isDir) {
    Preconditions.checkArgument(p != null, "Null path");
    if (p.toUri().getScheme() != null && p.toUri().getPath().isEmpty()) {
      return "";
    }
    String key = p.toUri().getPath().substring(1);
    if (isDir && !key.isEmpty()) {
      return key.endsWith(SLASH) ? key : key + SLASH;
    }
    return key;
  }

  public static void deleteAllObjects(ObjectStorage storage, Iterable<ObjectInfo> objects,
      int batchSize) {
    List<String> keysToDelete = Lists.newArrayList();
    for (ObjectInfo obj : objects) {
      keysToDelete.add(obj.key());

      if (keysToDelete.size() == batchSize) {
        batchDelete(storage, keysToDelete);
        keysToDelete.clear();
      }
    }

    if (!keysToDelete.isEmpty()) {
      batchDelete(storage, keysToDelete);
    }
  }

  private static void batchDelete(ObjectStorage storage, List<String> keys) {
    List<String> failedKeys = storage.batchDelete(keys);
    if (!failedKeys.isEmpty()) {
      throw new RuntimeException(String.format("Failed to delete %s objects, detail: %s",
          failedKeys.size(), Joiner.on(",").join(failedKeys)));
    }
  }

  public static Range calculateRange(final long offset, final long limit, final long objSize) {
    Preconditions.checkArgument(offset >= 0,
        String.format("offset is a negative number: %s", offset));
    Preconditions.checkArgument(offset <= objSize,
        String.format("offset: %s is bigger than object size: %s", offset, objSize));
    long len = limit < 0 ? objSize - offset : Math.min(objSize - offset, limit);
    return Range.of(offset, len);
  }
}
