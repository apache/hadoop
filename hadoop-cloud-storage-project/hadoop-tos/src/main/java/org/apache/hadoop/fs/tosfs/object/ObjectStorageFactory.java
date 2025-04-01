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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.tos.TOS;
import org.apache.hadoop.util.Preconditions;

import java.lang.reflect.InvocationTargetException;

import static org.apache.hadoop.fs.tosfs.conf.ConfKeys.FS_OBJECT_STORAGE_IMPL;

public final class ObjectStorageFactory {

  private static final Configuration DEFAULT_IMPLS = new Configuration();

  static {
    // Setup default object storage impl for scheme "tos" and "filestore".
    DEFAULT_IMPLS.set(ConfKeys.FS_OBJECT_STORAGE_IMPL.key("tos"), TOS.class.getName());
    DEFAULT_IMPLS.set(ConfKeys.FS_OBJECT_STORAGE_IMPL.key("filestore"), FileStore.class.getName());
  }

  private ObjectStorageFactory() {
  }

  public static ObjectStorage createWithPrefix(String prefix, String scheme, String bucket,
      Configuration conf) {
    ObjectStorage storage = create(scheme, bucket, conf);
    return new PrefixStorage(storage, prefix);
  }

  public static ObjectStorage create(String scheme, String bucket, Configuration conf) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(scheme), "Scheme is null or empty.");
    Preconditions.checkArgument(StringUtils.isNotEmpty(bucket), "Bucket is null or empty.");
    Preconditions.checkNotNull(conf, "Conf is null.");

    try {
      String confKey = FS_OBJECT_STORAGE_IMPL.key(scheme);
      String impl = conf.get(confKey, DEFAULT_IMPLS.get(confKey));

      Preconditions.checkArgument(StringUtils.isNotEmpty(impl),
          "Cannot locate the ObjectStorage implementation for scheme '%s'", scheme);
      ObjectStorage store =
          (ObjectStorage) Class.forName(impl).getDeclaredConstructor().newInstance();
      store.initialize(conf, bucket);
      return store;
    } catch (ClassNotFoundException |
             InvocationTargetException |
             InstantiationException |
             IllegalAccessException |
             NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }
}
