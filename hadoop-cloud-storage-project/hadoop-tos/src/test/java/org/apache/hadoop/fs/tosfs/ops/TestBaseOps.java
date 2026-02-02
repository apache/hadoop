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
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public interface TestBaseOps {

  default Path path(String... keys) {
    return new Path(String.format("/%s", Joiner.on("/").join(keys)));
  }

  default void assertFileExist(Path file) {
    assertNotNull(ObjectUtils.pathToKey(file));
  }

  default void assertFileDoesNotExist(String key) {
    assertNull(storage().head(key));
  }

  default void assertFileDoesNotExist(Path file) {
    assertFileDoesNotExist(ObjectUtils.pathToKey(file));
  }

  default void assertDirExist(String key) {
    assertNotNull(storage().head(key));
  }

  default void assertDirExist(Path path) {
    assertDirExist(ObjectUtils.pathToKey(path, true));
  }

  default void assertDirDoesNotExist(String key) {
    assertNull(storage().head(key));
  }

  default void assertDirDoesNotExist(Path path) {
    assertDirDoesNotExist(ObjectUtils.pathToKey(path, true));
  }

  default void mkdir(Path path) {
    storage().put(ObjectUtils.pathToKey(path, true), new byte[0]);
    assertDirExist(path);
  }

  default ObjectInfo touchFile(Path path, byte[] data) {
    byte[] checksum = storage().put(ObjectUtils.pathToKey(path), data);
    ObjectInfo obj = storage().head(ObjectUtils.pathToKey(path));
    assertArrayEquals(checksum, obj.checksum());
    return obj;
  }

  ObjectStorage storage();
}
