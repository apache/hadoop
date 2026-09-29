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
import org.apache.hadoop.fs.tosfs.RawFileStatus;
import org.apache.hadoop.fs.tosfs.RawFileSystem;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.object.DirectoryStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// TODO change to directory bucket configuration.
public class TestDirectoryFsOps extends TestBaseFsOps {

  public static Stream<Arguments> provideArguments() {
    List<Arguments> values = new ArrayList<>();

    ObjectStorage storage =
        ObjectStorageFactory.createWithPrefix(String.format("tos-%s/", UUIDUtils.random()),
            TOS_SCHEME, TestUtility.bucket(), new Configuration());
    values.add(Arguments.of(storage, new DirectoryFsOps((DirectoryStorage) storage, obj -> {
      long modifiedTime = RawFileSystem.dateToLong(obj.mtime());
      String path =
          String.format("%s://%s/%s", storage.scheme(), storage.bucket().name(), obj.key());
      return new RawFileStatus(obj.size(), obj.isDir(), 0, modifiedTime, new Path(path), "fake",
          obj.checksum());
    })));

    return values.stream();
  }

  @BeforeAll
  public static void beforeClass() {
    assumeTrue(TestEnv.checkTestEnabled());
  }

  @Override
  public void testRenameDir(ObjectStorage store, FsOps fsOps) {
    // Will remove this test case once test environment support
    assumeTrue(store.bucket().isDirectory());
  }

  @Override
  public void testRenameFile(ObjectStorage store, FsOps fsOps) {
    // Will remove this test case once test environment support
    assumeTrue(store.bucket().isDirectory());
  }

  @Override
  public void testCreateDirRecursive(ObjectStorage store, FsOps fsOps) {
    // Will remove this test case once test environment support
    assumeTrue(store.bucket().isDirectory());
  }
}
