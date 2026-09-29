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

package org.apache.hadoop.fs.tosfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.conf.FileStoreKeys;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.ChecksumType;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.util.TempFiles;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestTosChecksum {
  private static final String FILE_STORE_ROOT = TempFiles.newTempDir("TestTosChecksum");
  private static final String ALGORITHM_NAME = "mock-algorithm";
  private static final String PREFIX = UUIDUtils.random();

  private ObjectStorage objectStorage;

  @BeforeAll
  public static void beforeClass() {
    assumeTrue(TestEnv.checkTestEnabled());
  }

  public void setObjectStorage(ObjectStorage objectStorage) {
    this.objectStorage = objectStorage;
  }


  static Stream<Arguments> provideArguments() throws URISyntaxException {
    List<Arguments> values = new ArrayList<>();

    // Case 1: file store.
    Configuration fileStoreConf = new Configuration();
    fileStoreConf.set(FileStoreKeys.FS_FILESTORE_CHECKSUM_ALGORITHM, ALGORITHM_NAME);
    fileStoreConf.set(FileStoreKeys.FS_FILESTORE_CHECKSUM_TYPE, ChecksumType.MD5.name());
    fileStoreConf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key("filestore"), FILE_STORE_ROOT);
    URI uri0 = new URI("filestore://" + TestUtility.bucket() + "/");

    values.add(Arguments.of(
        ChecksumType.MD5,
        fileStoreConf,
        uri0,
        ObjectStorageFactory.create(uri0.getScheme(), uri0.getAuthority(), fileStoreConf)
    ));

    // Case 2: tos.
    Configuration tosConf = new Configuration();
    tosConf.set(TosKeys.FS_TOS_CHECKSUM_ALGORITHM, ALGORITHM_NAME);
    tosConf.set(TosKeys.FS_TOS_CHECKSUM_TYPE, ChecksumType.CRC32C.name());
    URI uri1 = new URI(TOS_SCHEME + "://" + TestUtility.bucket() + "/");

    values.add(Arguments.of(
        ChecksumType.CRC32C,
        tosConf,
        uri1,
        ObjectStorageFactory.create(uri1.getScheme(), uri1.getAuthority(), tosConf)
    ));

    return values.stream();
  }

  @AfterEach
  public void tearDown() {
    objectStorage.deleteAll(PREFIX);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testChecksumInfo(ChecksumType type, Configuration conf, URI uri,
      ObjectStorage objectStore) {
    setObjectStorage(objectStore);

    assertEquals(ALGORITHM_NAME, objectStore.checksumInfo().algorithm());
    assertEquals(type, objectStore.checksumInfo().checksumType());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testFileChecksum(ChecksumType type, Configuration conf, URI uri,
      ObjectStorage objectStore) throws Exception {
    setObjectStorage(objectStore);

    try (RawFileSystem fs = new RawFileSystem()) {
      fs.initialize(uri, conf);
      Path file = new Path("/" + PREFIX, "testFileChecksum");
      fs.create(file).close();
      FileChecksum checksum = fs.getFileChecksum(file, Long.MAX_VALUE);
      assertEquals(ALGORITHM_NAME, checksum.getAlgorithmName());

      String key = file.toString().substring(1);
      byte[] checksumData = objectStore.head(key).checksum();
      assertArrayEquals(checksumData, checksum.getBytes());
    }
  }
}
