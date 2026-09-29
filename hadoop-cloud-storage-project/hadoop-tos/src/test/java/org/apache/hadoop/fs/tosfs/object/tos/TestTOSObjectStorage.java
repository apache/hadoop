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

package org.apache.hadoop.fs.tosfs.object.tos;

import com.volcengine.tos.internal.model.CRC64Checksum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.common.Bytes;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.ChecksumType;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.fs.tosfs.object.MultipartUpload;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.fs.tosfs.object.exceptions.NotAppendableException;
import org.apache.hadoop.fs.tosfs.object.request.ListObjectsRequest;
import org.apache.hadoop.fs.tosfs.object.response.ListObjectsResponse;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.apache.hadoop.util.PureJavaCrc32C;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.Checksum;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestTOSObjectStorage {
  public static Stream<Arguments> provideArguments() {
    assumeTrue(TestEnv.checkTestEnabled());

    List<Arguments> values = new ArrayList<>();

    Configuration conf = new Configuration();
    conf.set(TosKeys.FS_TOS_CHECKSUM_TYPE, ChecksumType.CRC64ECMA.name());
    values.add(Arguments.of(
        ObjectStorageFactory.createWithPrefix(String.format("tos-%s/", UUIDUtils.random()),
            TOS_SCHEME, TestUtility.bucket(), conf),
        new CRC64Checksum(),
        ChecksumType.CRC64ECMA));

    conf = new Configuration();
    conf.set(TosKeys.FS_TOS_CHECKSUM_TYPE, ChecksumType.CRC32C.name());
    values.add(Arguments.of(
        ObjectStorageFactory.createWithPrefix(String.format("tos-%s/", UUIDUtils.random()),
            TOS_SCHEME, TestUtility.bucket(), conf),
        new PureJavaCrc32C(),
        ChecksumType.CRC32C));

    return values.stream();
  }

  private ObjectStorage tos;
  private ChecksumType type;

  private void setEnv(ObjectStorage objectStore, ChecksumType csType) {
    this.tos = objectStore;
    this.type = csType;
  }

  @AfterEach
  public void tearDown() throws Exception {
    CommonUtils.runQuietly(() -> tos.deleteAll(""));
    for (MultipartUpload upload : tos.listUploads("")) {
      tos.abortMultipartUpload(upload.key(), upload.uploadId());
    }
    tos.close();
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testHeadObj(ObjectStorage objectStore, Checksum ckmer, ChecksumType csType) {
    setEnv(objectStore, csType);

    String key = "testPutChecksum";
    byte[] data = TestUtility.rand(1024);
    ckmer.update(data, 0, data.length);
    assertEquals(ckmer.getValue(), parseChecksum(objectStore.put(key, data)));

    ObjectInfo objInfo = objectStore.head(key);
    assertEquals(ckmer.getValue(), parseChecksum(objInfo.checksum()));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testGetFileStatus(ObjectStorage objectStore, Checksum ckmer, ChecksumType csType) {
    setEnv(objectStore, csType);
    assumeFalse(objectStore.bucket().isDirectory());

    Configuration conf = new Configuration(objectStore.conf());
    conf.setBoolean(TosKeys.FS_TOS_GET_FILE_STATUS_ENABLED, true);
    objectStore.initialize(conf, objectStore.bucket().name());

    String key = "testFileStatus";
    byte[] data = TestUtility.rand(256);
    byte[] checksum = objectStore.put(key, data);

    ObjectInfo obj1 = objectStore.objectStatus(key);
    assertArrayEquals(checksum, obj1.checksum());
    assertEquals(key, obj1.key());
    assertEquals(obj1, objectStore.head(key));

    ObjectInfo obj2 = objectStore.objectStatus(key + "/");
    assertNull(obj2);

    String dirKey = "testDirStatus/";
    checksum = objectStore.put(dirKey, new byte[0]);

    ObjectInfo obj3 = objectStore.objectStatus("testDirStatus");
    assertArrayEquals(checksum, obj3.checksum());
    assertEquals(dirKey, obj3.key());
    assertEquals(obj3, objectStore.head(dirKey));
    assertNull(objectStore.head("testDirStatus"));
    ObjectInfo obj4 = objectStore.objectStatus(dirKey);
    assertArrayEquals(checksum, obj4.checksum());
    assertEquals(dirKey, obj4.key());
    assertEquals(obj4, objectStore.head(dirKey));

    String prefix = "testPrefix/";
    objectStore.put(prefix + "subfile", data);
    ObjectInfo obj5 = objectStore.objectStatus(prefix);
    assertEquals(prefix, obj5.key());
    assertArrayEquals(Constants.MAGIC_CHECKSUM, obj5.checksum());
    assertNull(objectStore.head(prefix));
    ObjectInfo obj6 = objectStore.objectStatus("testPrefix");
    assertEquals(prefix, obj6.key());
    assertArrayEquals(Constants.MAGIC_CHECKSUM, obj6.checksum());
    assertNull(objectStore.head("testPrefix"));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testObjectStatus(ObjectStorage objectStore, Checksum checksum, ChecksumType csType) {
    setEnv(objectStore, csType);
    assumeFalse(objectStore.bucket().isDirectory());

    String key = "testObjectStatus";
    byte[] data = TestUtility.rand(1024);
    checksum.update(data, 0, data.length);
    assertEquals(checksum.getValue(), parseChecksum(objectStore.put(key, data)));

    ObjectInfo objInfo = objectStore.objectStatus(key);
    assertEquals(checksum.getValue(), parseChecksum(objInfo.checksum()));

    objInfo = objectStore.head(key);
    assertEquals(checksum.getValue(), parseChecksum(objInfo.checksum()));

    String dir = key + "/";
    objectStore.put(dir, new byte[0]);
    objInfo = objectStore.objectStatus(dir);
    assertEquals(Constants.MAGIC_CHECKSUM, objInfo.checksum());

    objInfo = objectStore.head(dir);
    assertEquals(Constants.MAGIC_CHECKSUM, objInfo.checksum());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListObjs(ObjectStorage objectStore, Checksum checksum, ChecksumType csType) {
    setEnv(objectStore, csType);

    String key = "testListObjs";
    byte[] data = TestUtility.rand(1024);
    checksum.update(data, 0, data.length);
    for (int i = 0; i < 5; i++) {
      assertEquals(checksum.getValue(), parseChecksum(objectStore.put(key, data)));
    }

    ListObjectsRequest request =
        ListObjectsRequest.builder().prefix(key).startAfter(null).maxKeys(-1).delimiter("/")
            .build();
    Iterator<ListObjectsResponse> iter = objectStore.list(request).iterator();
    while (iter.hasNext()) {
      List<ObjectInfo> objs = iter.next().objects();
      for (ObjectInfo obj : objs) {
        assertEquals(checksum.getValue(), parseChecksum(obj.checksum()));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testPutChecksum(ObjectStorage objectStore, Checksum checksum, ChecksumType csType) {
    setEnv(objectStore, csType);

    String key = "testPutChecksum";
    byte[] data = TestUtility.rand(1024);
    checksum.update(data, 0, data.length);

    byte[] checksumStr = objectStore.put(key, data);

    assertEquals(checksum.getValue(), parseChecksum(checksumStr));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testMPUChecksum(ObjectStorage objectStore, Checksum checksum, ChecksumType csType) {
    setEnv(objectStore, csType);

    int partNumber = 2;
    String key = "testMPUChecksum";
    MultipartUpload mpu = objectStore.createMultipartUpload(key);
    byte[] data = TestUtility.rand(mpu.minPartSize() * partNumber);
    checksum.update(data, 0, data.length);

    List<Part> parts = new ArrayList<>();
    for (int i = 0; i < partNumber; i++) {
      final int index = i;
      Part part = objectStore.uploadPart(key, mpu.uploadId(), index + 1,
          () -> new ByteArrayInputStream(data, index * mpu.minPartSize(), mpu.minPartSize()),
          mpu.minPartSize());
      parts.add(part);
    }

    byte[] checksumStr = objectStore.completeUpload(key, mpu.uploadId(), parts);
    assertEquals(checksum.getValue(), parseChecksum(checksumStr));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testAppendable(ObjectStorage objectStore, Checksum checksum, ChecksumType csType) {
    setEnv(objectStore, csType);
    assumeFalse(objectStore.bucket().isDirectory());

    // Test create object with append then append.
    byte[] data = TestUtility.rand(256);
    String prefix = "a/testAppendable/";
    String key = prefix + "object.txt";
    objectStore.append(key, data);

    objectStore.append(key, new byte[0]);

    // Test create object with put then append.
    data = TestUtility.rand(256);
    objectStore.put(key, data);

    assertThrows(NotAppendableException.class, () -> objectStore.append(key, new byte[0]),
        "Expect not appendable.");

    objectStore.delete(key);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testDirectoryBucketAppendable(ObjectStorage objectStore, Checksum checksum,
      ChecksumType csType) {
    setEnv(objectStore, csType);
    assumeTrue(objectStore.bucket().isDirectory());

    byte[] data = TestUtility.rand(256);
    String prefix = "a/testAppendable/";
    String key = prefix + "object.txt";
    objectStore.put(key, data);

    objectStore.append(key, new byte[1024]);

    objectStore.delete(key);
  }

  private long parseChecksum(byte[] checksum) {
    switch (type) {
    case CRC32C:
    case CRC64ECMA:
      return Bytes.toLong(checksum);
    default:
      throw new IllegalArgumentException(
          String.format("Checksum type %s is not supported by TOS.", type.name()));
    }
  }
}
