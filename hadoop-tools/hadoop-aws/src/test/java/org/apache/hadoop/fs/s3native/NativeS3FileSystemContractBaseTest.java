/**
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

package org.apache.hadoop.fs.s3native;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem.NativeS3FsInputStream;
import org.junit.internal.AssumptionViolatedException;

public abstract class NativeS3FileSystemContractBaseTest
  extends FileSystemContractBaseTest {
  public static final String KEY_TEST_FS = "test.fs.s3n.name";
  private NativeFileSystemStore store;
  
  abstract NativeFileSystemStore getNativeFileSystemStore() throws IOException;

  @Override
  protected void setUp() throws Exception {
    Configuration conf = new Configuration();
    store = getNativeFileSystemStore();
    fs = new NativeS3FileSystem(store);
    String fsname = conf.get(KEY_TEST_FS);
    if (StringUtils.isEmpty(fsname)) {
      throw new AssumptionViolatedException(
          "No test FS defined in :" + KEY_TEST_FS);
    }
    fs.initialize(URI.create(fsname), conf);
  }
  
  @Override
  protected void tearDown() throws Exception {
    store.purge("test");
    super.tearDown();
  }

  public void testCanonicalName() throws Exception {
    assertNull("s3n doesn't support security token and shouldn't have canonical name",
               fs.getCanonicalServiceName());
  }

  public void testListStatusForRoot() throws Exception {
    FileStatus[] paths = fs.listStatus(path("/"));
    assertEquals(0, paths.length);
    
    Path testDir = path("/test");
    assertTrue(fs.mkdirs(testDir));
    
    paths = fs.listStatus(path("/"));
    assertEquals(1, paths.length);
    assertEquals(path("/test"), paths[0].getPath());
  }

  public void testNoTrailingBackslashOnBucket() throws Exception {
    assertTrue(fs.getFileStatus(new Path(fs.getUri().toString())).isDirectory());
  }

  private void createTestFiles(String base) throws IOException {
    store.storeEmptyFile(base + "/file1");
    store.storeEmptyFile(base + "/dir/file2");
    store.storeEmptyFile(base + "/dir/file3");
  }

  public void testDirWithDifferentMarkersWorks() throws Exception {

    for (int i = 0; i < 3; i++) {
      String base = "test/hadoop" + i;
      Path path = path("/" + base);

      createTestFiles(base);

      if (i == 0 ) {
        //do nothing, we are testing correctness with no markers
      }
      else if (i == 1) {
        // test for _$folder$ marker
        store.storeEmptyFile(base + "_$folder$");
        store.storeEmptyFile(base + "/dir_$folder$");
      }
      else if (i == 2) {
        // test the end slash file marker
        store.storeEmptyFile(base + "/");
        store.storeEmptyFile(base + "/dir/");
      }
      else if (i == 3) {
        // test both markers
        store.storeEmptyFile(base + "_$folder$");
        store.storeEmptyFile(base + "/dir_$folder$");
        store.storeEmptyFile(base + "/");
        store.storeEmptyFile(base + "/dir/");
      }

      assertTrue(fs.getFileStatus(path).isDirectory());
      assertEquals(2, fs.listStatus(path).length);
    }
  }

  public void testDeleteWithNoMarker() throws Exception {
    String base = "test/hadoop";
    Path path = path("/" + base);

    createTestFiles(base);

    fs.delete(path, true);

    path = path("/test");
    assertTrue(fs.getFileStatus(path).isDirectory());
    assertEquals(0, fs.listStatus(path).length);
  }

  public void testRenameWithNoMarker() throws Exception {
    String base = "test/hadoop";
    Path dest = path("/test/hadoop2");

    createTestFiles(base);

    fs.rename(path("/" + base), dest);

    Path path = path("/test");
    assertTrue(fs.getFileStatus(path).isDirectory());
    assertEquals(1, fs.listStatus(path).length);
    assertTrue(fs.getFileStatus(dest).isDirectory());
    assertEquals(2, fs.listStatus(dest).length);
  }

  public void testEmptyFile() throws Exception {
    store.storeEmptyFile("test/hadoop/file1");
    fs.open(path("/test/hadoop/file1")).close();
  }
  
  public void testBlockSize() throws Exception {
    Path file = path("/test/hadoop/file");
    createFile(file);
    assertEquals("Default block size", fs.getDefaultBlockSize(file),
    fs.getFileStatus(file).getBlockSize());

    // Block size is determined at read time
    long newBlockSize = fs.getDefaultBlockSize(file) * 2;
    fs.getConf().setLong("fs.s3n.block.size", newBlockSize);
    assertEquals("Double default block size", newBlockSize,
    fs.getFileStatus(file).getBlockSize());
  }
  
  public void testRetryOnIoException() throws Exception {
    class TestInputStream extends InputStream {
      boolean shouldThrow = true;
      int throwCount = 0;
      int pos = 0;
      byte[] bytes;
      boolean threwException = false;
      
      public TestInputStream() {
        bytes = new byte[256];
        for (int i = pos; i < 256; i++) {
          bytes[i] = (byte)i;
        }
      }
      
      @Override
      public int read() throws IOException {
        shouldThrow = !shouldThrow;
        if (shouldThrow) {
          throwCount++;
          threwException = true;
          throw new IOException();
        }
        assertFalse("IOException was thrown. InputStream should be reopened", threwException);
        return pos++;
      }
      
      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        shouldThrow = !shouldThrow;
        if (shouldThrow) {
          throwCount++;
          threwException = true;
          throw new IOException();
        }
        assertFalse("IOException was thrown. InputStream should be reopened", threwException);
        int sizeToRead = Math.min(len, 256 - pos);
        for (int i = 0; i < sizeToRead; i++) {
          b[i] = bytes[pos + i];
        }
        pos += sizeToRead;
        return sizeToRead;
      }

      public void reopenAt(long byteRangeStart) {
        threwException = false;
        pos = Long.valueOf(byteRangeStart).intValue();
      }

    }
    
    final TestInputStream is = new TestInputStream();
    
    class MockNativeFileSystemStore extends Jets3tNativeFileSystemStore {
      @Override
      public InputStream retrieve(String key, long byteRangeStart) throws IOException {
        is.reopenAt(byteRangeStart);
        return is;
      }
    }
    
    NativeS3FsInputStream stream = new NativeS3FsInputStream(new MockNativeFileSystemStore(), null, is, "");
    
    // Test reading methods.
    byte[] result = new byte[256];
    for (int i = 0; i < 128; i++) {
      result[i] = (byte)stream.read();
    }
    for (int i = 128; i < 256; i += 8) {
      byte[] temp = new byte[8];
      int read = stream.read(temp, 0, 8);
      assertEquals(8, read);
      System.arraycopy(temp, 0, result, i, 8);
    }
    
    // Assert correct
    for (int i = 0; i < 256; i++) {
      assertEquals((byte)i, result[i]);
    }
    
    // Test to make sure the throw path was exercised.
    // every read should have thrown 1 IOException except for the first read
    // 144 = 128 - 1 + (128 / 8)
    assertEquals(143, ((TestInputStream)is).throwCount);
  }

}
