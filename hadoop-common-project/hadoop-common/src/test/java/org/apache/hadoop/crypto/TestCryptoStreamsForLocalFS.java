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
package org.apache.hadoop.crypto;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestCryptoStreamsForLocalFS extends CryptoStreamsTestBase {
  private static final String TEST_ROOT_DIR =
      GenericTestUtils.getTempPath("work-dir/testcryptostreamsforlocalfs");

  private final File base = new File(TEST_ROOT_DIR);
  private final Path file = new Path(TEST_ROOT_DIR, "test-file");
  private static LocalFileSystem fileSys;
  
  @BeforeAll
  public static void init() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("fs.file.impl", LocalFileSystem.class.getName());
    fileSys = FileSystem.getLocal(conf);
    codec = CryptoCodec.getInstance(conf);
  }
  
  @AfterAll
  public static void shutdown() throws Exception {
  }
  
  @BeforeEach
  @Override
  public void setUp() throws IOException {
    fileSys.delete(new Path(TEST_ROOT_DIR), true);
    super.setUp();
  }
  
  @AfterEach
  public void cleanUp() throws IOException {
    FileUtil.setWritable(base, true);
    FileUtil.fullyDelete(base);
    assertTrue(!base.exists());
  }
  
  @Override
  protected OutputStream getOutputStream(int bufferSize, byte[] key, byte[] iv) 
      throws IOException {
    return new CryptoOutputStream(fileSys.create(file), codec, bufferSize, 
        key, iv);
  }
  
  @Override
  protected InputStream getInputStream(int bufferSize, byte[] key, byte[] iv) 
      throws IOException {
    return new CryptoInputStream(fileSys.open(file), codec, bufferSize, 
        key, iv);
  }
  
  @Disabled("ChecksumFSInputChecker doesn't support ByteBuffer read")
  @Override
  @Test
  @Timeout(value = 10)
  public void testByteBufferRead() throws Exception {}

  @Disabled("Wrapped stream doesn't support ByteBufferPositionedReadable")
  @Override
  @Test
  @Timeout(value = 10)
  public void testPositionedReadWithByteBuffer() throws IOException {}

  @Disabled("Wrapped stream doesn't support ByteBufferPositionedReadable")
  @Override
  @Test
  @Timeout(value = 10)
  public void testByteBufferReadFully() throws Exception {}
  
  @Disabled("ChecksumFSOutputSummer doesn't support Syncable")
  @Override
  @Test
  @Timeout(value = 10)
  public void testSyncable() throws IOException {}

  @Disabled("Wrapped stream doesn't support ByteBufferPositionedReadable")
  @Override
  @Test
  @Timeout(value = 10)
  public void testByteBufferPread() throws IOException {}
  
  @Disabled("ChecksumFSInputChecker doesn't support ByteBuffer read")
  @Override
  @Test
  @Timeout(value = 10)
  public void testCombinedOp() throws Exception {}
  
  @Disabled("ChecksumFSInputChecker doesn't support enhanced ByteBuffer access")
  @Override
  @Test
  @Timeout(value = 10)
  public void testHasEnhancedByteBufferAccess() throws Exception {
  }
  
  @Disabled("ChecksumFSInputChecker doesn't support seekToNewSource")
  @Override
  @Test
  @Timeout(value = 10)
  public void testSeekToNewSource() throws Exception {
  }

  @Disabled("Local file input stream does not support unbuffer")
  @Override
  @Test
  public void testUnbuffer() throws Exception {}
}
