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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestCryptoStreamsForLocalFS extends CryptoStreamsTestBase {
  private static final String TEST_ROOT_DIR =
      GenericTestUtils.getTempPath("work-dir/testcryptostreamsforlocalfs");

  private final File base = new File(TEST_ROOT_DIR);
  private final Path file = new Path(TEST_ROOT_DIR, "test-file");
  private static LocalFileSystem fileSys;
  
  @BeforeClass
  public static void init() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("fs.file.impl", LocalFileSystem.class.getName());
    fileSys = FileSystem.getLocal(conf);
    codec = CryptoCodec.getInstance(conf);
  }
  
  @AfterClass
  public static void shutdown() throws Exception {
  }
  
  @Before
  @Override
  public void setUp() throws IOException {
    fileSys.delete(new Path(TEST_ROOT_DIR), true);
    super.setUp();
  }
  
  @After
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
  
  @Ignore("ChecksumFSInputChecker doesn't support ByteBuffer read")
  @Override
  @Test(timeout=10000)
  public void testByteBufferRead() throws Exception {}
  
  @Ignore("ChecksumFSOutputSummer doesn't support Syncable")
  @Override
  @Test(timeout=10000)
  public void testSyncable() throws IOException {}
  
  @Ignore("ChecksumFSInputChecker doesn't support ByteBuffer read")
  @Override
  @Test(timeout=10000)
  public void testCombinedOp() throws Exception {}
  
  @Ignore("ChecksumFSInputChecker doesn't support enhanced ByteBuffer access")
  @Override
  @Test(timeout=10000)
  public void testHasEnhancedByteBufferAccess() throws Exception {
  }
  
  @Ignore("ChecksumFSInputChecker doesn't support seekToNewSource")
  @Override
  @Test(timeout=10000)
  public void testSeekToNewSource() throws Exception {
  }

  @Ignore("Local file input stream does not support unbuffer")
  @Override
  @Test
  public void testUnbuffer() throws Exception {}
}
