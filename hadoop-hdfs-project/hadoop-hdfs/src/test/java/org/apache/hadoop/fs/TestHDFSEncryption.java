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

package org.apache.hadoop.fs;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize;
import static org.apache.hadoop.fs.FileContextTestHelper.getFileData;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHDFSEncryption {
  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  private static final HdfsConfiguration CONF = new HdfsConfiguration();
  private static FileContext fc;
  private Path localFsRootPath;
  private Path src1;
  /* The KeyProvider, if any. */
  private static KeyProvider provider = null;

  private static File tmpDir;

  @BeforeClass
  public static void clusterSetupAtBegining() throws IOException,
      LoginException, URISyntaxException {
    tmpDir = new File(System.getProperty("test.build.data", "target"),
        UUID.randomUUID().toString()).getAbsoluteFile();
    tmpDir.mkdirs();

    CONF.set(KeyProviderFactory.KEY_PROVIDER_PATH,
            JavaKeyStoreProvider.SCHEME_NAME + "://file" + tmpDir + "/test.jks");
    initializeKeyProvider(CONF);
    try {
      createOneKey();
      KeyVersion blort = provider.getCurrentKey("blort");
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    cluster.waitClusterUp();

    URI uri0 = cluster.getURI(0);
    fc = FileContext.getFileContext(uri0, CONF);
    defaultWorkingDirectory = fc.makeQualified(new Path("/user/" +
        UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }

  private static void initializeKeyProvider(final Configuration conf)
    throws IOException {
    final List<KeyProvider> providers = KeyProviderFactory.getProviders(conf);
    if (providers == null) {
      return;
    }

    if (providers.size() == 0) {
      return;
    }

    if (providers.size() > 1) {
      final String err =
        "Multiple KeyProviders found. Only one is permitted.";
      throw new RuntimeException(err);
    }
    provider = providers.get(0);
    if (provider.isTransient()) {
      final String err =
        "A KeyProvider was found but it is a transient provider.";
      throw new RuntimeException(err);
    }
  }

  private static void createOneKey()
    throws java.security.NoSuchAlgorithmException, IOException {
    final org.apache.hadoop.crypto.key.KeyProvider.Options options =
      KeyProvider.options(CONF);
    provider.createKey("blort", options);
    provider.flush();
  }

  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Before
  public void setUp() throws Exception {
    File testBuildData = new File(System.getProperty("test.build.data",
            "build/test/data"), RandomStringUtils.randomAlphanumeric(10));
    Path rootPath = new Path(testBuildData.getAbsolutePath(),
            "root-uri");
    localFsRootPath = rootPath.makeQualified(LocalFileSystem.NAME, null);
    fc.mkdir(getTestRootPath(fc, "test"), FileContext.DEFAULT_PERM, true);
    src1 = getTestRootPath(fc, "testfile");
  }

  @After
  public void tearDown() throws Exception {
    final boolean del =
      fc.delete(new Path(fileContextTestHelper.getAbsoluteTestRootPath(fc), new Path("test")), true);
    assertTrue(del);
    fc.delete(localFsRootPath, true);
  }

  protected final FileContextTestHelper fileContextTestHelper =
    createFileContextHelper();

  protected FileContextTestHelper createFileContextHelper() {
    return new FileContextTestHelper();
  }

  protected Path getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  private Path getTestRootPath(FileContext fc, String path) {
    return fileContextTestHelper.getTestRootPath(fc, path);
  }

  protected IOException unwrapException(IOException e) {
    if (e instanceof RemoteException) {
      return ((RemoteException) e).unwrapRemoteException();
    }
    return e;
  }

  private static final int NUM_BLOCKS = 3;

  private static final byte[] data = getFileData(NUM_BLOCKS,
      getDefaultBlockSize());

  private void writeSomeData() throws Exception {
    writeSomeData(false, false);
  }

  private void writeSomeData(boolean doHFlush, boolean doHSync) throws Exception {
    final FSDataOutputStream out =
      fc.create(src1, EnumSet.of(CREATE), Options.CreateOpts.createParent());
    out.write(data, 0, data.length);
    if (doHFlush) {
      out.hflush();
    }

    if (doHSync) {
      out.hsync();
    }

    out.close();
  }

  private void writeAndVerify(boolean doHFlush, boolean doHSync) throws Exception {
    writeSomeData(doHFlush, doHSync);

    final FSDataInputStream in = fc.open(src1);
    try {
      final byte[] readBuf = new byte[getDefaultBlockSize() * NUM_BLOCKS];

      in.readFully(readBuf);
      assertTrue("Expected read-back data to be equal (hflush=" + doHFlush
        + " hfsync=" + doHSync + ")", Arrays.equals(data, readBuf));
    } finally {
      in.close();
    }
  }

  @Test
  public void testBasicEncryptionStreamNoFlushNoSync() throws Exception {
    writeAndVerify(false, false);
  }

  @Test
  public void testBasicEncryptionStreamFlushSync() throws Exception {
    writeAndVerify(true, true);
  }

  @Test
  public void testBasicEncryptionStreamNoFlushSync() throws Exception {
    writeAndVerify(false, true);
  }

  @Test
  public void testBasicEncryptionStreamFlushNoSync() throws Exception {
    writeAndVerify(true, false);
  }

  @Test
  public void testGetPos() throws Exception {
    writeSomeData();

    final FSDataInputStream in = fc.open(src1);

    int expectedGetPos = 0;
    while (in.read() != -1) {
      assertTrue(++expectedGetPos == in.getPos());
    }
  }

  @Test
  public void testDoubleClose() throws Exception {
    writeSomeData();

    final FSDataInputStream in = fc.open(src1);
    in.close();
    try {
      in.close();
    } catch (Exception e) {
      fail("Caught unexpected exception on double-close: " + e);
    }
  }

  @Test
  public void testHFlush() throws Exception {
    final DistributedFileSystem fs = cluster.getFileSystem();
    final FSDataOutputStream out =
      fc.create(src1, EnumSet.of(CREATE), Options.CreateOpts.createParent());
    out.write(data, 0, data.length);
    out.hflush();
    out.close();
  }

  @Test
  public void testSeekBogusArgs() throws Exception {
    writeSomeData();

    final FSDataInputStream in = fc.open(src1);
    try {
      in.seek(-1);
      fail("Expected IOException");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains("Cannot seek to negative offset", e);
    }

    try {
      in.seek(1 << 20);
      fail("Expected IOException");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains("Cannot seek after EOF", e);
    }
    in.close();
  }

  @Test
  public void testSeekForward() throws Exception {
    writeSomeData();

    final FSDataInputStream in = fc.open(src1);

    for (int seekInc = 1; seekInc < 1024; seekInc += 32) {
      long seekTo = 0;
      while (seekTo < data.length) {
        in.seek(seekTo);
        int b = in.read();
        byte expected = data[(int) seekTo];
        assertTrue("seek(" + seekTo + ") Expected: " + expected + ", but got: " + b,
          b == expected);
        seekTo += seekInc;
      }
    }
    in.close();
  }

  @Test
  public void testSeekBackwards() throws Exception {
    writeSomeData();

    final FSDataInputStream in = fc.open(src1);

    for (int seekInc = 1; seekInc < 1024; seekInc += 32) {
      long seekTo = data.length - 1;
      while (seekTo >= 0) {
        in.seek(seekTo);
        int b = in.read();
        byte expected = data[(int) seekTo];
        assertTrue("seek(" + seekTo + ") Expected: " + expected + ", but got: " + b,
          b == expected);
        seekTo -= seekInc;
      }
    }
    in.close();
  }

  @Test
  public void testPostionedReadable() throws Exception {
    writeSomeData();

    final FSDataInputStream in = fc.open(src1);

    try {
      final byte[] oneByteToRead = new byte[1];
      for (int i = 0; i < data.length; i++) {
        int nread = in.read(i, oneByteToRead, 0, 1);
        final byte b = oneByteToRead[0];
        byte expected = data[(int) i];
        assertTrue("read() expected only one byte to be read, but got " + nread, nread == 1);
        assertTrue("read() expected: " + expected + ", but got: " + b,
          b == expected);
      }
    } finally {
      in.close();
    }
  }
}
