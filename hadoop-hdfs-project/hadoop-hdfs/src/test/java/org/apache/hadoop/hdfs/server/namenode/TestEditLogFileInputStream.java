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
package org.apache.hadoop.hdfs.server.namenode;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.EnumMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestEditLogFileInputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestEditLogFileInputStream.class);
  private static final byte[] FAKE_LOG_DATA = TestEditLog.HADOOP20_SOME_EDITS;

  private final static File TEST_DIR = PathUtils
      .getTestDir(TestEditLogFileInputStream.class);

  @Test
  public void testReadURL() throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);
    doReturn(new ByteArrayInputStream(FAKE_LOG_DATA)).when(conn).getInputStream();
    doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();
    doReturn(Integer.toString(FAKE_LOG_DATA.length)).when(conn).getHeaderField("Content-Length");

    URLConnectionFactory factory = mock(URLConnectionFactory.class);
    doReturn(conn).when(factory).openConnection(Mockito.<URL> any(),
        anyBoolean());

    URL url = new URL("http://localhost/fakeLog");
    EditLogInputStream elis = EditLogFileInputStream.fromUrl(factory, url,
        HdfsServerConstants.INVALID_TXID, HdfsServerConstants.INVALID_TXID, false);
    // Read the edit log and verify that we got all of the data.
    EnumMap<FSEditLogOpCodes, Holder<Integer>> counts = FSImageTestUtil
        .countEditLogOpTypes(elis);
    assertThat(counts.get(FSEditLogOpCodes.OP_ADD).held, is(1));
    assertThat(counts.get(FSEditLogOpCodes.OP_SET_GENSTAMP_V1).held, is(1));
    assertThat(counts.get(FSEditLogOpCodes.OP_CLOSE).held, is(1));

    // Check that length header was picked up.
    assertEquals(FAKE_LOG_DATA.length, elis.length());
    elis.close();
  }

  /**
   * Regression test for HDFS-8965 which verifies that
   * FSEditLogFileInputStream#scanOp verifies Op checksums.
   */
  @Test(timeout=60000)
  public void testScanCorruptEditLog() throws Exception {
    Configuration conf = new Configuration();
    File editLog = new File(GenericTestUtils.getTempPath("testCorruptEditLog"));

    LOG.debug("Creating test edit log file: " + editLog);
    EditLogFileOutputStream elos = new EditLogFileOutputStream(conf,
        editLog.getAbsoluteFile(), 8192);
    elos.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    FSEditLogOp.OpInstanceCache cache = new FSEditLogOp.OpInstanceCache();
    FSEditLogOp.MkdirOp mkdirOp = FSEditLogOp.MkdirOp.getInstance(cache);
    mkdirOp.reset();
    mkdirOp.setRpcCallId(123);
    mkdirOp.setTransactionId(1);
    mkdirOp.setInodeId(789L);
    mkdirOp.setPath("/mydir");
    PermissionStatus perms = PermissionStatus.createImmutable(
        "myuser", "mygroup", FsPermission.createImmutable((short)0777));
    mkdirOp.setPermissionStatus(perms);
    elos.write(mkdirOp);
    mkdirOp.reset();
    mkdirOp.setRpcCallId(456);
    mkdirOp.setTransactionId(2);
    mkdirOp.setInodeId(123L);
    mkdirOp.setPath("/mydir2");
    perms = PermissionStatus.createImmutable(
        "myuser", "mygroup", FsPermission.createImmutable((short)0666));
    mkdirOp.setPermissionStatus(perms);
    elos.write(mkdirOp);
    elos.setReadyToFlush();
    elos.flushAndSync(false);
    elos.close();
    long fileLen = editLog.length();

    LOG.debug("Corrupting last 4 bytes of edit log file " + editLog +
        ", whose length is " + fileLen);
    RandomAccessFile rwf = new RandomAccessFile(editLog, "rw");
    rwf.seek(fileLen - 4);
    int b = rwf.readInt();
    rwf.seek(fileLen - 4);
    rwf.writeInt(b + 1);
    rwf.close();

    EditLogFileInputStream elis = new EditLogFileInputStream(editLog);
    Assert.assertEquals(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
        elis.getVersion(true));
    Assert.assertEquals(1, elis.scanNextOp());
    LOG.debug("Read transaction 1 from " + editLog);
    try {
      elis.scanNextOp();
      Assert.fail("Expected scanNextOp to fail when op checksum was corrupt.");
    } catch (IOException e) {
      LOG.debug("Caught expected checksum error when reading corrupt " +
          "transaction 2", e);
      GenericTestUtils.assertExceptionContains("Transaction is corrupt.", e);
    }
    elis.close();
  }
}
