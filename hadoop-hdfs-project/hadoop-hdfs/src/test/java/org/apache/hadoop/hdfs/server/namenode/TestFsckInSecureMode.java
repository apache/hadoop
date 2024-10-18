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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferTestCase;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.*;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.junit.Assert.assertTrue;

/**
 * TestFsck When NameNode run in SecureMode.
 */
public class TestFsckInSecureMode extends SaslDataTransferTestCase {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TestFsckInSecureMode.class.getName());
  private MiniDFSCluster cluster = null;
  private Configuration conf = null;
  private static LogCapturer auditLogCapture;
  // Pattern for:
  // allowed=true ugi=name ip=/address cmd=FSCK src=/ dst=null perm=null
  static final Pattern FSCK_PATTERN = Pattern.compile(
      "allowed=.*?\\s" +
          "ugi=.*?\\s" +
          "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" +
          "cmd=fsck\\ssrc=\\/\\sdst=null\\s" +
          "perm=null\\s" + "proto=.*");
  static final Pattern GET_FILE_INFO_PATTERN = Pattern.compile(
      "allowed=.*?\\s" +
          "ugi=.*?\\s" +
          "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" +
          "cmd=getfileinfo\\ssrc=\\/\\sdst=null\\s" +
          "perm=null\\s" + "proto=.*");

  static final Pattern NUM_MISSING_BLOCKS_PATTERN = Pattern.compile(
      ".*Missing blocks:\t\t([0123456789]*).*");

  static final Pattern NUM_CORRUPT_BLOCKS_PATTERN = Pattern.compile(
      ".*Corrupt blocks:\t\t([0123456789]*).*");

  private static final String LINE_SEPARATOR =
      System.getProperty("line.separator");

  @BeforeClass
  public static void beforeClass() {
    auditLogCapture = LogCapturer.captureLogs(FSNamesystem.AUDIT_LOG);
  }

  @AfterClass
  public static void afterClass() {
    auditLogCapture.stopCapturing();
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_CORRUPT_BLOCK_DELETE_IMMEDIATELY_ENABLED,
        false);
  }

  @After
  public void tearDown() throws Exception {
    shutdownCluster();
  }

  private void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFsckMoveWhenIOE() throws Exception {
    final int dfsBlockSize = 1024;
    final int numDatanodes = 4;
    conf = createSecureConfig("authentication");
    UserGroupInformation.setConfiguration(conf);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, dfsBlockSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 5);
    DFSTestUtil util = new DFSTestUtil("TestFsck", 5, 3,
        (5 * dfsBlockSize) + (dfsBlockSize - 1), 5 * dfsBlockSize);
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(numDatanodes).build();
    String topDir = "/srcdat";
    cluster.waitActive();
    FileSystem fs = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(getHdfsPrincipal(), getHdfsKeytab())
        .doAs(new PrivilegedExceptionAction<FileSystem>() {
          @Override
          public FileSystem run() throws Exception {
            return cluster.getFileSystem();
          }
        });
    util.createFiles(fs, topDir);
    util.waitReplication(fs, topDir, (short) 3);
    String outStr = TestFsck.runFsck(conf, 0, true, "/");
    assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    DFSClient dfsClient = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<DFSClient>() {
          @Override
          public DFSClient run() throws IOException {
            return new DFSClient(new InetSocketAddress("localhost",
                cluster.getNameNodePort()), conf);
          }
        });
    String[] fileNames = util.getFileNames(topDir);
    TestFsck.CorruptedTestFile[] ctFiles = new TestFsck.CorruptedTestFile[]{
        new TestFsck.CorruptedTestFile(fileNames[0], new HashSet<>(Arrays.asList(0)),
            dfsClient, numDatanodes, dfsBlockSize),
        new TestFsck.CorruptedTestFile(fileNames[1], new HashSet<>(Arrays.asList(2, 3)),
            dfsClient, numDatanodes, dfsBlockSize),
        new TestFsck.CorruptedTestFile(fileNames[2], new HashSet<>(Arrays.asList(4)),
            dfsClient, numDatanodes, dfsBlockSize),
        new TestFsck.CorruptedTestFile(fileNames[3], new HashSet<>(Arrays.asList(0, 1, 2, 3)),
            dfsClient, numDatanodes, dfsBlockSize),
        new TestFsck.CorruptedTestFile(fileNames[4], new HashSet<>(Arrays.asList(1, 2, 3, 4)),
            dfsClient, numDatanodes, dfsBlockSize)
    };
    int totalMissingBlocks = 0;
    for (TestFsck.CorruptedTestFile ctFile : ctFiles) {
      totalMissingBlocks += ctFile.getTotalMissingBlocks();
    }
    for (TestFsck.CorruptedTestFile ctFile : ctFiles) {
      ctFile.removeBlocks(cluster);
    }
    // Wait for fsck to discover all the missing blocks
    while (true) {
      outStr = TestFsck.runFsck(conf, 1, false, "/");
      String numMissing = null;
      String numCorrupt = null;
      for (String line : outStr.split(LINE_SEPARATOR)) {
        Matcher m = NUM_MISSING_BLOCKS_PATTERN.matcher(line);
        if (m.matches()) {
          numMissing = m.group(1);
        }
        m = NUM_CORRUPT_BLOCKS_PATTERN.matcher(line);
        if (m.matches()) {
          numCorrupt = m.group(1);
        }
        if (numMissing != null && numCorrupt != null) {
          break;
        }
      }
      if (numMissing == null || numCorrupt == null) {
        throw new IOException("failed to find number of missing or corrupt" +
            " blocks in fsck output.");
      }
      if (numMissing.equals(Integer.toString(totalMissingBlocks))) {
        assertTrue(numCorrupt.equals(Integer.toString(0)));
        assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
      }
    }
    assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));

    outStr = TestFsck.runFsck(conf, 1, false, "/", "-move");
    assertTrue(outStr.contains("java.io.IOException: failed to initialize"));
  }
}
