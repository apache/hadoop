/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.cblock;

import org.apache.hadoop.cblock.cli.CBlockCli;
import org.apache.hadoop.cblock.meta.VolumeDescriptor;
import org.apache.hadoop.cblock.util.MockStorageClient;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICE_LEVELDB_PATH_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A testing class for cblock command line tool.
 */
public class TestCBlockCLI {
  private static final long GB = 1 * 1024 * 1024 * 1024L;
  private static final int KB = 1024;
  private static CBlockCli cmd;
  private static OzoneConfiguration conf;
  private static CBlockManager cBlockManager;
  private static ByteArrayOutputStream outContent;
  private static PrintStream testPrintOut;

  @BeforeClass
  public static void setup() throws IOException {
    outContent = new ByteArrayOutputStream();
    ScmClient storageClient = new MockStorageClient();
    conf = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestCBlockCLI.class.getSimpleName());
    File filePath = new File(path);
    if (!filePath.exists() && !filePath.mkdirs()) {
      throw new IOException("Unable to create test DB dir");
    }
    conf.set(DFS_CBLOCK_SERVICERPC_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_CBLOCK_SERVICE_LEVELDB_PATH_KEY, path.concat(
        "/testCblockCli.dat"));
    cBlockManager = new CBlockManager(conf, storageClient);
    cBlockManager.start();
    testPrintOut = new PrintStream(outContent);
    cmd = new CBlockCli(conf, testPrintOut);
  }

  @AfterClass
  public static void clean() {
    if (cBlockManager != null) {
      cBlockManager.stop();
      cBlockManager.join();
      cBlockManager.clean();
    }
  }

  @After
  public void reset() {
    outContent.reset();
  }

  /**
   * Test the help command.
   * @throws Exception
   */
  @Test
  public void testCliHelp() throws Exception {
    PrintStream initialStdOut = System.out;
    System.setOut(testPrintOut);
    String[] args = {"-h"};
    cmd.run(args);
    String helpPrints =
        "usage: cblock\n" +
        " -c,--createVolume <user> <volume> <volumeSize in [GB/TB]> " +
            "<blockSize>   create a fresh new volume\n" +
        " -d,--deleteVolume <user> <volume>                         " +
            "              delete a volume\n" +
        " -h,--help                                                 " +
            "              help\n" +
        " -i,--infoVolume <user> <volume>                           " +
            "              info a volume\n" +
        " -l,--listVolume <user>                                    " +
            "              list all volumes\n" +
        " -s,--serverAddr <serverAddress>:<serverPort>              " +
            "              specify server address:port\n";
    assertEquals(helpPrints, outContent.toString());
    outContent.reset();
    System.setOut(initialStdOut);
  }

  /**
   * Test volume listing command.
   * @throws Exception
   */
  @Test
  public void testCliList() throws Exception {
    String userName0 = "userTestCliList0";
    String userName1 = "userTestCliList1";
    String userTestNotExist = "userTestNotExist";
    String volumeName0 = "volumeTest0";
    String volumeName1 = "volumeTest1";
    String volumeSize0 = "30GB";
    String volumeSize1 = "40GB";
    String blockSize = Integer.toString(4);
    String[] argsCreate0 =
        {"-c", userName0, volumeName0, volumeSize0, blockSize};
    cmd.run(argsCreate0);
    String[] argsCreate1 =
        {"-c", userName0, volumeName1, volumeSize1, blockSize};
    cmd.run(argsCreate1);
    String[] argsCreate2 =
        {"-c", userName1, volumeName0, volumeSize0, blockSize};
    cmd.run(argsCreate2);
    String[] argsList0 = {"-l"};
    cmd.run(argsList0);
    String[] outExpected1 = {
        "userTestCliList1:volumeTest0\t32212254720\t4096\n",
        "userTestCliList0:volumeTest0\t32212254720\t4096\n",
        "userTestCliList0:volumeTest1\t42949672960\t4096\n"};
    int length = 0;
    for (String str : outExpected1) {
      assertTrue(outContent.toString().contains(str));
      length += str.length();
    }
    assertEquals(length, outContent.toString().length());
    outContent.reset();

    String[] argsList1 = {"-l", userName1};
    cmd.run(argsList1);
    String outExpected2 = "userTestCliList1:volumeTest0\t32212254720\t4096\n";
    assertEquals(outExpected2, outContent.toString());
    outContent.reset();

    String[] argsList2 = {"-l", userTestNotExist};
    cmd.run(argsList2);
    String outExpected3 = "\n";
    assertEquals(outExpected3, outContent.toString());
  }

  /**
   * Test create volume command.
   * @throws Exception
   */
  @Test
  public void testCliCreate() throws Exception {
    String userName = "userTestCliCreate";
    String volumeName = "volumeTest";
    String volumeSize = "30GB";
    String blockSize = "4";
    String[] argsCreate = {"-c", userName, volumeName, volumeSize, blockSize};
    cmd.run(argsCreate);
    List<VolumeDescriptor> allVolumes = cBlockManager.getAllVolumes(userName);
    assertEquals(1, allVolumes.size());
    VolumeDescriptor volume = allVolumes.get(0);
    assertEquals(userName, volume.getUserName());
    assertEquals(volumeName, volume.getVolumeName());
    long volumeSizeB = volume.getVolumeSize();
    assertEquals(30, (int)(volumeSizeB/ GB));
    assertEquals(4, volume.getBlockSize()/ KB);
  }

  /**
   * Test delete volume command.
   * @throws Exception
   */
  @Test
  public void testCliDelete() throws Exception {
    String userName = "userTestCliDelete";
    String volumeName = "volumeTest";
    String volumeSize = "30GB";
    String blockSize = "4";
    String[] argsCreate = {"-c", userName, volumeName, volumeSize, blockSize};
    cmd.run(argsCreate);
    List<VolumeDescriptor> allVolumes = cBlockManager.getAllVolumes(userName);
    assertEquals(1, allVolumes.size());
    VolumeDescriptor volume = allVolumes.get(0);
    assertEquals(userName, volume.getUserName());
    assertEquals(volumeName, volume.getVolumeName());
    long volumeSizeB = volume.getVolumeSize();
    assertEquals(30, (int)(volumeSizeB/ GB));
    assertEquals(4, volume.getBlockSize()/ KB);

    String[] argsDelete = {"-d", userName, volumeName};
    cmd.run(argsDelete);
    allVolumes = cBlockManager.getAllVolumes(userName);
    assertEquals(0, allVolumes.size());
  }

  /**
   * Test info volume command.
   * @throws Exception
   */
  @Test
  public void testCliInfoVolume() throws Exception {
    String userName0 = "userTestCliInfo";
    String volumeName0 = "volumeTest0";
    String volumeSize = "8000GB";
    String blockSize = "4";
    String[] argsCreate0 = {
        "-c", userName0, volumeName0, volumeSize, blockSize};
    cmd.run(argsCreate0);
    String[] argsInfo = {"-i", userName0, volumeName0};
    cmd.run(argsInfo);
    // TODO : the usage field is not implemented yet, always 0 now.
    String outExpected = " userName:userTestCliInfo " +
        "volumeName:volumeTest0 " +
        "volumeSize:8589934592000 " +
        "blockSize:4096 (sizeInBlocks:2097152000) usageInBlocks:0\n";
    assertEquals(outExpected, outContent.toString());
  }
}
