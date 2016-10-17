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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileSystemTestWrapper;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.CreateEncryptionZoneFlag;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.server.namenode.EncryptionZoneManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class tests Trash functionality in Encryption Zones.
 */
public class TestTrashWithEncryptionZones {
  private Configuration conf;
  private FileSystemTestHelper fsHelper;

  private MiniDFSCluster cluster;
  private HdfsAdmin dfsAdmin;
  private DistributedFileSystem fs;
  private File testRootDir;
  private static final String TEST_KEY = "test_key";

  private FileSystemTestWrapper fsWrapper;
  private static Configuration clientConf;
  private static FsShell shell;

  private static AtomicInteger zoneCounter = new AtomicInteger(1);
  private static AtomicInteger fileCounter = new AtomicInteger(1);
  private static final int LEN = 8192;

  private static final EnumSet< CreateEncryptionZoneFlag > NO_TRASH =
      EnumSet.of(CreateEncryptionZoneFlag.NO_TRASH);
  private static final EnumSet<CreateEncryptionZoneFlag> PROVISION_TRASH =
      EnumSet.of(CreateEncryptionZoneFlag.PROVISION_TRASH);

  private String getKeyProviderURI() {
    return JavaKeyStoreProvider.SCHEME_NAME + "://file" +
        new Path(testRootDir.toString(), "test.jks").toUri();
  }

  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
    fsHelper = new FileSystemTestHelper();
    // Set up java key store
    String testRoot = fsHelper.getTestRootDir();
    testRootDir = new File(testRoot).getAbsoluteFile();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        getKeyProviderURI());
    conf.setBoolean(DFSConfigKeys
        .DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    // Lower the batch size for testing
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        2);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    org.apache.log4j.Logger
        .getLogger(EncryptionZoneManager.class).setLevel(Level.TRACE);
    fs = cluster.getFileSystem();
    fsWrapper = new FileSystemTestWrapper(fs);
    dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);
    setProvider();
    // Create a test key
    DFSTestUtil.createKey(TEST_KEY, cluster, conf);

    clientConf = new Configuration(conf);
    clientConf.setLong(FS_TRASH_INTERVAL_KEY, 1);
    shell = new FsShell(clientConf);
  }

  protected void setProvider() {
    // Need to set the client's KeyProvider to the NN's for JKS,
    // else the updates do not get flushed properly
    fs.getClient().setKeyProvider(cluster.getNameNode().getNamesystem()
        .getProvider());
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testDeleteWithinEncryptionZone() throws Exception {
    final Path zone = new Path("/zones");
    fs.mkdirs(zone);
    final Path zone1 = new Path("/zones/zone" + zoneCounter.getAndIncrement());
    fs.mkdirs(zone1);
    dfsAdmin.createEncryptionZone(zone1, TEST_KEY, PROVISION_TRASH);

    final Path encFile1 = new Path(zone1, "encFile" + fileCounter
        .getAndIncrement());
    DFSTestUtil.createFile(fs, encFile1, LEN, (short) 1, 0xFEED);

    //Verify file deletion
    DFSTestUtil.verifyDelete(shell, fs, encFile1, true);

    //Verify directory deletion
    DFSTestUtil.verifyDelete(shell, fs, zone1, true);
  }

  @Test
  public void testDeleteEZWithMultipleUsers() throws Exception {
    final Path zone = new Path("/zones");
    fs.mkdirs(zone);
    final Path zone1 = new Path("/zones/zone" + zoneCounter.getAndIncrement());
    fs.mkdirs(zone1);
    dfsAdmin.createEncryptionZone(zone1, TEST_KEY, NO_TRASH);

    fsWrapper.setPermission(zone1,
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

    final Path encFile1 = new Path(zone1, "encFile" + fileCounter
        .getAndIncrement());
    DFSTestUtil.createFile(fs, encFile1, LEN, (short) 1, 0xFEED);

    // create a non-privileged user
    final UserGroupInformation user = UserGroupInformation
        .createUserForTesting("user", new String[]{"mygroup"});

    final Path encFile2 = new Path(zone1, "encFile" + fileCounter
        .getAndIncrement());

    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        // create a file /zones/zone1/encFile2 in EZ
        // this file is owned by user:mygroup
        FileSystem fs2 = FileSystem.get(cluster.getConfiguration(0));
        DFSTestUtil.createFile(fs2, encFile2, LEN, (short) 1, 0xFEED);

        // Delete /zones/zone1/encFile2, which moves the file to
        // /zones/zone1/.Trash/user/Current/zones/zone1/encFile2
        DFSTestUtil.verifyDelete(shell, fs, encFile2, true);

        // Delete /zones/zone1 should not succeed as current user is not admin
        String[] argv = new String[]{"-rm", "-r", zone1.toString()};
        int res = ToolRunner.run(shell, argv);
        assertEquals("Non-admin could delete an encryption zone with multiple" +
            " users : " + zone1, 1, res);
        return null;
      }
    });

    shell = new FsShell(clientConf);
    DFSTestUtil.verifyDelete(shell, fs, zone1, true);
  }
}
