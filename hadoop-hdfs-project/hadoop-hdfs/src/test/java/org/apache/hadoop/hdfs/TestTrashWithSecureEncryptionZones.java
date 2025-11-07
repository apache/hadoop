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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.hadoop.fs.CommonConfigurationKeys
    .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic
    .FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic
    .KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.
    KMS_CLIENT_ENC_KEY_CACHE_SIZE;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.
    DFS_DATA_TRANSFER_PROTECTION_KEY;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.CreateEncryptionZoneFlag;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class tests Trash functionality in Encryption Zones with Kerberos
 * enabled.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestTrashWithSecureEncryptionZones {
  private static HdfsConfiguration baseConf;
  private static File baseDir;
  private static final EnumSet<CreateEncryptionZoneFlag> PROVISION_TRASH =
      EnumSet.of(CreateEncryptionZoneFlag.PROVISION_TRASH);

  private static final String HDFS_USER_NAME = "hdfs";
  private static final String SPNEGO_USER_NAME = "HTTP";
  private static final String OOZIE_USER_NAME = "oozie";
  private static final String OOZIE_PROXIED_USER_NAME = "oozie_user";

  private static String hdfsPrincipal;
  private static String spnegoPrincipal;
  private static String keytab;

  // MiniKDC
  private static MiniKdc kdc;

  // MiniKMS
  private static MiniKMS miniKMS;
  private static final String TEST_KEY = "test_key";
  private static final Path CURRENT = new Path("Current");

  // MiniDFS
  private static MiniDFSCluster cluster;
  private static HdfsConfiguration conf;
  private static FileSystem fs;
  private static HdfsAdmin dfsAdmin;
  private static Configuration clientConf;
  private static FsShell shell;

  private static AtomicInteger zoneCounter = new AtomicInteger(1);
  private static AtomicInteger fileCounter = new AtomicInteger(1);
  private static final int LEN = 8192;

  public static File getTestDir() throws Exception {
    File file = new File("dummy");
    file = file.getAbsoluteFile();
    file = file.getParentFile();
    file = new File(file, "target");
    file = new File(file, UUID.randomUUID().toString());
    if (!file.mkdirs()) {
      throw new RuntimeException("Could not create test directory: " + file);
    }
    return file;
  }

  @BeforeAll
  public static void init() throws Exception {
    baseDir = getTestDir();
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    baseConf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(UserGroupInformation
        .AuthenticationMethod.KERBEROS, baseConf);
    UserGroupInformation.setConfiguration(baseConf);
    assertTrue(UserGroupInformation.isSecurityEnabled(),
        "Expected configuration to enable security");

    File keytabFile = new File(baseDir, "test.keytab");
    keytab = keytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";

    kdc.createPrincipal(keytabFile,
        HDFS_USER_NAME + "/" + krbInstance,
        SPNEGO_USER_NAME + "/" + krbInstance,
        OOZIE_USER_NAME + "/" + krbInstance,
        OOZIE_PROXIED_USER_NAME + "/" + krbInstance);

    hdfsPrincipal = HDFS_USER_NAME + "/" + krbInstance + "@" + kdc.getRealm();
    spnegoPrincipal = SPNEGO_USER_NAME + "/" + krbInstance + "@" + kdc
        .getRealm();

    baseConf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        spnegoPrincipal);
    baseConf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    baseConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    baseConf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    baseConf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    // Set a small (2=4*0.5) KMSClient EDEK cache size to trigger
    // on demand refill upon the 3rd file creation
    baseConf.set(KMS_CLIENT_ENC_KEY_CACHE_SIZE, "4");
    baseConf.set(KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK, "0.5");

    String keystoresDir = baseDir.getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(
        TestSecureEncryptionZoneWithKMS.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, baseConf, false);
    baseConf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    baseConf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());

    File kmsFile = new File(baseDir, "kms-site.xml");
    if (kmsFile.exists()) {
      FileUtil.fullyDelete(kmsFile);
    }

    Configuration kmsConf = new Configuration(true);
    kmsConf.set(
        KMSConfiguration.KEY_PROVIDER_URI,
        "jceks://file@" + new Path(baseDir.toString(), "kms.keystore")
            .toUri());
    kmsConf.set("hadoop.kms.authentication.type", "kerberos");
    kmsConf.set("hadoop.kms.authentication.kerberos.keytab", keytab);
    kmsConf.set("hadoop.kms.authentication.kerberos.principal",
        "HTTP/localhost");
    kmsConf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    kmsConf.set("hadoop.kms.acl.GENERATE_EEK", "hdfs");

    Writer writer = new FileWriter(kmsFile);
    kmsConf.writeXml(writer);
    writer.close();

    // Start MiniKMS
    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
    miniKMS = miniKMSBuilder.setKmsConfDir(baseDir).build();
    miniKMS.start();

    baseConf.set(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_KEY_PROVIDER_PATH, getKeyProviderURI());
    baseConf.setBoolean(DFSConfigKeys
        .DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);

    conf = new HdfsConfiguration(baseConf);
    cluster = new MiniDFSCluster.Builder(conf)
        .build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);

    // Wait cluster to be active
    cluster.waitActive();

    // Create a test key
    DFSTestUtil.createKey(TEST_KEY, cluster, conf);
    clientConf = new Configuration(conf);
    clientConf.setLong(FS_TRASH_INTERVAL_KEY, 1);
    shell = new FsShell(clientConf);
  }

  @AfterAll
  public static void destroy() {
    IOUtils.cleanupWithLogger(null, fs);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

    if (kdc != null) {
      kdc.stop();
    }
    if (miniKMS != null) {
      miniKMS.stop();
    }
    FileUtil.fullyDelete(baseDir);
  }

  private static String getKeyProviderURI() {
    return KMSClientProvider.SCHEME_NAME + "://" +
        miniKMS.getKMSUrl().toExternalForm().replace("://", "@");
  }

  @Test
  public void testTrashCheckpoint() throws Exception {
    final Path zone1 = new Path("/zone" + zoneCounter.getAndIncrement());
    fs.mkdirs(zone1);
    final Path zone2 = new Path(zone1 + "/zone" +
        zoneCounter.getAndIncrement());
    fs.mkdirs(zone2);
    dfsAdmin.createEncryptionZone(zone2, TEST_KEY, PROVISION_TRASH);

    final Path encFile1 = new Path(zone2, "encFile" + fileCounter
        .getAndIncrement());
    DFSTestUtil.createFile(fs, encFile1, LEN, (short) 1, 0xFEED);

    //Verify Trash checkpoint within Encryption Zone
    Path trashDir = new Path(zone2, fs.TRASH_PREFIX + "/" + HDFS_USER_NAME +
        "/" + CURRENT);
    String trashPath = trashDir.toString() + encFile1.toString();
    Path deletedFile = verifyTrashLocationWithShellDelete(encFile1);
    assertEquals(trashPath, deletedFile.toUri().getPath(),
        "Deleted file not at the expected trash location: " + trashPath);

    //Verify Trash checkpoint outside the encryption zone when the whole
    // encryption zone is deleted and moved
    trashPath = fs.getHomeDirectory().toUri().getPath() + "/" + fs
        .TRASH_PREFIX + "/" + CURRENT + zone2;
    Path deletedDir = verifyTrashLocationWithShellDelete(zone2);
    assertEquals(trashPath, deletedDir.toUri().getPath(),
        "Deleted zone not at the expected trash location: " + trashPath);
  }

  @Test
  public void testTrashExpunge() throws Exception {
    final Path zone1 = new Path("/zone" + zoneCounter.getAndIncrement());
    fs.mkdirs(zone1);
    final Path zone2 = new Path("/zone" + zoneCounter.getAndIncrement());
    fs.mkdirs(zone2);
    dfsAdmin.createEncryptionZone(zone1, TEST_KEY, PROVISION_TRASH);

    final Path file1 = new Path(zone1, "encFile" + fileCounter
        .getAndIncrement());
    final Path file2 = new Path(zone2, "file" + fileCounter.getAndIncrement());
    DFSTestUtil.createFile(fs, file1, LEN, (short) 1, 0xFEED);
    DFSTestUtil.createFile(fs, file2, LEN, (short) 1, 0xFEED);

    //Verify Trash expunge within the encryption zone
    List<Path> trashPaths = Lists.newArrayList();
    trashPaths.add(verifyTrashLocationWithShellDelete(file1));
    trashPaths.add(verifyTrashLocationWithShellDelete(file2));
    verifyTrashExpunge(trashPaths);

    //Verify Trash expunge when the whole encryption zone has been deleted
    final Path file3 = new Path(zone1, "encFile" + fileCounter
        .getAndIncrement());
    DFSTestUtil.createFile(fs, file3, LEN, (short) 1, 0XFEED);
    Path trashPath = verifyTrashLocationWithShellDelete(file3);
    //Delete encryption zone
    DFSTestUtil.verifyDelete(shell, fs, zone1, true);
    verifyTrashExpunge(Lists.newArrayList(trashPath));

  }

  @Test
  public void testDeleteWithSkipTrash() throws Exception {
    final Path zone1 = new Path("/zone" + zoneCounter.getAndIncrement());
    fs.mkdirs(zone1);

    final Path encFile1 = new Path(zone1, "encFile" + fileCounter
        .getAndIncrement());
    final Path encFile2 = new Path(zone1, "encFile" + fileCounter
        .getAndIncrement());
    DFSTestUtil.createFile(fs, encFile1, LEN, (short) 1, 0xFEED);
    DFSTestUtil.createFile(fs, encFile2, LEN, (short) 1, 0xFEED);

    //Verify file deletion with skipTrash
    verifyDeleteWithSkipTrash(encFile1);

    //Verify file deletion without skipTrash
    DFSTestUtil.verifyDelete(shell, fs, encFile2, true);
  }

  @Test
  public void testDeleteEmptyDirectory() throws Exception {
    final Path zone1 = new Path("/zone" + zoneCounter.getAndIncrement());
    final Path zone2 = new Path("/zone" + zoneCounter.getAndIncrement());
    fs.mkdirs(zone1);
    fs.mkdirs(zone2);

    final Path trashDir1 = new Path(shell.getCurrentTrashDir(zone1) + "/" +
        zone1);
    final Path trashDir2 = new Path(shell.getCurrentTrashDir(zone1) + "/" +
        zone2);

    //Delete empty directory with -r option
    String[] argv1 = new String[]{"-rm", "-r", zone1.toString()};
    int res = ToolRunner.run(shell, argv1);
    assertEquals(0, res, "rm failed");
    assertTrue(fs.exists(trashDir1), "Empty directory not deleted even with -r : " + trashDir1);

    //Delete empty directory without -r option
    String[] argv2 = new String[]{"-rm", zone2.toString()};
    res = ToolRunner.run(shell, argv2);
    assertEquals(1, res, "rm on empty directory did not fail");
    assertTrue(!fs.exists(trashDir2), "Empty directory deleted without -r : " + trashDir2);
  }

  @Test
  public void testDeleteFromTrashWithinEZ() throws Exception {
    final Path zone1 = new Path("/zone" + zoneCounter.getAndIncrement());
    fs.mkdirs(zone1);
    dfsAdmin.createEncryptionZone(zone1, TEST_KEY, PROVISION_TRASH);

    final Path encFile1 = new Path(zone1, "encFile" + fileCounter
        .getAndIncrement());
    DFSTestUtil.createFile(fs, encFile1, LEN, (short) 1, 0xFEED);

    final Path trashFile = new Path(shell.getCurrentTrashDir(encFile1) + "/" +
        encFile1);

    String[] argv = new String[]{"-rm", "-r", encFile1.toString()};
    int res = ToolRunner.run(shell, argv);
    assertEquals(0, res, "rm failed");

    String[] argvDeleteTrash = new String[]{"-rm", "-r", trashFile.toString()};
    int resDeleteTrash = ToolRunner.run(shell, argvDeleteTrash);
    assertEquals(0, resDeleteTrash, "rm failed");
    assertFalse(fs.exists(trashFile), "File deleted from Trash : " + trashFile);
  }

  @Test
  public void testTrashRetentionAfterNamenodeRestart() throws Exception {
    final Path zone1 = new Path("/zone" + zoneCounter.getAndIncrement());
    fs.mkdirs(zone1);
    dfsAdmin.createEncryptionZone(zone1, TEST_KEY, PROVISION_TRASH);

    final Path encFile1 = new Path(zone1, "encFile" + fileCounter
        .getAndIncrement());
    DFSTestUtil.createFile(fs, encFile1, LEN, (short) 1, 0xFEED);

    final Path trashFile = new Path(shell.getCurrentTrashDir(encFile1) + "/" +
        encFile1);
    String[] argv = new String[]{"-rm", "-r", encFile1.toString()};
    int res = ToolRunner.run(shell, argv);
    assertEquals(0, res, "rm failed");

    assertTrue(fs.exists(trashFile), "File not in trash : " + trashFile);
    cluster.restartNameNode(0);
    cluster.waitActive();
    fs = cluster.getFileSystem();

    assertTrue(fs.exists(trashFile), "On Namenode restart, file deleted from trash : " + trashFile);
  }

  private Path verifyTrashLocationWithShellDelete(Path path)
      throws Exception {

    final Path trashFile = new Path(shell.getCurrentTrashDir(path) + "/" +
        path);
    File deletedFile = new File(String.valueOf(trashFile));
    assertFalse(deletedFile.exists(), "File already present in Trash before delete");

    DFSTestUtil.verifyDelete(shell, fs, path, trashFile, true);
    return trashFile;
  }

  private void verifyTrashExpunge(List<Path> trashFiles) throws Exception {
    String[] argv = new String[]{"-expunge"};
    int res = ToolRunner.run(shell, argv);
    assertEquals(0, res, "expunge failed");

    for (Path trashFile : trashFiles) {
      assertFalse(fs.exists(trashFile), "File exists in trash after expunge : " + trashFile);
    }
  }

  private void verifyDeleteWithSkipTrash(Path path) throws Exception {
    assertTrue(fs.exists(path), path + " file does not exist");

    final Path trashFile = new Path(shell.getCurrentTrashDir(path) + "/" +
        path);

    String[] argv = new String[]{"-rm", "-r", "-skipTrash", path.toString()};
    int res = ToolRunner.run(shell, argv);
    assertEquals(0, res, "rm failed");
    assertFalse(fs.exists(trashFile), "File in trash even with -skipTrash");
  }
}
