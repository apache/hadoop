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

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSTestWrapper;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestWrapper;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileSystemTestWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.EncryptionZoneManager;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestEncryptionZones {

  private Configuration conf;
  private FileSystemTestHelper fsHelper;

  private MiniDFSCluster cluster;
  private HdfsAdmin dfsAdmin;
  private DistributedFileSystem fs;

  protected FileSystemTestWrapper fsWrapper;
  protected FileContextTestWrapper fcWrapper;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    fsHelper = new FileSystemTestHelper();
    // Set up java key store
    String testRoot = fsHelper.getTestRootDir();
    File testRootDir = new File(testRoot).getAbsoluteFile();
    conf.set(KeyProviderFactory.KEY_PROVIDER_PATH,
        JavaKeyStoreProvider.SCHEME_NAME + "://file" + testRootDir + "/test.jks"
    );
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    Logger.getLogger(EncryptionZoneManager.class).setLevel(Level.TRACE);
    fs = cluster.getFileSystem();
    fsWrapper = new FileSystemTestWrapper(cluster.getFileSystem());
    fcWrapper = new FileContextTestWrapper(
        FileContext.getFileContext(cluster.getURI(), conf));
    dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public void assertNumZones(final int numZones) throws IOException {
    final List<EncryptionZone> zones = dfsAdmin.listEncryptionZones();
    assertEquals("Unexpected number of encryption zones!", numZones,
        zones.size());
  }

  /**
   * Checks that an encryption zone with the specified keyId and path (if not
   * null) is present.
   *
   * @throws IOException if a matching zone could not be found
   */
  public void assertZonePresent(String keyId, String path) throws IOException {
    final List<EncryptionZone> zones = dfsAdmin.listEncryptionZones();
    boolean match = false;
    for (EncryptionZone zone : zones) {
      boolean matchKey = (keyId == null);
      boolean matchPath = (path == null);
      if (keyId != null && zone.getKeyId().equals(keyId)) {
        matchKey = true;
      }
      if (path != null && zone.getPath().equals(path)) {
        matchPath = true;
      }
      if (matchKey && matchPath) {
        match = true;
        break;
      }
    }
    assertTrue("Did not find expected encryption zone with keyId " + keyId +
            " path " + path, match
    );
  }

  /**
   * Helper function to create a key in the Key Provider.
   */
  private void createKey(String keyId)
      throws NoSuchAlgorithmException, IOException {
    KeyProvider provider = cluster.getNameNode().getNamesystem().getProvider();
    final KeyProvider.Options options = KeyProvider.options(conf);
    provider.createKey(keyId, options);
    provider.flush();
  }

  @Test(timeout = 60000)
  public void testBasicOperations() throws Exception {

    int numZones = 0;

    /* Test failure of create EZ on a directory that doesn't exist. */
    final Path zone1 = new Path("/zone1");
    try {
      dfsAdmin.createEncryptionZone(zone1, null);
      fail("expected /test doesn't exist");
    } catch (IOException e) {
      assertExceptionContains("cannot find", e);
    }

    /* Normal creation of an EZ */
    fsWrapper.mkdir(zone1, FsPermission.getDirDefault(), true);
    dfsAdmin.createEncryptionZone(zone1, null);
    assertNumZones(++numZones);
    assertZonePresent(null, zone1.toString());

    /* Test failure of create EZ on a directory which is already an EZ. */
    try {
      dfsAdmin.createEncryptionZone(zone1, null);
    } catch (IOException e) {
      assertExceptionContains("already in an encryption zone", e);
    }

    /* Test failure of create EZ operation in an existing EZ. */
    final Path zone1Child = new Path(zone1, "child");
    fsWrapper.mkdir(zone1Child, FsPermission.getDirDefault(), false);
    try {
      dfsAdmin.createEncryptionZone(zone1Child, null);
      fail("EZ in an EZ");
    } catch (IOException e) {
      assertExceptionContains("already in an encryption zone", e);
    }

    /* create EZ on a folder with a folder fails */
    final Path notEmpty = new Path("/notEmpty");
    final Path notEmptyChild = new Path(notEmpty, "child");
    fsWrapper.mkdir(notEmptyChild, FsPermission.getDirDefault(), true);
    try {
      dfsAdmin.createEncryptionZone(notEmpty, null);
      fail("Created EZ on an non-empty directory with folder");
    } catch (IOException e) {
      assertExceptionContains("create an encryption zone", e);
    }
    fsWrapper.delete(notEmptyChild, false);

    /* create EZ on a folder with a file fails */
    fsWrapper.createFile(notEmptyChild);
    try {
      dfsAdmin.createEncryptionZone(notEmpty, null);
      fail("Created EZ on an non-empty directory with file");
    } catch (IOException e) {
      assertExceptionContains("create an encryption zone", e);
    }

    /* Test failure of creating an EZ passing a key that doesn't exist. */
    final Path zone2 = new Path("/zone2");
    fsWrapper.mkdir(zone2, FsPermission.getDirDefault(), false);
    final String myKeyId = "mykeyid";
    try {
      dfsAdmin.createEncryptionZone(zone2, myKeyId);
      fail("expected key doesn't exist");
    } catch (IOException e) {
      assertExceptionContains("doesn't exist.", e);
    }
    assertNumZones(1);

    /* Test success of creating an EZ when they key exists. */
    createKey(myKeyId);
    dfsAdmin.createEncryptionZone(zone2, myKeyId);
    assertNumZones(++numZones);
    assertZonePresent(myKeyId, zone2.toString());

    /* Test failure of create encryption zones as a non super user. */
    final UserGroupInformation user = UserGroupInformation.
        createUserForTesting("user", new String[] { "mygroup" });
    final Path nonSuper = new Path("/nonSuper");
    fsWrapper.mkdir(nonSuper, FsPermission.getDirDefault(), false);

    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        final HdfsAdmin userAdmin =
            new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
        try {
          userAdmin.createEncryptionZone(nonSuper, null);
          fail("createEncryptionZone is superuser-only operation");
        } catch (AccessControlException e) {
          assertExceptionContains("Superuser privilege is required", e);
        }
        return null;
      }
    });

    // Test success of creating an encryption zone a few levels down.
    Path deepZone = new Path("/d/e/e/p/zone");
    fsWrapper.mkdir(deepZone, FsPermission.getDirDefault(), true);
    dfsAdmin.createEncryptionZone(deepZone, null);
    assertNumZones(++numZones);
    assertZonePresent(null, deepZone.toString());
  }

  /**
   * Test listing encryption zones as a non super user.
   */
  @Test(timeout = 60000)
  public void testListEncryptionZonesAsNonSuperUser() throws Exception {

    final UserGroupInformation user = UserGroupInformation.
        createUserForTesting("user", new String[] { "mygroup" });

    final Path testRoot = new Path(fsHelper.getTestRootDir());
    final Path superPath = new Path(testRoot, "superuseronly");
    final Path allPath = new Path(testRoot, "accessall");

    fsWrapper.mkdir(superPath, new FsPermission((short) 0700), true);
    dfsAdmin.createEncryptionZone(superPath, null);

    fsWrapper.mkdir(allPath, new FsPermission((short) 0707), true);
    dfsAdmin.createEncryptionZone(allPath, null);

    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        final HdfsAdmin userAdmin =
            new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
        try {
          userAdmin.listEncryptionZones();
        } catch (AccessControlException e) {
          assertExceptionContains("Superuser privilege is required", e);
        }
        return null;
      }
    });
  }

  /**
   * Test success of Rename EZ on a directory which is already an EZ.
   */
  private void doRenameEncryptionZone(FSTestWrapper wrapper) throws Exception {
    final Path testRoot = new Path(fsHelper.getTestRootDir());
    final Path pathFoo = new Path(testRoot, "foo");
    final Path pathFooBaz = new Path(pathFoo, "baz");
    wrapper.mkdir(pathFoo, FsPermission.getDirDefault(), true);
    dfsAdmin.createEncryptionZone(pathFoo, null);
    wrapper.mkdir(pathFooBaz, FsPermission.getDirDefault(), true);
    try {
      wrapper.rename(pathFooBaz, testRoot);
    } catch (IOException e) {
      assertExceptionContains(pathFooBaz.toString() + " can't be moved from" +
              " an encryption zone.", e
      );
    }
  }

  @Test(timeout = 60000)
  public void testRenameFileSystem() throws Exception {
    doRenameEncryptionZone(fsWrapper);
  }

  @Test(timeout = 60000)
  public void testRenameFileContext() throws Exception {
    doRenameEncryptionZone(fcWrapper);
  }

  private void validateFiles(Path p1, Path p2, int len) throws Exception {
    FSDataInputStream in1 = fs.open(p1);
    FSDataInputStream in2 = fs.open(p2);
    for (int i = 0; i < len; i++) {
      assertEquals("Mismatch at byte " + i, in1.read(), in2.read());
    }
    in1.close();
    in2.close();
  }

  private FileEncryptionInfo getFileEncryptionInfo(Path path) throws Exception {
    LocatedBlocks blocks = fs.getClient().getLocatedBlocks(path.toString(), 0);
    return blocks.getFileEncryptionInfo();
  }

  @Test(timeout = 120000)
  public void testReadWrite() throws Exception {
    final HdfsAdmin dfsAdmin =
        new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    // Create a base file for comparison
    final Path baseFile = new Path("/base");
    final int len = 8192;
    DFSTestUtil.createFile(fs, baseFile, len, (short) 1, 0xFEED);
    // Create the first enc file
    final Path zone = new Path("/zone");
    fs.mkdirs(zone);
    dfsAdmin.createEncryptionZone(zone, null);
    final Path encFile1 = new Path(zone, "myfile");
    DFSTestUtil.createFile(fs, encFile1, len, (short) 1, 0xFEED);
    // Read them back in and compare byte-by-byte
    validateFiles(baseFile, encFile1, len);
    // Roll the key of the encryption zone
    List<EncryptionZone> zones = dfsAdmin.listEncryptionZones();
    assertEquals("Expected 1 EZ", 1, zones.size());
    String keyId = zones.get(0).getKeyId();
    cluster.getNamesystem().getProvider().rollNewVersion(keyId);
    // Read them back in and compare byte-by-byte
    validateFiles(baseFile, encFile1, len);
    // Write a new enc file and validate
    final Path encFile2 = new Path(zone, "myfile2");
    DFSTestUtil.createFile(fs, encFile2, len, (short) 1, 0xFEED);
    // FEInfos should be different
    FileEncryptionInfo feInfo1 = getFileEncryptionInfo(encFile1);
    FileEncryptionInfo feInfo2 = getFileEncryptionInfo(encFile2);
    assertFalse("EDEKs should be different", Arrays
        .equals(feInfo1.getEncryptedDataEncryptionKey(),
            feInfo2.getEncryptedDataEncryptionKey()));
    assertNotEquals("Key was rolled, versions should be different",
        feInfo1.getEzKeyVersionName(), feInfo2.getEzKeyVersionName());
    // Contents still equal
    validateFiles(encFile1, encFile2, len);
  }

  @Test(timeout = 60000)
  public void testCipherSuiteNegotiation() throws Exception {
    final HdfsAdmin dfsAdmin =
        new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    final Path zone = new Path("/zone");
    fs.mkdirs(zone);
    dfsAdmin.createEncryptionZone(zone, null);
    // Create a file in an EZ, which should succeed
    DFSTestUtil
        .createFile(fs, new Path(zone, "success1"), 0, (short) 1, 0xFEED);
    // Pass no cipherSuites, fail
    fs.getClient().cipherSuites = Lists.newArrayListWithCapacity(0);
    try {
      DFSTestUtil.createFile(fs, new Path(zone, "fail"), 0, (short) 1, 0xFEED);
      fail("Created a file without specifying a CipherSuite!");
    } catch (UnknownCipherSuiteException e) {
      assertExceptionContains("No cipher suites", e);
    }
    // Pass some unknown cipherSuites, fail
    fs.getClient().cipherSuites = Lists.newArrayListWithCapacity(3);
    fs.getClient().cipherSuites.add(CipherSuite.UNKNOWN);
    fs.getClient().cipherSuites.add(CipherSuite.UNKNOWN);
    fs.getClient().cipherSuites.add(CipherSuite.UNKNOWN);
    try {
      DFSTestUtil.createFile(fs, new Path(zone, "fail"), 0, (short) 1, 0xFEED);
      fail("Created a file without specifying a CipherSuite!");
    } catch (UnknownCipherSuiteException e) {
      assertExceptionContains("No cipher suites", e);
    }
    // Pass some unknown and a good cipherSuites, success
    fs.getClient().cipherSuites = Lists.newArrayListWithCapacity(3);
    fs.getClient().cipherSuites.add(CipherSuite.AES_CTR_NOPADDING);
    fs.getClient().cipherSuites.add(CipherSuite.UNKNOWN);
    fs.getClient().cipherSuites.add(CipherSuite.UNKNOWN);
    DFSTestUtil
        .createFile(fs, new Path(zone, "success2"), 0, (short) 1, 0xFEED);
    fs.getClient().cipherSuites = Lists.newArrayListWithCapacity(3);
    fs.getClient().cipherSuites.add(CipherSuite.UNKNOWN);
    fs.getClient().cipherSuites.add(CipherSuite.UNKNOWN);
    fs.getClient().cipherSuites.add(CipherSuite.AES_CTR_NOPADDING);
    DFSTestUtil
        .createFile(fs, new Path(zone, "success3"), 4096, (short) 1, 0xFEED);
    // Check KeyProvider state
    // Flushing the KP on the NN, since it caches, and init a test one
    cluster.getNamesystem().getProvider().flush();
    KeyProvider provider = KeyProviderFactory.getProviders(conf).get(0);
    List<String> keys = provider.getKeys();
    assertEquals("Expected NN to have created one key per zone", 1,
        keys.size());
    List<KeyProvider.KeyVersion> allVersions = Lists.newArrayList();
    for (String key : keys) {
      List<KeyProvider.KeyVersion> versions = provider.getKeyVersions(key);
      assertEquals("Should only have one key version per key", 1,
          versions.size());
      allVersions.addAll(versions);
    }
    // Check that the specified CipherSuite was correctly saved on the NN
    for (int i = 2; i <= 3; i++) {
      FileEncryptionInfo feInfo =
          getFileEncryptionInfo(new Path(zone.toString() +
              "/success" + i));
      assertEquals(feInfo.getCipherSuite(), CipherSuite.AES_CTR_NOPADDING);
    }
  }

}
