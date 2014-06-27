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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;

import static org.junit.Assert.fail;

public class TestEncryptionZonesAPI {

  private static final Path TEST_PATH = new Path("/test");
  private static final Path TEST_PATH_WITH_CHILD = new Path(TEST_PATH, "foo");
  private static final Path TEST_PATH_WITH_MULTIPLE_CHILDREN =
    new Path(TEST_PATH_WITH_CHILD, "baz");
  private static final String TEST_KEYID = "mykeyid";
  private final Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  private static File tmpDir;
  private FileSystem fs;

  @Before
  public void setUpCluster() throws IOException {
    tmpDir = new File(System.getProperty("test.build.data", "target"),
        UUID.randomUUID().toString()).getAbsoluteFile();
    conf.set(KeyProviderFactory.KEY_PROVIDER_PATH,
        JavaKeyStoreProvider.SCHEME_NAME + "://file" + tmpDir + "/test.jks");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    fs = createFileSystem(conf);
  }

  protected FileSystem createFileSystem(Configuration conf) throws IOException {
    return cluster.getFileSystem();
  }

  @After
  public void shutDownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /** Test failure of Create EZ on a directory that doesn't exist. */
  @Test(timeout = 30000)
  public void testCreateEncryptionZoneDirectoryDoesntExist() throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    try {
      dfsAdmin.createEncryptionZone(TEST_PATH, null);
      fail("expected /test doesn't exist");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("cannot find", e);
    }
  }

  /** Test failure of Create EZ on a directory which is already an EZ. */
  @Test(timeout = 30000)
  public void testCreateEncryptionZoneWhichAlreadyExists()
    throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    dfsAdmin.createEncryptionZone(TEST_PATH, null);
    try {
      dfsAdmin.createEncryptionZone(TEST_PATH, null);
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("already in an encryption zone",
          e);
    }
  }

  /** Test success of Create EZ in which a key is created. */
  @Test(timeout = 30000)
  public void testCreateEncryptionZoneAndGenerateKeyDirectoryEmpty()
    throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    dfsAdmin.createEncryptionZone(TEST_PATH, null);
  }

  /** Test failure of Create EZ operation in an existing EZ. */
  @Test(timeout = 30000)
  public void testCreateEncryptionZoneInExistingEncryptionZone()
    throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    dfsAdmin.createEncryptionZone(TEST_PATH, null);
    FileSystem.mkdirs(fs, TEST_PATH_WITH_CHILD,
        new FsPermission((short) 0777));
    try {
      dfsAdmin.createEncryptionZone(TEST_PATH_WITH_CHILD, null);
      fail("EZ in an EZ");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("already in an encryption zone", e);
    }
  }

  /** Test failure of creating an EZ using a non-empty directory. */
  @Test(timeout = 30000)
  public void testCreateEncryptionZoneAndGenerateKeyDirectoryNotEmpty()
    throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    FileSystem.create(fs, new Path("/test/foo"),
            new FsPermission((short) 0777));
    try {
      dfsAdmin.createEncryptionZone(TEST_PATH, null);
      fail("expected key doesn't exist");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("create an encryption zone", e);
    }
  }

  /** Test failure of creating an EZ passing a key that doesn't exist. */
  @Test(timeout = 30000)
  public void testCreateEncryptionZoneKeyDoesntExist() throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    try {
      dfsAdmin.createEncryptionZone(TEST_PATH, TEST_KEYID);
      fail("expected key doesn't exist");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("doesn't exist.", e);
    }
    final List<EncryptionZone> zones = dfsAdmin.listEncryptionZones();
    Preconditions.checkState(zones.size() == 0, "More than one zone found?");
  }

  /** Test success of creating an EZ when they key exists. */
  @Test(timeout = 30000)
  public void testCreateEncryptionZoneKeyExist() throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    createAKey(TEST_KEYID);
    dfsAdmin.createEncryptionZone(TEST_PATH, TEST_KEYID);
    final List<EncryptionZone> zones = dfsAdmin.listEncryptionZones();
    Preconditions.checkState(zones.size() == 1, "More than one zone found?");
    final EncryptionZone ez = zones.get(0);
      GenericTestUtils.assertMatches(ez.toString(),
              "EncryptionZone \\[path=/test, keyId=");
  }

  /** Helper function to create a key in the Key Provider. */
  private void createAKey(String keyId)
    throws NoSuchAlgorithmException, IOException {
    KeyProvider provider =
        cluster.getNameNode().getNamesystem().getProvider();
    final KeyProvider.Options options = KeyProvider.options(conf);
    provider.createKey(keyId, options);
    provider.flush();
  }

  /** Test failure of create/delete encryption zones as a non super user. */
  @Test(timeout = 30000)
  public void testCreateAndDeleteEncryptionZoneAsNonSuperUser()
    throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);

    final UserGroupInformation user = UserGroupInformation.
      createUserForTesting("user", new String[] { "mygroup" });

    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0700));

    user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final HdfsAdmin userAdmin =
            new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
          try {
            userAdmin.createEncryptionZone(TEST_PATH, null);
            fail("createEncryptionZone is superuser-only operation");
          } catch (AccessControlException e) {
            GenericTestUtils.assertExceptionContains(
                    "Superuser privilege is required", e);
          }
          return null;
        }
      });
    dfsAdmin.createEncryptionZone(TEST_PATH, null);

    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        final HdfsAdmin userAdmin =
                new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
        try {
          userAdmin.deleteEncryptionZone(TEST_PATH);
          fail("deleteEncryptionZone is superuser-only operation");
        } catch (AccessControlException e) {
          GenericTestUtils.assertExceptionContains(
                  "Superuser privilege is required", e);
        }
        return null;
      }
    });
  }

  /** Test failure of deleting an EZ passing a directory that doesn't exist. */
  @Test(timeout = 30000)
  public void testDeleteEncryptionZoneDirectoryDoesntExist() throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    try {
      dfsAdmin.deleteEncryptionZone(TEST_PATH);
      fail("Directory doesn't exist");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
        "is not the root of an encryption zone", e);
    }
  }

  /** Test failure of deleting an EZ which is not empty. */
  @Test(timeout = 30000)
  public void testDeleteEncryptionZoneAndGenerateKeyDirectoryNotEmpty()
    throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    dfsAdmin.createEncryptionZone(TEST_PATH, null);
    FileSystem.create(fs, new Path("/test/foo"),
      new FsPermission((short) 0777));
    try {
      dfsAdmin.deleteEncryptionZone(TEST_PATH);
      fail("Directory not empty");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("non-empty directory", e);
    }
  }

  /** Test success of deleting an EZ. */
  @Test(timeout = 30000)
  public void testDeleteEncryptionZone()
    throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    dfsAdmin.createEncryptionZone(TEST_PATH, null);
    List<EncryptionZone> zones = dfsAdmin.listEncryptionZones();
    Preconditions.checkState(zones.size() == 1, "More than one zone found?");
    dfsAdmin.deleteEncryptionZone(TEST_PATH);
    zones = dfsAdmin.listEncryptionZones();
    Preconditions.checkState(zones.size() == 0, "More than one zone found?");
  }

  /**
   * Test failure of deleting an EZ on a subdir that is not the root of an EZ.
   */
  @Test(timeout = 30000)
  public void testDeleteEncryptionZoneInExistingEncryptionZone()
    throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    dfsAdmin.createEncryptionZone(TEST_PATH, null);
    FileSystem.mkdirs(fs, TEST_PATH_WITH_CHILD, new FsPermission((short) 0777));
    try {
      dfsAdmin.deleteEncryptionZone(TEST_PATH_WITH_CHILD);
      fail("EZ in an EZ");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
        "is not the root of an encryption zone", e);
    }
  }

  /**
   * Test success of creating and deleting an encryption zone a few levels down.
   */
  @Test(timeout = 30000)
  public void testCreateAndDeleteEncryptionZoneDownAFewLevels()
    throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    dfsAdmin.createEncryptionZone(TEST_PATH, null);
    FileSystem.mkdirs(fs, TEST_PATH_WITH_MULTIPLE_CHILDREN,
      new FsPermission((short) 0777));
    try {
      dfsAdmin.deleteEncryptionZone(TEST_PATH_WITH_MULTIPLE_CHILDREN);
      fail("EZ in an EZ");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
        "is not the root of an encryption zone", e);
    }
    final List<EncryptionZone> zones = dfsAdmin.listEncryptionZones();
    Preconditions.checkState(zones.size() == 1, "More than one zone found?");
    final EncryptionZone ez = zones.get(0);
      GenericTestUtils.assertMatches(ez.toString(),
         "EncryptionZone \\[path=/test, keyId=");
  }

  /** Test failure of creating an EZ using a non-empty directory. */
  @Test(timeout = 30000)
  public void testCreateFileInEncryptionZone() throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH, new FsPermission((short) 0777));
    dfsAdmin.createEncryptionZone(TEST_PATH, null);
    FileSystem.create(fs, TEST_PATH_WITH_CHILD, new FsPermission((short) 0777));

    final List<EncryptionZone> zones = dfsAdmin.listEncryptionZones();
    final EncryptionZone ez = zones.get(0);
      GenericTestUtils.assertMatches(ez.toString(),
         "EncryptionZone \\[path=/test, keyId=");
  }

  /** Test listing encryption zones. */
  @Test(timeout = 30000)
  public void testListEncryptionZones() throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    final int N_EZs = 5;
    final Set<String> ezPathNames = new HashSet<String>(N_EZs);
    for (int i = 0; i < N_EZs; i++) {
      final Path p = new Path(TEST_PATH, "" + i);
      ezPathNames.add(p.toString());
      FileSystem.mkdirs(fs, p, new FsPermission((short) 0777));
      dfsAdmin.createEncryptionZone(p, null);
    }

    final List<EncryptionZone> zones = dfsAdmin.listEncryptionZones();
    Preconditions.checkState(zones.size() == N_EZs, "wrong number of EZs returned");
    for (EncryptionZone z : zones) {
      final String ezPathName = z.getPath();
      Preconditions.checkState(ezPathNames.remove(
          ezPathName), "Path " + ezPathName + " not returned from listEZ");
    }
    Preconditions.checkState(ezPathNames.size() == 0);
  }

  /** Test listing encryption zones as a non super user. */
  @Test(timeout = 30000)
  public void testListEncryptionZonesAsNonSuperUser() throws Exception {
    final HdfsAdmin dfsAdmin =
      new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);

    final UserGroupInformation user = UserGroupInformation.
      createUserForTesting("user", new String[] {"mygroup"});

    final Path TEST_PATH_SUPERUSER_ONLY = new Path(TEST_PATH, "superuseronly");
    final Path TEST_PATH_ALL = new Path(TEST_PATH, "accessall");

    FileSystem.mkdirs(fs, TEST_PATH_SUPERUSER_ONLY,
      new FsPermission((short) 0700));
    dfsAdmin.createEncryptionZone(TEST_PATH_SUPERUSER_ONLY, null);
    FileSystem.mkdirs(fs, TEST_PATH_ALL,
      new FsPermission((short) 0707));
    dfsAdmin.createEncryptionZone(TEST_PATH_ALL, null);

    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        final HdfsAdmin userAdmin =
                new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
        try {
          final List<EncryptionZone> zones = userAdmin.listEncryptionZones();
        } catch (AccessControlException e) {
          GenericTestUtils.assertExceptionContains(
                  "Superuser privilege is required", e);
        }
        return null;
      }
    });
  }

  /** Test success of Rename EZ on a directory which is already an EZ. */
  @Test(timeout = 30000)
  public void testRenameEncryptionZone()
          throws Exception {
    final HdfsAdmin dfsAdmin =
            new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    FileSystem.mkdirs(fs, TEST_PATH_WITH_CHILD,
      new FsPermission((short) 0777));
    dfsAdmin.createEncryptionZone(TEST_PATH_WITH_CHILD, null);
    FileSystem.mkdirs(fs, TEST_PATH_WITH_MULTIPLE_CHILDREN,
       new FsPermission((short) 0777));
    try {
      fs.rename(TEST_PATH_WITH_MULTIPLE_CHILDREN, TEST_PATH);
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
              "/test/foo/baz can't be moved from an encryption zone.", e);
    }
  }
}
