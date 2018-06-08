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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.CreateEncryptionZoneFlag;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.*;

/**
 * Test after enable Erasure Coding on cluster, exercise Java API make sure they
 * are working as expected.
 *
 */
public class TestErasureCodingExerciseAPIs {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private HdfsAdmin dfsAdmin;
  private FileSystemTestWrapper fsWrapper;
  private static final int BLOCK_SIZE = 1 << 20; // 1MB
  private ErasureCodingPolicy ecPolicy;

  private static ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(TestErasureCodingExerciseAPIs.class);


  @Before
  public void setupCluster() throws IOException {
    ecPolicy = getEcPolicy();
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);

    // Set up java key store
    String testRootDir = Paths.get(new FileSystemTestHelper().getTestRootDir())
        .toString();
    Path targetFile = new Path(new File(testRootDir).getAbsolutePath(),
        "test.jks");
    String keyProviderURI = JavaKeyStoreProvider.SCHEME_NAME + "://file"
        + targetFile.toUri();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        keyProviderURI);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()).
        build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fsWrapper = new FileSystemTestWrapper(fs);
    dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);
    DFSTestUtil.enableAllECPolicies(fs);
    fs.setErasureCodingPolicy(new Path("/"), ecPolicy.getName());
  }

  /**
   * FileSystem.[access, setOwner, setTime] API call should succeed without
   * failure.
   * @throws IOException if any IO operation failed.
   * @throws InterruptedException
   */
  @Test
  public void testAccess() throws IOException, InterruptedException {
    final Path p1 = new Path("/p1");
    final String userName = "user1";
    final String groupName = "group1";

    fs.mkdir(p1, new FsPermission((short) 0444));
    fs.setOwner(p1, userName, groupName);
    UserGroupInformation userGroupInfo = UserGroupInformation
        .createUserForTesting(userName, new String[]{groupName});

    FileSystem userFs = userGroupInfo.doAs(
        (PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(conf));

    userFs.setOwner(p1, userName, groupName);
    userFs.access(p1, FsAction.READ);

    long mtime = System.currentTimeMillis() - 1000L;
    long atime = System.currentTimeMillis() - 2000L;
    fs.setTimes(p1, mtime, atime);
    FileStatus fileStatus = fs.getFileStatus(p1);

    assertEquals(userName, fileStatus.getOwner());
    assertEquals(groupName, fileStatus.getGroup());
    assertEquals(new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ),
        fileStatus.getPermission());
    assertEquals(mtime, fileStatus.getModificationTime());
    assertEquals(atime, fileStatus.getAccessTime());
  }

  /**
   * FileSystem.[setQuota, getQuotaUsage, getContentSummary,
   * setQuotaByStorageType] API call should succeed without failure.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testQuota() throws IOException {
    final Path qDir = new Path("/quotaDir");
    fs.mkdirs(qDir);
    fs.setQuota(qDir, 6, HdfsConstants.QUOTA_DONT_SET);
    QuotaUsage usage = fs.getQuotaUsage(qDir);
    assertEquals(fs.getContentSummary(qDir), usage);

    fs.setQuotaByStorageType(qDir, StorageType.DEFAULT, 10);

  }

  /**
   * FileSystem.[addCachePool, modifyCachePool,removeCachePool] API call
   * should without failure. FileSystem.[addCacheDirective,
   * modifyCacheDirective, removeCacheDirective] are noop.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testCache() throws IOException {
    fs.addCachePool(new CachePoolInfo("pool1"));

    fs.modifyCachePool(new CachePoolInfo("pool1"));
    fs.removeCachePool("pool1");

    fs.addCachePool(new CachePoolInfo("pool1"));

    // Below calls should be noop.
    long id = fs.addCacheDirective(new CacheDirectiveInfo.Builder()
        .setPool("pool1").setPath(new Path("/pool2"))
        .build());
    RemoteIterator<CacheDirectiveEntry> iter = fs.listCacheDirectives(
        new CacheDirectiveInfo.Builder().setPool("pool1").build());
    assertTrue(iter.hasNext());
    assertEquals("pool1", iter.next().getInfo().getPool());

    fs.modifyCacheDirective(new CacheDirectiveInfo.Builder()
        .setId(id).setReplication((short) 2).build());
    fs.removeCacheDirective(id);
  }

  /**
   * FileSystem.[addErasureCodingPolicies, disableErasureCodingPolicy,
   * getErasureCodingPolicy, removeErasureCodingPolicy, setErasureCodingPolicy
   * unsetErasureCodingPolicy] API call still should be succeed without
   * failure.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testErasureCodingPolicy() throws IOException {
    final Path tDir = new Path("/ecpDir");
    fs.mkdirs(tDir);
    ErasureCodingPolicy defaultPolicy
        = SystemErasureCodingPolicies.getPolicies().get(0);
    fs.setErasureCodingPolicy(tDir, defaultPolicy.getName());
    ErasureCodingPolicy fPolicy = fs.getErasureCodingPolicy(tDir);
    assertEquals(defaultPolicy, fPolicy);

    final int cellSize = 1024 * 1024;
    final ECSchema schema = new ECSchema("rs", 5, 3);
    ErasureCodingPolicy newPolicy =
        new ErasureCodingPolicy(schema, cellSize);
    fs.addErasureCodingPolicies(new ErasureCodingPolicy[]{newPolicy});
    assertEquals(SystemErasureCodingPolicies.getPolicies().size() + 1,
        fs.getAllErasureCodingPolicies().size());

    fs.disableErasureCodingPolicy(
        ErasureCodingPolicy.composePolicyName(schema, cellSize));
    assertEquals(SystemErasureCodingPolicies.getPolicies().size() + 1,
        fs.getAllErasureCodingPolicies().size());

    fs.unsetErasureCodingPolicy(tDir);
    fPolicy = fs.getErasureCodingPolicy(tDir);
    assertNotNull(fPolicy);

    fs.removeErasureCodingPolicy(
        ErasureCodingPolicy.composePolicyName(schema, cellSize));
    assertEquals(SystemErasureCodingPolicies.getPolicies().size() + 1,
        fs.getAllErasureCodingPolicies().size());
  }

  /**
   * FileSystem.[getAclStatus, modifyAclEntries, removeAclEntries, removeAcl
   * removeDefaultAcl] API call should succeed without failure.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testACLAPI() throws IOException {
    Path p = new Path("/aclTest");
    fs.mkdirs(p, FsPermission.createImmutable((short) 0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE));
    fs.setAcl(p, aclSpec);

    AclStatus as = fs.getAclStatus(p);

    for (AclEntry entry : aclSpec) {
      assertTrue(String.format("as: %s, entry: %s", as, entry),
          as.getEntries().contains(entry));
    }
    List<AclEntry> maclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, "bar", READ_EXECUTE),
        aclEntry(DEFAULT, USER, "bar", READ_EXECUTE));
    fs.modifyAclEntries(p, maclSpec);

    as = fs.getAclStatus(p);
    for (AclEntry entry : maclSpec) {
      assertTrue(String.format("as: %s, entry: %s", as, entry),
          as.getEntries().contains(entry));
    }

    fs.removeAclEntries(p, maclSpec);
    fs.removeDefaultAcl(p);
    fs.removeAcl(p);
    assertEquals(0, fs.getAclStatus(p).getEntries().size());
  }


  /**
   * FileSystem.[setXAttr, getXAttr, getXAttrs, removeXAttr, listXAttrs] API
   * call should succeed without failure.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testAttr() throws IOException {
    final Path p = new Path("/attrTest");
    fs.mkdirs(p);
    final Path filePath = new Path(p, "file");
    try (DataOutputStream dos = fs.create(filePath)) {
      dos.writeBytes("write something");
    }

    final String name = "user.a1";
    final byte[] value = {0x31, 0x32, 0x33};
    fs.setXAttr(filePath, name, value, EnumSet.of(XAttrSetFlag.CREATE));

    Map<String, byte[]> xattrs = fs.getXAttrs(filePath);
    assertEquals(1, xattrs.size());
    assertArrayEquals(value, xattrs.get(name));
    assertArrayEquals(value, fs.getXAttr(filePath, name));

    List<String> listXAttrs = fs.listXAttrs(filePath);
    assertEquals(1, listXAttrs.size());

    fs.removeXAttr(filePath, name);

    xattrs = fs.getXAttrs(filePath);
    assertEquals(0, xattrs.size());
    listXAttrs = fs.listXAttrs(filePath);
    assertEquals(0, listXAttrs.size());
  }

  /**
   * FileSystem.[allowSnapshot, createSnapshot, deleteSnapshot,
   * renameSnapshot, getSnapshotDiffReport, disallowSnapshot] API call should
   * succeed without failure.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testSnapshotAPI() throws IOException {
    Path p = new Path("/snapshotTest");
    fs.mkdirs(p);
    fs.allowSnapshot(p);

    fs.createSnapshot(p, "s1");
    Path f = new Path("/snapshotTest/f1");
    try (DataOutputStream dos = fs.create(f)) {
      dos.writeBytes("write something");
    }

    fs.createSnapshot(p, "s2");
    fs.renameSnapshot(p, "s2", "s3");
    SnapshotDiffReport report = fs.getSnapshotDiffReport(p, "s1",
        "s3");
    assertEquals("s1", report.getFromSnapshot());
    assertEquals("s3", report.getLaterSnapshotName());

    fs.deleteSnapshot(p, "s1");
    fs.deleteSnapshot(p, "s3");

    fs.disallowSnapshot(p);
  }

  /**
   * FileSystem.[createSymlink, getFileLinkStatus] API call should succeed
   * without failure.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testSymbolicLink() throws IOException {
    Path p = new Path("/slTest");
    fs.mkdirs(p);
    Path f = new Path("/slTest/file");
    try (DataOutputStream dos = fs.create(f)) {
      dos.writeBytes("write something");
    }

    Path sl = new Path("/slTest1/sl");

    fs.createSymlink(f, sl, true);
    assertEquals(fs.getLinkTarget(sl), f);
    FileStatus linkStatus = fs.getFileLinkStatus(sl);
    assertTrue(linkStatus.isSymlink());
  }

  /**
   * FileSystem.[create, open, append, concat, getFileChecksum, rename,
   * delete] API call should succeed without failure.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testFileOpsAPI() throws IOException {
    Path p = new Path("/fileTest");
    fs.mkdirs(p);
    Path f1 = new Path(p, "file1");
    Path fa = new Path(p, "filea");

    try (DataOutputStream dos = fs.create(f1)) {
      dos.writeBytes("create with some content");
    }

    try (DataOutputStream dos = fs.create(fa)) {
      dos.writeBytes("create with some content");
    }

    // setReplication is a noop
    short replication = fs.getDefaultReplication();
    fs.setReplication(f1, (short) 5);
    assertEquals(replication, fs.getDefaultReplication(f1));

    BlockLocation[] locations = fs.getFileBlockLocations(f1, 0, 1);
    assertEquals(1, locations.length);

    FileStatus status1 = fs.getFileStatus(f1);
    assertFalse(status1.isDirectory());
    assertTrue(status1.getPath().toString().contains(p.toString()));
    FileStatus statusa = fs.getFileStatus(fa);
    assertFalse(statusa.isDirectory());
    assertTrue(statusa.getPath().toString().contains(fa.toString()));

    FileStatus[] statuses = fs.listStatus(p);
    assertEquals(2, statuses.length);
    assertEquals(status1, statuses[0]);
    assertEquals(statusa, statuses[1]);

    RemoteIterator<FileStatus> iter = fs.listStatusIterator(p);
    assertEquals(status1, iter.next());
    assertEquals(statusa, iter.next());
    assertFalse(iter.hasNext());

    Path[] concatPs = new Path[]{
        new Path(p, "c1"),
        new Path(p, "c2"),
        new Path(p, "c3"),
    };

    for (Path cp : concatPs) {
      try (DataOutputStream dos = fs.create(cp)) {
        dos.writeBytes("concat some content");
      }
    }
    fs.concat(f1, concatPs);

    FileChecksum checksum1 = fs.getFileChecksum(f1);
    Path f2 = new Path("/fileTest/file2");

    fs.rename(f1, f2);
    FileStatus fileStatus = fs.getFileStatus(f2);
    assertTrue(fileStatus.getPath().toString().contains("/fileTest/file2"));

    FileChecksum checksum2 = fs.getFileChecksum(f2);
    assertEquals(checksum1, checksum2);
    fs.delete(f2, true);

    RemoteIterator<Path> corruptFileBlocks = fs.listCorruptFileBlocks(f2);
    assertFalse(corruptFileBlocks.hasNext());
  }


  /**
   * FileSystem.[createEncryptionZone, getLocatedBlocks, getEZForPath,
   * reencryptEncryptionZone, addDelegationTokens] API call should succeed
   * without failure.
   * @throws IOException if any IO operation failed.
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testEncryptionZone() throws IOException,
      NoSuchAlgorithmException {
    final Path zoneRoot = new Path("ecRoot");
    final Path zonePath = new Path(zoneRoot, "/ec");
    fsWrapper.mkdir(zonePath, FsPermission.getDirDefault(), true);

    final String testKey = "test_key";
    DFSTestUtil.createKey(testKey, cluster, conf);
    final EnumSet<CreateEncryptionZoneFlag> noTrash =
        EnumSet.of(CreateEncryptionZoneFlag.NO_TRASH);
    dfsAdmin.createEncryptionZone(zonePath, testKey, noTrash);

    final Path fp = new Path(zonePath, "encFile");
    DFSTestUtil.createFile(fs, fp, 1 << 13, (short) 1, 0xFEEE);
    LocatedBlocks blocks = fs.getClient().getLocatedBlocks(fp.toString(), 0);
    FileEncryptionInfo fei = blocks.getFileEncryptionInfo();
    assertEquals(testKey, fei.getKeyName());
    EncryptionZone ez = fs.getEZForPath(fp);

    assertEquals(zonePath.toString(), ez.getPath());
    dfsAdmin.reencryptEncryptionZone(zonePath,
        HdfsConstants.ReencryptAction.START);

    Credentials creds = new Credentials();
    final Token<?>[] tokens = fs.addDelegationTokens("JobTracker", creds);
    assertEquals(1, tokens.length);
  }

  /**
   * FileSystem.[setStoragePolicy, unsetStoragePolicy] API call should succeed
   * without failure.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testStoragePolicy() throws IOException {
    Path p = new Path("/storagePolicyTest");
    fs.mkdirs(p);
    final Path sp = new Path(p, "/sp");
    try (DataOutputStream dos = fs.create(sp)) {
      dos.writeBytes("create with some content");
    }

    final BlockStoragePolicySuite suite = BlockStoragePolicySuite
        .createDefaultSuite();
    final BlockStoragePolicy hot = suite.getPolicy("HOT");

    fs.setStoragePolicy(sp, hot.getName());
    assertEquals(fs.getStoragePolicy(sp), hot);

    fs.unsetStoragePolicy(sp);
    assertEquals(fs.getStoragePolicy(sp), hot);
  }

  /**
   * append is not supported in EC.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testAppend() throws IOException {
    Path p = new Path("/fileTest");
    fs.mkdirs(p);
    Path f = new Path("/fileTest/appendFile");

    try (DataOutputStream dos = fs.create(f)) {
      dos.writeBytes("create with some content");
    }

    try {
      fs.append(f);
      fail("append is not supported on erasure coded file");
    } catch (IOException ioe) {
      //Work as expected.
    }
  }


  /**
   * truncate is not supported in EC.
   * @throws IOException if any IO operation failed.
   */
  @Test
  public void testTruncate() throws IOException {
    Path p = new Path("/truncateTest");
    fs.mkdirs(p);
    Path f = new Path("/truncateTest/truncatefile");
    try (DataOutputStream dos = fs.create(f)) {
      dos.writeBytes("create with some content");
    }

    try {
      fs.truncate(f, 0);
      fail("truncate is not supported on erasure coded file.");
    } catch (IOException ex) {
      //Work as expected.
    }
  }

  @After
  public void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
