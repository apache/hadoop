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
package org.apache.hadoop.test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public class TestHdfsHelper extends TestDirHelper {

  @Override
  @Test
  public void dummy() {
  }

  public static final String HADOOP_MINI_HDFS = "test.hadoop.hdfs";

  private static final ThreadLocal<Configuration> HDFS_CONF_TL = new InheritableThreadLocal<Configuration>();

  private static final ThreadLocal<Path> HDFS_TEST_DIR_TL = new InheritableThreadLocal<Path>();

  @Override
  public Statement apply(Statement statement, FrameworkMethod frameworkMethod, Object o) {
    TestHdfs testHdfsAnnotation = frameworkMethod.getAnnotation(TestHdfs.class);
    if (testHdfsAnnotation != null) {
      statement = new HdfsStatement(statement, frameworkMethod.getName());
    }
    return super.apply(statement, frameworkMethod, o);
  }

  private static class HdfsStatement extends Statement {
    private Statement statement;
    private String testName;

    public HdfsStatement(Statement statement, String testName) {
      this.statement = statement;
      this.testName = testName;
    }

    @Override
    public void evaluate() throws Throwable {
      MiniDFSCluster miniHdfs = null;
      Configuration conf = HadoopUsersConfTestHelper.getBaseConf();
      if (Boolean.parseBoolean(System.getProperty(HADOOP_MINI_HDFS, "true"))) {
        miniHdfs = startMiniHdfs(conf);
        conf = miniHdfs.getConfiguration(0);
      }
      try {
        HDFS_CONF_TL.set(conf);
        HDFS_TEST_DIR_TL.set(resetHdfsTestDir(conf));
        statement.evaluate();
      } finally {
        HDFS_CONF_TL.remove();
        HDFS_TEST_DIR_TL.remove();
      }
    }

    private static AtomicInteger counter = new AtomicInteger();

    private Path resetHdfsTestDir(Configuration conf) {

      Path testDir = new Path("/tmp/" + testName + "-" +
        counter.getAndIncrement());
      try {
        // currentUser
        FileSystem fs = FileSystem.get(conf);
        fs.delete(testDir, true);
        fs.mkdirs(testDir);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
      return testDir;
    }
  }

  /**
   * Returns the HDFS test directory for the current test, only available when the
   * test method has been annotated with {@link TestHdfs}.
   *
   * @return the HDFS test directory for the current test. It is an full/absolute
   *         <code>Path</code>.
   */
  public static Path getHdfsTestDir() {
    Path testDir = HDFS_TEST_DIR_TL.get();
    if (testDir == null) {
      throw new IllegalStateException("This test does not use @TestHdfs");
    }
    return testDir;
  }

  /**
   * Returns a FileSystemAccess <code>JobConf</code> preconfigured with the FileSystemAccess cluster
   * settings for testing. This configuration is only available when the test
   * method has been annotated with {@link TestHdfs}. Refer to {@link HTestCase}
   * header for details)
   *
   * @return the FileSystemAccess <code>JobConf</code> preconfigured with the FileSystemAccess cluster
   *         settings for testing
   */
  public static Configuration getHdfsConf() {
    Configuration conf = HDFS_CONF_TL.get();
    if (conf == null) {
      throw new IllegalStateException("This test does not use @TestHdfs");
    }
    return new Configuration(conf);
  }

  public static final Path ENCRYPTION_ZONE = new Path("/ez");
  public static final Path ENCRYPTED_FILE = new Path("/ez/encfile");
  public static final Path ERASURE_CODING_DIR = new Path("/ec");
  public static final Path ERASURE_CODING_FILE = new Path("/ec/ecfile");
  public static final ErasureCodingPolicy ERASURE_CODING_POLICY =
      StripedFileTestUtil.getDefaultECPolicy();

  private static MiniDFSCluster MINI_DFS = null;

  private static synchronized MiniDFSCluster startMiniHdfs(Configuration conf) throws Exception {
    if (MINI_DFS == null) {
      if (System.getProperty("hadoop.log.dir") == null) {
        System.setProperty("hadoop.log.dir", new File(TEST_DIR_ROOT, "hadoop-log").getAbsolutePath());
      }
      if (System.getProperty("test.build.data") == null) {
        System.setProperty("test.build.data", new File(TEST_DIR_ROOT, "hadoop-data").getAbsolutePath());
      }

      conf = new Configuration(conf);
      HadoopUsersConfTestHelper.addUserConf(conf);
      conf.set("fs.hdfs.impl.disable.cache", "true");
      conf.set("dfs.block.access.token.enable", "false");
      conf.set("dfs.permissions", "true");
      conf.set("hadoop.security.authentication", "simple");
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
      FileSystemTestHelper helper = new FileSystemTestHelper();
      Path targetFile = new Path(new File(helper.getTestRootDir())
          .getAbsolutePath(), "test.jks");
      final String jceksPath = JavaKeyStoreProvider.SCHEME_NAME + "://file" +
          targetFile.toUri();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
          jceksPath);
      MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
      int totalDataNodes = ERASURE_CODING_POLICY.getNumDataUnits() +
          ERASURE_CODING_POLICY.getNumParityUnits();
      builder.numDataNodes(totalDataNodes);
      MiniDFSCluster miniHdfs = builder.build();
      final String testkey = "testkey";
      DFSTestUtil.createKey(testkey, miniHdfs, conf);

      DistributedFileSystem fileSystem = miniHdfs.getFileSystem();
      fileSystem.enableErasureCodingPolicy(ERASURE_CODING_POLICY.getName());
      fileSystem.getClient().setKeyProvider(miniHdfs.getNameNode()
          .getNamesystem().getProvider());

      fileSystem.mkdirs(new Path("/tmp"));
      fileSystem.mkdirs(new Path("/user"));
      fileSystem.setPermission(new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
      fileSystem.setPermission(new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
      fileSystem.mkdirs(ENCRYPTION_ZONE);
      fileSystem.createEncryptionZone(ENCRYPTION_ZONE, testkey);
      fileSystem.create(ENCRYPTED_FILE).close();

      fileSystem.mkdirs(ERASURE_CODING_DIR);
      fileSystem.setErasureCodingPolicy(ERASURE_CODING_DIR,
          ERASURE_CODING_POLICY.getName());
      fileSystem.create(ERASURE_CODING_FILE).close();

      MINI_DFS = miniHdfs;
    }
    return MINI_DFS;
  }

}
