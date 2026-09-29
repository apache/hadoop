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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This class tests the ACLs system through the full code path.  It overlaps
 * slightly with the ACL tests in common, but the approach is more holistic.
 *
 * <b>NOTE:</b> Because of the mechanics of JAXP, when the KMS config files are
 * written to disk, a config param with a blank value ("") will be written in a
 * way that the KMS will read as unset, which is different from blank. For this
 * reason, when testing the effects of blank config params, this test class
 * sets the values of those config params to a space (" ").  A whitespace value
 * will be preserved by JAXP when writing out the config files and will be
 * interpreted by KMS as a blank value. (The KMS strips whitespace from ACL
 * values before interpreting them.)
 */
public class TestAclsEndToEnd {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestAclsEndToEnd.class.getName());
  private static final String TEXT =
      "The blue zone is for loading and unloading only. "
      + "Please park in the red zone.";
  private static final Path ZONE1 = new Path("/tmp/BLUEZONE");
  private static final Path ZONE2 = new Path("/tmp/REDZONE");
  private static final Path ZONE3 = new Path("/tmp/LOADINGZONE");
  private static final Path ZONE4 = new Path("/tmp/UNLOADINGZONE");
  private static final Path FILE1 = new Path(ZONE1, "file1");
  private static final Path FILE1A = new Path(ZONE1, "file1a");
  private static final Path FILE2 = new Path(ZONE2, "file2");
  private static final Path FILE3 = new Path(ZONE3, "file3");
  private static final Path FILE4 = new Path(ZONE4, "file4");
  private static final String KEY1 = "key1";
  private static final String KEY2 = "key2";
  private static final String KEY3 = "key3";
  private static UserGroupInformation realUgi;
  private static String realUser;

  private MiniKMS miniKMS;
  private File kmsDir;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @BeforeAll
  public static void captureUser() throws IOException {
    realUgi = UserGroupInformation.getCurrentUser();
    realUser = System.getProperty("user.name");
  }

  /**
   * Extract the URI for the miniKMS.
   *
   * @return the URI for the miniKMS
   */
  private String getKeyProviderURI() {
    return KMSClientProvider.SCHEME_NAME + "://" +
        miniKMS.getKMSUrl().toExternalForm().replace("://", "@");
  }

  /**
   * Write out the config files needed by the miniKMS.  The miniKMS doesn't
   * provide a way to set the configs directly, so the only way to pass config
   * parameters is to write them out into config files.
   *
   * @param confDir the directory into which to write the configs
   * @param conf the config to write.
   * @throws IOException
   */
  private void writeConf(File confDir, Configuration conf)
      throws IOException {
    URI keystore = new Path(kmsDir.getAbsolutePath(), "kms.keystore").toUri();

    conf.set(KMSConfiguration.KEY_PROVIDER_URI, "jceks://file@" + keystore);
    conf.set("hadoop.kms.authentication.type", "simple");

    Writer writer =
        new FileWriter(new File(confDir, KMSConfiguration.KMS_SITE_XML));
    conf.writeXml(writer);
    writer.close();

    writer = new FileWriter(new File(confDir, KMSConfiguration.KMS_ACLS_XML));
    conf.writeXml(writer);
    writer.close();

    //create empty core-site.xml
    writer = new FileWriter(new File(confDir, "core-site.xml"));
    new Configuration(false).writeXml(writer);
    writer.close();
  }

  /**
   * Setup a fresh miniKMS and miniDFS.
   *
   * @param conf the configuration to use for both the miniKMS and miniDFS
   * @throws Exception thrown if setup fails
   */
  private void setup(Configuration conf) throws Exception {
    setup(conf, true, true);
  }

  /**
   * Setup a fresh miniDFS and a miniKMS.  The resetKms parameter controls
   * whether the miniKMS will start fresh or reuse the existing data.
   *
   * @param conf the configuration to use for both the miniKMS and miniDFS
   * @param resetKms whether to start a fresh miniKMS
   * @throws Exception thrown if setup fails
   */
  private void setup(Configuration conf, boolean resetKms) throws Exception {
    setup(conf, resetKms, true);
  }

  /**
   * Setup a miniDFS and miniKMS.  The resetKms and resetDfs parameters control
   * whether the services will start fresh or reuse the existing data.
   *
   * @param conf the configuration to use for both the miniKMS and miniDFS
   * @param resetKms whether to start a fresh miniKMS
   * @param resetDfs whether to start a fresh miniDFS
   * @throws Exception thrown if setup fails
   */
  private void setup(Configuration conf, boolean resetKms, boolean resetDfs)
          throws Exception {
    if (resetKms) {
      FileSystemTestHelper fsHelper = new FileSystemTestHelper();

      kmsDir = new File(fsHelper.getTestRootDir()).getAbsoluteFile();

      assertTrue(kmsDir.mkdirs());
    }

    writeConf(kmsDir, conf);

    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();

    miniKMS = miniKMSBuilder.setKmsConfDir(kmsDir).build();
    miniKMS.start();

    conf = new HdfsConfiguration();

    // Set up java key store
    conf.set(ProxyUsers.CONF_HADOOP_PROXYUSER + "." + realUser + ".users",
        "keyadmin,hdfs,user");
    conf.set(ProxyUsers.CONF_HADOOP_PROXYUSER + "." + realUser + ".hosts",
        "*");
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        getKeyProviderURI());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);

    MiniDFSCluster.Builder clusterBuilder = new MiniDFSCluster.Builder(conf);

    cluster = clusterBuilder.numDataNodes(1).format(resetDfs).build();
    fs = cluster.getFileSystem();
  }

  /**
   * Stop the miniKMS and miniDFS.
   */
  private void teardown() {
    // Restore login user
    UserGroupInformation.setLoginUser(realUgi);

    if (cluster != null) {
      cluster.shutdown();
    }

    miniKMS.stop();
  }

  /**
   * Return a new {@link Configuration} with KMS ACLs appropriate to pass the
   * full ACL test in {@link #doFullAclTest()} set.
   *
   * @param hdfsUgi the hdfs user
   * @param keyadminUgi the keyadmin user
   * @return the configuration
   */
  private static Configuration getBaseConf(UserGroupInformation hdfsUgi,
      UserGroupInformation keyadminUgi) {
    Configuration conf = new Configuration();

    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        keyadminUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DELETE",
        keyadminUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.ROLLOVER",
        keyadminUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET", " ");
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_KEYS",
        keyadminUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        hdfsUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.SET_KEY_MATERIAL", " ");
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        hdfsUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK", "*");

    return conf;
  }

  /**
   * Set the recommended blacklists.
   *
   * @param hdfsUgi the hdfs user
   */
  private static void setBlacklistAcls(Configuration conf,
      UserGroupInformation hdfsUgi) {

    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.CREATE",
        hdfsUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.DELETE",
        hdfsUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.ROLLOVER",
        hdfsUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.GET", "*");
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.SET_KEY_MATERIAL",
        "*");
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.DECRYPT_EEK",
        hdfsUgi.getUserName());
  }

  /**
   * Set the key ACLs appropriate to pass the full ACL test in
   * {@link #doFullAclTest()} using the specified prefix.  The prefix should
   * either be "whitelist.key.acl." or "key.acl.key1.".
   *
   * @param conf the configuration
   * @param prefix the ACL prefix
   * @param hdfsUgi the hdfs user
   * @param keyadminUgi the keyadmin user
   * @param userUgi the normal user
   */
  private static void setKeyAcls(Configuration conf, String prefix,
      UserGroupInformation hdfsUgi,
      UserGroupInformation keyadminUgi,
      UserGroupInformation userUgi) {

    conf.set(prefix + "MANAGEMENT", keyadminUgi.getUserName());
    conf.set(prefix + "READ", hdfsUgi.getUserName());
    conf.set(prefix + "GENERATE_EEK", hdfsUgi.getUserName());
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + KEY1 + ".DECRYPT_EEK",
        userUgi.getUserName());
  }

  /**
   * Test the full life cycle of a key using a config with whitelist key ACLs.
   * The configuration used is the correct configuration to pass the full ACL
   * test in {@link #doFullAclTest()}.
   *
   * @throws Exception thrown on test failure
   */
  @Test
  public void testGoodWithWhitelist() throws Exception {
    UserGroupInformation hdfsUgi =
        UserGroupInformation.createProxyUserForTesting("hdfs",
          realUgi, new String[] {"supergroup"});
    UserGroupInformation keyadminUgi =
        UserGroupInformation.createProxyUserForTesting("keyadmin",
          realUgi, new String[] {"keyadmin"});
    UserGroupInformation userUgi =
        UserGroupInformation.createProxyUserForTesting("user",
          realUgi,  new String[] {"staff"});

    Configuration conf = getBaseConf(hdfsUgi, keyadminUgi);

    setBlacklistAcls(conf, hdfsUgi);
    setKeyAcls(conf, KMSConfiguration.WHITELIST_KEY_ACL_PREFIX,
        hdfsUgi, keyadminUgi, userUgi);
    doFullAclTest(conf, hdfsUgi, keyadminUgi, userUgi);
  }

  /**
   * Test the full life cycle of a key using a config with key ACLs.
   * The configuration used is the correct configuration to pass the full ACL
   * test in {@link #doFullAclTest()}.
   *
   * @throws Exception thrown on test failure
   */
  @Test
  public void testGoodWithKeyAcls() throws Exception {
    UserGroupInformation hdfsUgi =
        UserGroupInformation.createProxyUserForTesting("hdfs",
          realUgi, new String[] {"supergroup"});
    UserGroupInformation keyadminUgi =
        UserGroupInformation.createProxyUserForTesting("keyadmin",
          realUgi, new String[] {"keyadmin"});
    UserGroupInformation userUgi =
        UserGroupInformation.createProxyUserForTesting("user",
          realUgi,  new String[] {"staff"});
    Configuration conf = getBaseConf(hdfsUgi, keyadminUgi);

    setBlacklistAcls(conf, hdfsUgi);
    setKeyAcls(conf, KeyAuthorizationKeyProvider.KEY_ACL + KEY1 + ".",
        hdfsUgi, keyadminUgi, userUgi);
    doFullAclTest(conf, hdfsUgi, keyadminUgi, userUgi);
  }

  /**
   * Test the full life cycle of a key using a config with whitelist key ACLs
   * and without blacklist ACLs.  The configuration used is the correct
   * configuration to pass the full ACL test in {@link #doFullAclTest()}.
   *
   * @throws Exception thrown on test failure
   */
  @Test
  public void testGoodWithWhitelistWithoutBlacklist() throws Exception {
    UserGroupInformation hdfsUgi =
        UserGroupInformation.createProxyUserForTesting("hdfs",
          realUgi, new String[] {"supergroup"});
    UserGroupInformation keyadminUgi =
        UserGroupInformation.createProxyUserForTesting("keyadmin",
          realUgi, new String[] {"keyadmin"});
    UserGroupInformation userUgi =
        UserGroupInformation.createProxyUserForTesting("user",
          realUgi,  new String[] {"staff"});
    Configuration conf = getBaseConf(hdfsUgi, keyadminUgi);

    setKeyAcls(conf, KMSConfiguration.WHITELIST_KEY_ACL_PREFIX,
        hdfsUgi, keyadminUgi, userUgi);
    doFullAclTest(conf, hdfsUgi, keyadminUgi, userUgi);
  }

  /**
   * Test the full life cycle of a key using a config with whitelist key ACLs
   * and without blacklist ACLs. The configuration used is the correct
   * configuration to pass the full ACL test in {@link #doFullAclTest()}.
   *
   * @throws Exception thrown on test failure
   */
  @Test
  public void testGoodWithKeyAclsWithoutBlacklist() throws Exception {
    UserGroupInformation hdfsUgi =
        UserGroupInformation.createProxyUserForTesting("hdfs",
          realUgi, new String[] {"supergroup"});
    UserGroupInformation keyadminUgi =
        UserGroupInformation.createProxyUserForTesting("keyadmin",
          realUgi, new String[] {"keyadmin"});
    UserGroupInformation userUgi =
        UserGroupInformation.createProxyUserForTesting("user",
          realUgi,  new String[] {"staff"});
    Configuration conf = getBaseConf(hdfsUgi, keyadminUgi);

    setKeyAcls(conf, KeyAuthorizationKeyProvider.KEY_ACL + KEY1 + ".",
        hdfsUgi, keyadminUgi, userUgi);
    doFullAclTest(conf, hdfsUgi, keyadminUgi, userUgi);
  }

  /**
   * Run a full key life cycle test using the provided configuration and users.
   *
   * @param conf the configuration
   * @param hdfs the user to use as the hdfs user
   * @param keyadmin the user to use as the keyadmin user
   * @param user the user to use as the normal user
   * @throws Exception thrown if there is a test failure
   */
  private void doFullAclTest(final Configuration conf,
      final UserGroupInformation hdfsUgi,
      final UserGroupInformation keyadminUgi,
      final UserGroupInformation userUgi) throws Exception {

    try {
      setup(conf);

      // Create a test key
      assertTrue(createKey(keyadminUgi, KEY1, conf),
          "Exception during creation of key " + KEY1 + " by " + keyadminUgi.getUserName());

      // Fail to create a test key
      assertFalse(createKey(hdfsUgi, KEY2, conf),
          "Allowed creation of key " + KEY2 + " by " + hdfsUgi.getUserName());
      assertFalse(createKey(userUgi, KEY2, conf),
          "Allowed creation of key " + KEY2 + " by " + userUgi.getUserName());

      // Create a directory and chown it to the normal user.
      fs.mkdirs(ZONE1);
      fs.setOwner(ZONE1, userUgi.getUserName(),
          userUgi.getPrimaryGroupName());

      // Create an EZ
      assertTrue(createEncryptionZone(hdfsUgi, KEY1, ZONE1),
          "Exception during creation of EZ " + ZONE1 + " by "
          + hdfsUgi.getUserName() + " using key " + KEY1);

      // Fail to create an EZ
      assertFalse(createEncryptionZone(keyadminUgi, KEY1, ZONE2),
          "Allowed creation of EZ " + ZONE2 + " by "
          + keyadminUgi.getUserName() + " using key " + KEY1);
      assertFalse(createEncryptionZone(userUgi, KEY1, ZONE2),
          "Allowed creation of EZ " + ZONE2 + " by "
          + userUgi.getUserName() + " using key " + KEY1);

      // Create a file in the zone
      assertTrue(createFile(userUgi, FILE1, TEXT),
          "Exception during creation of file " + FILE1 + " by "
              + userUgi.getUserName());

      // Fail to create a file in the zone
      assertFalse(createFile(hdfsUgi, FILE1A, TEXT),
          "Allowed creation of file " + FILE1A + " by "
              + hdfsUgi.getUserName());
      assertFalse(createFile(keyadminUgi, FILE1A, TEXT),
          "Allowed creation of file " + FILE1A + " by "
              + keyadminUgi.getUserName());

      // Read a file in the zone
      assertTrue(compareFile(userUgi, FILE1, TEXT),
          "Exception while reading file " + FILE1 + " by "
              + userUgi.getUserName());

      // Fail to read a file in the zone
      assertFalse(compareFile(hdfsUgi, FILE1, TEXT),
          "Allowed reading of file " + FILE1 + " by "
              + hdfsUgi.getUserName());
      assertFalse(compareFile(keyadminUgi, FILE1, TEXT),
          "Allowed reading of file " + FILE1 + " by "
              + keyadminUgi.getUserName());

      // Remove the zone
      fs.delete(ZONE1, true);

      // Fail to remove the key
      assertFalse(deleteKey(hdfsUgi, KEY1), "Allowed deletion of file " + FILE1 + " by "
          + hdfsUgi.getUserName());
      assertFalse(deleteKey(userUgi, KEY1), "Allowed deletion of file " + FILE1 + " by "
          + userUgi.getUserName());

      // Remove
      assertTrue(deleteKey(keyadminUgi, KEY1),
          "Exception during deletion of file " + FILE1 + " by "
              + keyadminUgi.getUserName());
    } finally {
      fs.delete(ZONE1, true);
      fs.delete(ZONE2, true);
      teardown();
    }
  }

  /**
   * Test that key creation is correctly governed by ACLs.
   * @throws Exception thrown if setup fails
   */
  @Test
  public void testCreateKey() throws Exception {
    Configuration conf = new Configuration();

    // Correct config with whitelist ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf);

      assertTrue(createKey(realUgi, KEY1, conf),
          "Exception during key creation with correct config using whitelist key ACLs");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Correct config with default ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf);

      assertTrue(createKey(realUgi, KEY2, conf),
          "Exception during key creation with correct config using default key ACLs");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because of blacklist
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.CREATE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf);

      assertFalse(createKey(realUgi, KEY3, conf),
          "Allowed key creation with blacklist for CREATE");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Missing KMS ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE", " ");
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf);

      assertFalse(createKey(realUgi, KEY3, conf),
          "Allowed key creation without CREATE KMS ACL");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Missing key ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        realUgi.getUserName());

    try {
      setup(conf);

      assertFalse(createKey(realUgi, KEY3, conf),
          "Allowed key creation without MANAGMENT key ACL");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because the key ACL set ignores the default ACL set for key3
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + KEY3 + ".DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf);

      assertFalse(createKey(realUgi, KEY3, conf),
          "Allowed key creation when default key ACL should have been"
              + " overridden by key ACL");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Allowed because the default setting for KMS ACLs is fully permissive
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf);

      assertTrue(createKey(realUgi, KEY3, conf),
          "Exception during key creation with default KMS ACLs");
    } finally {
      teardown();
    }
  }

  /**
   * Test that zone creation is correctly governed by ACLs.
   * @throws Exception thrown if setup fails
   */
  @Test
  public void testCreateEncryptionZone() throws Exception {
    Configuration conf = new Configuration();

    // Create a test key
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf);

      assertTrue(createKey(realUgi, KEY1, conf),
          "Exception during key creation");
    } finally {
      teardown();
    }

    // We tear everything down and then restart it with the ACLs we want to
    // test so that there's no contamination from the ACLs needed for setup.
    // To make that work, we have to tell the setup() method not to create a
    // new KMS directory.
    conf = new Configuration();

    // Correct config with whitelist ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE1);

      assertTrue(createEncryptionZone(realUgi, KEY1, ZONE1),
          "Exception during zone creation with correct config using"
              + " whitelist key ACLs");
    } finally {
      fs.delete(ZONE1, true);
      teardown();
    }

    conf = new Configuration();

    // Correct config with default ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE2);

      assertTrue(createEncryptionZone(realUgi, KEY1, ZONE2),
          "Exception during zone creation with correct config using"
              + " default key ACLs");
    } finally {
      fs.delete(ZONE2, true);
      teardown();
    }

    conf = new Configuration();

    // Denied because the key ACL set ignores the default ACL set for key1
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + KEY1 + ".DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE3);

      assertFalse(createEncryptionZone(realUgi, KEY1, ZONE3),
          "Allowed creation of zone when default key ACLs should have"
              + " been overridden by key ACL");
    } finally {
      fs.delete(ZONE3, true);
      teardown();
    }

    conf = new Configuration();

    // Correct config with blacklist
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE3);

      assertFalse(createEncryptionZone(realUgi, KEY1, ZONE3),
          "Allowed zone creation of zone with blacklisted GET_METADATA");
    } finally {
      fs.delete(ZONE3, true);
      teardown();
    }

    conf = new Configuration();

    // Correct config with blacklist
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE3);

      assertFalse(createEncryptionZone(realUgi, KEY1, ZONE3),
          "Allowed zone creation of zone with blacklisted GENERATE_EEK");
    } finally {
      fs.delete(ZONE3, true);
      teardown();
    }

    conf = new Configuration();

    // Missing KMS ACL but works because defaults for KMS ACLs are fully
    // permissive
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE3);

      assertTrue(createEncryptionZone(realUgi, KEY1, ZONE3),
          "Exception during zone creation with default KMS ACLs");
    } finally {
      fs.delete(ZONE3, true);
      teardown();
    }

    conf = new Configuration();

    // Missing GET_METADATA KMS ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA", " ");
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE4);

      assertFalse(createEncryptionZone(realUgi, KEY1, ZONE4),
          "Allowed zone creation without GET_METADATA KMS ACL");
    } finally {
      fs.delete(ZONE4, true);
      teardown();
    }

    conf = new Configuration();

    // Missing GET_METADATA KMS ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK", " ");
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE4);

      assertFalse(createEncryptionZone(realUgi, KEY1, ZONE4),
          "Allowed zone creation without GENERATE_EEK KMS ACL");
    } finally {
      fs.delete(ZONE4, true);
      teardown();
    }

    conf = new Configuration();

    // Missing READ key ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE4);

      assertFalse(createEncryptionZone(realUgi, KEY1, ZONE4),
          "Allowed zone creation without READ ACL");
    } finally {
      fs.delete(ZONE4, true);
      teardown();
    }

    conf = new Configuration();

    // Missing GENERATE_EEK key ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());

    try {
      setup(conf, false);

      fs.mkdirs(ZONE4);

      assertFalse(createEncryptionZone(realUgi, KEY1, ZONE4),
          "Allowed zone creation without GENERATE_EEK ACL");
    } finally {
      fs.delete(ZONE4, true);
      teardown();
    }
  }

  /**
   * Test that in-zone file creation is correctly governed by ACLs.
   * @throws Exception thrown if setup fails
   */
  @Test
  public void testCreateFileInEncryptionZone() throws Exception {
    Configuration conf = new Configuration();

    // Create a test key
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    assertTrue(new File(kmsDir, "kms.keystore").length() == 0);

    try {
      setup(conf);

      assertTrue(createKey(realUgi, KEY1, conf),
          "Exception during key creation");
      fs.mkdirs(ZONE1);
      assertTrue(createEncryptionZone(realUgi, KEY1, ZONE1),
          "Exception during zone creation");
      fs.mkdirs(ZONE2);
      assertTrue(createEncryptionZone(realUgi, KEY1, ZONE2),
          "Exception during zone creation");
      fs.mkdirs(ZONE3);
      assertTrue(createEncryptionZone(realUgi, KEY1, ZONE3),
          "Exception during zone creation");
      fs.mkdirs(ZONE4);
      assertTrue(createEncryptionZone(realUgi, KEY1, ZONE4),
          "Exception during zone creation");
    } catch (Throwable ex) {
      fs.delete(ZONE1, true);
      fs.delete(ZONE2, true);
      fs.delete(ZONE3, true);
      fs.delete(ZONE4, true);

      throw ex;
    } finally {
      teardown();
    }

    // We tear everything down and then restart it with the ACLs we want to
    // test so that there's no contamination from the ACLs needed for setup.
    // To make that work, we have to tell the setup() method not to create a
    // new KMS directory or DFS dierctory.

    conf = new Configuration();

    // Correct config with whitelist ACLs
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertTrue(createFile(realUgi, FILE1, TEXT),
          "Exception during file creation with correct config" + " using whitelist ACL");
    } finally {
      fs.delete(ZONE1, true);
      teardown();
    }

    conf = new Configuration();

    // Correct config with default ACLs
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertTrue(createFile(realUgi, FILE2, TEXT),
          "Exception during file creation with correct config using whitelist ACL");
    } finally {
      fs.delete(ZONE2, true);
      teardown();
    }

    conf = new Configuration();

    // Denied because the key ACL set ignores the default ACL set for key1
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + KEY1 + ".READ",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(createFile(realUgi, FILE3, TEXT),
          "Allowed file creation when default key ACLs should have been overridden by key ACL");
    } catch (Exception ex) {
      fs.delete(ZONE3, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied by blacklist
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(createFile(realUgi, FILE3, TEXT),
          "Allowed file creation with blacklist for GENERATE_EEK");
    } catch (Exception ex) {
      fs.delete(ZONE3, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied by blacklist
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(createFile(realUgi, FILE3, TEXT),
          "Allowed file creation with blacklist for DECRYPT_EEK");
    } catch (Exception ex) {
      fs.delete(ZONE3, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Allowed because default KMS ACLs are fully permissive
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertTrue(createFile(realUgi, FILE3, TEXT),
          "Exception during file creation with default KMS ACLs");
    } catch (Exception ex) {
      fs.delete(ZONE3, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because of missing GENERATE_EEK KMS ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK", " ");
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(createFile(realUgi, FILE4, TEXT),
          "Allowed file creation without GENERATE_EEK KMS ACL");
    } catch (Exception ex) {
      fs.delete(ZONE3, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because of missing DECRYPT_EEK KMS ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK", " ");
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(createFile(realUgi, FILE3, TEXT),
          "Allowed file creation without DECRYPT_EEK KMS ACL");
    } catch (Exception ex) {
      fs.delete(ZONE3, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because of missing GENERATE_EEK key ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(createFile(realUgi, FILE3, TEXT),
          "Allowed file creation without GENERATE_EEK key ACL");
    } catch (Exception ex) {
      fs.delete(ZONE3, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because of missing DECRYPT_EEK key ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(createFile(realUgi, FILE3, TEXT),
          "Allowed file creation without DECRYPT_EEK key ACL");
    } catch (Exception ex) {
      fs.delete(ZONE3, true);

      throw ex;
    } finally {
      teardown();
    }
  }

  /**
   * Test that in-zone file read is correctly governed by ACLs.
   * @throws Exception thrown if setup fails
   */
  @Test
  public void testReadFileInEncryptionZone() throws Exception {
    Configuration conf = new Configuration();

    // Create a test key
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GET_METADATA",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "READ",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    assertTrue(new File(kmsDir, "kms.keystore").length() == 0);

    try {
      setup(conf);

      assertTrue(createKey(realUgi, KEY1, conf),
          "Exception during key creation");
      fs.mkdirs(ZONE1);
      assertTrue(createEncryptionZone(realUgi, KEY1, ZONE1),
          "Exception during zone creation");
      assertTrue(createFile(realUgi, FILE1, TEXT),
          "Exception during file creation");
    } catch (Throwable ex) {
      fs.delete(ZONE1, true);

      throw ex;
    } finally {
      teardown();
    }

    // We tear everything down and then restart it with the ACLs we want to
    // test so that there's no contamination from the ACLs needed for setup.
    // To make that work, we have to tell the setup() method not to create a
    // new KMS directory or DFS dierctory.

    conf = new Configuration();

    // Correct config with whitelist ACLs
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertTrue(compareFile(realUgi, FILE1, TEXT),
          "Exception while reading file with correct config with whitelist ACLs");
    } catch (Throwable ex) {
      fs.delete(ZONE1, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Correct config with default ACLs
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertTrue(compareFile(realUgi, FILE1, TEXT),
          "Exception while reading file with correct config with default ACLs");
    } catch (Throwable ex) {
      fs.delete(ZONE1, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because the key ACL set ignores the default ACL set for key1
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + KEY1 + ".READ",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(compareFile(realUgi, FILE1, TEXT),
          "Allowed file read when default key ACLs should have been overridden by key ACL");
    } catch (Throwable ex) {
      fs.delete(ZONE1, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied by blacklist
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.DECRYPT_EEK",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(compareFile(realUgi, FILE1, TEXT),
          "Allowed file read with blacklist for DECRYPT_EEK");
    } catch (Throwable ex) {
      fs.delete(ZONE1, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Allowed because default KMS ACLs are fully permissive
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertTrue(compareFile(realUgi, FILE1, TEXT),
          "Exception while reading file with default KMS ACLs");
    } catch (Throwable ex) {
      fs.delete(ZONE1, true);

      throw ex;
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because of missing DECRYPT_EEK KMS ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DECRYPT_EEK", " ");
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false, false);

      assertFalse(compareFile(realUgi, FILE1, TEXT),
          "Allowed file read without DECRYPT_EEK KMS ACL");
    } catch (Throwable ex) {
      fs.delete(ZONE1, true);

      throw ex;
    } finally {
      teardown();
    }

    // Denied because of missing DECRYPT_EEK key ACL
    conf = new Configuration();

    try {
      setup(conf, false, false);

      assertFalse(compareFile(realUgi, FILE1, TEXT),
          "Allowed file read without DECRYPT_EEK key ACL");
    } catch (Throwable ex) {
      fs.delete(ZONE1, true);

      throw ex;
    } finally {
      teardown();
    }
  }

  /**
   * Test that key deletion is correctly governed by ACLs.
   * @throws Exception thrown if setup fails
   */
  @Test
  public void testDeleteKey() throws Exception {
    Configuration conf = new Configuration();

    // Create a test key
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.CREATE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf);

      assertTrue(createKey(realUgi, KEY1, conf),
          "Exception during key creation");
      assertTrue(createKey(realUgi, KEY2, conf),
          "Exception during key creation");
      assertTrue(createKey(realUgi, KEY3, conf),
          "Exception during key creation");
    } finally {
      teardown();
    }

    // We tear everything down and then restart it with the ACLs we want to
    // test so that there's no contamination from the ACLs needed for setup.
    // To make that work, we have to tell the setup() method not to create a
    // new KMS directory.

    conf = new Configuration();

    // Correct config with whitelist ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DELETE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf, false);

      assertTrue(deleteKey(realUgi, KEY1), "Exception during key deletion with correct config"
          + " using whitelist key ACLs");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Correct config with default ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DELETE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf, false);

      assertTrue(deleteKey(realUgi, KEY2), "Exception during key deletion with correct config"
          + " using default key ACLs");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because of blacklist
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DELETE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.CONFIG_PREFIX + "blacklist.DELETE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf, false);

      assertFalse(deleteKey(realUgi, KEY3),
          "Allowed key deletion with blacklist for DELETE");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Missing KMS ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DELETE", " ");
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf, false);

      assertFalse(deleteKey(realUgi, KEY3),
          "Allowed key deletion without DELETE KMS ACL");
    } finally {
      teardown();
    }

    conf = new Configuration();


    // Missing key ACL
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DELETE",
        realUgi.getUserName());

    try {
      setup(conf, false);

      assertFalse(deleteKey(realUgi, KEY3),
          "Allowed key deletion without MANAGMENT key ACL");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Denied because the key ACL set ignores the default ACL set for key3
    conf.set(KMSConfiguration.CONFIG_PREFIX + "acl.DELETE",
        realUgi.getUserName());
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + KEY3 + ".DECRYPT_EEK",
        realUgi.getUserName());

    try {
      setup(conf, false);

      assertFalse(deleteKey(realUgi, KEY3),
          "Allowed key deletion when default key ACL should have been overridden by key ACL");
    } finally {
      teardown();
    }

    conf = new Configuration();

    // Allowed because the default setting for KMS ACLs is fully permissive
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT",
        realUgi.getUserName());

    try {
      setup(conf, false);

      assertTrue(deleteKey(realUgi, KEY3),
          "Exception during key deletion with default KMS ACLs");
    } finally {
      teardown();
    }
  }

  /**
   * Create a key as the specified user.
   *
   * @param ugi the target user
   * @param key the target key
   * @param conf the configuration
   * @return whether the key creation succeeded
   */
  private boolean createKey(UserGroupInformation ugi, final String key,
      final Configuration conf) {

    return doUserOp(ugi, new UserOp() {
      @Override
      public void execute() throws IOException {
        try {
          DFSTestUtil.createKey(key, cluster, conf);
        } catch (NoSuchAlgorithmException ex) {
          throw new IOException(ex);
        }
      }
    });
  }

  /**
   * Create a zone as the specified user.
   *
   * @param ugi the target user
   * @param key the target key
   * @param zone the target zone
   * @return whether the zone creation succeeded
   */
  private boolean createEncryptionZone(UserGroupInformation ugi,
      final String key, final Path zone) {

    return doUserOp(ugi, new UserOp() {
      @Override
      public void execute() throws IOException {
        cluster.getFileSystem().createEncryptionZone(zone, key);
      }
    });
  }

  /**
   * Create a file as the specified user.
   *
   * @param ugi the target user
   * @param file the target file
   * @param text the target file contents
   * @return whether the file creation succeeded
   */
  private boolean createFile(UserGroupInformation ugi,
      final Path file, final String text) {

    return doUserOp(ugi, new UserOp() {
      @Override
      public void execute() throws IOException {
        FSDataOutputStream dout = cluster.getFileSystem().create(file);
        PrintWriter out = new PrintWriter(new OutputStreamWriter(dout));

        out.println(text);
        out.close();
      }
    });
  }

  /**
   * Read a file as the specified user and compare the contents to expectations.
   *
   * @param ugi the target user
   * @param file the target file
   * @param text the expected file contents
   * @return true if the file read succeeded and the contents were as expected
   */
  private boolean compareFile(UserGroupInformation ugi,
      final Path file, final String text) {

    return doUserOp(ugi, new UserOp() {
      @Override
      public void execute() throws IOException {
        FSDataInputStream din =  cluster.getFileSystem().open(file);
        BufferedReader in = new BufferedReader(new InputStreamReader(din));

        assertEquals(text, in.readLine(),
            "The text read does not match the text written");
      }
    });
  }

  /**
   * Delete a key as the specified user.
   *
   * @param ugi the target user
   * @param key the target key
   * @return whether the key deletion succeeded
   */
  private boolean deleteKey(UserGroupInformation ugi, final String key)
      throws IOException, InterruptedException {

    return doUserOp(ugi, new UserOp() {
      @Override
      public void execute() throws IOException {
        cluster.getNameNode().getNamesystem().getProvider().deleteKey(key);
      }
    });
  }

  /**
   * Perform an operation as the given user.  This method requires setting the
   * login user. This method does not restore the login user to the setting
   * from prior to the method call.
   *
   * @param ugi the target user
   * @param op the operation to perform
   * @return true if the operation succeeded without throwing an exception
   */
  private boolean doUserOp(UserGroupInformation ugi, final UserOp op) {
    UserGroupInformation.setLoginUser(ugi);

    // Create a test key
    return ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          op.execute();

          return true;
        } catch (IOException ex) {
          LOG.error("IOException thrown during doAs() operation", ex);

          return false;
        }
      }
    });
  }

  /**
   * Simple interface that defines an operation to perform.
   */
  private static interface UserOp {
    public void execute() throws IOException;
  }
}
