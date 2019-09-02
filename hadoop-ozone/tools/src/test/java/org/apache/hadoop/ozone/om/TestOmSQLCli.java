/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.scm.cli.SQLCLI;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This class tests the CLI that transforms om.db into SQLite DB files.
 */
@RunWith(Parameterized.class)
public class TestOmSQLCli {
  private MiniOzoneCluster cluster = null;

  private OzoneConfiguration conf;
  private SQLCLI cli;

  private String userName = "userTest";
  private String adminName = "adminTest";
  private String volumeName0 = "volumeTest0";
  private String volumeName1 = "volumeTest1";
  private String bucketName0 = "bucketTest0";
  private String bucketName1 = "bucketTest1";
  private String bucketName2 = "bucketTest2";
  private String keyName0 = "key0";
  private String keyName1 = "key1";
  private String keyName2 = "key2";
  private String keyName3 = "key3";

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        // Uncomment the below line if we support leveldb in future.
        //{OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_LEVELDB},
        {OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB}
    });
  }

  private String metaStoreType;

  public TestOmSQLCli(String type) {
    metaStoreType = type;
  }

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    OzoneBucket bucket0 =
        TestDataUtil.createVolumeAndBucket(cluster, volumeName0, bucketName0);
    OzoneBucket bucket1 =
        TestDataUtil.createVolumeAndBucket(cluster, volumeName1, bucketName1);
    OzoneBucket bucket2 =
        TestDataUtil.createVolumeAndBucket(cluster, volumeName0, bucketName2);

    TestDataUtil.createKey(bucket0, keyName0, "");
    TestDataUtil.createKey(bucket1, keyName1, "");
    TestDataUtil.createKey(bucket2, keyName2, "");
    TestDataUtil.createKey(bucket2, keyName3, "");

    cluster.getOzoneManager().stop();
    cluster.getStorageContainerManager().stop();
    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL, metaStoreType);
    cli = new SQLCLI(conf);
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  // After HDDS-357, we have to fix SQLCli.
  // TODO: fix SQLCli
  @Ignore
  @Test
  public void testOmDB() throws Exception {
    String dbOutPath =  GenericTestUtils.getTempPath(
        UUID.randomUUID() + "/out_sql.db");

    String dbRootPath = conf.get(HddsConfigKeys.OZONE_METADATA_DIRS);
    String dbPath = dbRootPath + "/" + OM_DB_NAME;
    String[] args = {"-p", dbPath, "-o", dbOutPath};

    cli.run(args);

    Connection conn = connectDB(dbOutPath);
    String sql = "SELECT * FROM volumeList";
    ResultSet rs = executeQuery(conn, sql);
    List<String> expectedValues =
        new ArrayList<>(Arrays.asList(volumeName0, volumeName1));
    while (rs.next()) {
      String userNameRs = rs.getString("userName");
      String volumeNameRs = rs.getString("volumeName");
      assertEquals(userName,  userNameRs.substring(1));
      assertTrue(expectedValues.remove(volumeNameRs));
    }
    assertEquals(0, expectedValues.size());

    sql = "SELECT * FROM volumeInfo";
    rs = executeQuery(conn, sql);
    expectedValues =
        new ArrayList<>(Arrays.asList(volumeName0, volumeName1));
    while (rs.next()) {
      String adName = rs.getString("adminName");
      String ownerName = rs.getString("ownerName");
      String volumeName = rs.getString("volumeName");
      assertEquals(adminName, adName);
      assertEquals(userName, ownerName);
      assertTrue(expectedValues.remove(volumeName));
    }
    assertEquals(0, expectedValues.size());

    sql = "SELECT * FROM aclInfo";
    rs = executeQuery(conn, sql);
    expectedValues =
        new ArrayList<>(Arrays.asList(volumeName0, volumeName1));
    while (rs.next()) {
      String adName = rs.getString("adminName");
      String ownerName = rs.getString("ownerName");
      String volumeName = rs.getString("volumeName");
      String type = rs.getString("type");
      String uName = rs.getString("userName");
      String rights = rs.getString("rights");
      assertEquals(adminName, adName);
      assertEquals(userName, ownerName);
      assertEquals("USER", type);
      assertEquals(userName, uName);
      assertEquals("READ_WRITE", rights);
      assertTrue(expectedValues.remove(volumeName));
    }
    assertEquals(0, expectedValues.size());

    sql = "SELECT * FROM bucketInfo";
    rs = executeQuery(conn, sql);
    HashMap<String, String> expectedMap = new HashMap<>();
    expectedMap.put(bucketName0, volumeName0);
    expectedMap.put(bucketName2, volumeName0);
    expectedMap.put(bucketName1, volumeName1);
    while (rs.next()) {
      String volumeName = rs.getString("volumeName");
      String bucketName = rs.getString("bucketName");
      boolean versionEnabled = rs.getBoolean("versionEnabled");
      String storegeType = rs.getString("storageType");
      assertEquals(volumeName, expectedMap.remove(bucketName));
      assertFalse(versionEnabled);
      assertEquals("DISK", storegeType);
    }
    assertEquals(0, expectedMap.size());

    sql = "SELECT * FROM keyInfo";
    rs = executeQuery(conn, sql);
    HashMap<String, List<String>> expectedMap2 = new HashMap<>();
    // no data written, data size will be 0
    expectedMap2.put(keyName0,
        Arrays.asList(volumeName0, bucketName0, "0"));
    expectedMap2.put(keyName1,
        Arrays.asList(volumeName1, bucketName1, "0"));
    expectedMap2.put(keyName2,
        Arrays.asList(volumeName0, bucketName2, "0"));
    expectedMap2.put(keyName3,
        Arrays.asList(volumeName0, bucketName2, "0"));
    while (rs.next()) {
      String volumeName = rs.getString("volumeName");
      String bucketName = rs.getString("bucketName");
      String keyName = rs.getString("keyName");
      int dataSize = rs.getInt("dataSize");
      List<String> vals = expectedMap2.remove(keyName);
      assertNotNull(vals);
      assertEquals(vals.get(0), volumeName);
      assertEquals(vals.get(1), bucketName);
      assertEquals(vals.get(2), Integer.toString(dataSize));
    }
    assertEquals(0, expectedMap2.size());

    conn.close();
    Files.delete(Paths.get(dbOutPath));
  }

  private ResultSet executeQuery(Connection conn, String sql)
      throws SQLException {
    Statement stmt = conn.createStatement();
    return stmt.executeQuery(sql);
  }

  private Connection connectDB(String dbPath) throws Exception {
    Class.forName("org.sqlite.JDBC");
    String connectPath =
        String.format("jdbc:sqlite:%s", dbPath);
    return DriverManager.getConnection(connectPath);
  }
}
