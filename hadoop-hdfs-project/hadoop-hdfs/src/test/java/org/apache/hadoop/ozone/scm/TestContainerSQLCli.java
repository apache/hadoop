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
package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.scm.cli.SQLCLI;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the CLI that transforms container into SQLite DB files.
 */
public class TestContainerSQLCli {
  private static SQLCLI cli;

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeClass
  public static void init() throws Exception {
    long datanodeCapacities = 3 * OzoneConsts.TB;
    conf = new OzoneConfiguration();
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(1)
        .storageCapacities(new long[] {datanodeCapacities, datanodeCapacities})
        .setHandlerType("distributed").build();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    cluster.waitForHeartbeatProcessed();

    // create two containers to be retrieved later.
    storageContainerLocationClient.allocateContainer(
        "container0");
    storageContainerLocationClient.allocateContainer(
        "container1");

    cluster.shutdown();
    cli = new SQLCLI();
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    IOUtils.cleanup(null, storageContainerLocationClient, cluster);
  }

  @Test
  public void testConvertContainerDB() throws Exception {
    String dbOutPath = cluster.getDataDirectory() + "/out_sql.db";
    // TODO : the following will fail due to empty Datanode list, need to fix.
    //String dnUUID = cluster.getDataNodes().get(0).getDatanodeUuid();
    String dbRootPath = conf.get(OzoneConfigKeys.OZONE_CONTAINER_METADATA_DIRS);
    String dbPath = dbRootPath + "/" + CONTAINER_DB;
    String[] args = {"-p", dbPath, "-o", dbOutPath};
    Connection conn;
    String sql;
    ResultSet rs;

    cli.run(args);

    //verify the sqlite db
    // only checks the container names are as expected. Because other fields
    // such as datanode UUID are generated randomly each time
    conn = connectDB(dbOutPath);
    sql = "SELECT * FROM containerInfo";
    rs = executeQuery(conn, sql);
    ArrayList<String> containerNames = new ArrayList<>();
    while (rs.next()) {
      containerNames.add(rs.getString("containerName"));
      //assertEquals(dnUUID, rs.getString("leaderUUID"));
    }
    assertTrue(containerNames.size() == 2 &&
        containerNames.contains("container0") &&
        containerNames.contains("container1"));

    sql = "SELECT * FROM containerMembers";
    rs = executeQuery(conn, sql);
    containerNames = new ArrayList<>();
    while (rs.next()) {
      containerNames.add(rs.getString("containerName"));
      //assertEquals(dnUUID, rs.getString("datanodeUUID"));
    }
    assertTrue(containerNames.size() == 2 &&
        containerNames.contains("container0") &&
        containerNames.contains("container1"));

    sql = "SELECT * FROM datanodeInfo";
    rs = executeQuery(conn, sql);
    int count = 0;
    while (rs.next()) {
      assertEquals("127.0.0.1", rs.getString("ipAddr"));
      //assertEquals(dnUUID, rs.getString("datanodeUUID"));
      count += 1;
    }
    assertEquals(1, count);
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
