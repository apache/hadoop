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
import org.apache.hadoop.ozone.scm.block.BlockManagerImpl;
import org.apache.hadoop.ozone.scm.cli.SQLCLI;
import org.apache.hadoop.ozone.scm.container.ContainerMapping;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
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
import java.util.HashMap;

import static org.apache.hadoop.ozone.OzoneConsts.BLOCK_DB;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB;
import static org.apache.hadoop.ozone.OzoneConsts.KB;
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

  private static ContainerMapping mapping;
  private static NodeManager nodeManager;
  private static BlockManagerImpl blockManager;

  private static String pipelineName1;
  private static String pipelineName2;

  private static HashMap<String, String> blockContainerMap;

  private final static long DEFAULT_BLOCK_SIZE = 4 * KB;

  @BeforeClass
  public static void init() throws Exception {
    long datanodeCapacities = 3 * OzoneConsts.TB;
    blockContainerMap = new HashMap<>();

    conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE, 2);
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(1)
        .storageCapacities(new long[] {datanodeCapacities, datanodeCapacities})
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    cluster.waitForHeartbeatProcessed();
    cluster.shutdown();

    nodeManager = cluster.getStorageContainerManager().getScmNodeManager();
    mapping = new ContainerMapping(conf, nodeManager, 128);
    blockManager = new BlockManagerImpl(conf, nodeManager, mapping, 128);

    // blockManager.allocateBlock() will create containers if there is none
    // stored in levelDB. The number of containers to create is the value of
    // OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE which we set to 2.
    // so the first allocateBlock() will create two containers. A random one
    // is assigned for the block.
    AllocatedBlock ab1 = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE);
    pipelineName1 = ab1.getPipeline().getContainerName();
    blockContainerMap.put(ab1.getKey(), pipelineName1);

    AllocatedBlock ab2;
    // we want the two blocks on the two provisioned containers respectively,
    // however blockManager picks containers randomly, keep retry until we
    // assign the second block to the other container. This seems to be the only
    // way to get the two containers.
    // although each retry will create a block and assign to a container. So
    // the size of blockContainerMap will vary each time the test is run.
    while (true) {
      ab2 = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE);
      pipelineName2 = ab2.getPipeline().getContainerName();
      blockContainerMap.put(ab2.getKey(), pipelineName2);
      if (!pipelineName2.equals(pipelineName1)) {
        break;
      }
    }

    blockManager.close();
    mapping.close();
    nodeManager.close();

    cli = new SQLCLI();
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    IOUtils.cleanup(null, storageContainerLocationClient, cluster);
  }

  @Test
  public void testConvertBlockDB() throws Exception {
    String dbOutPath = cluster.getDataDirectory() + "/out_sql.db";
    String dbRootPath = conf.get(OzoneConfigKeys.OZONE_CONTAINER_METADATA_DIRS);
    String dbPath = dbRootPath + "/" + BLOCK_DB;
    String[] args = {"-p", dbPath, "-o", dbOutPath};

    cli.run(args);

    Connection conn = connectDB(dbOutPath);
    String sql = "SELECT * FROM blockContainer";
    ResultSet rs = executeQuery(conn, sql);
    while(rs.next()) {
      String blockKey = rs.getString("blockKey");
      String containerName = rs.getString("containerName");
      assertTrue(blockContainerMap.containsKey(blockKey) &&
          blockContainerMap.remove(blockKey).equals(containerName));
    }
    assertEquals(0, blockContainerMap.size());
    Files.delete(Paths.get(dbOutPath));
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
        containerNames.contains(pipelineName1) &&
        containerNames.contains(pipelineName2));

    sql = "SELECT * FROM containerMembers";
    rs = executeQuery(conn, sql);
    containerNames = new ArrayList<>();
    while (rs.next()) {
      containerNames.add(rs.getString("containerName"));
      //assertEquals(dnUUID, rs.getString("datanodeUUID"));
    }
    assertTrue(containerNames.size() == 2 &&
        containerNames.contains(pipelineName1) &&
        containerNames.contains(pipelineName2));

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
