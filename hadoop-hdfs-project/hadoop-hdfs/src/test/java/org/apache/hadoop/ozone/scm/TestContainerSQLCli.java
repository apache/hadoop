/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright containerOwnership.  The ASF licenses this file
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

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.scm.block.BlockManagerImpl;
import org.apache.hadoop.ozone.scm.cli.SQLCLI;
import org.apache.hadoop.ozone.scm.container.ContainerMapping;
import org.apache.hadoop.ozone.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.ozone.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

import static org.apache.hadoop.ozone.OzoneConsts.BLOCK_DB;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CONTAINER_DB;
import static org.apache.hadoop.ozone.OzoneConsts.KB;
import static org.apache.hadoop.ozone.OzoneConsts.NODEPOOL_DB;
//import static org.apache.hadoop.ozone.OzoneConsts.OPEN_CONTAINERS_DB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the CLI that transforms container into SQLite DB files.
 */
@RunWith(Parameterized.class)
public class TestContainerSQLCli {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_LEVELDB},
        {OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB}
    });
  }

  private static String metaStoreType;

  public TestContainerSQLCli(String type) {
    metaStoreType = type;
  }

  private static SQLCLI cli;

  private MiniOzoneClassicCluster cluster;
  private OzoneConfiguration conf;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  private ContainerMapping mapping;
  private NodeManager nodeManager;
  private BlockManagerImpl blockManager;

  private Pipeline pipeline1;
  private Pipeline pipeline2;

  private HashMap<String, String> blockContainerMap;

  private final static long DEFAULT_BLOCK_SIZE = 4 * KB;
  private static OzoneProtos.ReplicationFactor factor;
  private static OzoneProtos.ReplicationType type;
  private static final String containerOwner = "OZONE";


  @Before
  public void setup() throws Exception {
    long datanodeCapacities = 3 * OzoneConsts.TB;
    blockContainerMap = new HashMap<>();

    conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE, 2);
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, ContainerPlacementPolicy.class);
    if(conf.getBoolean(ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT)){
      factor = OzoneProtos.ReplicationFactor.THREE;
      type = OzoneProtos.ReplicationType.RATIS;
    } else {
      factor = OzoneProtos.ReplicationFactor.ONE;
      type = OzoneProtos.ReplicationType.STAND_ALONE;
    }
    cluster = new MiniOzoneClassicCluster.Builder(conf).numDataNodes(2)
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

    // loop until both the two datanodes are up, try up to about 4 seconds.
    for (int c = 0; c < 40; c++) {
      if (nodeManager.getAllNodes().size() == 2) {
        break;
      }
      Thread.sleep(100);
    }
    assertEquals(2, nodeManager.getAllNodes().size());
    AllocatedBlock ab1 = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE, type,
        factor, containerOwner);
    pipeline1 = ab1.getPipeline();
    blockContainerMap.put(ab1.getKey(), pipeline1.getContainerName());

    AllocatedBlock ab2;
    // we want the two blocks on the two provisioned containers respectively,
    // however blockManager picks containers randomly, keep retry until we
    // assign the second block to the other container. This seems to be the only
    // way to get the two containers.
    // although each retry will create a block and assign to a container. So
    // the size of blockContainerMap will vary each time the test is run.
    while (true) {
      ab2 = blockManager
          .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, containerOwner);
      pipeline2 = ab2.getPipeline();
      blockContainerMap.put(ab2.getKey(), pipeline2.getContainerName());
      if (!pipeline1.getContainerName().equals(pipeline2.getContainerName())) {
        break;
      }
    }

    blockManager.close();
    mapping.close();
    nodeManager.close();

    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL, metaStoreType);
    cli = new SQLCLI(conf);

  }

  @After
  public void shutdown() throws InterruptedException {
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient, cluster);
  }

  @Test
  public void testConvertBlockDB() throws Exception {
    String dbOutPath = cluster.getDataDirectory() + "/out_sql.db";
    String dbRootPath = conf.get(OzoneConfigKeys.OZONE_METADATA_DIRS);
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
  public void testConvertNodepoolDB() throws Exception {
    String dbOutPath = cluster.getDataDirectory() + "/out_sql.db";
    String dbRootPath = conf.get(OzoneConfigKeys.OZONE_METADATA_DIRS);
    String dbPath = dbRootPath + "/" + NODEPOOL_DB;
    String[] args = {"-p", dbPath, "-o", dbOutPath};

    cli.run(args);

    // verify the sqlite db
    HashMap<String, String> expectedPool = new HashMap<>();
    for (DatanodeID dnid : nodeManager.getAllNodes()) {
      expectedPool.put(dnid.getDatanodeUuid(), "DefaultNodePool");
    }
    Connection conn = connectDB(dbOutPath);
    String sql = "SELECT * FROM nodePool";
    ResultSet rs = executeQuery(conn, sql);
    while(rs.next()) {
      String datanodeUUID = rs.getString("datanodeUUID");
      String poolName = rs.getString("poolName");
      assertTrue(expectedPool.remove(datanodeUUID).equals(poolName));
    }
    assertEquals(0, expectedPool.size());

    Files.delete(Paths.get(dbOutPath));
  }

  @Test
  public void testConvertContainerDB() throws Exception {
    String dbOutPath = cluster.getDataDirectory() + "/out_sql.db";
    // TODO : the following will fail due to empty Datanode list, need to fix.
    //String dnUUID = cluster.getDataNodes().get(0).getDatanodeUuid();
    String dbRootPath = conf.get(OzoneConfigKeys.OZONE_METADATA_DIRS);
    String dbPath = dbRootPath + "/" + SCM_CONTAINER_DB;
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
        containerNames.contains(pipeline1.getContainerName()) &&
        containerNames.contains(pipeline2.getContainerName()));

    sql = "SELECT * FROM containerMembers";
    rs = executeQuery(conn, sql);
    containerNames = new ArrayList<>();
    while (rs.next()) {
      containerNames.add(rs.getString("containerName"));
      //assertEquals(dnUUID, rs.getString("datanodeUUID"));
    }
    assertTrue(containerNames.size() == 2 &&
        containerNames.contains(pipeline1.getContainerName()) &&
        containerNames.contains(pipeline2.getContainerName()));

    sql = "SELECT * FROM datanodeInfo";
    rs = executeQuery(conn, sql);
    int count = 0;
    while (rs.next()) {
      assertEquals("127.0.0.1", rs.getString("ipAddr"));
      //assertEquals(dnUUID, rs.getString("datanodeUUID"));
      count += 1;
    }
    // the two containers maybe on the same datanode, maybe not.
    int expected = pipeline1.getLeader().getDatanodeUuid().equals(
        pipeline2.getLeader().getDatanodeUuid())? 1 : 2;
    assertEquals(expected, count);
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
