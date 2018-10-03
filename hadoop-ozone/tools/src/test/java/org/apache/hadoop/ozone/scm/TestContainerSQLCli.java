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

import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.block.BlockManagerImpl;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.ozone.scm.cli.SQLCLI;
import org.apache.hadoop.test.GenericTestUtils;
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
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.SCM_CONTAINER_DB;
import static org.apache.hadoop.ozone.OzoneConsts.KB;
import static org.junit.Assert.assertEquals;

/**
 * This class tests the CLI that transforms container into SQLite DB files.
 */
@RunWith(Parameterized.class)
public class TestContainerSQLCli {

  private EventQueue eventQueue;

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

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private String datanodeIpAddress;

  private ContainerManager containerManager;
  private NodeManager nodeManager;
  private BlockManagerImpl blockManager;

  private HashMap<Long, Long> blockContainerMap;

  private final static long DEFAULT_BLOCK_SIZE = 4 * KB;
  private static HddsProtos.ReplicationFactor factor;
  private static HddsProtos.ReplicationType type;
  private static final String CONTAINER_OWNER = "OZONE";


  @Before
  public void setup() throws Exception {
    blockContainerMap = new HashMap<>();

    conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE, 2);
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, ContainerPlacementPolicy.class);
    if(conf.getBoolean(ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT)){
      factor = HddsProtos.ReplicationFactor.THREE;
      type = HddsProtos.ReplicationType.RATIS;
    } else {
      factor = HddsProtos.ReplicationFactor.ONE;
      type = HddsProtos.ReplicationType.STAND_ALONE;
    }
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(2).build();
    cluster.waitForClusterToBeReady();
    datanodeIpAddress = cluster.getHddsDatanodes().get(0)
        .getDatanodeDetails().getIpAddress();
    cluster.getOzoneManager().stop();
    cluster.getStorageContainerManager().stop();
    eventQueue = new EventQueue();
    nodeManager = cluster.getStorageContainerManager().getScmNodeManager();
    containerManager = new SCMContainerManager(conf, nodeManager, 128,
        eventQueue);
    blockManager = new BlockManagerImpl(
        conf, nodeManager, containerManager, eventQueue);
    eventQueue.addHandler(SCMEvents.CHILL_MODE_STATUS, blockManager);
    eventQueue.fireEvent(SCMEvents.CHILL_MODE_STATUS, false);
    GenericTestUtils.waitFor(() -> {
      return !blockManager.isScmInChillMode();
    }, 10, 1000 * 15);
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
        factor, CONTAINER_OWNER);
    blockContainerMap.put(ab1.getBlockID().getLocalID(),
        ab1.getBlockID().getContainerID());

    AllocatedBlock ab2;
    // we want the two blocks on the two provisioned containers respectively,
    // however blockManager picks containers randomly, keep retry until we
    // assign the second block to the other container. This seems to be the only
    // way to get the two containers.
    // although each retry will create a block and assign to a container. So
    // the size of blockContainerMap will vary each time the test is run.
    while (true) {
      ab2 = blockManager
          .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, CONTAINER_OWNER);
      blockContainerMap.put(ab2.getBlockID().getLocalID(),
          ab2.getBlockID().getContainerID());
      if (ab1.getBlockID().getContainerID() !=
          ab2.getBlockID().getContainerID()) {
        break;
      }
    }

    blockManager.close();
    containerManager.close();
    nodeManager.close();

    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL, metaStoreType);
    cli = new SQLCLI(conf);

  }

  @After
  public void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testConvertContainerDB() throws Exception {
    String dbOutPath = GenericTestUtils.getTempPath(
        UUID.randomUUID() + "/out_sql.db");
    // TODO : the following will fail due to empty Datanode list, need to fix.
    //String dnUUID = cluster.getDataNodes().get(0).getUuid();
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
    ArrayList<Long> containerIDs = new ArrayList<>();
    while (rs.next()) {
      containerIDs.add(rs.getLong("containerID"));
      //assertEquals(dnUUID, rs.getString("leaderUUID"));
    }
    /* TODO: fix this later when the SQLCLI is fixed.
    assertTrue(containerIDs.size() == 2 &&
        containerIDs.contains(pipeline1.getContainerName()) &&
        containerIDs.contains(pipeline2.getContainerName()));

    sql = "SELECT * FROM containerMembers";
    rs = executeQuery(conn, sql);
    containerIDs = new ArrayList<>();
    while (rs.next()) {
      containerIDs.add(rs.getLong("containerID"));
      //assertEquals(dnUUID, rs.getString("datanodeUUID"));
    }
    assertTrue(containerIDs.size() == 2 &&
        containerIDs.contains(pipeline1.getContainerName()) &&
        containerIDs.contains(pipeline2.getContainerName()));

    sql = "SELECT * FROM datanodeInfo";
    rs = executeQuery(conn, sql);
    int count = 0;
    while (rs.next()) {
      assertEquals(datanodeIpAddress, rs.getString("ipAddress"));
      //assertEquals(dnUUID, rs.getString("datanodeUUID"));
      count += 1;
    }
    // the two containers maybe on the same datanode, maybe not.
    int expected = pipeline1.getLeader().getUuid().equals(
        pipeline2.getLeader().getUuid())? 1 : 2;
    assertEquals(expected, count);
    */
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
