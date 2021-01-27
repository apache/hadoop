/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode.mountmanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.MountInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.FsVolumeReferences;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ProvidedVolumeImpl;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.MountManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.ProvidedVolumeCommand;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;

/**
 * Test to verify creation, transfer and handling of provided volume commands
 * between DN and NN. The NN is expected to send all any provided volume
 * changes (add, remove and refresh) to the DN. This is done via heartbeat
 * response. A new DN is also expected to receive information about volumes
 * which were added to the cluster before the DN was activated.
 */
public class TestProvidedVolHeartbeatResponse {
  private final String sourcePath = "/source";
  private String qualifiedRemotePath;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSNamesystem namesystem;
  private DatanodeRegistration nodeReg;
  private DatanodeDescriptor dd;
  private DataNode dn;
  private NameNode nn;
  private static DataNodeFaultInjector oldInjector;
  /**
   * Used to pause DN BPServiceActor threads. BPSA threads acquire the
   * shared read lock. The test acquires the write lock for exclusive access.
   */
  private static ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private DFSClient client;

  private static final String CONFIG_VALUE = "b";
  private static final String CONFIG_KEY = "a";
  private static final String CONFIG_KEY_2 = "c";
  private static final String CONFIG_VALUE_2 = "d";
  private static final Map<String, String> CONFIG_MAP
      = Collections.singletonMap(CONFIG_KEY, CONFIG_VALUE);
  private static final Map<String, String> CONFIG_MAP_2
      = Collections.singletonMap(CONFIG_KEY_2, CONFIG_VALUE_2);

  @BeforeClass
  public static void setupHeartBeatController() {
    oldInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector.set(new DataNodeFaultInjector() {
      @Override
      public void startOfferService() {
        lock.readLock().lock();
      }
      @Override
      public void endOfferService() {
        lock.readLock().unlock();
      }
    });
  }

  @AfterClass
  public static void resetHeartBeatController() {
    DataNodeFaultInjector.set(oldInjector);
  }

  @Before
  public void createTestCluster() throws IOException {
    conf = new Configuration();
    MiniDFSCluster.setupNamenodeProvidedConfiguration(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).storageTypes(
        new StorageType[] {StorageType.DISK, StorageType.PROVIDED}).build();
    cluster.waitActive();

    namesystem = cluster.getNamesystem();
    String poolId = namesystem.getBlockPoolId();
    nodeReg = InternalDataNodeTestUtils.
        getDNRegistrationForBP(cluster.getDataNodes().get(0), poolId);
    dd = NameNodeAdapter.getDatanode(namesystem, nodeReg);
    nn = cluster.getNameNode(0);
    dn = cluster.getDataNodes().get(0);

    URI clusterURI = cluster.getURI(0);
    qualifiedRemotePath =
        new Path(sourcePath).makeQualified(clusterURI, null).toString();

    client = new DFSClient(nn.getNameNodeAddress(), conf);
    createTempFileInSourcePath();
    verifyMountsCount(0);
  }

  @After
  public void stopCluster() {
    cluster.shutdown();
  }

  @Test
  public void testProvidedVolumeCommandResponse() throws IOException {
    // block any heartbeats to the NN from the DN
    lock.writeLock().lock();
    try {
      client.addMount(qualifiedRemotePath, "/mount", null);

      // expect one add command with one volume
      DatanodeCommand[] cmds = getCommandsForDN();
      assertEquals(1, cmds.length);
      assertTrue(cmds[0] instanceof ProvidedVolumeCommand);
      ProvidedVolumeCommand cmd = (ProvidedVolumeCommand) cmds[0];
      assertEquals(DatanodeProtocol.DNA_PROVIDEDVOLADD, cmd.getAction());
      ProvidedVolumeInfo volume = cmd.getProvidedVolume();
      assertNotNull(volume);
      assertEquals(qualifiedRemotePath, volume.getRemotePath());

      // no new volume, expect no commands
      cmds = getCommandsForDN();
      assertEquals(0, cmds.length);

      // expect command with remote configurations
      client.addMount(qualifiedRemotePath, "/mount2", CONFIG_MAP);
      cmds = getCommandsForDN();
      assertEquals(1, cmds.length);
      assertTrue(cmds[0] instanceof ProvidedVolumeCommand);
      cmd = (ProvidedVolumeCommand) cmds[0];
      assertEquals(DatanodeProtocol.DNA_PROVIDEDVOLADD, cmd.getAction());
      volume = cmd.getProvidedVolume();
      assertNotNull(volume);
      assertEquals(qualifiedRemotePath, volume.getRemotePath());
      assertEquals(1, volume.getConfig().size());
      assertEquals(CONFIG_VALUE, volume.getConfig().get(CONFIG_KEY));

      // expect command to remove mount point
      client.removeMount("/mount");
      cmds = getCommandsForDN();
      assertEquals(1, cmds.length);
      assertTrue(cmds[0] instanceof ProvidedVolumeCommand);
      cmd = (ProvidedVolumeCommand) cmds[0];
      assertEquals(DatanodeProtocol.DNA_PROVIDEDVOLREMOVE, cmd.getAction());
      volume = cmd.getProvidedVolume();
      assertEquals(qualifiedRemotePath, volume.getRemotePath());

      client.removeMount("/mount2");
      cmds = getCommandsForDN();
      assertEquals(1, cmds.length);
      assertTrue(cmds[0] instanceof ProvidedVolumeCommand);
      cmd = (ProvidedVolumeCommand) cmds[0];
      assertEquals(DatanodeProtocol.DNA_PROVIDEDVOLREMOVE, cmd.getAction());
      volume = cmd.getProvidedVolume();
      assertEquals(qualifiedRemotePath, volume.getRemotePath());
    } finally {
      lock.writeLock().unlock();
    }
  }

  private DatanodeCommand[] getCommandsForDN() throws IOException {
    return NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem).getCommands();
  }

  @Test
  public void testMultipleCmdsTransferOnNN() throws IOException {
    // block any heartbeats to the NN from the DN
    lock.writeLock().lock();
    try {
      client.addMount(qualifiedRemotePath, "/mount", CONFIG_MAP);
      client.addMount(qualifiedRemotePath, "/mount2", CONFIG_MAP_2);

      // expect two add command one each for the mount points added above
      DatanodeCommand[] cmds = getCommandsForDN();
      assertEquals(2, cmds.length);
      List<String> expectedKeys
          = new ArrayList<>(Arrays.asList(CONFIG_KEY, CONFIG_KEY_2));
      List<String> expectedValues
          = new ArrayList<>(Arrays.asList(CONFIG_VALUE, CONFIG_VALUE_2));
      for (DatanodeCommand cmdInstance : cmds) {
        assertTrue(cmdInstance instanceof ProvidedVolumeCommand);
        ProvidedVolumeCommand cmd = (ProvidedVolumeCommand) cmdInstance;
        assertEquals(DatanodeProtocol.DNA_PROVIDEDVOLADD, cmd.getAction());
        ProvidedVolumeInfo volume = cmd.getProvidedVolume();
        assertNotNull(volume);
        assertEquals(qualifiedRemotePath, volume.getRemotePath());
        assertNotNull(volume.getConfig());
        assertEquals(1, volume.getConfig().size());
        expectedKeys.remove(volume.getConfig().keySet().iterator().next());
        expectedValues.remove(volume.getConfig().values().iterator().next());
      }
      assertEquals(0, expectedKeys.size());
      assertEquals(0, expectedValues.size());

      // expect no new commands
      cmds = getCommandsForDN();
      assertEquals(0, cmds.length);

      // expect multiple remove volume commands
      client.removeMount("/mount");
      client.removeMount("/mount2");
      cmds = getCommandsForDN();
      assertEquals(2, cmds.length);
      for (DatanodeCommand cmd : cmds) {
        assertTrue(cmd instanceof ProvidedVolumeCommand);
        assertEquals(DatanodeProtocol.DNA_PROVIDEDVOLREMOVE, cmd.getAction());
        ProvidedVolumeInfo volume =
            ((ProvidedVolumeCommand) cmd).getProvidedVolume();
        assertEquals(qualifiedRemotePath, volume.getRemotePath());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Test
  public void testMultipleCmdsHandlingOnDN() throws Exception {
    Map<String, FsVolumeSpi> oldVolumes= getVolumeIdMap();
    assertEquals(2, oldVolumes.size());
    verifyConfAbsence(oldVolumes, CONFIG_KEY);
    verifyConfAbsence(oldVolumes, CONFIG_KEY_2);

    assertEquals(1, cluster.getDataNodes().size());

    lock.writeLock().lock();
    try {
      client.addMount(qualifiedRemotePath, "/mount", CONFIG_MAP);
      client.addMount(qualifiedRemotePath, "/mount2", CONFIG_MAP_2);

      // verify mounts were created
      List<MountInfo> mountInfos =
          client.listMounts(false).getMountInfos();
      assertEquals(2, mountInfos.size());
      Map<String, String> mountMap = mountInfos.stream().collect(
          Collectors.toMap(p -> p.getMountPath(), p -> p.getRemotePath()));
      assertEquals(qualifiedRemotePath, mountMap.get("/mount"));
      assertEquals(qualifiedRemotePath, mountMap.get("/mount2"));
    } finally {
      lock.writeLock().unlock();
    }

    triggerHeartbeats();
    Map<String, FsVolumeSpi> allVolumes = getVolumeIdMap();
    assertEquals(4, allVolumes.size());

    verifyConfAbsence(oldVolumes, CONFIG_KEY);
    verifyConfAbsence(oldVolumes, CONFIG_KEY_2);

    verifyMountsCount(2);
    verifyNNProvidedVolsExistInDN(dn);

    lock.writeLock().lock();
    try {
      client.removeMount("/mount");
      client.removeMount("/mount2");

      List<MountInfo> mountPaths =
          client.listMounts(false).getMountInfos();
      assertEquals(0, mountPaths.size());
    } finally {
      lock.writeLock().unlock();
    }

    triggerHeartbeats();
    allVolumes = getVolumeIdMap();
    assertEquals(2, allVolumes.size());
  }

  /**
   * Mounts two provided volumes with different configs. The test verifies that
   * the volumes are created with their own config and the configs are not
   * distributed to other volumes.
   */
  @Test
  public void testMultipleConfsHandlingOnDN() throws Exception {
    Map<String, FsVolumeSpi> oldVolumes = getVolumeIdMap();
    assertEquals(2, oldVolumes.size());
    verifyConfAbsence(oldVolumes, CONFIG_KEY);
    verifyConfAbsence(oldVolumes, CONFIG_KEY_2);

    assertEquals(1, cluster.getDataNodes().size());

    lock.writeLock().lock();
    try {
      client.addMount(qualifiedRemotePath, "/mount", CONFIG_MAP);
    } finally {
      lock.writeLock().unlock();
    }
    triggerHeartbeats();
    Map<String, FsVolumeSpi> allVolumes1 = getVolumeIdMap();
    assertEquals(3, allVolumes1.size());
    FsVolumeSpi provideVol1 = getNewlyAddedProvidedVol(oldVolumes, allVolumes1);
    verifyMountsCount(1);
    verifyNNProvidedVolsExistInDN(dn);

    lock.writeLock().lock();
    try {
      client.addMount(qualifiedRemotePath, "/mount2", CONFIG_MAP_2);
    } finally {
      lock.writeLock().unlock();
    }
    triggerHeartbeats();
    Map<String, FsVolumeSpi> allVolumes2 = getVolumeIdMap();
    assertEquals(4, allVolumes2.size());
    FsVolumeSpi providedVol2
        = getNewlyAddedProvidedVol(allVolumes1, allVolumes2);
    verifyMountsCount(2);
    verifyNNProvidedVolsExistInDN(dn);

    verifyConfAbsence(oldVolumes, CONFIG_KEY);
    verifyConfAbsence(oldVolumes, CONFIG_KEY_2);

    verifyProvidedVolConfigOnDN(CONFIG_VALUE, CONFIG_KEY, provideVol1);
    verifyProvidedVolConfigOnDN(null, CONFIG_KEY_2, provideVol1);
    verifyProvidedVolConfigOnDN(CONFIG_VALUE_2, CONFIG_KEY_2, providedVol2);
    verifyProvidedVolConfigOnDN(null, CONFIG_KEY, providedVol2);
  }

  @Test
  public void testProvidedVolResponseProto() throws Exception {
    Map<String, FsVolumeSpi> oldVolumes= getVolumeIdMap();
    assertEquals(2, oldVolumes.size());
    verifyConfAbsence(oldVolumes, CONFIG_KEY);

    lock.writeLock().lock();
    try {
      client.addMount(qualifiedRemotePath, "/mount", CONFIG_MAP);
    } finally {
      lock.writeLock().unlock();
    }

    triggerHeartbeats();
    Map<String, FsVolumeSpi> allVolumes = getVolumeIdMap();
    assertEquals(3, allVolumes.size());

    // verify a new volume is created with the provided vol config
    FsVolumeSpi newVolume = getNewlyAddedProvidedVol(oldVolumes, allVolumes);
    verifyProvidedVolConfigOnDN(CONFIG_VALUE, CONFIG_KEY, newVolume);

    // verify conf of old volumes is unchanged
    verifyConfAbsence(oldVolumes, CONFIG_KEY);

    // verify mount ids
    verifyMountsCount(1);
    verifyNNProvidedVolsExistInDN(dn);

    // read remote file to verify block pool
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    FileStatus[] status = fileSystem.listStatus(new Path("/mount"));
    assertEquals(1, status.length);
    FSDataInputStream stream = fileSystem.open(status[0].getPath());
    byte[] content = new byte[100];
    stream.read(content, 0, content.length);

    // test remove mount proto
    lock.writeLock().lock();
    try {
      client.removeMount("/mount");
    } finally {
      lock.writeLock().unlock();
    }

    triggerHeartbeats();
    allVolumes = getVolumeIdMap();
    assertEquals(2, allVolumes.size());
  }

  @Test
  public void testNewDnResponseHasAllVolumes() throws Exception {
    Map<String, FsVolumeSpi> oldVolumes = getVolumeIdMap();
    assertEquals(2, oldVolumes.size());
    verifyConfAbsence(oldVolumes, CONFIG_KEY);

    lock.writeLock().lock();
    try {
      client.addMount(qualifiedRemotePath, "/mount", CONFIG_MAP);
    } finally {
      lock.writeLock().unlock();
    }
    triggerHeartbeats();
    Map<String, FsVolumeSpi> allVolumes = getVolumeIdMap();
    assertEquals(3, allVolumes.size());

    // verify a new volume is created with the provided vol config
    FsVolumeSpi newVolume = getNewlyAddedProvidedVol(oldVolumes, allVolumes);
    verifyProvidedVolConfigOnDN(CONFIG_VALUE, CONFIG_KEY, newVolume);
    String newVolumeStorageID = newVolume.getStorageID();

    // verify conf of old volumes is unchanged
    verifyConfAbsence(oldVolumes, CONFIG_KEY);

    assertEquals(1, cluster.getDataNodes().size());

    // add one PROVIDED DN.
    StorageType[] storages =
        new StorageType[] {StorageType.DISK, StorageType.PROVIDED};
    cluster.startDataNodes(conf, 1,
        new StorageType[][] {storages}, true,
        HdfsServerConstants.StartupOption.REGULAR,
        null, null, null, null, false, false, false, null);
    cluster.waitActive();
    assertEquals(2, cluster.getDataNodes().size());

    ArrayList<DataNode> dns = cluster.getDataNodes();
    DataNode dn2 = null;
    for (DataNode dataNode : dns) {
      if (dataNode != dn) {
        dn2 = dataNode;
      }
    }
    assertNotNull(dn2);
    triggerHeartbeats();

    allVolumes = getVolumeIdMap(dn2);
    assertEquals(3, allVolumes.size());
    verifyMountsCount(1);
    verifyNNProvidedVolsExistInDN(dn2);

    // find the mounted volume in the new DN
    List<FsVolumeSpi> otherVols = new ArrayList<>();
    for (FsVolumeSpi vol : allVolumes.values()) {
      if (vol.getStorageID().equals(newVolumeStorageID)) {
        newVolume = vol;
      } else {
        otherVols.add(vol);
      }
    }
    assertEquals(2, otherVols.size());
    assertNotNull(newVolume);

    verifyProvidedVolConfigOnDN(CONFIG_VALUE, CONFIG_KEY, newVolume);
    for (FsVolumeSpi vol : otherVols) {
      verifyProvidedVolConfigOnDN(null, CONFIG_KEY, vol);
    }
  }

  @Test
  public void testVolumeResponseHandlingOnDN() throws Exception {
    Map<String, FsVolumeSpi> oldVolumes = getVolumeIdMap();
    assertEquals(2, oldVolumes.size());
    verifyConfAbsence(oldVolumes, CONFIG_KEY);

    ProvidedVolumeInfo volume = new ProvidedVolumeInfo(UUID.randomUUID(),
        null, qualifiedRemotePath, CONFIG_MAP);
    DatanodeCommand[] cmds = new DatanodeCommand[] {
        ProvidedVolumeCommand.buildAddCmd(volume)};

    DatanodeProtocolClientSideTranslatorPB spyNN =
        InternalDataNodeTestUtils.spyOnBposToNN(dn, nn);
    lock.writeLock().lock();
    try {
      NNHAStatusHeartbeat ha = new NNHAStatusHeartbeat(HAServiceState.ACTIVE,
          nn.getFSImage().getLastAppliedOrWrittenTxId());
      HeartbeatResponse response =
          new HeartbeatResponse(cmds, ha, null,
              ThreadLocalRandom.current().nextLong() | 1L);
      doReturn(response).when(spyNN).sendHeartbeat(any(), any(), anyLong(),
          anyLong(), anyInt(), anyInt(), anyInt(), any(),
          anyBoolean(), any(SlowPeerReports.class),
          any(SlowDiskReports.class), null);
    } finally {
      lock.writeLock().unlock();
    }

    triggerHeartbeats();
    Map<String, FsVolumeSpi> allVolumes = getVolumeIdMap();
    assertEquals(3, allVolumes.size());

    // verify a new volume is created with the provided vol config
    FsVolumeSpi newVolume = getNewlyAddedProvidedVol(oldVolumes, allVolumes);
    assertEquals(volume.getId(), newVolume.getStorageID());
    verifyProvidedVolConfigOnDN(CONFIG_VALUE, CONFIG_KEY, newVolume);

    // verify conf of old volumes is unchanged
    verifyConfAbsence(oldVolumes, CONFIG_KEY);

    // simulate remove volume command and verify volume removal
    cmds = new DatanodeCommand[] {ProvidedVolumeCommand.buildRemoveCmd(volume)};
    lock.writeLock().lock();
    try {
      NNHAStatusHeartbeat ha = new NNHAStatusHeartbeat(HAServiceState.ACTIVE,
          nn.getFSImage().getLastAppliedOrWrittenTxId());
      HeartbeatResponse response =
          new HeartbeatResponse(cmds, ha, null,
              ThreadLocalRandom.current().nextLong() | 1L);
      doReturn(response).when(spyNN).sendHeartbeat(any(), any(), anyLong(),
          anyLong(), anyInt(), anyInt(), anyInt(), any(),
          anyBoolean(), any(SlowPeerReports.class),
          any(SlowDiskReports.class), null);
    } finally {
      lock.writeLock().unlock();
    }

    triggerHeartbeats();
    allVolumes = getVolumeIdMap();
    assertEquals(2, allVolumes.size());
    verifyConfAbsence(oldVolumes, CONFIG_KEY);
    for (String id : allVolumes.keySet()) {
      assertNotEquals(newVolume.getStorageID(), id);
    }
  }

  /**
   * Verifies a given config key is missing in the the given volumes.
   * @param volumes volumes to be checked
   * @param key
   */
  private void verifyConfAbsence(Map<String, FsVolumeSpi> volumes, String key) {
    for (FsVolumeSpi oldVolume : volumes.values()) {
      verifyProvidedVolConfigOnDN(null, key, oldVolume);
    }
  }

  /**
   * Finds and verifies that a new provided vol has been created and is of
   * provided storage type.
   * @param beforeVols All volumes existing before add command.
   * @param afterVols All volumes existing after add command.
   * @return The newly added provided volume.
   */
  private FsVolumeSpi getNewlyAddedProvidedVol(
      Map<String, FsVolumeSpi> beforeVols, Map<String, FsVolumeSpi> afterVols) {
    Map<String, FsVolumeSpi> afterVolsCopy = new HashMap<>(afterVols);
    for (String volId : beforeVols.keySet()) {
      afterVolsCopy.remove(volId);
    }

    assertFalse("New provided vol not found", afterVolsCopy.isEmpty());
    assertEquals(1, afterVolsCopy.size());
    FsVolumeSpi newVolume = afterVolsCopy.values().iterator().next();
    assertEquals(StorageType.PROVIDED, newVolume.getStorageType());
    return newVolume;
  }

  private Map<String, FsVolumeSpi> getVolumeIdMap() throws IOException {
    return getVolumeIdMap(dn);
  }
  private Map<String, FsVolumeSpi> getVolumeIdMap(DataNode node)
      throws IOException {
    FsVolumeReferences volumes = node.getFSDataset().getFsVolumeReferences();
    Map<String, FsVolumeSpi> volIdMap = new HashMap<>();
    for (FsVolumeSpi vol : volumes) {
      volIdMap.put(vol.getStorageID(), vol);
    }
    volumes.close();
    return volIdMap;
  }

  /**
   * Verifies existence of volumes corresponding to a mounts in the
   * {@link MountManager}.
   * @param dn The datanode whose volumes need to be inspected
   */
  private void verifyNNProvidedVolsExistInDN(DataNode dataNode)
      throws IOException {
    Map<Path, ProvidedVolumeInfo> mounts =
        nn.getNamesystem().getMountManager().getMountPoints();

    Map<String, FsVolumeSpi> volumes = getVolumeIdMap(dataNode);

    for (ProvidedVolumeInfo volumeInfo : mounts.values()) {
      assertTrue(volumes.containsKey(volumeInfo.getId()));
    }
  }

  /**
   * Verifies the number of mounts in the mount manager.
   * @param count The expected number of mounts
   */
  private void verifyMountsCount(int count) throws IOException {
    Map<Path, ProvidedVolumeInfo> mounts =
        nn.getNamesystem().getMountManager().getMountPoints();
    assertEquals(count, mounts.size());
  }

  /**
   * Verifies presence of a specific key=value config in a DN provided config.
   * @param value The expected value, null if the key should be missing.
   * @param key The expected key.
   * @param vol The volume whose config needs to be verified.
   */
  private void verifyProvidedVolConfigOnDN(String value, String key,
      FsVolumeSpi vol) {
    if (!vol.getStorageType().equals(StorageType.PROVIDED)) {
      return;
    }

    String configValue = ((ProvidedVolumeImpl) vol).getConfigValue(key);
    assertEquals(value, configValue);
  }

  /**
   * Creates a temporary file in the mini hdfs instance to be used as source
   * for mounting.
   */
  private void createTempFileInSourcePath() throws IOException {
    Path child = new Path(UUID.randomUUID().toString());
    Path testFile = new Path(sourcePath, child);
    OutputStream out = cluster.getFileSystem().create(testFile);
    for (int i = 0; i < 10; ++i) {
      out.write(UUID.randomUUID().toString().getBytes());
    }
    out.close();
  }

  /**
   * Triggers heartbeats and sleeps for tiny window to allow DNs to process
   * the response.
   */
  private void triggerHeartbeats() throws Exception {
    cluster.triggerHeartbeats();
    Thread.sleep(100);
  }
}