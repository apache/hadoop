package org.apache.hadoop.hdds.scm.container.metrics;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.fail;

/**
 * Class used to test {@link SCMContainerManagerMetrics}.
 */
public class TestSCMContainerManagerMetrics {

  private MiniOzoneCluster cluster;
  private StorageContainerManager scm;
  private XceiverClientManager xceiverClientManager;
  private String containerOwner = "OZONE";

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    xceiverClientManager = new XceiverClientManager(conf);
  }


  @After
  public void teardown() {
    cluster.shutdown();
  }

  @Test
  public void testContainerOpsMetrics() throws IOException {
    MetricsRecordBuilder metrics;
    ContainerManager containerManager = scm.getContainerManager();
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());

    long numSuccessfulCreateContainers = getLongCounter(
        "NumSuccessfulCreateContainers", metrics);

    ContainerInfo containerInfo = containerManager.allocateContainer(
        HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, containerOwner);

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assert.assertEquals(getLongCounter("NumSuccessfulCreateContainers",
        metrics), ++numSuccessfulCreateContainers);

    try {
      containerManager.allocateContainer(
          HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE, containerOwner);
      fail("testContainerOpsMetrics failed");
    } catch (IOException ex) {
      // Here it should fail, so it should have the old metric value.
      metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
      Assert.assertEquals(getLongCounter("NumSuccessfulCreateContainers",
          metrics), numSuccessfulCreateContainers);
      Assert.assertEquals(getLongCounter("NumFailureCreateContainers",
          metrics), 1);
    }

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    long numSuccessfulDeleteContainers = getLongCounter(
        "NumSuccessfulDeleteContainers", metrics);

    containerManager.deleteContainer(
        new ContainerID(containerInfo.getContainerID()));

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assert.assertEquals(getLongCounter("NumSuccessfulDeleteContainers",
        metrics), numSuccessfulDeleteContainers + 1);


    try {
      // Give random container to delete.
      containerManager.deleteContainer(
          new ContainerID(RandomUtils.nextLong(10000, 20000)));
      fail("testContainerOpsMetrics failed");
    } catch (IOException ex) {
      // Here it should fail, so it should have the old metric value.
      metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
      Assert.assertEquals(getLongCounter("NumSuccessfulDeleteContainers",
          metrics), numSuccessfulCreateContainers);
      Assert.assertEquals(getLongCounter("NumFailureDeleteContainers",
          metrics), 1);
    }

    containerManager.listContainer(
        new ContainerID(containerInfo.getContainerID()), 1);
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assert.assertEquals(getLongCounter("NumListContainerOps",
        metrics), 1);
  }
}
