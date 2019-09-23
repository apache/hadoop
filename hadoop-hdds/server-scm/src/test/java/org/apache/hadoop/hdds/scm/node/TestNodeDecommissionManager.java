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
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.Arrays;
import java.util.ArrayList;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Fail.fail;

/**
 * Unit tests for the decommision manager.
 */

public class TestNodeDecommissionManager {

  private NodeDecommissionManager decom;
  private StorageContainerManager scm;
  private NodeManager nodeManager;
  private OzoneConfiguration conf;
  private String storageDir;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    storageDir = GenericTestUtils.getTempPath(
        TestDeadNodeHandler.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);
    nodeManager = createNodeManager(conf);
    decom = new NodeDecommissionManager(conf, nodeManager, null, null);
  }

  @Test
  public void testHostStringsParseCorrectly()
      throws InvalidHostStringException {
    NodeDecommissionManager.HostDefinition def =
        new NodeDecommissionManager.HostDefinition("foobar");
    assertEquals("foobar", def.getHostname());
    assertEquals(-1, def.getPort());

    def = new NodeDecommissionManager.HostDefinition(" foobar ");
    assertEquals("foobar", def.getHostname());
    assertEquals(-1, def.getPort());

    def = new NodeDecommissionManager.HostDefinition("foobar:1234");
    assertEquals("foobar", def.getHostname());
    assertEquals(1234, def.getPort());

    def = new NodeDecommissionManager.HostDefinition(
        "foobar.mycompany.com:1234");
    assertEquals("foobar.mycompany.com", def.getHostname());
    assertEquals(1234, def.getPort());

    try {
      def = new NodeDecommissionManager.HostDefinition("foobar:abcd");
      fail("InvalidHostStringException should have been thrown");
    } catch (InvalidHostStringException e) {
    }
  }

  @Test
  public void testAnyInvalidHostThrowsException()
      throws InvalidHostStringException{
    List<DatanodeDetails> dns = generateDatanodes();

    // Try to decommission a host that does exist, but give incorrect port
    try {
      decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress()+":10"));
      fail("InvalidHostStringException expected");
    } catch (InvalidHostStringException e) {
    }

    // Try to decommission a host that does not exist
    try {
      decom.decommissionNodes(Arrays.asList("123.123.123.123"));
      fail("InvalidHostStringException expected");
    } catch (InvalidHostStringException e) {
    }

    // Try to decommission a host that does exist and a host that does not
    try {
      decom.decommissionNodes(Arrays.asList(
          dns.get(1).getIpAddress(), "123,123,123,123"));
      fail("InvalidHostStringException expected");
    } catch (InvalidHostStringException e) {
    }

    // Try to decommission a host with many DNs on the address with no port
    try {
      decom.decommissionNodes(Arrays.asList(
          dns.get(0).getIpAddress()));
      fail("InvalidHostStringException expected");
    } catch (InvalidHostStringException e) {
    }

    // Try to decommission a host with many DNs on the address with a port
    // that does not exist
    try {
      decom.decommissionNodes(Arrays.asList(
          dns.get(0).getIpAddress()+":10"));
      fail("InvalidHostStringException expected");
    } catch (InvalidHostStringException e) {
    }
  }

  @Test
  public void testNodesCanBeDecommissionedAndRecommissioned()
      throws InvalidHostStringException {
    List<DatanodeDetails> dns = generateDatanodes();

    // Decommission 2 valid nodes
    decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()));
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());

    // Running the command again gives no error - nodes already decommissioning
    // are silently ignored.
    decom.decommissionNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()));

    // Attempt to decommission dn(10) which has multiple hosts on the same IP
    // and we hardcoded ports to 3456, 4567, 5678
    DatanodeDetails multiDn = dns.get(10);
    String multiAddr =
        multiDn.getIpAddress()+":"+multiDn.getPorts().get(0).getValue();
    decom.decommissionNodes(Arrays.asList(multiAddr));
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        nodeManager.getNodeStatus(multiDn).getOperationalState());

    // Recommission all 3 hosts
    decom.recommissionNodes(Arrays.asList(
        multiAddr, dns.get(1).getIpAddress(), dns.get(2).getIpAddress()));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(10)).getOperationalState());
  }

  @Test
  public void testNodesCanBePutIntoMaintenanceAndRecommissioned()
      throws InvalidHostStringException {
    List<DatanodeDetails> dns = generateDatanodes();

    // Put 2 valid nodes into maintenance
    decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()), 100);
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());

    // Running the command again gives no error - nodes already decommissioning
    // are silently ignored.
    decom.startMaintenanceNodes(Arrays.asList(dns.get(1).getIpAddress(),
        dns.get(2).getIpAddress()), 100);

    // Attempt to decommission dn(10) which has multiple hosts on the same IP
    // and we hardcoded ports to 3456, 4567, 5678
    DatanodeDetails multiDn = dns.get(10);
    String multiAddr =
        multiDn.getIpAddress()+":"+multiDn.getPorts().get(0).getValue();
    decom.startMaintenanceNodes(Arrays.asList(multiAddr), 100);
    assertEquals(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        nodeManager.getNodeStatus(multiDn).getOperationalState());

    // Recommission all 3 hosts
    decom.recommissionNodes(Arrays.asList(
        multiAddr, dns.get(1).getIpAddress(), dns.get(2).getIpAddress()));
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(1)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(2)).getOperationalState());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        nodeManager.getNodeStatus(dns.get(10)).getOperationalState());
  }

  private SCMNodeManager createNodeManager(OzoneConfiguration config)
      throws IOException, AuthenticationException {
    scm = HddsTestUtils.getScm(config);
    return (SCMNodeManager) scm.getScmNodeManager();
  }

  /**
   * Generate a list of random DNs and return the list. A total of 11 DNs will
   * be generated and registered with the node manager. Index 0 and 10 will
   * have the same IP and host and the rest will have unique IPs and Hosts.
   * The DN at index 10, has 3 hard coded ports of 3456, 4567, 5678. All other
   * DNs will have ports set to 0.
   * @return The list of DatanodeDetails Generated
   */
  private List<DatanodeDetails> generateDatanodes() {
    List<DatanodeDetails> dns = new ArrayList<>();
    for (int i=0; i<10; i++) {
      DatanodeDetails dn = TestUtils.randomDatanodeDetails();
      dns.add(dn);
      nodeManager.register(dn, null, null);
    }
    // We have 10 random DNs, we want to create another one that is on the same
    // host as some of the others.
    DatanodeDetails multiDn = dns.get(0);

    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID().toString())
        .setHostName(multiDn.getHostName())
        .setIpAddress(multiDn.getIpAddress())
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.STANDALONE, 3456))
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.RATIS, 4567))
        .addPort(DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.REST, 5678))
        .setNetworkLocation(multiDn.getNetworkLocation());

    DatanodeDetails dn = builder.build();
    nodeManager.register(dn, null, null);
    dns.add(dn);
    return dns;
  }

}
