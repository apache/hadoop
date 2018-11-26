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
package org.apache.hadoop.yarn.csi.adaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import csi.v0.Csi;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.impl.pb.client.CsiAdaptorProtocolPBClientImpl;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ValidateVolumeCapabilitiesRequestPBImpl;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.csi.client.CsiClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.AccessMode.MULTI_NODE_MULTI_WRITER;
import static org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeType.FILE_SYSTEM;

/**
 * UT for {@link CsiAdaptorProtocolService}.
 */
public class TestCsiAdaptorService {

  private static File testRoot = null;
  private static String domainSocket = null;

  @BeforeClass
  public static void setUp() throws IOException {
    testRoot = GenericTestUtils.getTestDir("csi-test");
    File socketPath = new File(testRoot, "csi.sock");
    FileUtils.forceMkdirParent(socketPath);
    domainSocket = "unix://" + socketPath.getAbsolutePath();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (testRoot != null) {
      FileUtils.deleteDirectory(testRoot);
    }
  }

  @Test
  public void testValidateVolume() throws IOException, YarnException {
    ServerSocket ss = new ServerSocket(0);
    ss.close();
    InetSocketAddress address = new InetSocketAddress(ss.getLocalPort());
    Configuration conf = new Configuration();
    conf.setSocketAddr(
        YarnConfiguration.NM_CSI_ADAPTOR_PREFIX + "test-driver.address",
        address);
    CsiAdaptorProtocolService service =
        new CsiAdaptorProtocolService("test-driver", domainSocket);

    // inject a fake CSI client
    // this client validates if the ValidateVolumeCapabilitiesRequest
    // is integrity, and then reply a fake response
    service.setCsiClient(new CsiClient() {
      @Override
      public Csi.GetPluginInfoResponse getPluginInfo() {
        return Csi.GetPluginInfoResponse.newBuilder()
            .setName("test-plugin")
            .setVendorVersion("0.1")
            .build();
      }

      @Override
      public Csi.ValidateVolumeCapabilitiesResponse validateVolumeCapabilities(
          Csi.ValidateVolumeCapabilitiesRequest request) {
        // validate we get all info from the request
        Assert.assertEquals("volume-id-0000123", request.getVolumeId());
        Assert.assertEquals(1, request.getVolumeCapabilitiesCount());
        Assert.assertEquals(Csi.VolumeCapability.AccessMode
            .newBuilder().setModeValue(5).build(),
            request.getVolumeCapabilities(0).getAccessMode());
        Assert.assertTrue(request.getVolumeCapabilities(0).hasMount());
        Assert.assertEquals(2, request.getVolumeCapabilities(0)
            .getMount().getMountFlagsCount());
        Assert.assertTrue(request.getVolumeCapabilities(0)
            .getMount().getMountFlagsList().contains("mountFlag1"));
        Assert.assertTrue(request.getVolumeCapabilities(0)
            .getMount().getMountFlagsList().contains("mountFlag2"));
        Assert.assertEquals(2, request.getVolumeAttributesCount());
        Assert.assertEquals("v1", request.getVolumeAttributesMap().get("k1"));
        Assert.assertEquals("v2", request.getVolumeAttributesMap().get("k2"));
        // return a fake result
        return Csi.ValidateVolumeCapabilitiesResponse.newBuilder()
            .setSupported(false)
            .setMessage("this is a test")
            .build();
      }
    });

    service.init(conf);
    service.start();

    try (CsiAdaptorProtocolPBClientImpl client =
        new CsiAdaptorProtocolPBClientImpl(1L, address, new Configuration())) {
      ValidateVolumeCapabilitiesRequest request =
          ValidateVolumeCapabilitiesRequestPBImpl
              .newInstance("volume-id-0000123",
                  ImmutableList.of(
                      new ValidateVolumeCapabilitiesRequest
                          .VolumeCapability(
                              MULTI_NODE_MULTI_WRITER, FILE_SYSTEM,
                          ImmutableList.of("mountFlag1", "mountFlag2"))),
              ImmutableMap.of("k1", "v1", "k2", "v2"));

      ValidateVolumeCapabilitiesResponse response = client
          .validateVolumeCapacity(request);

      Assert.assertEquals(false, response.isSupported());
      Assert.assertEquals("this is a test", response.getResponseMessage());
    } finally {
      service.stop();
    }
  }

  @Test
  public void testValidateVolumeWithNMProxy() throws Exception {
    ServerSocket ss = new ServerSocket(0);
    ss.close();
    InetSocketAddress address = new InetSocketAddress(ss.getLocalPort());
    Configuration conf = new Configuration();
    conf.setSocketAddr(
        YarnConfiguration.NM_CSI_ADAPTOR_PREFIX + "test-driver.address",
        address);
    CsiAdaptorProtocolService service =
        new CsiAdaptorProtocolService("test-driver", domainSocket);

    // inject a fake CSI client
    // this client validates if the ValidateVolumeCapabilitiesRequest
    // is integrity, and then reply a fake response
    service.setCsiClient(new CsiClient() {
      @Override
      public Csi.GetPluginInfoResponse getPluginInfo() {
        return Csi.GetPluginInfoResponse.newBuilder()
            .setName("test-plugin")
            .setVendorVersion("0.1")
            .build();
      }

      @Override
      public Csi.ValidateVolumeCapabilitiesResponse validateVolumeCapabilities(
          Csi.ValidateVolumeCapabilitiesRequest request) {
        // validate we get all info from the request
        Assert.assertEquals("volume-id-0000123", request.getVolumeId());
        Assert.assertEquals(1, request.getVolumeCapabilitiesCount());
        Assert.assertEquals(Csi.VolumeCapability.AccessMode
                .newBuilder().setModeValue(5).build(),
            request.getVolumeCapabilities(0).getAccessMode());
        Assert.assertTrue(request.getVolumeCapabilities(0).hasMount());
        Assert.assertEquals(2, request.getVolumeCapabilities(0)
            .getMount().getMountFlagsCount());
        Assert.assertTrue(request.getVolumeCapabilities(0)
            .getMount().getMountFlagsList().contains("mountFlag1"));
        Assert.assertTrue(request.getVolumeCapabilities(0)
            .getMount().getMountFlagsList().contains("mountFlag2"));
        Assert.assertEquals(2, request.getVolumeAttributesCount());
        Assert.assertEquals("v1", request.getVolumeAttributesMap().get("k1"));
        Assert.assertEquals("v2", request.getVolumeAttributesMap().get("k2"));
        // return a fake result
        return Csi.ValidateVolumeCapabilitiesResponse.newBuilder()
            .setSupported(false)
            .setMessage("this is a test")
            .build();
      }
    });

    service.init(conf);
    service.start();

    YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    CsiAdaptorProtocol adaptorClient = NMProxy
        .createNMProxy(conf, CsiAdaptorProtocol.class, currentUser, rpc,
            NetUtils.createSocketAddrForHost("localhost", ss.getLocalPort()));
    ValidateVolumeCapabilitiesRequest request =
        ValidateVolumeCapabilitiesRequestPBImpl
            .newInstance("volume-id-0000123",
                ImmutableList.of(new ValidateVolumeCapabilitiesRequest
                    .VolumeCapability(
                        MULTI_NODE_MULTI_WRITER, FILE_SYSTEM,
                    ImmutableList.of("mountFlag1", "mountFlag2"))),
                ImmutableMap.of("k1", "v1", "k2", "v2"));

    ValidateVolumeCapabilitiesResponse response = adaptorClient
        .validateVolumeCapacity(request);
    Assert.assertEquals(false, response.isSupported());
    Assert.assertEquals("this is a test", response.getResponseMessage());

    service.stop();
  }

  @Test (expected = ServiceStateException.class)
  public void testMissingConfiguration() {
    Configuration conf = new Configuration();
    CsiAdaptorProtocolService service =
        new CsiAdaptorProtocolService("test-driver", domainSocket);
    service.init(conf);
  }

  @Test (expected = ServiceStateException.class)
  public void testInvalidServicePort() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
        + "test-driver-0001.address",
        "0.0.0.0:-100"); // this is an invalid address
    CsiAdaptorProtocolService service =
        new CsiAdaptorProtocolService("test-driver-0001", domainSocket);
    service.init(conf);
  }

  @Test (expected = ServiceStateException.class)
  public void testInvalidHost() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
            + "test-driver-0001.address",
        "192.0.1:8999"); // this is an invalid ip address
    CsiAdaptorProtocolService service =
        new CsiAdaptorProtocolService("test-driver-0001", domainSocket);
    service.init(conf);
  }
}
