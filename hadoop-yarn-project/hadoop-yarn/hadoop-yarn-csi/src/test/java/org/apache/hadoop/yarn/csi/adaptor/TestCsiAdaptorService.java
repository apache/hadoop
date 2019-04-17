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
import org.apache.hadoop.yarn.api.CsiAdaptorPlugin;
import org.apache.hadoop.yarn.api.impl.pb.client.CsiAdaptorProtocolPBClientImpl;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ValidateVolumeCapabilitiesRequestPBImpl;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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

import static org.assertj.core.api.Assertions.assertThat;
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

  private interface FakeCsiAdaptor extends CsiAdaptorPlugin {

    default void init(String driverName, Configuration conf)
        throws YarnException {
      return;
    }

    default String getDriverName() {
      return null;
    }

    default GetPluginInfoResponse getPluginInfo(GetPluginInfoRequest request)
        throws YarnException, IOException {
      return null;
    }

    default ValidateVolumeCapabilitiesResponse validateVolumeCapacity(
        ValidateVolumeCapabilitiesRequest request) throws YarnException,
        IOException {
      return null;
    }

    default NodePublishVolumeResponse nodePublishVolume(
        NodePublishVolumeRequest request) throws YarnException, IOException {
      return null;
    }

    default NodeUnpublishVolumeResponse nodeUnpublishVolume(
        NodeUnpublishVolumeRequest request) throws YarnException, IOException{
      return null;
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
    conf.set(
        YarnConfiguration.NM_CSI_DRIVER_PREFIX + "test-driver.endpoint",
        "unix:///tmp/test-driver.sock");

    // inject a fake CSI adaptor
    // this client validates if the ValidateVolumeCapabilitiesRequest
    // is integrity, and then reply a fake response
    CsiAdaptorPlugin plugin = new FakeCsiAdaptor() {
      @Override
      public String getDriverName() {
        return "test-driver";
      }


      @Override
      public GetPluginInfoResponse getPluginInfo(GetPluginInfoRequest request) {
        return GetPluginInfoResponse.newInstance("test-plugin", "0.1");
      }

      @Override
      public ValidateVolumeCapabilitiesResponse validateVolumeCapacity(
          ValidateVolumeCapabilitiesRequest request) throws YarnException,
          IOException {
        // validate we get all info from the request
        Assert.assertEquals("volume-id-0000123", request.getVolumeId());
        Assert.assertEquals(1, request.getVolumeCapabilities().size());
        Assert.assertEquals(Csi.VolumeCapability.AccessMode
                .newBuilder().setModeValue(5).build().getMode().name(),
            request.getVolumeCapabilities().get(0).getAccessMode().name());
        Assert.assertEquals(2, request.getVolumeCapabilities().get(0)
            .getMountFlags().size());
        Assert.assertTrue(request.getVolumeCapabilities().get(0)
            .getMountFlags().contains("mountFlag1"));
        Assert.assertTrue(request.getVolumeCapabilities().get(0)
            .getMountFlags().contains("mountFlag2"));
        Assert.assertEquals(2, request.getVolumeAttributes().size());
        Assert.assertEquals("v1", request.getVolumeAttributes().get("k1"));
        Assert.assertEquals("v2", request.getVolumeAttributes().get("k2"));
        // return a fake result
        return ValidateVolumeCapabilitiesResponse
            .newInstance(false, "this is a test");
      }
    };

    CsiAdaptorProtocolService service =
        new CsiAdaptorProtocolService(plugin);
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
    conf.set(
        YarnConfiguration.NM_CSI_DRIVER_PREFIX + "test-driver.endpoint",
        "unix:///tmp/test-driver.sock");

    // inject a fake CSI adaptor
    // this client validates if the ValidateVolumeCapabilitiesRequest
    // is integrity, and then reply a fake response
    FakeCsiAdaptor plugin = new FakeCsiAdaptor() {
      @Override
      public String getDriverName() {
        return "test-driver";
      }

      @Override
      public GetPluginInfoResponse getPluginInfo(
          GetPluginInfoRequest request) throws YarnException, IOException {
        return GetPluginInfoResponse.newInstance("test-plugin", "0.1");
      }

      @Override
      public ValidateVolumeCapabilitiesResponse validateVolumeCapacity(
          ValidateVolumeCapabilitiesRequest request)
          throws YarnException, IOException {
        // validate we get all info from the request
        Assert.assertEquals("volume-id-0000123", request.getVolumeId());
        Assert.assertEquals(1, request.getVolumeCapabilities().size());
        Assert.assertEquals(
            Csi.VolumeCapability.AccessMode.newBuilder().setModeValue(5)
                .build().getMode().name(),
            request.getVolumeCapabilities().get(0).getAccessMode().name());
        Assert.assertEquals(2,
            request.getVolumeCapabilities().get(0).getMountFlags().size());
        Assert.assertTrue(request.getVolumeCapabilities().get(0).getMountFlags()
            .contains("mountFlag1"));
        Assert.assertTrue(request.getVolumeCapabilities().get(0).getMountFlags()
            .contains("mountFlag2"));
        Assert.assertEquals(2, request.getVolumeAttributes().size());
        Assert.assertEquals("v1", request.getVolumeAttributes().get("k1"));
        Assert.assertEquals("v2", request.getVolumeAttributes().get("k2"));
        // return a fake result
        return ValidateVolumeCapabilitiesResponse
            .newInstance(false, "this is a test");
      }
    };

    CsiAdaptorProtocolService service =
        new CsiAdaptorProtocolService(plugin);
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
        new CsiAdaptorProtocolService(new FakeCsiAdaptor() {});
    service.init(conf);
  }

  @Test (expected = ServiceStateException.class)
  public void testInvalidServicePort() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
        + "test-driver-0001.address",
        "0.0.0.0:-100"); // this is an invalid address
    CsiAdaptorProtocolService service =
        new CsiAdaptorProtocolService(new FakeCsiAdaptor() {});
    service.init(conf);
  }

  @Test (expected = ServiceStateException.class)
  public void testInvalidHost() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
            + "test-driver-0001.address",
        "192.0.1:8999"); // this is an invalid ip address
    CsiAdaptorProtocolService service =
        new CsiAdaptorProtocolService(new FakeCsiAdaptor() {});
    service.init(conf);
  }

  @Test
  public void testCustomizedAdaptor() throws IOException, YarnException {
    ServerSocket ss = new ServerSocket(0);
    ss.close();
    InetSocketAddress address = new InetSocketAddress(ss.getLocalPort());
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_CSI_DRIVER_NAMES, "customized-driver");
    conf.setSocketAddr(
        YarnConfiguration.NM_CSI_ADAPTOR_PREFIX + "customized-driver.address",
        address);
    conf.set(
        YarnConfiguration.NM_CSI_ADAPTOR_PREFIX + "customized-driver.class",
        "org.apache.hadoop.yarn.csi.adaptor.MockCsiAdaptor");
    conf.set(
        YarnConfiguration.NM_CSI_DRIVER_PREFIX + "customized-driver.endpoint",
        "unix:///tmp/customized-driver.sock");

    CsiAdaptorServices services =
        new CsiAdaptorServices();
    services.init(conf);
    services.start();

    YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    CsiAdaptorProtocol adaptorClient = NMProxy
        .createNMProxy(conf, CsiAdaptorProtocol.class, currentUser, rpc,
            NetUtils.createSocketAddrForHost("localhost", ss.getLocalPort()));

    // Test getPluginInfo
    GetPluginInfoResponse pluginInfo =
        adaptorClient.getPluginInfo(GetPluginInfoRequest.newInstance());
    assertThat(pluginInfo.getDriverName()).isEqualTo("customized-driver");
    assertThat(pluginInfo.getVersion()).isEqualTo("1.0");

    // Test validateVolumeCapacity
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
    Assert.assertEquals(true, response.isSupported());
    Assert.assertEquals("verified via MockCsiAdaptor",
        response.getResponseMessage());

    services.stop();
  }

  @Test
  public void testMultipleCsiAdaptors() throws IOException, YarnException {
    ServerSocket driver1Addr = new ServerSocket(0);
    ServerSocket driver2Addr = new ServerSocket(0);

    InetSocketAddress address1 =
        new InetSocketAddress(driver1Addr.getLocalPort());
    InetSocketAddress address2 =
        new InetSocketAddress(driver2Addr.getLocalPort());

    Configuration conf = new Configuration();

    // Two csi-drivers configured
    conf.set(YarnConfiguration.NM_CSI_DRIVER_NAMES,
        "customized-driver-1,customized-driver-2");

    // customized-driver-1
    conf.setSocketAddr(YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
            + "customized-driver-1.address", address1);
    conf.set(YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
            + "customized-driver-1.class",
        "org.apache.hadoop.yarn.csi.adaptor.MockCsiAdaptor");
    conf.set(YarnConfiguration.NM_CSI_DRIVER_PREFIX
            + "customized-driver-1.endpoint",
        "unix:///tmp/customized-driver-1.sock");

    // customized-driver-2
    conf.setSocketAddr(YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
        + "customized-driver-2.address", address2);
    conf.set(YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
            + "customized-driver-2.class",
        "org.apache.hadoop.yarn.csi.adaptor.MockCsiAdaptor");
    conf.set(YarnConfiguration.NM_CSI_DRIVER_PREFIX
            + "customized-driver-2.endpoint",
        "unix:///tmp/customized-driver-2.sock");

    driver1Addr.close();
    driver2Addr.close();

    CsiAdaptorServices services =
        new CsiAdaptorServices();
    services.init(conf);
    services.start();

    YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    CsiAdaptorProtocol client1 = NMProxy
        .createNMProxy(conf, CsiAdaptorProtocol.class, currentUser, rpc,
            NetUtils.createSocketAddrForHost("localhost",
                driver1Addr.getLocalPort()));

    // ***************************************************
    // Verify talking with customized-driver-1
    // ***************************************************
    // Test getPluginInfo
    GetPluginInfoResponse pluginInfo =
        client1.getPluginInfo(GetPluginInfoRequest.newInstance());
    assertThat(pluginInfo.getDriverName()).isEqualTo("customized-driver-1");
    assertThat(pluginInfo.getVersion()).isEqualTo("1.0");

    // Test validateVolumeCapacity
    ValidateVolumeCapabilitiesRequest request =
        ValidateVolumeCapabilitiesRequestPBImpl
            .newInstance("driver-1-volume-00001",
                ImmutableList.of(new ValidateVolumeCapabilitiesRequest
                    .VolumeCapability(
                    MULTI_NODE_MULTI_WRITER, FILE_SYSTEM,
                    ImmutableList.of())), ImmutableMap.of());

    ValidateVolumeCapabilitiesResponse response = client1
        .validateVolumeCapacity(request);
    Assert.assertEquals(true, response.isSupported());
    Assert.assertEquals("verified via MockCsiAdaptor",
        response.getResponseMessage());


    // ***************************************************
    // Verify talking with customized-driver-2
    // ***************************************************
    CsiAdaptorProtocol client2 = NMProxy
        .createNMProxy(conf, CsiAdaptorProtocol.class, currentUser, rpc,
            NetUtils.createSocketAddrForHost("localhost",
                driver2Addr.getLocalPort()));
    GetPluginInfoResponse pluginInfo2 =
        client2.getPluginInfo(GetPluginInfoRequest.newInstance());
    assertThat(pluginInfo2.getDriverName()).isEqualTo("customized-driver-2");
    assertThat(pluginInfo2.getVersion()).isEqualTo("1.0");

    services.stop();
  }
}
