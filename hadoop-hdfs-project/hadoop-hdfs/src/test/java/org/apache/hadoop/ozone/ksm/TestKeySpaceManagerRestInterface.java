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

package org.apache.hadoop.ozone.ksm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.ksm.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This class is to test the REST interface exposed by KeySpaceManager.
 */
public class TestKeySpaceManagerRestInterface {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new OzoneConfiguration();
    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .build();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  public void testGetServiceList() throws Exception {
    KeySpaceManagerHttpServer server =
        cluster.getKeySpaceManager().getHttpServer();
    HttpClient client = HttpClients.createDefault();
    String connectionUri = "http://" +
        NetUtils.getHostPortString(server.getHttpAddress());
    HttpGet httpGet = new HttpGet(connectionUri + "/serviceList");
    HttpResponse response = client.execute(httpGet);
    String serviceListJson = EntityUtils.toString(response.getEntity());

    ObjectMapper objectMapper = new ObjectMapper();
    TypeReference<List<ServiceInfo>> serviceInfoReference =
        new TypeReference<List<ServiceInfo>>() {};
    List<ServiceInfo> serviceInfos = objectMapper.readValue(
        serviceListJson, serviceInfoReference);
    Map<OzoneProtos.NodeType, ServiceInfo> serviceMap = new HashMap<>();
    for (ServiceInfo serviceInfo : serviceInfos) {
      serviceMap.put(serviceInfo.getNodeType(), serviceInfo);
    }

    InetSocketAddress ksmAddress =
        OzoneClientUtils.getKsmAddressForClients(conf);
    ServiceInfo ksmInfo = serviceMap.get(OzoneProtos.NodeType.KSM);

    Assert.assertEquals(ksmAddress.getHostName(), ksmInfo.getHostname());
    Assert.assertEquals(ksmAddress.getPort(),
        ksmInfo.getPort(KeySpaceManagerProtocolProtos.ServicePort.Type.RPC));
    Assert.assertEquals(server.getHttpAddress().getPort(),
        ksmInfo.getPort(KeySpaceManagerProtocolProtos.ServicePort.Type.HTTP));

    InetSocketAddress scmAddress =
        OzoneClientUtils.getScmAddressForClients(conf);
    ServiceInfo scmInfo = serviceMap.get(OzoneProtos.NodeType.SCM);

    Assert.assertEquals(scmAddress.getHostName(), scmInfo.getHostname());
    Assert.assertEquals(scmAddress.getPort(),
        scmInfo.getPort(KeySpaceManagerProtocolProtos.ServicePort.Type.RPC));
  }

}
