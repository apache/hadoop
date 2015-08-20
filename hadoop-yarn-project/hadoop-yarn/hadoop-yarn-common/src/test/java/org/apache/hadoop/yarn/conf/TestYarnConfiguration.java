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

package org.apache.hadoop.yarn.conf;

import org.junit.Assert;

import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestYarnConfiguration {

  @Test
  public void testDefaultRMWebUrl() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    String rmWebUrl = WebAppUtils.getRMWebAppURLWithScheme(conf);
    // shouldn't have a "/" on the end of the url as all the other uri routinnes
    // specifically add slashes and Jetty doesn't handle double slashes.
    Assert.assertNotSame("RM Web Url is not correct", "http://0.0.0.0:8088",
        rmWebUrl);
  }

  @Test
  public void testRMWebUrlSpecified() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    // seems a bit odd but right now we are forcing webapp for RM to be
    // RM_ADDRESS
    // for host and use the port from the RM_WEBAPP_ADDRESS
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "fortesting:24543");
    conf.set(YarnConfiguration.RM_ADDRESS, "rmtesting:9999");
    String rmWebUrl = WebAppUtils.getRMWebAppURLWithScheme(conf);
    String[] parts = rmWebUrl.split(":");
    Assert.assertEquals("RM Web URL Port is incrrect", 24543,
        Integer.valueOf(parts[parts.length - 1]).intValue());
    Assert.assertNotSame(
        "RM Web Url not resolved correctly. Should not be rmtesting",
        "http://rmtesting:24543", rmWebUrl);
  }

  @Test
  public void testGetSocketAddressForNMWithHA() {
    YarnConfiguration conf = new YarnConfiguration();

    // Set NM address
    conf.set(YarnConfiguration.NM_ADDRESS, "0.0.0.0:1234");

    // Set HA
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_ID, "rm1");
    assertTrue(HAUtil.isHAEnabled(conf));

    InetSocketAddress addr = conf.getSocketAddr(YarnConfiguration.NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_PORT);
    assertEquals(1234, addr.getPort());
  }

  @Test
  public void testGetSocketAddr() throws Exception {

    YarnConfiguration conf;
    InetSocketAddress resourceTrackerAddress;

    //all default
    conf = new YarnConfiguration();
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);
    assertEquals(
      new InetSocketAddress(
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS.split(":")[0],
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT),
        resourceTrackerAddress);

    //with address
    conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "10.0.0.1");
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);
    assertEquals(
      new InetSocketAddress(
        "10.0.0.1",
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT),
        resourceTrackerAddress);

    //address and socket
    conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "10.0.0.2:5001");
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);
    assertEquals(
      new InetSocketAddress(
        "10.0.0.2",
        5001),
        resourceTrackerAddress);

    //bind host only
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_BIND_HOST, "10.0.0.3");
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);
    assertEquals(
      new InetSocketAddress(
        "10.0.0.3",
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT),
        resourceTrackerAddress);

    //bind host and address no port
    conf.set(YarnConfiguration.RM_BIND_HOST, "0.0.0.0");
    conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "10.0.0.2");
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);
    assertEquals(
      new InetSocketAddress(
        "0.0.0.0",
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT),
        resourceTrackerAddress);

    //bind host and address with port
    conf.set(YarnConfiguration.RM_BIND_HOST, "0.0.0.0");
    conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "10.0.0.2:5003");
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);
    assertEquals(
      new InetSocketAddress(
        "0.0.0.0",
        5003),
        resourceTrackerAddress);

  }

  @Test
  public void testUpdateConnectAddr() throws Exception {
    YarnConfiguration conf;
    InetSocketAddress resourceTrackerConnectAddress;
    InetSocketAddress serverAddress;

    //no override, old behavior.  Won't work on a host named "yo.yo.yo"
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "yo.yo.yo");
    serverAddress = new InetSocketAddress(
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS.split(":")[0],
        Integer.valueOf(YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS.split(":")[1]));

    resourceTrackerConnectAddress = conf.updateConnectAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        serverAddress);

    assertFalse(resourceTrackerConnectAddress.toString().startsWith("yo.yo.yo"));

    //cause override with address
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "yo.yo.yo");
    conf.set(YarnConfiguration.RM_BIND_HOST, "0.0.0.0");
    serverAddress = new InetSocketAddress(
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS.split(":")[0],
        Integer.valueOf(YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS.split(":")[1]));

    resourceTrackerConnectAddress = conf.updateConnectAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        serverAddress);

    assertTrue(resourceTrackerConnectAddress.toString().startsWith("yo.yo.yo"));

    //tests updateConnectAddr won't add suffix to NM service address configurations
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "yo.yo.yo");
    conf.set(YarnConfiguration.NM_BIND_HOST, "0.0.0.0");
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_ID, "rm1");

    serverAddress = new InetSocketAddress(
        YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS.split(":")[0],
        Integer.valueOf(YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS.split(":")[1]));

    InetSocketAddress localizerAddress = conf.updateConnectAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS,
        serverAddress);

    assertTrue(localizerAddress.toString().startsWith("yo.yo.yo"));
    assertNull(conf.get(
        HAUtil.addSuffix(YarnConfiguration.NM_LOCALIZER_ADDRESS, "rm1")));
  }
}
