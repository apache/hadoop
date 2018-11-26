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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.*;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.*;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

/**
 * Unit tests for DevicePluginAdapter.
 * About interaction with vendor plugin
 * */
public class TestDevicePluginAdapter {

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestDevicePluginAdapter.class);

  private YarnConfiguration conf;
  private String tempResourceTypesFile;

  @Before
  public void setup() throws Exception {
    this.conf = new YarnConfiguration();
    // setup resource-types.xml
    ResourceUtils.resetResourceTypes();
    String resourceTypesFile = "resource-types-pluggable-devices.xml";
    this.tempResourceTypesFile =
        TestResourceUtils.setupResourceTypes(this.conf, resourceTypesFile);
  }

  @After
  public void tearDown() throws IOException {
    // cleanup resource-types.xml
    File dest = new File(this.tempResourceTypesFile);
    if (dest.exists()) {
      dest.delete();
    }
  }

  @Test
  public void testDeviceResourceUpdaterImpl() throws YarnException {
    Resource nodeResource = mock(Resource.class);
    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin);
    adapter.initialize(mock(Context.class));
    adapter.getNodeResourceHandlerInstance()
        .updateConfiguredResource(nodeResource);
    verify(spyPlugin, times(1)).getDevices();
    verify(nodeResource, times(1)).setResourceValue(
        resourceName, 3);
  }

  private class MyPlugin implements DevicePlugin {
    private final static String RESOURCE_NAME = "cmpA.com/hdwA";
    @Override
    public DeviceRegisterRequest getRegisterRequestInfo() {
      return DeviceRegisterRequest.Builder.newInstance()
          .setResourceName(RESOURCE_NAME)
          .setPluginVersion("v1.0").build();
    }

    @Override
    public Set<Device> getDevices() {
      TreeSet<Device> r = new TreeSet<>();
      r.add(Device.Builder.newInstance()
          .setId(0)
          .setDevPath("/dev/hdwA0")
          .setMajorNumber(256)
          .setMinorNumber(0)
          .setBusID("0000:80:00.0")
          .setHealthy(true)
          .build());
      r.add(Device.Builder.newInstance()
          .setId(1)
          .setDevPath("/dev/hdwA1")
          .setMajorNumber(256)
          .setMinorNumber(0)
          .setBusID("0000:80:01.0")
          .setHealthy(true)
          .build());
      r.add(Device.Builder.newInstance()
          .setId(2)
          .setDevPath("/dev/hdwA2")
          .setMajorNumber(256)
          .setMinorNumber(0)
          .setBusID("0000:80:02.0")
          .setHealthy(true)
          .build());
      return r;
    }

    @Override
    public DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices,
        YarnRuntimeType yarnRuntime) throws Exception {
      return null;
    }

    @Override
    public void onDevicesReleased(Set<Device> releasedDevices) {

    }
  } // MyPlugin

}
