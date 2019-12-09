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

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Assert;
import org.junit.Test;

public class TestApplicationClientProtocolRecords {

  /*
   * This test validates the scenario in which the client sets a null value for a
   * particular environment.
   *
   */
  @Test
  public void testCLCPBImplNullEnv() throws IOException {
    Map<String, LocalResource> localResources = Collections.emptyMap();
    Map<String, String> environment = new HashMap<String, String>();
    List<String> commands = Collections.emptyList();
    Map<String, ByteBuffer> serviceData = Collections.emptyMap();
    Credentials containerCreds = new Credentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    containerCreds.writeTokenStorageToStream(dob);
    ByteBuffer containerTokens =
        ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    Map<ApplicationAccessType, String> acls = Collections.emptyMap();

    environment.put("testCLCPBImplNullEnv", null);

    ContainerLaunchContext clc =
        ContainerLaunchContext.newInstance(localResources, environment,
            commands, serviceData, containerTokens, acls);

    ContainerLaunchContext clcProto = new ContainerLaunchContextPBImpl(
        ((ContainerLaunchContextPBImpl) clc).getProto());

    Assert.assertEquals("",
        clcProto.getEnvironment().get("testCLCPBImplNullEnv"));

  }

  /*
   * This test validates the scenario in which the client sets a null value for
   * local resource URL.
   */
  @Test
  public void testCLCPBImplNullResourceURL() throws IOException {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    try {
      LocalResource rsrc_alpha = recordFactory.newRecordInstance(LocalResource.class);
      rsrc_alpha.setResource(null);
      rsrc_alpha.setSize(-1);
      rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
      rsrc_alpha.setType(LocalResourceType.FILE);
      rsrc_alpha.setTimestamp(System.currentTimeMillis());
      Map<String, LocalResource> localResources =
          new HashMap<String, LocalResource>();
      localResources.put("null_url_resource", rsrc_alpha);
      ContainerLaunchContext containerLaunchContext = recordFactory.newRecordInstance(ContainerLaunchContext.class);
      containerLaunchContext.setLocalResources(localResources);
      Assert.fail("Setting an invalid local resource should be an error!");
    } catch (NullPointerException e) {
      Assert.assertTrue(e.getMessage().contains("Null resource URL for local resource"));
    }
  }

  /*
   * This test validates the scenario in which the client sets a null value for
   * local resource type.
   */
  @Test
  public void testCLCPBImplNullResourceType() throws IOException {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    try {
      LocalResource resource = recordFactory.newRecordInstance(LocalResource.class);
      resource.setResource(URL.fromPath(new Path(".")));
      resource.setSize(-1);
      resource.setVisibility(LocalResourceVisibility.APPLICATION);
      resource.setType(null);
      resource.setTimestamp(System.currentTimeMillis());
      Map<String, LocalResource> localResources =
          new HashMap<String, LocalResource>();
      localResources.put("null_type_resource", resource);
      ContainerLaunchContext containerLaunchContext = recordFactory.newRecordInstance(ContainerLaunchContext.class);
      containerLaunchContext.setLocalResources(localResources);
      Assert.fail("Setting an invalid local resource should be an error!");
    } catch (NullPointerException e) {
      Assert.assertTrue(e.getMessage().contains("Null resource type for local resource"));
    }
  }

  /*
   * This test validates the scenario in which the client sets a null value for
   * local resource type.
   */
  @Test
  public void testCLCPBImplNullResourceVisibility() throws IOException {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    try {
      LocalResource resource = recordFactory.newRecordInstance(LocalResource.class);
      resource.setResource(URL.fromPath(new Path(".")));
      resource.setSize(-1);
      resource.setVisibility(null);
      resource.setType(LocalResourceType.FILE);
      resource.setTimestamp(System.currentTimeMillis());
      Map<String, LocalResource> localResources =
          new HashMap<String, LocalResource>();
      localResources.put("null_visibility_resource", resource);
      ContainerLaunchContext containerLaunchContext = recordFactory.newRecordInstance(ContainerLaunchContext.class);
      containerLaunchContext.setLocalResources(localResources);
      Assert.fail("Setting an invalid local resource should be an error!");
    } catch (NullPointerException e) {
      Assert.assertTrue(e.getMessage().contains("Null resource visibility for local resource"));
    }
  }
}
