/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.webapp;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.resourcetypes.ResourceTypesTestHelper;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.webapp.View;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.mockito.Mockito.mock;

/**
 * Tests for ContainerBlock.
 */
public class ContainerBlockTest {
  private static class CustomResourceTypesConfigurationProvider
      extends LocalConfigurationProvider {

    private static String resourceName;

    @Override
    public InputStream getConfigurationInputStream(Configuration bootstrapConf,
        String name) throws YarnException, IOException {
      if (YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE.equals(name)) {
        return new ByteArrayInputStream(
            ("<configuration>\n" +
            " <property>\n" +
            "   <name>yarn.resource-types</name>\n" +
            "   <value>" + resourceName + "</value>\n" +
            " </property>\n" +
            " <property>\n" +
            "   <name>yarn.resource-types.a-custom-resource.units</name>\n" +
            "   <value>G</value>\n" +
            " </property>\n" +
            "</configuration>\n").getBytes());
      } else {
        return super.getConfigurationInputStream(bootstrapConf, name);
      }
    }

    public static String getResourceName() {
      return resourceName;
    }
  }

  private void initResourceTypes(String resourceName) {
    CustomResourceTypesConfigurationProvider.resourceName = resourceName;

    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        CustomResourceTypesConfigurationProvider.class.getName());
    ResourceUtils.resetResourceTypes(configuration);
  }

  private ContainerReport createContainerReport() {
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
    ContainerReport container = ContainerReport.newInstance(containerId, null,
        NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234, 5678,
        "diagnosticInfo", "logURL", 0, ContainerState.COMPLETE,
        "http://" + NodeId.newInstance("host", 2345).toString());

    return container;
  }
  @Test
  public void testRenderResourcesString() {
    initResourceTypes(ResourceInformation.GPU_URI);

    Resource resource = ResourceTypesTestHelper.newResource(
        DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        ImmutableMap.<String, String>builder()
            .put(ResourceInformation.GPU_URI, "5").build());

    ContainerBlock block = new ContainerBlock(
        mock(ApplicationBaseProtocol.class),
        mock(View.ViewContext.class));

    ContainerReport containerReport = createContainerReport();
    containerReport.setAllocatedResource(resource);
    ContainerInfo containerInfo = new ContainerInfo(containerReport);
    String resources = block.getResources(containerInfo);

    Assert.assertEquals("8192 Memory, 4 VCores, 5 yarn.io/gpu", resources);
  }

}