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

package org.apache.hadoop.yarn.submarine.utils;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * This class is to test {@link SubmarineResourceUtils}.
 */
public class TestSubmarineResourceUtils {
  /**
   * With the dependencies of hadoop 3.2.0, Need to create a
   * CustomResourceTypesConfigurationProvider implementations. If the
   * dependencies are upgraded to hadoop 3.3.0. It can be replaced by
   * org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationProvi-
   * der
   */
  private static class CustomResourceTypesConfigurationProvider
      extends LocalConfigurationProvider {

    @Override
    public InputStream getConfigurationInputStream(Configuration bootstrapConf,
        String name) throws YarnException, IOException {
      if (YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE.equals(name)) {
        return new ByteArrayInputStream(
            ("<configuration>\n" +
                " <property>\n" +
                "   <name>yarn.resource-types</name>\n" +
                "   <value>" + CUSTOM_RESOURCE_NAME + "</value>\n" +
                " </property>\n" +
                " <property>\n" +
                "   <name>yarn.resource-types.a-custom-resource.units</name>\n"
                +
                "   <value>G</value>\n" +
                " </property>\n" +
                "</configuration>\n").getBytes());
      } else {
        return super.getConfigurationInputStream(bootstrapConf, name);
      }
    }
  }

  private static final String CUSTOM_RESOURCE_NAME = "a-custom-resource";

  private void initResourceTypes() {
    // If the dependencies are upgraded to hadoop 3.3.0. It can be replaced by
    // org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationPro-
    // vider
    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        CustomResourceTypesConfigurationProvider.class.getName());
    ResourceUtils.resetResourceTypes(configuration);
  }

  @After
  public void cleanup() {
    ResourceUtils.resetResourceTypes(new Configuration());
  }

  @Test
  public void testConvertResourceWithCustomResource() {
    initResourceTypes();
    Resource res = Resource.newInstance(4096, 12,
        ImmutableMap.of(CUSTOM_RESOURCE_NAME, 20L));

    org.apache.hadoop.yarn.service.api.records.Resource serviceResource =
        SubmarineResourceUtils.convertYarnResourceToServiceResource(res);

    assertEquals(12, serviceResource.getCpus().intValue());
    assertEquals(4096, (int) Integer.valueOf(serviceResource.getMemory()));
    Map<String, ResourceInformation> additionalResources =
        serviceResource.getAdditional();

    // Additional resources also includes vcores and memory
    assertEquals(3, additionalResources.size());
    ResourceInformation customResourceRI =
        additionalResources.get(CUSTOM_RESOURCE_NAME);
    assertEquals("G", customResourceRI.getUnit());
    assertEquals(20L, (long) customResourceRI.getValue());
  }

}