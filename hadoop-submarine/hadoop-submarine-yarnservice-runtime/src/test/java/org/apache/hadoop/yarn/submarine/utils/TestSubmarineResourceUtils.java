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
import org.apache.hadoop.yarn.service.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationProvider;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.After;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * This class is to test {@link SubmarineResourceUtils}.
 */
public class TestSubmarineResourceUtils {
  private static final String CUSTOM_RESOURCE_NAME = "a-custom-resource";

  private void initResourceTypes() {
    CustomResourceTypesConfigurationProvider.initResourceTypes(
        ImmutableMap.<String, String>builder()
            .put(CUSTOM_RESOURCE_NAME, "G")
            .build());
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