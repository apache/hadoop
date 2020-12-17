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

package org.apache.hadoop.yarn.util.resource;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * This class can generate an XML configuration file of custom resource types.
 * See createInitial ResourceTypes for the default values. All custom resource
 * type is prefixed with CUSTOM_RESOURCE_PREFIX. Please use the
 * getConfigurationInputStream method to get an InputStream of the XML.
 *
 */
public class CustomResourceTypesConfigurationProvider
    extends LocalConfigurationProvider {

  @Override
  public InputStream getConfigurationInputStream(Configuration bootstrapConf,
      String name) throws YarnException, IOException {
    if (YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE.equals(name)) {
      return new ByteArrayInputStream(
          customResourceTypes.getXml().getBytes());
    } else {
      return super.getConfigurationInputStream(bootstrapConf, name);
    }
  }

  private static final String CUSTOM_RESOURCE_PREFIX = "custom-resource-";
  private static final String UNIT_KILO = "k";
  private static CustomResourceTypes customResourceTypes =
      createCustomResourceTypes(2, UNIT_KILO);

  public static void initResourceTypes(Map<String, String> resourcesWithUnits) {
    CustomResourceTypesConfigurationProvider.setResourceTypes(
        resourcesWithUnits);
    initResourceTypesInternal();
  }

  public static void initResourceTypes(int count, String units) {
    CustomResourceTypesConfigurationProvider.setResourceTypes(count, units);
    initResourceTypesInternal();
  }

  public static void initResourceTypes(String... resourceTypes) {
    // Initialize resource map
    Map<String, ResourceInformation> riMap = new HashMap<>();

    // Initialize mandatory resources
    riMap.put(ResourceInformation.MEMORY_URI, ResourceInformation.MEMORY_MB);
    riMap.put(ResourceInformation.VCORES_URI, ResourceInformation.VCORES);

    for (String newResource : resourceTypes) {
      riMap.put(newResource, ResourceInformation
          .newInstance(newResource, "", 0, ResourceTypes.COUNTABLE, 0,
              Integer.MAX_VALUE));
    }

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);
  }

  private static void initResourceTypesInternal() {
    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        CustomResourceTypesConfigurationProvider.class.getName());
    ResourceUtils.resetResourceTypes(configuration);
  }

  private static CustomResourceTypes createCustomResourceTypes(
      int count, String units) {
    List<String> resourceNames = generateResourceTypeNames(count);
    Map<String, String> resourcesWithUnits = resourceNames.stream().collect(
        Collectors.toMap(e -> e, e -> units));
    return createCustomResourceTypes(resourcesWithUnits);
  }

  private static CustomResourceTypes createCustomResourceTypes(
      Map<String, String> resourcesWithUnits) {
    int count = resourcesWithUnits.size();
    List<String> resourceNames = Lists.newArrayList(
        resourcesWithUnits.keySet());

    List<String> resourceUnitXmlElements = IntStream.range(0, count)
            .boxed()
            .map(i -> getResourceUnitsXml(resourceNames.get(i),
                resourcesWithUnits.get(resourceNames.get(i))))
            .collect(toList());

    StringBuilder sb = new StringBuilder("<configuration>\n");
    sb.append(getResourceTypesXml(resourceNames));

    for (String resourceUnitXml : resourceUnitXmlElements) {
      sb.append(resourceUnitXml);

    }
    sb.append("</configuration>");

    return new CustomResourceTypes(sb.toString(), count);
  }

  private static List<String> generateResourceTypeNames(int count) {
    return IntStream.range(0, count)
            .boxed()
            .map(i -> CUSTOM_RESOURCE_PREFIX + (i + 1))
            .collect(toList());
  }

  private static String getResourceUnitsXml(String resource, String units) {
    return "<property>\n" +
        "<name>yarn.resource-types." + resource+ ".units</name>\n" +
        "<value>" + units + "</value>\n" +
        "</property>\n";
  }

  private static String getResourceTypesXml(List<String> resources) {
    final String resourceTypes = String.join(",", resources);

    return "<property>\n" +
        "<name>yarn.resource-types</name>\n" +
        "<value>" + resourceTypes + "</value>\n" + "</property>\n";
  }

  public static void reset() {
    customResourceTypes = createCustomResourceTypes(2, UNIT_KILO);
  }

  public static void setResourceTypes(int count, String units) {
    customResourceTypes = createCustomResourceTypes(count, units);
  }

  public static void setResourceTypes(Map<String, String> resourcesWithUnits) {
    customResourceTypes = createCustomResourceTypes(resourcesWithUnits);
  }

  public static List<String> getCustomResourceTypes() {
    return generateResourceTypeNames(customResourceTypes.getCount());
  }

  private static class CustomResourceTypes {
    private int count;
    private String xml;

    CustomResourceTypes(String xml, int count) {
      this.xml = xml;
      this.count = count;
    }

    public int getCount() {
      return count;
    }
    public String getXml() {
      return xml;
    }
  }

}
