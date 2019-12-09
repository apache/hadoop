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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.fairscheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * This class can generate an XML configuration file of custom resource types.
 * See createInitialResourceTypes for the default values. All custom resource
 * type is prefixed with CUSTOM_RESOURCE_PREFIX. Please use the
 * getConfigurationInputStream method to get an InputStream of the XML. If you
 * want to have different number of resources in your tests, please see usages
 * of this class in this test class:
 * {@link TestRMWebServicesFairSchedulerCustomResourceTypes}
 *
 */
public class CustomResourceTypesConfigurationProvider
    extends LocalConfigurationProvider {

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

  private static final String CUSTOM_RESOURCE_PREFIX = "customResource-";

  private static CustomResourceTypes customResourceTypes =
      createInitialResourceTypes();

  private static CustomResourceTypes createInitialResourceTypes() {
    return createCustomResourceTypes(2);
  }

  private static CustomResourceTypes createCustomResourceTypes(int count) {
    List<String> resourceTypeNames = generateResourceTypeNames(count);

    List<String> resourceUnitXmlElements = IntStream.range(0, count)
            .boxed()
            .map(i -> getResourceUnitsXml(resourceTypeNames.get(i)))
            .collect(toList());

    StringBuilder sb = new StringBuilder("<configuration>\n");
    sb.append(getResourceTypesXml(resourceTypeNames));

    for (String resourceUnitXml : resourceUnitXmlElements) {
      sb.append(resourceUnitXml);

    }
    sb.append("</configuration>");

    return new CustomResourceTypes(sb.toString(), count);
  }

  private static List<String> generateResourceTypeNames(int count) {
    return IntStream.range(0, count)
            .boxed()
            .map(i -> CUSTOM_RESOURCE_PREFIX + i)
            .collect(toList());
  }

  private static String getResourceUnitsXml(String resource) {
    return "<property>\n" + "<name>yarn.resource-types." + resource
        + ".units</name>\n" + "<value>k</value>\n" + "</property>\n";
  }

  private static String getResourceTypesXml(List<String> resources) {
    final String resourceTypes = makeCommaSeparatedString(resources);

    return "<property>\n" + "<name>yarn.resource-types</name>\n" + "<value>"
        + resourceTypes + "</value>\n" + "</property>\n";
  }

  private static String makeCommaSeparatedString(List<String> resources) {
    return resources.stream().collect(Collectors.joining(","));
  }

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

  public static void reset() {
    customResourceTypes = createInitialResourceTypes();
  }

  public static void setNumberOfResourceTypes(int count) {
    customResourceTypes = createCustomResourceTypes(count);
  }

  public static List<String> getCustomResourceTypes() {
    return generateResourceTypeNames(customResourceTypes.getCount());
  }
}
