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


import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.List;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.XmlCustomResourceTypeTestCase.toXml;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlLong;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test helper class is primarily used by
 * {@link TestRMWebServicesFairSchedulerCustomResourceTypes}.
 */
public class FairSchedulerXmlVerifications {

  private static final Set<String> RESOURCE_FIELDS = Sets.newHashSet(
      "minResources", "amUsedResources", "amMaxResources", "fairResources",
      "clusterResources", "reservedResources", "maxResources", "usedResources",
      "steadyFairResources", "demandResources");
  private final Set<String> customResourceTypes;

  FairSchedulerXmlVerifications(List<String> customResourceTypes) {
    this.customResourceTypes = Sets.newHashSet(customResourceTypes);
  }

  public void verify(Element element) {
    verifyResourcesContainDefaultResourceTypes(element, RESOURCE_FIELDS);
    verifyResourcesContainCustomResourceTypes(element, RESOURCE_FIELDS);
  }

  private void verifyResourcesContainDefaultResourceTypes(Element queue,
      Set<String> resourceCategories) {
    for (String resourceCategory : resourceCategories) {
      boolean hasResourceCategory = hasChild(queue, resourceCategory);
      assertTrue("Queue " + queue + " does not have resource category key: "
          + resourceCategory, hasResourceCategory);
      verifyResourceContainsDefaultResourceTypes(
              (Element) queue.getElementsByTagName(resourceCategory).item(0));
    }
  }

  private void verifyResourceContainsDefaultResourceTypes(
      Element element) {
    Object memory = opt(element, "memory");
    Object vCores = opt(element, "vCores");

    assertNotNull("Key 'memory' not found in: " + element, memory);
    assertNotNull("Key 'vCores' not found in: " + element, vCores);
  }

  private void verifyResourcesContainCustomResourceTypes(Element queue,
      Set<String> resourceCategories) {
    for (String resourceCategory : resourceCategories) {
      assertTrue("Queue " + queue + " does not have key for resourceCategory: "
          + resourceCategory, hasChild(queue, resourceCategory));
      verifyResourceContainsCustomResourceTypes(
              (Element) queue.getElementsByTagName(resourceCategory).item(0));
    }
  }

  private void verifyResourceContainsCustomResourceTypes(
      Element resourceCategory) {
    assertEquals(
        toXml(resourceCategory)
            + " should have only one resourceInformations child!",
        1, resourceCategory.getElementsByTagName("resourceInformations")
            .getLength());
    Element resourceInformations = (Element) resourceCategory
        .getElementsByTagName("resourceInformations").item(0);

    NodeList customResources =
        resourceInformations.getElementsByTagName("resourceInformation");

    // customResources will include vcores / memory as well
    assertEquals(
        "Different number of custom resource types found than expected",
        customResourceTypes.size(), customResources.getLength() - 2);

    for (int i = 0; i < customResources.getLength(); i++) {
      Element customResource = (Element) customResources.item(i);
      String name = getXmlString(customResource, "name");
      String unit = getXmlString(customResource, "units");
      String resourceType = getXmlString(customResource, "resourceType");
      Long value = getXmlLong(customResource, "value");

      if (ResourceInformation.MEMORY_URI.equals(name)
          || ResourceInformation.VCORES_URI.equals(name)) {
        continue;
      }

      assertTrue("Custom resource type " + name + " not found",
          customResourceTypes.contains(name));
      assertEquals("k", unit);
      assertEquals(ResourceTypes.COUNTABLE,
          ResourceTypes.valueOf(resourceType));
      assertNotNull("Resource value should not be null for resource type "
          + resourceType + ", listing xml contents: " + toXml(customResource),
          value);
    }
  }

  private Object opt(Node node, String child) {
    NodeList nodes = getElementsByTagNameInternal(node, child);
    if (nodes.getLength() > 0) {
      return nodes.item(0);
    }

    return null;
  }

  private boolean hasChild(Node node, String child) {
    return getElementsByTagNameInternal(node, child).getLength() > 0;
  }

  private NodeList getElementsByTagNameInternal(Node node, String child) {
    if (node instanceof Element) {
      return ((Element) node).getElementsByTagName(child);
    } else if (node instanceof Document) {
      return ((Document) node).getElementsByTagName(child);
    } else {
      throw new IllegalStateException("Unknown type of wrappedObject: " + node
          + ", type: " + node.getClass());
    }
  }
}
