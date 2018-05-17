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

package org.apache.hadoop.yarn.resourcetypes;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contains helper methods to create Resource and ResourceInformation objects.
 * ResourceInformation can be created from a resource name
 * and a resource descriptor as well that comprises amount and unit.
 */
public final class ResourceTypesTestHelper {

  private static final Pattern RESOURCE_VALUE_AND_UNIT_PATTERN =
      Pattern.compile("(\\d+)([A-za-z]*)");

  private ResourceTypesTestHelper() {}

  private static final RecordFactory RECORD_FACTORY = RecordFactoryProvider
          .getRecordFactory(null);

  private static final class ResourceValueAndUnit {
    private final Long value;
    private final String unit;

    private ResourceValueAndUnit(Long value, String unit) {
      this.value = value;
      this.unit = unit;
    }
  }

  public static Resource newResource(long memory, int vCores, Map<String,
          String> customResources) {
    Resource resource = RECORD_FACTORY.newRecordInstance(Resource.class);
    resource.setMemorySize(memory);
    resource.setVirtualCores(vCores);

    for (Map.Entry<String, String> customResource :
            customResources.entrySet()) {
      String resourceName = customResource.getKey();
      ResourceInformation resourceInformation =
              createResourceInformation(resourceName,
                      customResource.getValue());
      resource.setResourceInformation(resourceName, resourceInformation);
    }
    return resource;
  }

  public static ResourceInformation createResourceInformation(String
          resourceName, String descriptor) {
    ResourceValueAndUnit resourceValueAndUnit =
            getResourceValueAndUnit(descriptor);
    return ResourceInformation
            .newInstance(resourceName, resourceValueAndUnit.unit,
                    resourceValueAndUnit.value);
  }

  private static ResourceValueAndUnit getResourceValueAndUnit(String val) {
    Matcher matcher = RESOURCE_VALUE_AND_UNIT_PATTERN.matcher(val);
    if (!matcher.find()) {
      throw new RuntimeException("Invalid pattern of resource descriptor: " +
              val);
    } else if (matcher.groupCount() != 2) {
      throw new RuntimeException("Capturing group count in string " +
              val + " is not 2!");
    }
    long value = Long.parseLong(matcher.group(1));

    return new ResourceValueAndUnit(value, matcher.group(2));
  }

}
