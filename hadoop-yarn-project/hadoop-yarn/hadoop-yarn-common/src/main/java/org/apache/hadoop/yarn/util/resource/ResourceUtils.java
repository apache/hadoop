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

package org.apache.hadoop.yarn.util.resource;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Helper class to read the resource-types to be supported by the system.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ResourceUtils {

  public static final String UNITS = ".units";
  public static final String TYPE = ".type";

  private static final Set<String> DISALLOWED_NAMES = new HashSet<>();
  static {
    DISALLOWED_NAMES.add("memory");
    DISALLOWED_NAMES.add(ResourceInformation.MEMORY_MB.getName());
    DISALLOWED_NAMES.add(ResourceInformation.VCORES.getName());
  }

  private static volatile Object lock;
  private static Map<String, ResourceInformation> readOnlyResources;

  static final Log LOG = LogFactory.getLog(ResourceUtils.class);

  private ResourceUtils() {
  }

  private static void checkMandatatoryResources(
      Map<String, ResourceInformation> resourceInformationMap)
      throws YarnRuntimeException {
    String memory = ResourceInformation.MEMORY_MB.getName();
    String vcores = ResourceInformation.VCORES.getName();
    if (resourceInformationMap.containsKey(memory)) {
      ResourceInformation memInfo = resourceInformationMap.get(memory);
      String memUnits = ResourceInformation.MEMORY_MB.getUnits();
      ResourceTypes memType = ResourceInformation.MEMORY_MB.getResourceType();
      if (!memInfo.getUnits().equals(memUnits) || !memInfo.getResourceType()
          .equals(memType)) {
        throw new YarnRuntimeException(
            "Attempt to re-define mandatory resource 'memory-mb'. It can only"
                + " be of type 'COUNTABLE' and have units 'M'.");
      }
    }

    if (resourceInformationMap.containsKey(vcores)) {
      ResourceInformation vcoreInfo = resourceInformationMap.get(vcores);
      String vcoreUnits = ResourceInformation.VCORES.getUnits();
      ResourceTypes vcoreType = ResourceInformation.VCORES.getResourceType();
      if (!vcoreInfo.getUnits().equals(vcoreUnits) || !vcoreInfo
          .getResourceType().equals(vcoreType)) {
        throw new YarnRuntimeException(
            "Attempt to re-define mandatory resource 'vcores'. It can only be"
                + " of type 'COUNTABLE' and have units ''(no units).");
      }
    }
  }

  private static void addManadtoryResources(
      Map<String, ResourceInformation> res) {
    ResourceInformation ri;
    if (!res.containsKey(ResourceInformation.MEMORY_MB.getName())) {
      LOG.info("Adding resource type - name = " + ResourceInformation.MEMORY_MB
          .getName() + ", units = " + ResourceInformation.MEMORY_MB.getUnits()
          + ", type = " + ResourceTypes.COUNTABLE);
      ri = ResourceInformation
          .newInstance(ResourceInformation.MEMORY_MB.getName(),
              ResourceInformation.MEMORY_MB.getUnits());
      res.put(ResourceInformation.MEMORY_MB.getName(), ri);
    }
    if (!res.containsKey(ResourceInformation.VCORES.getName())) {
      LOG.info("Adding resource type - name = " + ResourceInformation.VCORES
          .getName() + ", units = , type = " + ResourceTypes.COUNTABLE);
      ri =
          ResourceInformation.newInstance(ResourceInformation.VCORES.getName());
      res.put(ResourceInformation.VCORES.getName(), ri);
    }
  }

  @VisibleForTesting
  static void initializeResourcesMap(Configuration conf,
      Map<String, ResourceInformation> resourceInformationMap) {

    String[] resourceNames = conf.getStrings(YarnConfiguration.RESOURCE_TYPES);
    if (resourceNames != null && resourceNames.length != 0) {
      for (String resourceName : resourceNames) {
        String resourceUnits = conf.get(
            YarnConfiguration.RESOURCE_TYPES + "." + resourceName + UNITS, "");
        String resourceTypeName = conf.get(
            YarnConfiguration.RESOURCE_TYPES + "." + resourceName + TYPE,
            ResourceTypes.COUNTABLE.toString());
        if (resourceName == null || resourceName.isEmpty()
            || resourceUnits == null || resourceTypeName == null) {
          throw new YarnRuntimeException(
              "Incomplete configuration for resource type '" + resourceName
                  + "'. One of name, units or type is configured incorrectly.");
        }
        if (DISALLOWED_NAMES.contains(resourceName)) {
          throw new YarnRuntimeException(
              "Resource type cannot be named '" + resourceName
                  + "'. That name is disallowed.");
        }
        ResourceTypes resourceType = ResourceTypes.valueOf(resourceTypeName);
        LOG.info("Adding resource type - name = " + resourceName + ", units = "
            + resourceUnits + ", type = " + resourceTypeName);
        if (resourceInformationMap.containsKey(resourceName)) {
          throw new YarnRuntimeException(
              "Error in config, key '" + resourceName + "' specified twice");
        }
        resourceInformationMap.put(resourceName, ResourceInformation
            .newInstance(resourceName, resourceUnits, 0L, resourceType));
      }
    }
    checkMandatatoryResources(resourceInformationMap);
    addManadtoryResources(resourceInformationMap);
    readOnlyResources = Collections.unmodifiableMap(resourceInformationMap);
  }

  /**
   * Get the resource types to be supported by the system.
   * @return A map of the resource name to a ResouceInformation object
   *         which contains details such as the unit.
   */
  public static Map<String, ResourceInformation> getResourceTypes() {
    return getResourceTypes(null,
        YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE);
  }

  private static Map<String, ResourceInformation> getResourceTypes(
      Configuration conf, String resourceFile) {
    if (lock == null) {
      synchronized (ResourceUtils.class) {
        if (lock == null) {
          synchronized (ResourceUtils.class) {
            lock = new Object();
            Map<String, ResourceInformation> resources = new HashMap<>();
            if (conf == null) {
              conf = new YarnConfiguration();
            }
            try {
              InputStream ris = getConfInputStream(resourceFile, conf);
              LOG.debug("Found " + resourceFile + ", adding to configuration");
              conf.addResource(ris);
              initializeResourcesMap(conf, resources);
              return resources;
            } catch (FileNotFoundException fe) {
              LOG.info("Unable to find '" + resourceFile
                  + "'. Falling back to memory and vcores as resources", fe);
              initializeResourcesMap(conf, resources);
            } catch (IOException ie) {
              LOG.fatal(
                  "Exception trying to read resource types configuration '"
                      + resourceFile + "'.", ie);
              throw new YarnRuntimeException(ie);
            } catch (YarnException ye) {
              LOG.fatal(
                  "YARN Exception trying to read resource types configuration '"
                      + resourceFile + "'.", ye);
              throw new YarnRuntimeException(ye);
            }
          }
        }
      }
    }
    return readOnlyResources;
  }

  static InputStream getConfInputStream(String resourceFile, Configuration conf)
      throws IOException, YarnException {

    ConfigurationProvider provider =
        ConfigurationProviderFactory.getConfigurationProvider(conf);
    InputStream ris = provider.getConfigurationInputStream(conf, resourceFile);
    if (ris == null) {
      if (conf.getResource(resourceFile) == null) {
        throw new FileNotFoundException("Unable to find " + resourceFile);
      }
      throw new IOException(
          "Unable to open resource types file '" + resourceFile
              + "'. Using provider " + provider);
    }
    return ris;
  }

  @VisibleForTesting
  static void resetResourceTypes() {
    lock = null;
  }
}
