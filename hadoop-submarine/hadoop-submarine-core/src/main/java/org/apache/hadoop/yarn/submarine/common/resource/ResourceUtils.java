/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.common.resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements some methods with the almost the same logic as
 * org.apache.hadoop.yarn.util.resource.ResourceUtils of hadoop 3.3.
 * If the hadoop dependencies are upgraded to 3.3, this class can be refactored
 * with org.apache.hadoop.yarn.util.resource.ResourceUtils.
 */
public final class ResourceUtils {

  private final static String RES_PATTERN = "^[^=]+=\\d+\\s?\\w*$";
  private final static String SET_RESOURCE_VALUE_METHOD = "setResourceValue";
  private final static String SET_MEMORY_SIZE_METHOD = "setMemorySize";
  private final static String DEPRECATED_SET_MEMORY_SIZE_METHOD =
      "setMemory";
  private final static String GET_MEMORY_SIZE_METHOD = "getMemorySize";
  private final static String DEPRECATED_GET_MEMORY_SIZE_METHOD =
      "getMemory";
  private final static String GET_RESOURCE_VALUE_METHOD = "getResourceValue";
  private final static String GET_RESOURCE_TYPE_METHOD =
      "getResourcesTypeInfo";
  private final static String REINITIALIZE_RESOURCES_METHOD =
      "reinitializeResources";
  public static final String MEMORY_URI = "memory-mb";
  public static final String VCORES_URI = "vcores";
  public static final String GPU_URI = "yarn.io/gpu";
  public static final String FPGA_URI = "yarn.io/fpga";

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceUtils.class);

  private ResourceUtils() {}

  public static Resource createResourceFromString(String resourceStr) {
    Map<String, Long> typeToValue = parseResourcesString(resourceStr);
    Resource resource = Resource.newInstance(0, 0);
    for (Map.Entry<String, Long> entry : typeToValue.entrySet()) {
      if(entry.getKey().equals(VCORES_URI)) {
        resource.setVirtualCores(entry.getValue().intValue());
        continue;
      } else if (entry.getKey().equals(MEMORY_URI)) {
        setMemorySize(resource, entry.getValue());
        continue;
      }
      setResource(resource, entry.getKey(), entry.getValue().intValue());
    }
    return resource;
  }

  private static Map<String, Long> parseResourcesString(String resourcesStr) {
    Map<String, Long> resources = new HashMap<>();
    String[] pairs = resourcesStr.trim().split(",");
    for (String resource : pairs) {
      resource = resource.trim();
      if (!resource.matches(RES_PATTERN)) {
        throw new IllegalArgumentException("\"" + resource + "\" is not a "
            + "valid resource type/amount pair. "
            + "Please provide key=amount pairs separated by commas.");
      }
      String[] splits = resource.split("=");
      String key = splits[0], value = splits[1];
      String units = getUnits(value);

      String valueWithoutUnit = value.substring(0,
          value.length()- units.length()).trim();
      long resourceValue = Long.parseLong(valueWithoutUnit);

      // Convert commandline unit to standard YARN unit.
      if (units.equals("M") || units.equals("m")) {
        units = "Mi";
      } else if (units.equals("G") || units.equals("g")) {
        units = "Gi";
      } else if (!units.isEmpty()){
        throw new IllegalArgumentException("Acceptable units are M/G or empty");
      }

      // special handle memory-mb and memory
      if (key.equals(MEMORY_URI)) {
        if (!units.isEmpty()) {
          resourceValue = UnitsConversionUtil.convert(units, "Mi",
              resourceValue);
        }
      }

      if (key.equals("memory")) {
        key = MEMORY_URI;
        resourceValue = UnitsConversionUtil.convert(units, "Mi",
            resourceValue);
      }

      // special handle gpu
      if (key.equals("gpu")) {
        key = GPU_URI;
      }

      // special handle fpga
      if (key.equals("fpga")) {
        key = FPGA_URI;
      }

      resources.put(key, resourceValue);
    }
    return resources;
  }

  /**
   * As hadoop 2.9.2 and lower don't support resources except cpu and memory.
   * Use reflection to set GPU or other resources for compatibility with
   * hadoop 2.9.2
   */
  public static void setResource(Resource resource, String resourceName,
                                 int resourceValue) {
    try {
      Method method = resource.getClass().getMethod(SET_RESOURCE_VALUE_METHOD,
          String.class, long.class);
      method.invoke(resource, resourceName, resourceValue);
    } catch (NoSuchMethodException e) {
      LOG.error("There is no '" + SET_RESOURCE_VALUE_METHOD + "' API in this" +
          "version of YARN", e);
      throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOG.error("Failed to invoke '" + SET_RESOURCE_VALUE_METHOD +
          "' method to set GPU resources", e);
      throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
    }
    return;
  }

  public static void setMemorySize(Resource resource, Long memorySize) {
    boolean useWithIntParameter = false;
    // For hadoop 2.9.2 and above
    try {
      Method method = resource.getClass().getMethod(SET_MEMORY_SIZE_METHOD,
          long.class);
      method.setAccessible(true);
      method.invoke(resource, memorySize);
    } catch (NoSuchMethodException nsme) {
      LOG.info("There is no '" + SET_MEMORY_SIZE_METHOD + "(long)' API in" +
          " this version of YARN");
      useWithIntParameter = true;
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOG.error("Failed to invoke '" + SET_MEMORY_SIZE_METHOD +
          "' method", e);
      throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
    }
    // For hadoop 2.7.3
    if (useWithIntParameter) {
      try {
        LOG.info("Trying to use '" + DEPRECATED_SET_MEMORY_SIZE_METHOD +
            "(int)' API for this version of YARN");
        Method method = resource.getClass().getMethod(
            DEPRECATED_SET_MEMORY_SIZE_METHOD, int.class);
        method.invoke(resource, memorySize.intValue());
      } catch (NoSuchMethodException e) {
        LOG.error("There is no '" + DEPRECATED_SET_MEMORY_SIZE_METHOD +
            "(int)' API in this version of YARN", e);
        throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
      } catch (IllegalAccessException | InvocationTargetException e) {
        LOG.error("Failed to invoke '" + DEPRECATED_SET_MEMORY_SIZE_METHOD +
            "' method", e);
        throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
      }
    }
  }

  public static long getMemorySize(Resource resource) {
    boolean useWithIntParameter = false;
    long memory = 0;
    // For hadoop 2.9.2 and above
    try {
      Method method = resource.getClass().getMethod(GET_MEMORY_SIZE_METHOD);
      method.setAccessible(true);
      memory = (long) method.invoke(resource);
    } catch (NoSuchMethodException e) {
      LOG.info("There is no '" + GET_MEMORY_SIZE_METHOD + "' API in" +
          " this version of YARN");
      useWithIntParameter = true;
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOG.error("Failed to invoke '" + GET_MEMORY_SIZE_METHOD +
          "' method", e);
      throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
    }
    // For hadoop 2.7.3
    if (useWithIntParameter) {
      try {
        LOG.info("Trying to use '" + DEPRECATED_GET_MEMORY_SIZE_METHOD +
            "' API for this version of YARN");
        Method method = resource.getClass().getMethod(
            DEPRECATED_GET_MEMORY_SIZE_METHOD);
        method.setAccessible(true);
        memory = ((Integer) method.invoke(resource)).longValue();
      } catch (NoSuchMethodException e) {
        LOG.error("There is no '" + DEPRECATED_GET_MEMORY_SIZE_METHOD +
                "' API in this version of YARN", e);
        throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
      } catch (IllegalAccessException | InvocationTargetException e) {
        LOG.error("Failed to invoke '" + DEPRECATED_GET_MEMORY_SIZE_METHOD +
            "' method", e);
        throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
      }
    }
    return memory;
  }

  /**
   * As hadoop 2.9.2 and lower don't support resources except cpu and memory.
   * Use reflection to set GPU or other resources for compatibility with
   * hadoop 2.9.2
   */
  public static long getResourceValue(Resource resource, String resourceName) {
    long resourceValue = 0;
    try {
      Method method = resource.getClass().getMethod(GET_RESOURCE_VALUE_METHOD,
          String.class);
      Object value = method.invoke(resource, resourceName);
      resourceValue = (long) value;
    } catch (NoSuchMethodException e) {
      LOG.info("There is no '" + GET_RESOURCE_VALUE_METHOD + "' API in this" +
          " version of YARN");
    } catch (InvocationTargetException e) {
      if (e.getTargetException().getClass().getName().equals(
          "org.apache.hadoop.yarn.exceptions.ResourceNotFoundException")) {
        LOG.info("Not found resource " + resourceName);
      } else {
        LOG.info("Failed to invoke '" + GET_RESOURCE_VALUE_METHOD + "'" +
            " method to get resource " + resourceName);
        throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
      }
    } catch (IllegalAccessException | ClassCastException e) {
      LOG.error("Failed to invoke '" + GET_RESOURCE_VALUE_METHOD +
          "' method to get resource " + resourceName, e);
      throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
    }
    return resourceValue;
  }

  /**
   * As hadoop 2.9.2 and lower don't support resources except cpu and memory.
   * Use reflection to add GPU or other resources for compatibility with
   * hadoop 2.9.2
   */
  public static void configureResourceType(String resrouceName) {
    Class resourceTypeInfo;
    try{
      resourceTypeInfo = Class.forName(
          "org.apache.hadoop.yarn.api.records.ResourceTypeInfo");
      Class resourceUtils = Class.forName(
          "org.apache.hadoop.yarn.util.resource.ResourceUtils");
      Method method = resourceUtils.getMethod(GET_RESOURCE_TYPE_METHOD);
      Object resTypes = method.invoke(null);

      Method resourceTypeInstance = resourceTypeInfo.getMethod("newInstance",
          String.class, String.class);
      Object resourceType = resourceTypeInstance.invoke(null, resrouceName, "");
      ((ArrayList)resTypes).add(resourceType);

      Method reInitialMethod = resourceUtils.getMethod(
          REINITIALIZE_RESOURCES_METHOD, List.class);
      reInitialMethod.invoke(null, resTypes);

    } catch (ClassNotFoundException e) {
      LOG.info("There is no specified class API in this" +
          " version of YARN");
      LOG.info(e.getMessage());
      throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
    } catch (NoSuchMethodException nsme) {
      LOG.info("There is no '" + GET_RESOURCE_VALUE_METHOD + "' API in this" +
          " version of YARN");
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOG.info("Failed to invoke 'configureResourceType' method ", e);
      throw new SubmarineRuntimeException(e.getMessage(), e.getCause());
    }
  }

  private static String getUnits(String resourceValue) {
    return parseResourceValue(resourceValue)[0];
  }

  /**
   * Extract unit and actual value from resource value.
   * @param resourceValue Value of the resource
   * @return Array containing unit and value. [0]=unit, [1]=value
   * @throws IllegalArgumentException if units contain non alpha characters
   */
  private static String[] parseResourceValue(String resourceValue) {
    String[] resource = new String[2];
    int i = 0;
    for (; i < resourceValue.length(); i++) {
      if (Character.isAlphabetic(resourceValue.charAt(i))) {
        break;
      }
    }
    String units = resourceValue.substring(i);

    if (StringUtils.isAlpha(units) || units.equals("")) {
      resource[0] = units;
      resource[1] = resourceValue.substring(0, i);
      return resource;
    } else {
      throw new IllegalArgumentException("Units '" + units + "'"
          + " contains non alphabet characters, which is not allowed.");
    }
  }

}
