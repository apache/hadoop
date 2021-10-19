/*
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

package org.apache.hadoop.yarn.service.utils;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.exceptions.BadConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.util.Map;

/**
 * Methods to aid in config, both in the Configuration class and
 * with other parts of setting up Slider-initated processes.
 *
 * Some of the methods take an argument of a map iterable for their sources; this allows
 * the same method
 */
public class ConfigHelper {
  private static final Logger log = LoggerFactory.getLogger(ConfigHelper.class);

  /**
   * Set an entire map full of values
   *
   * @param config config to patch
   * @param map map of data
   * @param origin origin data
   */
  public static void addConfigMap(Configuration config,
                                  Map<String, String> map,
                                  String origin) throws BadConfigException {
    addConfigMap(config, map.entrySet(), origin);
  }

  /**
   * Set an entire map full of values
   *
   * @param config config to patch
   * @param map map of data
   * @param origin origin data
   */
  public static void addConfigMap(Configuration config,
                                  Iterable<Map.Entry<String, String>> map,
                                  String origin) throws BadConfigException {
    for (Map.Entry<String, String> mapEntry : map) {
      String key = mapEntry.getKey();
      String value = mapEntry.getValue();
      if (value == null) {
        throw new BadConfigException("Null value for property " + key);
      }
      config.set(key, value, origin);
    }
  }

  /**
   * Convert to an XML string
   * @param conf configuration
   * @return conf
   * @throws IOException
   */
  public static String toXml(Configuration conf) throws IOException {
    StringWriter writer = new StringWriter();
    conf.writeXml(writer);
    return writer.toString();
  }


  /**
   * Register a resource as a default resource.
   * Do not attempt to use this unless you understand that the
   * order in which default resources are loaded affects the outcome,
   * and that subclasses of Configuration often register new default
   * resources
   * @param resource the resource name
   * @return the URL or null
   */
  public static URL registerDefaultResource(String resource) {
    URL resURL = getResourceUrl(resource);
    if (resURL != null) {
      Configuration.addDefaultResource(resource);
    }
    return resURL;
  }

  /**
   * Load a configuration from a resource on this classpath.
   * If the resource is not found, an empty configuration is returned
   * @param resource the resource name
   * @return the loaded configuration.
   */
  public static Configuration loadFromResource(String resource) {
    Configuration conf = new Configuration(false);
    URL resURL = getResourceUrl(resource);
    if (resURL != null) {
      log.debug("loaded resources from {}", resURL);
      conf.addResource(resource);
    } else{
      log.debug("failed to find {} on the classpath", resource);
    }
    return conf;

  }

  /**
   * Get the URL to a resource, null if not on the CP
   * @param resource resource to look for
   * @return the URL or null
   */
  public static URL getResourceUrl(String resource) {
    return ConfigHelper.class.getClassLoader()
                                  .getResource(resource);
  }

  /**
   * This goes through the keyset of one configuration and retrieves each value
   * from a value source -a different or the same configuration. This triggers
   * the property resolution process of the value, resolving any variables against
   * in-config or inherited configurations
   * @param keysource source of keys
   * @param valuesource the source of values
   * @return a new configuration where <code>foreach key in keysource, get(key)==valuesource.get(key)</code>
   */
  public static Configuration resolveConfiguration(
      Iterable<Map.Entry<String, String>> keysource,
      Configuration valuesource) {
    Configuration result = new Configuration(false);
    for (Map.Entry<String, String> entry : keysource) {
      String key = entry.getKey();
      String value = valuesource.get(key);
      Preconditions.checkState(value != null,
          "no reference for \"%s\" in values", key);
      result.set(key, value);
    }
    return result;
  }

}
