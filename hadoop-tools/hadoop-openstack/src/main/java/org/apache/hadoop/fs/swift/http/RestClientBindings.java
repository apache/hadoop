/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;

import java.net.URI;
import java.util.Properties;

import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.*;

/**
 * This class implements the binding logic between Hadoop configurations
 * and the swift rest client.
 * <p>
 * The swift rest client takes a Properties instance containing
 * the string values it uses to bind to a swift endpoint.
 * <p>
 * This class extracts the values for a specific filesystem endpoint
 * and then builds an appropriate Properties file.
 */
public final class RestClientBindings {
  private static final Logger LOG =
      LoggerFactory.getLogger(RestClientBindings.class);

  public static final String E_INVALID_NAME = "Invalid swift hostname '%s':" +
          " hostname must in form container.service";

  /**
   * Public for testing : build the full prefix for use in resolving
   * configuration items
   *
   * @param service service to use
   * @return the prefix string <i>without any trailing "."</i>
   */
  public static String buildSwiftInstancePrefix(String service) {
    return SWIFT_SERVICE_PREFIX + service;
  }

  /**
   * Raise an exception for an invalid service name
   *
   * @param hostname hostname that was being parsed
   * @return an exception to throw
   */
  private static SwiftConfigurationException invalidName(String hostname) {
    return new SwiftConfigurationException(
            String.format(E_INVALID_NAME, hostname));
  }

  /**
   * Get the container name from the hostname -the single element before the
   * first "." in the hostname
   *
   * @param hostname hostname to split
   * @return the container
   * @throws SwiftConfigurationException
   */
  public static String extractContainerName(String hostname) throws
          SwiftConfigurationException {
    int i = hostname.indexOf(".");
    if (i <= 0) {
      throw invalidName(hostname);
    }
    return hostname.substring(0, i);
  }

  public static String extractContainerName(URI uri) throws
          SwiftConfigurationException {
    return extractContainerName(uri.getHost());
  }

  /**
   * Get the service name from a longer hostname string
   *
   * @param hostname hostname
   * @return the separated out service name
   * @throws SwiftConfigurationException if the hostname was invalid
   */
  public static String extractServiceName(String hostname) throws
          SwiftConfigurationException {
    int i = hostname.indexOf(".");
    if (i <= 0) {
      throw invalidName(hostname);
    }
    String service = hostname.substring(i + 1);
    if (service.isEmpty() || service.contains(".")) {
      //empty service contains dots in -not currently supported
      throw invalidName(hostname);
    }
    return service;
  }

  public static String extractServiceName(URI uri) throws
          SwiftConfigurationException {
    return extractServiceName(uri.getHost());
  }

  /**
   * Build a properties instance bound to the configuration file -using
   * the filesystem URI as the source of the information.
   *
   * @param fsURI filesystem URI
   * @param conf  configuration
   * @return a properties file with the instance-specific properties extracted
   *         and bound to the swift client properties.
   * @throws SwiftConfigurationException if the configuration is invalid
   */
  public static Properties bind(URI fsURI, Configuration conf) throws
          SwiftConfigurationException {
    String host = fsURI.getHost();
    if (host == null || host.isEmpty()) {
      //expect shortnames -> conf names
      throw invalidName(host);
    }

    String container = extractContainerName(host);
    String service = extractServiceName(host);

    //build filename schema
    String prefix = buildSwiftInstancePrefix(service);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filesystem " + fsURI
              + " is using configuration keys " + prefix);
    }
    Properties props = new Properties();
    props.setProperty(SWIFT_SERVICE_PROPERTY, service);
    props.setProperty(SWIFT_CONTAINER_PROPERTY, container);
    copy(conf, prefix + DOT_AUTH_URL, props, SWIFT_AUTH_PROPERTY, true);
    copy(conf, prefix + DOT_USERNAME, props, SWIFT_USERNAME_PROPERTY, true);
    copy(conf, prefix + DOT_APIKEY, props, SWIFT_APIKEY_PROPERTY, false);
    copy(conf, prefix + DOT_PASSWORD, props, SWIFT_PASSWORD_PROPERTY,
            props.contains(SWIFT_APIKEY_PROPERTY) ? true : false);
    copy(conf, prefix + DOT_TENANT, props, SWIFT_TENANT_PROPERTY, false);
    copy(conf, prefix + DOT_REGION, props, SWIFT_REGION_PROPERTY, false);
    copy(conf, prefix + DOT_HTTP_PORT, props, SWIFT_HTTP_PORT_PROPERTY, false);
    copy(conf, prefix +
            DOT_HTTPS_PORT, props, SWIFT_HTTPS_PORT_PROPERTY, false);

    copyBool(conf, prefix + DOT_PUBLIC, props, SWIFT_PUBLIC_PROPERTY, false);
    copyBool(conf, prefix + DOT_LOCATION_AWARE, props,
             SWIFT_LOCATION_AWARE_PROPERTY, false);

    return props;
  }

  /**
   * Extract a boolean value from the configuration and copy it to the
   * properties instance.
   * @param conf     source configuration
   * @param confKey  key in the configuration file
   * @param props    destination property set
   * @param propsKey key in the property set
   * @param defVal default value
   */
  private static void copyBool(Configuration conf,
                               String confKey,
                               Properties props,
                               String propsKey,
                               boolean defVal) {
    boolean b = conf.getBoolean(confKey, defVal);
    props.setProperty(propsKey, Boolean.toString(b));
  }

  private static void set(Properties props, String key, String optVal) {
    if (optVal != null) {
      props.setProperty(key, optVal);
    }
  }

  /**
   * Copy a (trimmed) property from the configuration file to the properties file.
   * <p>
   * If marked as required and not found in the configuration, an
   * exception is raised.
   * If not required -and missing- then the property will not be set.
   * In this case, if the property is already in the Properties instance,
   * it will remain untouched.
   *
   * @param conf     source configuration
   * @param confKey  key in the configuration file
   * @param props    destination property set
   * @param propsKey key in the property set
   * @param required is the property required
   * @throws SwiftConfigurationException if the property is required but was
   *                                     not found in the configuration instance.
   */
  public static void copy(Configuration conf, String confKey, Properties props,
                          String propsKey,
                          boolean required) throws SwiftConfigurationException {
    //TODO: replace. version compatibility issue conf.getTrimmed fails with NoSuchMethodError
    String val = conf.get(confKey);
    if (val != null) {
      val = val.trim();
    }
    if (required && val == null) {
      throw new SwiftConfigurationException(
              "Missing mandatory configuration option: "
                      +
                      confKey);
    }
    set(props, propsKey, val);
  }


}
