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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for managing HDFS federation.
 */
public final class FederationUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationUtil.class);

  private FederationUtil() {
    // Utility Class
  }

  /**
   * Get a JMX data from a web endpoint.
   *
   * @param beanQuery JMX bean.
   * @param webAddress Web address of the JMX endpoint.
   * @return JSON with the JMX data
   */
  public static JSONArray getJmx(String beanQuery, String webAddress) {
    JSONArray ret = null;
    BufferedReader reader = null;
    try {
      String host = webAddress;
      int port = -1;
      if (webAddress.indexOf(":") > 0) {
        String[] webAddressSplit = webAddress.split(":");
        host = webAddressSplit[0];
        port = Integer.parseInt(webAddressSplit[1]);
      }
      URL jmxURL = new URL("http", host, port, "/jmx?qry=" + beanQuery);
      URLConnection conn = jmxURL.openConnection();
      conn.setConnectTimeout(5 * 1000);
      conn.setReadTimeout(5 * 1000);
      InputStream in = conn.getInputStream();
      InputStreamReader isr = new InputStreamReader(in, "UTF-8");
      reader = new BufferedReader(isr);

      StringBuilder sb = new StringBuilder();
      String line = null;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      String jmxOutput = sb.toString();

      // Parse JSON
      JSONObject json = new JSONObject(jmxOutput);
      ret = json.getJSONArray("beans");
    } catch (IOException e) {
      LOG.error("Cannot read JMX bean {} from server {}: {}",
          beanQuery, webAddress, e.getMessage());
    } catch (JSONException e) {
      LOG.error("Cannot parse JMX output for {} from server {}: {}",
          beanQuery, webAddress, e.getMessage());
    } catch (Exception e) {
      LOG.error("Cannot parse JMX output for {} from server {}: {}",
          beanQuery, webAddress, e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error("Problem closing {}", webAddress, e);
        }
      }
    }
    return ret;
  }

  /**
   * Create an instance of an interface with a constructor using a context.
   *
   * @param conf Configuration for the class names.
   * @param context Context object to pass to the instance.
   * @param contextClass Type of the context passed to the constructor.
   * @param clazz Class of the object to return.
   * @return New instance of the specified class that implements the desired
   *         interface and a single parameter constructor containing a
   *         StateStore reference.
   */
  private static <T, R> T newInstance(final Configuration conf,
      final R context, final Class<R> contextClass, final Class<T> clazz) {
    try {
      if (contextClass == null) {
        // Default constructor if no context
        Constructor<T> constructor = clazz.getConstructor();
        return constructor.newInstance();
      } else {
        // Constructor with context
        Constructor<T> constructor = clazz.getConstructor(
            Configuration.class, contextClass);
        return constructor.newInstance(conf, context);
      }
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: {}", clazz.getSimpleName(), e);
      return null;
    }
  }

  /**
   * Creates an instance of a FileSubclusterResolver from the configuration.
   *
   * @param conf Configuration that defines the file resolver class.
   * @param router Router service.
   * @return New file subcluster resolver.
   */
  public static FileSubclusterResolver newFileSubclusterResolver(
      Configuration conf, Router router) {
    Class<? extends FileSubclusterResolver> clazz = conf.getClass(
        DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS_DEFAULT,
        FileSubclusterResolver.class);
    return newInstance(conf, router, Router.class, clazz);
  }

  /**
   * Creates an instance of an ActiveNamenodeResolver from the configuration.
   *
   * @param conf Configuration that defines the namenode resolver class.
   * @param obj Context object passed to class constructor.
   * @return New active namenode resolver.
   */
  public static ActiveNamenodeResolver newActiveNamenodeResolver(
      Configuration conf, StateStoreService stateStore) {
    Class<? extends ActiveNamenodeResolver> clazz = conf.getClass(
        DFSConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        DFSConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS_DEFAULT,
        ActiveNamenodeResolver.class);
    return newInstance(conf, stateStore, StateStoreService.class, clazz);
  }
}
