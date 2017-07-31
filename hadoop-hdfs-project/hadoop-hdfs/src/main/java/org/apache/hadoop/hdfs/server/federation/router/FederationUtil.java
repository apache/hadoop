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

import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
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
   * Create an instance of an interface with a constructor using a state store
   * constructor.
   *
   * @param conf Configuration
   * @param context Context object to pass to the instance.
   * @param contextType Type of the context passed to the constructor.
   * @param configurationKeyName Configuration key to retrieve the class to load
   * @param defaultClassName Default class to load if the configuration key is
   *          not set
   * @param clazz Class/interface that must be implemented by the instance.
   * @return New instance of the specified class that implements the desired
   *         interface and a single parameter constructor containing a
   *         StateStore reference.
   */
  private static <T, R> T newInstance(final Configuration conf,
      final R context, final Class<R> contextClass,
      final String configKeyName, final String defaultClassName,
      final Class<T> clazz) {

    String className = conf.get(configKeyName, defaultClassName);
    try {
      Class<?> instance = conf.getClassByName(className);
      if (clazz.isAssignableFrom(instance)) {
        if (contextClass == null) {
          // Default constructor if no context
          @SuppressWarnings("unchecked")
          Constructor<T> constructor =
              (Constructor<T>) instance.getConstructor();
          return constructor.newInstance();
        } else {
          // Constructor with context
          @SuppressWarnings("unchecked")
          Constructor<T> constructor = (Constructor<T>) instance.getConstructor(
              Configuration.class, contextClass);
          return constructor.newInstance(conf, context);
        }
      } else {
        throw new RuntimeException("Class " + className + " not instance of "
            + clazz.getCanonicalName());
      }
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: " + className, e);
      return null;
    }
  }

  /**
   * Creates an instance of a FileSubclusterResolver from the configuration.
   *
   * @param conf Configuration that defines the file resolver class.
   * @param obj Context object passed to class constructor.
   * @return FileSubclusterResolver
   */
  public static FileSubclusterResolver newFileSubclusterResolver(
      Configuration conf, StateStoreService stateStore) {
    return newInstance(conf, stateStore, StateStoreService.class,
        DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        DFSConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS_DEFAULT,
        FileSubclusterResolver.class);
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
