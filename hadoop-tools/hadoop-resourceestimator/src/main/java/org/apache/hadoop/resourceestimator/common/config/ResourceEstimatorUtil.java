/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.common.config;

import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.resourceestimator.common.exception.ResourceEstimatorException;

/**
 * General resourceestimator utils.
 */
public final class ResourceEstimatorUtil {

  private static final Class<?>[] EMPTY_ARRAY = new Class[0];

  private ResourceEstimatorUtil() {}

  /**
   * Helper method to create instances of Object using the class name specified
   * in the configuration object.
   *
   * @param conf                the yarn configuration
   * @param configuredClassName the configuration provider key
   * @param defaultValue        the default implementation class
   * @param type                the required interface/base class
   * @param <T>                 The type of the instance to create
   * @return the instances created
   * @throws ResourceEstimatorException if the provider initialization fails.
   */
  @SuppressWarnings("unchecked") public static <T> T createProviderInstance(
      Configuration conf, String configuredClassName, String defaultValue,
      Class<T> type) throws ResourceEstimatorException {
    String className = conf.get(configuredClassName);
    if (className == null) {
      className = defaultValue;
    }
    try {
      Class<?> concreteClass = Class.forName(className);
      if (type.isAssignableFrom(concreteClass)) {
        Constructor<T> meth =
            (Constructor<T>) concreteClass.getDeclaredConstructor(EMPTY_ARRAY);
        meth.setAccessible(true);
        return meth.newInstance();
      } else {
        StringBuilder errMsg = new StringBuilder();
        errMsg.append("Class: ").append(className).append(" not instance of ")
            .append(type.getCanonicalName());
        throw new ResourceEstimatorException(errMsg.toString());
      }
    } catch (ClassNotFoundException e) {
      StringBuilder errMsg = new StringBuilder();
      errMsg.append("Could not instantiate : ").append(className)
          .append(" due to exception: ").append(e.getCause());
      throw new ResourceEstimatorException(errMsg.toString());
    } catch (ReflectiveOperationException e) {
      StringBuilder errMsg = new StringBuilder();
      errMsg.append("Could not instantiate : ").append(className)
          .append(" due to exception: ").append(e.getCause());
      throw new ResourceEstimatorException(errMsg.toString());
    }
  }
}
