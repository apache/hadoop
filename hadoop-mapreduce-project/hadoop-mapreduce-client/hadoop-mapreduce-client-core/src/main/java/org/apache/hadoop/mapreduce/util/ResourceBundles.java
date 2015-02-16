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

package org.apache.hadoop.mapreduce.util;

import java.util.Locale;
import java.util.ResourceBundle;
import java.util.MissingResourceException;

/**
 * Helper class to handle resource bundles in a saner way
 */
public class ResourceBundles {

  /**
   * Get a resource bundle
   * @param bundleName of the resource
   * @return the resource bundle
   * @throws MissingResourceException
   */
  public static ResourceBundle getBundle(String bundleName) {
    return ResourceBundle.getBundle(bundleName.replace('$', '_'),
        Locale.getDefault(), Thread.currentThread().getContextClassLoader());
  }

  /**
   * Get a resource given bundle name and key
   * @param <T> type of the resource
   * @param bundleName name of the resource bundle
   * @param key to lookup the resource
   * @param suffix for the key to lookup
   * @param defaultValue of the resource
   * @return the resource or the defaultValue
   * @throws ClassCastException if the resource found doesn't match T
   */
  @SuppressWarnings("unchecked")
  public static synchronized <T> T getValue(String bundleName, String key,
                                            String suffix, T defaultValue) {
    T value;
    try {
      ResourceBundle bundle = getBundle(bundleName);
      value = (T) bundle.getObject(getLookupKey(key, suffix));
    }
    catch (Exception e) {
      return defaultValue;
    }
    return value;
  }

  private static String getLookupKey(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) return key;
    return key + suffix;
  }

  /**
   * Get the counter group display name
   * @param group the group name to lookup
   * @param defaultValue of the group
   * @return the group display name
   */
  public static String getCounterGroupName(String group, String defaultValue) {
    return getValue(group, "CounterGroupName", "", defaultValue);
  }

  /**
   * Get the counter display name
   * @param group the counter group name for the counter
   * @param counter the counter name to lookup
   * @param defaultValue of the counter
   * @return the counter display name
   */
  public static String getCounterName(String group, String counter,
                                      String defaultValue) {
    return getValue(group, counter, ".name", defaultValue);
  }
}
