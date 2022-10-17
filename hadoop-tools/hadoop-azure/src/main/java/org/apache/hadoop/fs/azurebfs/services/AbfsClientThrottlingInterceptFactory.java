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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to get an instance of throttling intercept class per account.
 */
final class AbfsClientThrottlingInterceptFactory {

  private AbfsClientThrottlingInterceptFactory() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsClientThrottlingInterceptFactory.class);

  // Map which stores instance of ThrottlingIntercept class per account
  private static Map<String, AbfsClientThrottlingIntercept> instanceMapping
      = new ConcurrentHashMap<>();

  /**
   *
   * @param accountName The account for which we need instance of throttling intercept
     @param abfsConfiguration The object of abfsconfiguration class
   * @return Instance of AbfsClientThrottlingIntercept class
   */
  static synchronized AbfsClientThrottlingIntercept getInstance(String accountName,
                                                                AbfsConfiguration abfsConfiguration) {
    AbfsClientThrottlingIntercept instance;
    if (!abfsConfiguration.isAutoThrottlingEnabled()) {
      return null;
    }
    // If singleton is enabled use a static instance of the intercept class for all accounts
    if (!abfsConfiguration.isAccountThrottlingEnabled()) {
      instance = AbfsClientThrottlingIntercept.initializeSingleton(abfsConfiguration);
    } else {
      // Return the instance from the map
      if (instanceMapping.get(accountName) == null) {
        LOG.debug("The accountName is: {} ", accountName);
        instance = new AbfsClientThrottlingIntercept(accountName, abfsConfiguration);
        instanceMapping.put(accountName, instance);
      } else {
        instance = instanceMapping.get(accountName);
      }
    }
    return instance;
  }
}
