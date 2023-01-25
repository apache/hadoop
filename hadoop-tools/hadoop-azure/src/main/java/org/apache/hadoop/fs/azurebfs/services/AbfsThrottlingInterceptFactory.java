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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.util.WeakReferenceMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to get an instance of throttling intercept class per account.
 */
final class AbfsThrottlingInterceptFactory {

  private AbfsThrottlingInterceptFactory() {
  }

  private static AbfsConfiguration abfsConfig;

  /**
   * List of references notified of loss.
   */
  private static List<String> lostReferences = new ArrayList<>();

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsThrottlingInterceptFactory.class);

  /**
   * Map which stores instance of ThrottlingIntercept class per account.
   */
  private static WeakReferenceMap<String, AbfsThrottlingIntercept>
      interceptMap = new WeakReferenceMap<>(
      AbfsThrottlingInterceptFactory::factory,
      AbfsThrottlingInterceptFactory::referenceLost);

  /**
   * Returns instance of throttling intercept.
   * @param accountName Account name.
   * @return instance of throttling intercept.
   */
  private static AbfsClientThrottlingIntercept factory(final String accountName) {
    return new AbfsClientThrottlingIntercept(accountName, abfsConfig);
  }

  /**
   * Reference lost callback.
   * @param accountName key lost.
   */
  private static void referenceLost(String accountName) {
    lostReferences.add(accountName);
  }

  /**
   * Returns an instance of AbfsThrottlingIntercept.
   *
   * @param accountName The account for which we need instance of throttling intercept.
     @param abfsConfiguration The object of abfsconfiguration class.
    * @return Instance of AbfsThrottlingIntercept.
   */
  static synchronized AbfsThrottlingIntercept getInstance(String accountName,
      AbfsConfiguration abfsConfiguration) {
    abfsConfig =  abfsConfiguration;
    AbfsThrottlingIntercept intercept;
    if (!abfsConfiguration.isAutoThrottlingEnabled()) {
      return AbfsNoOpThrottlingIntercept.INSTANCE;
    }
    // If singleton is enabled use a static instance of the intercept class for all accounts
    if (!abfsConfiguration.accountThrottlingEnabled()) {
      intercept = AbfsClientThrottlingIntercept.initializeSingleton(
          abfsConfiguration);
    } else {
      // Return the instance from the map
      intercept = interceptMap.get(accountName);
      if (intercept == null) {
        intercept = new AbfsClientThrottlingIntercept(accountName,
            abfsConfiguration);
        interceptMap.put(accountName, intercept);
      }
    }
    return intercept;
  }
}
