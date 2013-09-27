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
package org.apache.hadoop.mapreduce.v2.hs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.util.ReflectionUtils;

public class HistoryServerStateStoreServiceFactory {

  /**
   * Constructs an instance of the configured storage class
   * 
   * @param conf the configuration
   * @return the state storage instance
   */
  public static HistoryServerStateStoreService getStore(Configuration conf) {
    Class<? extends HistoryServerStateStoreService> storeClass =
        HistoryServerNullStateStoreService.class;
    boolean recoveryEnabled = conf.getBoolean(
        JHAdminConfig.MR_HS_RECOVERY_ENABLE,
        JHAdminConfig.DEFAULT_MR_HS_RECOVERY_ENABLE);
    if (recoveryEnabled) {
      storeClass = conf.getClass(JHAdminConfig.MR_HS_STATE_STORE, null,
          HistoryServerStateStoreService.class);
      if (storeClass == null) {
        throw new RuntimeException("Unable to locate storage class, check "
            + JHAdminConfig.MR_HS_STATE_STORE);
      }
    }
    return ReflectionUtils.newInstance(storeClass, conf);
  }
}
