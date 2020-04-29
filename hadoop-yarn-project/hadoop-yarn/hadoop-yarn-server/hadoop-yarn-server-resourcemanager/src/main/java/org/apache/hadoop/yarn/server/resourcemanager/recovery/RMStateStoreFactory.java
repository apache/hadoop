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
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class RMStateStoreFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMStateStoreFactory.class);
  
  public static RMStateStore getStore(Configuration conf) {
    Class<? extends RMStateStore> storeClass =
        conf.getClass(YarnConfiguration.RM_STORE,
            MemoryRMStateStore.class, RMStateStore.class);
    LOG.info("Using RMStateStore implementation - " + storeClass);
    return ReflectionUtils.newInstance(storeClass, conf);
  }
}