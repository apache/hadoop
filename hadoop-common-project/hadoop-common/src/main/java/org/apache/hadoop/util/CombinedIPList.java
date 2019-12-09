/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class to stores ips/hosts/subnets.
 */
public class CombinedIPList implements IPList {

  public static final Logger LOG =
      LoggerFactory.getLogger(CombinedIPList.class);

  private final IPList[] networkLists;

  public CombinedIPList(String fixedBlackListFile,
      String variableBlackListFile, long cacheExpiryInSeconds) {

    IPList fixedNetworkList = new FileBasedIPList(fixedBlackListFile);
    if (variableBlackListFile != null) {
      IPList variableNetworkList = new CacheableIPList(
          new FileBasedIPList(variableBlackListFile), cacheExpiryInSeconds);
      networkLists = new IPList[]{fixedNetworkList, variableNetworkList};
    } else {
      networkLists = new IPList[]{fixedNetworkList};
    }
  }

  @Override
  public boolean isIn(String ipAddress) {
    if (ipAddress == null) {
      throw new IllegalArgumentException("ipAddress is null");
    }

    for (IPList networkList : networkLists) {
      if (networkList.isIn(ipAddress)) {
        return true;
      }
    }
    return false;
  }
}