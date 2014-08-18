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
package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CombinedIPWhiteList implements IPList {

  public static final Log LOG = LogFactory.getLog(CombinedIPWhiteList.class);
  private static final String LOCALHOST_IP = "127.0.0.1";

  private final IPList[] networkLists;

  public CombinedIPWhiteList(String fixedWhiteListFile,
      String variableWhiteListFile, long cacheExpiryInSeconds) {

    IPList fixedNetworkList = new FileBasedIPList(fixedWhiteListFile);
    if (variableWhiteListFile != null){
      IPList variableNetworkList = new CacheableIPList(
          new FileBasedIPList(variableWhiteListFile),cacheExpiryInSeconds);
      networkLists = new IPList[] {fixedNetworkList, variableNetworkList};
    }
    else {
      networkLists = new IPList[] {fixedNetworkList};
    }
  }
  @Override
  public boolean isIn(String ipAddress) {
    if (ipAddress == null) {
      throw new IllegalArgumentException("ipAddress is null");
    }

    if (LOCALHOST_IP.equals(ipAddress)) {
      return true;
    }

    for (IPList networkList:networkLists) {
      if (networkList.isIn(ipAddress)) {
        return true;
      }
    }
    return false;
  }
}
