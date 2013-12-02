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

package org.apache.hadoop.yarn.client.api;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.Token;

import com.google.common.annotations.VisibleForTesting;

/**
 * It manages NMTokens required for communicating with Node manager. Its a
 * static token cache.
 */
@Public
@Evolving
public class NMTokenCache {
  private static ConcurrentHashMap<String, Token> nmTokens;
  
  
  static {
    nmTokens = new ConcurrentHashMap<String, Token>();
  }
  
  /**
   * Returns NMToken, null if absent
   * @param nodeAddr
   * @return {@link Token} NMToken required for communicating with node
   * manager
   */
  @Public
  @Evolving
  public static Token getNMToken(String nodeAddr) {
    return nmTokens.get(nodeAddr);
  }
  
  /**
   * Sets the NMToken for node address
   * @param nodeAddr node address (host:port)
   * @param token NMToken
   */
  @Public
  @Evolving
  public static void setNMToken(String nodeAddr, Token token) {
    nmTokens.put(nodeAddr, token);
  }
  
  /**
   * Returns true if NMToken is present in cache.
   */
  @Private
  @VisibleForTesting
  public static boolean containsNMToken(String nodeAddr) {
    return nmTokens.containsKey(nodeAddr);
  }
  
  /**
   * Returns the number of NMTokens present in cache.
   */
  @Private
  @VisibleForTesting
  public static int numberOfNMTokensInCache() {
    return nmTokens.size();
  }
  
  /**
   * Removes NMToken for specified node manager
   * @param nodeAddr node address (host:port)
   */
  @Private
  @VisibleForTesting
  public static void removeNMToken(String nodeAddr) {
    nmTokens.remove(nodeAddr);
  }
  
  /**
   * It will remove all the nm tokens from its cache
   */
  @Private
  @VisibleForTesting
  public static void clearCache() {
    nmTokens.clear();
  }
}
