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

package org.apache.hadoop.security;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.TokenInfo;

@Evolving
@LimitedPrivate({"MapReduce", "HDFS"})
/**
 * Interface used by RPC to get the Security information for a given 
 * protocol.
 */
public abstract class SecurityInfo {

  /**
   * Get the KerberosInfo for a given protocol.
   * @param protocol interface class
   * @param conf configuration
   * @return KerberosInfo
   */
  public abstract KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf);

  /**
   * Get the TokenInfo for a given protocol.
   * @param protocol interface class
   * @param conf configuration object.
   * @return TokenInfo instance
   */
  public abstract TokenInfo getTokenInfo(Class<?> protocol, Configuration conf);

}
