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
package org.apache.hadoop.net;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import java.net.InetAddress;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class InetAddressUtils {

  private static final Logger LOG = LoggerFactory.getLogger(InetAddressUtils.class);

  private InetAddressUtils() {
  }

  /**
   * Gets the fully qualified domain name for this IP address.  It uses the internally cached
   * <code>canonicalHostName</code> when it is available, but will fall back to attempting a reverse
   * DNS lookup when needed.  This is useful when an FQDN is required, and you are running in a
   * managed environment where IP addresses can change.
   *
   * @param addr the target address
   * @return the fully qualified domain name for this IP address, or the textual representation of
   *        the IP address.
   */
  public static String getCanonicalHostName(InetAddress addr) {
    String canonicalHostName = addr.getCanonicalHostName();
    if (canonicalHostName.equals(addr.getHostAddress())) {
      try {
        return DNS.reverseDns(addr, null);
      } catch (NamingException lookupFailure) {
        LOG.warn("Failed to perform reverse lookup: {}", addr, lookupFailure);
      }
    }
    return canonicalHostName;
  }

}
