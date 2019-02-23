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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;

/**
 * This mock resolver class returns the predefined resolving results.
 * By default it uses a default "test.foo.bar" domain with two IP addresses.
 */
public class MockDomainNameResolver implements DomainNameResolver {

  public static final String DOMAIN = "test.foo.bar";
  // This host will be used to mock non-resolvable host
  public static final String UNKNOW_DOMAIN = "unknown.foo.bar";
  public static final byte[] BYTE_ADDR_1 = new byte[]{10, 1, 1, 1};
  public static final byte[] BYTE_ADDR_2 = new byte[]{10, 1, 1, 2};
  public static final String ADDR_1 = "10.1.1.1";
  public static final String ADDR_2 = "10.1.1.2";

  /** Internal mapping of domain names and IP addresses. */
  private Map<String, InetAddress[]> addrs = new TreeMap<>();


  public MockDomainNameResolver() {
    try {
      InetAddress nn1Address = InetAddress.getByAddress(BYTE_ADDR_1);
      InetAddress nn2Address = InetAddress.getByAddress(BYTE_ADDR_2);
      addrs.put(DOMAIN, new InetAddress[]{nn1Address, nn2Address});
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public InetAddress[] getAllByDomainName(String domainName)
      throws UnknownHostException {
    if (!addrs.containsKey(domainName)) {
      throw new UnknownHostException(domainName + " is not resolvable");
    }
    return addrs.get(domainName);
  }

  @VisibleForTesting
  public void setAddressMap(Map<String, InetAddress[]> addresses) {
    this.addrs = addresses;
  }
}
