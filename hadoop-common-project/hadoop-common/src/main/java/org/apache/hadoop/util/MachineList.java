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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.net.util.SubnetUtils;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container class which holds a list of ip/host addresses and 
 * answers membership queries.
 *
 * Accepts list of ip addresses, ip addreses in CIDR format and/or 
 * host addresses.
 */

public class MachineList {
  
  public static final Logger LOG = LoggerFactory.getLogger(MachineList.class);
  public static final String WILDCARD_VALUE = "*";

  /**
   * InetAddressFactory is used to obtain InetAddress from host.
   * This class makes it easy to simulate host to ip mappings during testing.
   *
   */
  public static class InetAddressFactory {

    static final InetAddressFactory S_INSTANCE = new InetAddressFactory();

    public InetAddress getByName (String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }
  }

  private final boolean all;
  private final Set<InetAddress> inetAddresses;
  private final Collection<String> entries;
  private final List<SubnetUtils.SubnetInfo> cidrAddresses;
  private final InetAddressFactory addressFactory;

  /**
   * 
   * @param hostEntries comma separated ip/cidr/host addresses
   */
  public MachineList(String hostEntries) {
    this(hostEntries, InetAddressFactory.S_INSTANCE);
  }

  public MachineList(String hostEntries, InetAddressFactory addressFactory) {
    this(StringUtils.getTrimmedStringCollection(hostEntries), addressFactory);
  }

  /**
   *
   * @param hostEntries collection of separated ip/cidr/host addresses
   */
  public MachineList(Collection<String> hostEntries) {
    this(hostEntries, InetAddressFactory.S_INSTANCE);
  }

  /**
   * Accepts a collection of ip/cidr/host addresses
   * 
   * @param hostEntries
   * @param addressFactory addressFactory to convert host to InetAddress
   */
  public MachineList(Collection<String> hostEntries,
      InetAddressFactory addressFactory) {
    this.addressFactory = addressFactory;
    if (hostEntries != null) {
      entries = new ArrayList<>(hostEntries);
      if ((hostEntries.size() == 1) && (hostEntries.contains(WILDCARD_VALUE))) {
        all = true;
        inetAddresses = null;
        cidrAddresses = null; 
      } else {
        all = false;
        Set<InetAddress> addrs = new HashSet<>();
        List<SubnetUtils.SubnetInfo> cidrs = new LinkedList<SubnetUtils.SubnetInfo>();
        for (String hostEntry : hostEntries) {
          //ip address range
          if (hostEntry.indexOf("/") > -1) {
            try {
              SubnetUtils subnet = new SubnetUtils(hostEntry);
              subnet.setInclusiveHostCount(true);
              cidrs.add(subnet.getInfo());
            } catch (IllegalArgumentException e) {
              LOG.warn("Invalid CIDR syntax : " + hostEntry);
              throw e;
            }
          } else {
            try {
              addrs.add(addressFactory.getByName(hostEntry));
            } catch (UnknownHostException e) {
              LOG.warn(e.toString());
            }
          }
        }
        inetAddresses = (addrs.size() > 0) ? addrs : null;
        cidrAddresses = (cidrs.size() > 0) ? cidrs : null;
      }
    } else {
      all = false;
      inetAddresses = null;
      cidrAddresses = null;
      entries = Collections.emptyList();
    }
  }
  /**
   * Accepts an ip address and return true if ipAddress is in the list.
   * {@link #includes(InetAddress)} should be preferred
   * to avoid possibly re-resolving the ip address.
   *
   * @param ipAddress
   * @return true if ipAddress is part of the list
   */
  public boolean includes(String ipAddress) {
    
    if (all) {
      return true;
    }
    
    if (ipAddress == null) {
      throw new IllegalArgumentException("ipAddress is null.");
    }

    try {
      return includes(addressFactory.getByName(ipAddress));
    } catch (UnknownHostException e) {
      return false;
    }
  }

  /**
   * Accepts an inet address and return true if address is in the list.
   * @param address
   * @return true if address is part of the list
   */
  public boolean includes(InetAddress address) {
    if (all) {
      return true;
    }
    if (address == null) {
      throw new IllegalArgumentException("address is null.");
    }
    if (inetAddresses != null && inetAddresses.contains(address)) {
      return true;
    }
    // iterate through the ip ranges for inclusion
    if (cidrAddresses != null) {
      String ipAddress = address.getHostAddress();
      for(SubnetUtils.SubnetInfo cidrAddress : cidrAddresses) {
        if(cidrAddress.isInRange(ipAddress)) {
          return true;
        }
      }
    }
    return false;
  }
  /**
   * returns the contents of the MachineList as a Collection&lt;String&gt; .
   * This can be used for testing .
   *
   * @return contents of the MachineList.
   */
  @VisibleForTesting
  public Collection<String> getCollection() {
    return entries;
  }
}
