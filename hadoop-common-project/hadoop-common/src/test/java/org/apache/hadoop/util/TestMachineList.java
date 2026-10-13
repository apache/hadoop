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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;
import org.junit.Test;

public class TestMachineList {
  private static String IP_LIST = "10.119.103.110,10.119.103.112,10.119.103.114";
  private static String IP_LIST_SPACES = 
    " 10.119.103.110 , 10.119.103.112,10.119.103.114 ,10.119.103.110, ";
  private static String CIDR_LIST = "10.222.0.0/16,10.241.23.0/24";
  private static String CIDR_LIST1 = "10.222.0.0/16";
  private static String CIDR_LIST2 = "10.241.23.0/24";
  private static String INVALID_CIDR = "10.241/24";
  private static String IP_CIDR_LIST =
    "10.222.0.0/16,10.119.103.110,10.119.103.112,10.119.103.114,10.241.23.0/24";
  private static final String IPV6_CIDR_LIST = "2001:db8::/32,fd00:1:2:3::/64";
  private static final String IPV4_IPV6_LIST =
      "10.222.0.0/16,2001:db8::/32,10.119.103.110,fd00::5";
  private static String HOST_LIST = "host1,host4";
  private static String HOSTNAME_IP_CIDR_LIST =
    "host1,10.222.0.0/16,10.119.103.110,10.119.103.112,10.119.103.114,10.241.23.0/24,host4,";

  class TestAddressFactory extends MachineList.InetAddressFactory {
    private Map<String, InetAddress> cache = new HashMap<>();
    InetAddress put(String ip) throws UnknownHostException {
      return put(ip, ip);
    }
    InetAddress put(String ip, String... hosts) throws UnknownHostException {
      InetAddress addr = InetAddress.getByName(ip);
      for (String host : hosts) {
        addr = InetAddress.getByAddress(host, addr.getAddress());
        cache.put(host, addr);
        // last host wins the PTR lookup.
        cache.put(ip, addr);
      }
      return addr;
    }
    @Override
    public InetAddress getByName(String host) throws UnknownHostException {
      InetAddress addr = cache.get(host);
      if (addr == null) {
        if (!InetAddresses.isInetAddress(host)) {
          throw new UnknownHostException(host);
        }
        // ip resolves to itself to fake being unresolvable.
        addr = InetAddress.getByName(host);
        addr = InetAddress.getByAddress(host, addr.getAddress());
      }
      return addr;
    }
  }

  @Test
  public void testWildCard() {
    //create MachineList with a list of of IPs
    MachineList ml = new MachineList("*", new TestAddressFactory());

    //test for inclusion with any IP
    assertTrue(ml.includes("10.119.103.112"));
    assertTrue(ml.includes("1.2.3.4"));
  }

  @Test
  public void testIPList() {
    //create MachineList with a list of of IPs
    MachineList ml = new MachineList(IP_LIST, new TestAddressFactory());

    //test for inclusion with an known IP
    assertTrue(ml.includes("10.119.103.112"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("10.119.103.111"));
  }

  @Test
  public void testIPListSpaces() {
    //create MachineList with a ip string which has duplicate ip and spaces
    MachineList ml = new MachineList(IP_LIST_SPACES, new TestAddressFactory());

    //test for inclusion with an known IP
    assertTrue(ml.includes("10.119.103.112"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("10.119.103.111"));
  }

  @Test
  public void testStaticIPHostNameList()throws UnknownHostException {
    // create MachineList with a list of of Hostnames
    TestAddressFactory addressFactory = new TestAddressFactory();
    addressFactory.put("1.2.3.1", "host1");
    addressFactory.put("1.2.3.4", "host4");

    MachineList ml = new MachineList(
        StringUtils.getTrimmedStringCollection(HOST_LIST), addressFactory);

    // test for inclusion with an known IP
    assertTrue(ml.includes("1.2.3.4"));

    // test for exclusion with an unknown IP
    assertFalse(ml.includes("1.2.3.5"));
  }

  @Test
  public void testHostNames() throws UnknownHostException {
    // create MachineList with a list of of Hostnames
    TestAddressFactory addressFactory = new TestAddressFactory();
    addressFactory.put("1.2.3.1", "host1");
    addressFactory.put("1.2.3.4", "host4", "differentname");
    addressFactory.put("1.2.3.5", "host5");

    MachineList ml = new MachineList(
        StringUtils.getTrimmedStringCollection(HOST_LIST), addressFactory );

    //test for inclusion with an known IP
    assertTrue(ml.includes("1.2.3.4"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("1.2.3.5"));
  }

  @Test
  public void testHostNamesReverserIpMatch() throws UnknownHostException {
    // create MachineList with a list of of Hostnames
    TestAddressFactory addressFactory = new TestAddressFactory();
    addressFactory.put("1.2.3.1", "host1");
    addressFactory.put("1.2.3.4", "host4");
    addressFactory.put("1.2.3.5", "host5");

    MachineList ml = new MachineList(
        StringUtils.getTrimmedStringCollection(HOST_LIST), addressFactory );

    //test for inclusion with an known IP
    assertTrue(ml.includes("1.2.3.4"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("1.2.3.5"));
  }

  @Test
  public void testCIDRs() {
    //create MachineList with a list of of ip ranges specified in CIDR format
    MachineList ml = new MachineList(CIDR_LIST, new TestAddressFactory());

    //test for inclusion/exclusion 
    assertFalse(ml.includes("10.221.255.255"));
    assertTrue(ml.includes("10.222.0.0")); 
    assertTrue(ml.includes("10.222.0.1"));
    assertTrue(ml.includes("10.222.0.255"));
    assertTrue(ml.includes("10.222.255.0"));
    assertTrue(ml.includes("10.222.255.254"));
    assertTrue(ml.includes("10.222.255.255"));
    assertFalse(ml.includes("10.223.0.0"));

    assertTrue(ml.includes("10.241.23.0"));
    assertTrue(ml.includes("10.241.23.1"));
    assertTrue(ml.includes("10.241.23.254"));
    assertTrue(ml.includes("10.241.23.255"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("10.119.103.111"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullIpAddress() {
    //create MachineList with a list of of ip ranges specified in CIDR format
    MachineList ml = new MachineList(CIDR_LIST, new TestAddressFactory());

    //test for exclusion with a null IP
    assertFalse(ml.includes((String) null));
    assertFalse(ml.includes((InetAddress) null));
  }

  @Test
  public void testCIDRWith16bitmask() {
    //create MachineList with a list of of ip ranges specified in CIDR format
    MachineList ml = new MachineList(CIDR_LIST1, new TestAddressFactory());

    //test for inclusion/exclusion 
    assertFalse(ml.includes("10.221.255.255"));
    assertTrue(ml.includes("10.222.0.0")); 
    assertTrue(ml.includes("10.222.0.1"));
    assertTrue(ml.includes("10.222.0.255"));
    assertTrue(ml.includes("10.222.255.0"));
    assertTrue(ml.includes("10.222.255.254"));
    assertTrue(ml.includes("10.222.255.255"));
    assertFalse(ml.includes("10.223.0.0"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("10.119.103.111"));
  }

  @Test
  public void testCIDRWith8BitMask() {
    //create MachineList with a list of of ip ranges specified in CIDR format
    MachineList ml = new MachineList(CIDR_LIST2, new TestAddressFactory());

    //test for inclusion/exclusion  
    assertFalse(ml.includes("10.241.22.255"));
    assertTrue(ml.includes("10.241.23.0")); 
    assertTrue(ml.includes("10.241.23.1"));
    assertTrue(ml.includes("10.241.23.254"));
    assertTrue(ml.includes("10.241.23.255")); 
    assertFalse(ml.includes("10.241.24.0"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("10.119.103.111"));
  }

  //test invalid cidr
  @Test
  public void testInvalidCIDR() {
    //create MachineList with an Invalid CIDR
    try {
      MachineList ml = new MachineList(INVALID_CIDR, new TestAddressFactory());
    fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected Exception
    } catch (Throwable t) {
      fail ("Expected only IllegalArgumentException");
    }
  }
  //
  @Test
  public void testIPandCIDRs() {
    //create MachineList with a list of of ip ranges and ip addresses
    MachineList ml = new MachineList(IP_CIDR_LIST, new TestAddressFactory());

    //test for inclusion with an known IP
    assertTrue(ml.includes("10.119.103.112"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("10.119.103.111"));

    //CIDR Ranges
    assertFalse(ml.includes("10.221.255.255"));
    assertTrue(ml.includes("10.222.0.0")); 
    assertTrue(ml.includes("10.222.255.255"));
    assertFalse(ml.includes("10.223.0.0"));

    assertFalse(ml.includes("10.241.22.255"));
    assertTrue(ml.includes("10.241.23.0")); 
    assertTrue(ml.includes("10.241.23.255")); 
    assertFalse(ml.includes("10.241.24.0"));
  }

  @Test
  public void testHostNameIPandCIDRs() {
    //create MachineList with a mix of ip addresses , hostnames and ip ranges
    MachineList ml = new MachineList(HOSTNAME_IP_CIDR_LIST,
        new TestAddressFactory());

    //test for inclusion with an known IP
    assertTrue(ml.includes("10.119.103.112"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("10.119.103.111"));

    //CIDR Ranges
    assertFalse(ml.includes("10.221.255.255"));
    assertTrue(ml.includes("10.222.0.0")); 
    assertTrue(ml.includes("10.222.255.255"));
    assertFalse(ml.includes("10.223.0.0"));

    assertFalse(ml.includes("10.241.22.255"));
    assertTrue(ml.includes("10.241.23.0")); 
    assertTrue(ml.includes("10.241.23.255")); 
    assertFalse(ml.includes("10.241.24.0"));
  }

  @Test
  public void testGetCollection() {
    //create MachineList with a mix of ip addresses , hostnames and ip ranges
    MachineList ml =
        new MachineList(HOSTNAME_IP_CIDR_LIST, new TestAddressFactory());

    Collection<String> col = ml.getCollection();
    //test getCollectionton to return the full collection
    assertEquals(7,ml.getCollection().size());

    for (String item:StringUtils.getTrimmedStringCollection(HOSTNAME_IP_CIDR_LIST)) {
      assertTrue(col.contains(item));
    }
  }

  @Test
  public void testIPv4CIDREdgeCases() {
    MachineList ml = new MachineList("10.1.2.3/32,10.1.3.2/31", new TestAddressFactory());

    assertThat(ml.includes("10.1.2.2")).isFalse();
    assertThat(ml.includes("10.1.2.3")).isTrue();
    assertThat(ml.includes("10.1.2.4")).isFalse();

    assertThat(ml.includes("10.1.3.1")).isFalse();
    assertThat(ml.includes("10.1.3.2")).isTrue();
    assertThat(ml.includes("10.1.3.3")).isTrue();
    assertThat(ml.includes("10.1.3.4")).isFalse();

    ml = new MachineList("0.0.0.0/0", new TestAddressFactory());
    assertThat(ml.includes("1.2.3.4")).isTrue();
    assertThat(ml.includes("255.255.255.255")).isTrue();
    assertThat(ml.includes("2001:db8::1")).isFalse();

    assertThatThrownBy(() -> new MachineList("10.0.0.0/33", new TestAddressFactory()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testIPv6List() {
    MachineList ml = new MachineList("2001:db8::1,fd00::5", new TestAddressFactory());

    assertThat(ml.includes("2001:db8::1")).isTrue();
    assertThat(ml.includes("2001:db8:0:0:0:0:0:1")).isTrue();
    assertThat(ml.includes("fd00::5")).isTrue();
    assertThat(ml.includes("2001:db8::2")).isFalse();
    assertThat(ml.includes("10.119.103.112")).isFalse();
  }

  @Test
  public void testIPv6CIDRs() {
    MachineList ml = new MachineList(IPV6_CIDR_LIST, new TestAddressFactory());

    assertThat(ml.includes("2001:db7:ffff:ffff:ffff:ffff:ffff:ffff")).isFalse();
    assertThat(ml.includes("2001:db8::")).isTrue();
    assertThat(ml.includes("2001:db8::1")).isTrue();
    assertThat(ml.includes("2001:db8:0:0:0:0:0:1")).isTrue();
    assertThat(ml.includes("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff")).isTrue();
    assertThat(ml.includes("2001:db9::")).isFalse();

    assertThat(ml.includes("fd00:1:2:2:ffff:ffff:ffff:ffff")).isFalse();
    assertThat(ml.includes("fd00:1:2:3::")).isTrue();
    assertThat(ml.includes("fd00:1:2:3:ffff:ffff:ffff:ffff")).isTrue();
    assertThat(ml.includes("fd00:1:2:4::")).isFalse();

    // host bits in the entry are ignored, as for an IPv4 CIDR
    ml = new MachineList("2001:db8::1/32", new TestAddressFactory());
    assertThat(ml.includes("2001:db8:ffff::2")).isTrue();
    assertThat(ml.includes("2001:db9::1")).isFalse();
  }

  @Test
  public void testIPv6CIDRPrefixNotOnByteBoundary() {
    MachineList ml =
        new MachineList("2001:db8:8000::/33,fd00:0:0:8::/61", new TestAddressFactory());

    assertThat(ml.includes("2001:db8:7fff:ffff:ffff:ffff:ffff:ffff")).isFalse();
    assertThat(ml.includes("2001:db8:8000::")).isTrue();
    assertThat(ml.includes("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff")).isTrue();
    assertThat(ml.includes("2001:db9::")).isFalse();

    assertThat(ml.includes("fd00:0:0:7:ffff:ffff:ffff:ffff")).isFalse();
    assertThat(ml.includes("fd00:0:0:8::")).isTrue();
    assertThat(ml.includes("fd00:0:0:f:ffff:ffff:ffff:ffff")).isTrue();
    assertThat(ml.includes("fd00:0:0:10::")).isFalse();
  }

  @Test
  public void testIPv6CIDRWithShortestAndLongestPrefix() {
    MachineList ml = new MachineList("2001:db8::1/128", new TestAddressFactory());
    assertThat(ml.includes("2001:db8::")).isFalse();
    assertThat(ml.includes("2001:db8::1")).isTrue();
    assertThat(ml.includes("2001:db8::2")).isFalse();
    assertThat(ml.includes("2001:db8::8001")).isFalse();

    ml = new MachineList("::/0", new TestAddressFactory());
    assertThat(ml.includes("::")).isTrue();
    assertThat(ml.includes("2001:db8::1")).isTrue();
    assertThat(ml.includes("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")).isTrue();
    assertThat(ml.includes("10.119.103.112")).isFalse();
  }

  @Test
  public void testIPv4AndIPv6CIDRs() {
    MachineList ml = new MachineList(IPV4_IPV6_LIST, new TestAddressFactory());

    assertThat(ml.includes("10.222.255.255")).isTrue();
    assertThat(ml.includes("10.223.0.0")).isFalse();
    assertThat(ml.includes("10.119.103.110")).isTrue();
    assertThat(ml.includes("2001:db8:ffff::1")).isTrue();
    assertThat(ml.includes("2001:db9::1")).isFalse();
    assertThat(ml.includes("fd00::5")).isTrue();
    assertThat(ml.includes("fd00::6")).isFalse();
  }

  @Test
  public void testCIDRsOfOtherAddressFamily() throws UnknownHostException {
    // an IPv6 address is not in an IPv4 range, the lookup must not throw
    MachineList ml = new MachineList(CIDR_LIST, new TestAddressFactory());
    assertThat(ml.includes("2001:db8::1")).isFalse();
    assertThat(ml.includes(InetAddress.getByName("2001:db8::1"))).isFalse();
    assertThat(ml.includes("::10.222.0.1")).isFalse();

    ml = new MachineList(IPV6_CIDR_LIST, new TestAddressFactory());
    assertThat(ml.includes("10.222.0.1")).isFalse();
    // 32.1.13.184 has the same leading bytes as 2001:db8::
    assertThat(ml.includes(InetAddress.getByName("32.1.13.184"))).isFalse();
  }

  @Test
  public void testIPv4MappedIPv6Address() {
    // the JDK turns an IPv4-mapped IPv6 address into an IPv4 address
    MachineList ml = new MachineList(CIDR_LIST, new TestAddressFactory());
    assertThat(ml.includes("::ffff:10.222.0.1")).isTrue();
    assertThat(ml.includes("::ffff:10.223.0.0")).isFalse();

    ml = new MachineList("::/0", new TestAddressFactory());
    assertThat(ml.includes("::ffff:10.222.0.1")).isFalse();
  }

  @Test
  public void testIPv6CIDRIgnoresScopeIdOfAddress() throws UnknownHostException {
    MachineList ml = new MachineList("fe80::/10", new TestAddressFactory());
    byte[] linkLocal = InetAddress.getByName("fe80::1").getAddress();
    byte[] siteLocal = InetAddress.getByName("fec0::1").getAddress();

    assertThat(ml.includes(Inet6Address.getByAddress(null, linkLocal, 1))).isTrue();
    assertThat(ml.includes(Inet6Address.getByAddress(null, siteLocal, 1))).isFalse();
  }

  @Test
  public void testInvalidIPv6CIDR() {
    String[] invalidCidrs = {"2001:db8::/129", "2001:db8::/1000", "2001:db8::/-1",
        "2001:db8::/+32", "2001:db8::/x", "2001:db8::/", "2001:db8::/32/64", "2001:db8::g/32",
        ":/32", "[2001:db8::]/32", "fe80::%1/64", "fe80::%/64", "2001:db8::\uff11/128",
        "::ffff:10.222.0.0/112"};
    for (String invalidCidr : invalidCidrs) {
      assertThatThrownBy(() -> new MachineList(invalidCidr, new TestAddressFactory()))
          .as(invalidCidr)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
