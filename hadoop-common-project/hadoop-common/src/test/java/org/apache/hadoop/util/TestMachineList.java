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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.junit.Test;
import org.mockito.Mockito;

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
  private static String HOST_LIST = "host1,host4";
  private static String HOSTNAME_IP_CIDR_LIST =
    "host1,10.222.0.0/16,10.119.103.110,10.119.103.112,10.119.103.114,10.241.23.0/24,host4,";

  @Test
  public void testWildCard() {
    //create MachineList with a list of of IPs
    MachineList ml = new MachineList("*");

    //test for inclusion with any IP
    assertTrue(ml.includes("10.119.103.112"));
    assertTrue(ml.includes("1.2.3.4"));
  }

  @Test
  public void testIPList() {
    //create MachineList with a list of of IPs
    MachineList ml = new MachineList(IP_LIST);

    //test for inclusion with an known IP
    assertTrue(ml.includes("10.119.103.112"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("10.119.103.111"));
  }

  @Test
  public void testIPListSpaces() {
    //create MachineList with a ip string which has duplicate ip and spaces
    MachineList ml = new MachineList(IP_LIST_SPACES);

    //test for inclusion with an known IP
    assertTrue(ml.includes("10.119.103.112"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("10.119.103.111"));
  }

  @Test
  public void testStaticIPHostNameList()throws UnknownHostException {
    //create MachineList with a list of of Hostnames
    InetAddress addressHost1 = InetAddress.getByName("1.2.3.1");
    InetAddress addressHost4 = InetAddress.getByName("1.2.3.4");

    MachineList.InetAddressFactory addressFactory = 
      Mockito.mock(MachineList.InetAddressFactory.class);
    Mockito.when(addressFactory.getByName("host1")).thenReturn(addressHost1);
    Mockito.when(addressFactory.getByName("host4")).thenReturn(addressHost4);

    MachineList ml = new MachineList(
        StringUtils.getTrimmedStringCollection(HOST_LIST), addressFactory);

    //test for inclusion with an known IP
    assertTrue(ml.includes("1.2.3.4"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("1.2.3.5"));
  }

  @Test
  public void testHostNames() throws UnknownHostException {
    //create MachineList with a list of of Hostnames
    InetAddress addressHost1 = InetAddress.getByName("1.2.3.1");
    InetAddress addressHost4 = InetAddress.getByName("1.2.3.4");
    InetAddress addressMockHost4 = Mockito.mock(InetAddress.class);
    Mockito.when(addressMockHost4.getCanonicalHostName()).thenReturn("differentName");

    InetAddress addressMockHost5 = Mockito.mock(InetAddress.class);
    Mockito.when(addressMockHost5.getCanonicalHostName()).thenReturn("host5");

    MachineList.InetAddressFactory addressFactory = 
      Mockito.mock(MachineList.InetAddressFactory.class);
    Mockito.when(addressFactory.getByName("1.2.3.4")).thenReturn(addressMockHost4);
    Mockito.when(addressFactory.getByName("1.2.3.5")).thenReturn(addressMockHost5);
    Mockito.when(addressFactory.getByName("host1")).thenReturn(addressHost1);
    Mockito.when(addressFactory.getByName("host4")).thenReturn(addressHost4);

    MachineList ml = new MachineList(
        StringUtils.getTrimmedStringCollection(HOST_LIST), addressFactory );

    //test for inclusion with an known IP
    assertTrue(ml.includes("1.2.3.4"));

    //test for exclusion with an unknown IP
    assertFalse(ml.includes("1.2.3.5"));
  }

  @Test
  public void testHostNamesReverserIpMatch() throws UnknownHostException {
    //create MachineList with a list of of Hostnames
    InetAddress addressHost1 = InetAddress.getByName("1.2.3.1");
    InetAddress addressHost4 = InetAddress.getByName("1.2.3.4");
    InetAddress addressMockHost4 =  Mockito.mock(InetAddress.class);
    Mockito.when(addressMockHost4.getCanonicalHostName()).thenReturn("host4");

    InetAddress addressMockHost5 =  Mockito.mock(InetAddress.class);
    Mockito.when(addressMockHost5.getCanonicalHostName()).thenReturn("host5");

    MachineList.InetAddressFactory addressFactory = 
      Mockito.mock(MachineList.InetAddressFactory.class);
    Mockito.when(addressFactory.getByName("1.2.3.4")).thenReturn(addressMockHost4);
    Mockito.when(addressFactory.getByName("1.2.3.5")).thenReturn(addressMockHost5);
    Mockito.when(addressFactory.getByName("host1")).thenReturn(addressHost1);
    Mockito.when(addressFactory.getByName("host4")).thenReturn(addressHost4);

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
    MachineList ml = new MachineList(CIDR_LIST);

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

  @Test
  public void testCIDRWith16bitmask() {
    //create MachineList with a list of of ip ranges specified in CIDR format
    MachineList ml = new MachineList(CIDR_LIST1);

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
    MachineList ml = new MachineList(CIDR_LIST2);

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
    new MachineList(INVALID_CIDR);
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
    MachineList ml = new MachineList(IP_CIDR_LIST);

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
    MachineList ml = new MachineList(HOSTNAME_IP_CIDR_LIST);

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
    MachineList ml = new MachineList(HOSTNAME_IP_CIDR_LIST);

    Collection<String> col = ml.getCollection();
    //test getCollectionton to return the full collection
    assertEquals(7,ml.getCollection().size());

    for (String item:StringUtils.getTrimmedStringCollection(HOSTNAME_IP_CIDR_LIST)) {
      assertTrue(col.contains(item));
    }
  }
}
