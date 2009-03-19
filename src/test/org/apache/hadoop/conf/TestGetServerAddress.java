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
package org.apache.hadoop.conf;

import junit.framework.TestCase;
import org.apache.hadoop.net.NetUtils;

import java.net.InetSocketAddress;

/**
 * Created 22-Jan-2009 14:15:59
 */

public class TestGetServerAddress extends TestCase {
  private Configuration conf;
  private static final String ADDRESS_TUPLE = "addressTuple";
  private static final String BIND_ADDRESS_PORT = "bindAddressPort";
  private static final String BIND_ADDRESS_NAME = "bindAddressName";
  private static final String NOT_A_HOST_PORT_PAIR = "Not a host:port pair: ";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
  }

  private String lookup() {
    String address = NetUtils.getServerAddress(conf,
            BIND_ADDRESS_NAME,
            BIND_ADDRESS_PORT,
            ADDRESS_TUPLE);
    assertNotNull("Null string for the server address", address);
    return address;
  }

  private void setAddressTuple(String value) {
    conf.set(ADDRESS_TUPLE, value);
  }

  private void setAddressPort(String value) {
    conf.set(BIND_ADDRESS_PORT, value);
  }

  private void setAddressName(String value) {
    conf.set(BIND_ADDRESS_NAME, value);
  }

  private void assertContains(String expected, Throwable throwable) {
    assertContains(expected, throwable.getMessage());
  }

    private void assertContains(String expected, String value) {
    assertNotNull("Expected " + expected + " got null string", value);
    assertTrue("No \"" + expected + "\" in \"" + value + "\"",
            value.contains(expected));
  }

  private void expectLookupFailure(String exceptionText) {
    try {
      String address = lookup();
      fail("Expected an an exception, got " + address);
    } catch (IllegalArgumentException expected) {
      assertContains(exceptionText, expected);
    }
  }

  public void testNoValues() {
    expectLookupFailure(ADDRESS_TUPLE);
  }

  public void testPortHasPriority() throws Throwable {
    setAddressTuple("name:8080");
    String port = "1234";
    setAddressPort(port);
    assertContains(port, lookup());
  }


  public void testNameHasPriority() throws Throwable {
    setAddressTuple("name:8080");
    String name = "localhost";
    setAddressName(name);
    assertContains(name, lookup());
  }

  public void testNameAndPort() throws Throwable {
    setAddressName("name");
    setAddressPort("8080");
    expectLookupFailure("No value for addressTuple");
  }

  public void testNameNoPort() throws Throwable {
    setAddressName("name");
    expectLookupFailure("No value for addressTuple");
  }


  public void testEmptyString() throws Throwable {
    setAddressTuple("");
    String address = lookup();
    try {
      InetSocketAddress addr = NetUtils.createSocketAddr(address, -1);
      fail("Expected an an exception, got " + address + " and hence " + addr);
    } catch (RuntimeException expected) {
      assertContains(NOT_A_HOST_PORT_PAIR, expected);
    }
  }


  public void testAddressIsNotATuple() {
    setAddressTuple("localhost");
    String address = lookup();
    try {
      InetSocketAddress addr = NetUtils.createSocketAddr(address, -1);
      fail("Expected an an exception, got " + address + " and hence " + addr);
    } catch (RuntimeException expected) {
      assertContains(NOT_A_HOST_PORT_PAIR, expected);
      assertContains("localhost", expected);
    }
  }

  public void testAddressIsATriple() {
    setAddressTuple("localhost:8080:1234");
    String address = lookup();
    try {
      InetSocketAddress addr = NetUtils.createSocketAddr(address, 0);
      fail("Expected an an exception, got " + address + " and hence " + addr);
    } catch (NumberFormatException expected) {
      assertContains("8080:1234", expected);
    }
  }

  public void testBindAddressIsATriple() {
    setAddressPort("8080:1234");
    setAddressTuple("localhost:8");
    //non-tuples are not picked up when the socket is created
    String address = lookup();
    try {
      InetSocketAddress addr = NetUtils.createSocketAddr(address, 0);
      fail("Expected an an exception, got " + address + " and hence " + addr);
    } catch (NumberFormatException expected) {
      assertContains("8080:1234", expected);
    }
  }

  public void testAddressPortIsInvalid() {
    setAddressTuple("localhost:twelve");
    String address = lookup();
    try {
      InetSocketAddress addr = NetUtils.createSocketAddr(address, 0);
      fail("Expected an an exception, got " + address + " and hence " + addr);
    } catch (NumberFormatException expected) {
      assertContains("twelve", expected);
    }
  }

  public void testAddressPortIsSigned() {
    setAddressPort("-23");
    setAddressTuple("localhost:8");
    String address = lookup();
    try {
      InetSocketAddress addr = NetUtils.createSocketAddr(address, 0);
      fail("Expected an an exception, got " + address + " and hence " + addr);
    } catch (IllegalArgumentException expected) {
      assertContains("port out of range:-23", expected);
    }
  }


}
