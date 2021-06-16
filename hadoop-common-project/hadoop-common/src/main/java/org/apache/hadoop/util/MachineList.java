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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
   */
  public static class InetAddressFactory {

    static final InetAddressFactory S_INSTANCE = new InetAddressFactory();

    public InetAddress getByName(String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }
  }

  private final boolean all;
  private TrieTree node;
  private final Collection<String> entries;

  /**
   * @param hostEntries comma separated ip/cidr/host addresses
   */
  public MachineList(String hostEntries) {
    this(hostEntries, InetAddressFactory.S_INSTANCE);
  }

  public MachineList(String hostEntries, InetAddressFactory addressFactory) {
    this(StringUtils.getTrimmedStringCollection(hostEntries), addressFactory);
  }

  /**
   * @param hostEntries collection of separated ip/cidr/host addresses
   */
  public MachineList(Collection<String> hostEntries) {
    this(hostEntries, InetAddressFactory.S_INSTANCE);
  }

  /**
   * Accepts a collection of ip/cidr/host addresses.
   *
   * @param hostEntries
   * @param addressFactory addressFactory to convert host to InetAddress
   */
  public MachineList(Collection<String> hostEntries,
                     InetAddressFactory addressFactory) {
    if (hostEntries != null) {
      entries = new ArrayList<>(hostEntries);
      if ((hostEntries.size() == 1) && (hostEntries.contains(WILDCARD_VALUE))) {
        all = true;
        node = null;
      } else {
        all = false;
        node = new TrieTree();
        for (String hostEntry : hostEntries) {
          String[] splits = hostEntry.split(":");
          HashSet<String> userSet = null;
          if (splits.length == 2) {
            userSet = new HashSet<>();
            String[] userArray = splits[1].split("\\|");
            if (userArray.length > 0) {
              for (String u : userArray) {
                String user = u.trim();
                if (user.length() > 0) {
                  userSet.add(user);
                }
              }
            }
          }
          String host = splits[0];
          String binaryIp;
          try {
            if (host.contains("/")) {
              String[] cidrArray = host.split("/");
              String ip = cidrArray[0];
              int mask = Integer.parseInt(cidrArray[1]);
              if (!isValidIPv4(ip)) {
                LOG.warn("Invalid CIDR syntax : " + hostEntry);
                throw new IllegalArgumentException();
              }
              binaryIp = subtractBinaryNumber(ipToBinaryNumber(ip), mask);
            } else {
              binaryIp = ipToBinaryNumber(
                  addressFactory.getByName(host).getHostAddress());
            }
          } catch (UnknownHostException e) {
            LOG.warn(e.toString());
            continue;
          }
          node.insert(binaryIp, userSet);
        }
        node = node.isEmpty() ? null : node;
      }
    } else {
      all = false;
      node = null;
      entries = Collections.emptyList();
    }
  }

  /**
   * Accepts an ip address and return true if ipAddress is in the TrieTree.
   * @param ipAddress
   * @return true if ipAddress is part is in the TrieTree.
   */
  public boolean includes(String ipAddress) {

    if (all) {
      return true;
    }

    if (ipAddress == null) {
      throw new IllegalArgumentException("ipAddress is null.");
    }
    // iterate through the ip ranges for inclusion
    if (node != null) {
      String binaryIp = ipToBinaryNumber(ipAddress);
      if (node.search(binaryIp, null)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Accept an inet address and a user and
   * return true if address and user are in the TrieTree .
   *
   * @param ipAddress A address.
   * @return true if address is part of the TrieTree.
   */
  public boolean includes(String ipAddress, String user) {
    if (all) {
      return true;
    }
    if (ipAddress == null) {
      throw new IllegalArgumentException("ipAddress is null.");
    }
    // iterate through the ip ranges for inclusion
    if (node != null) {
      String binaryIp = ipToBinaryNumber(ipAddress);
      if (node.search(binaryIp, user)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Accept an inet address  and return true
   * if address is  in the TrieTree.
   *
   * @param address A InetAddress.
   * @return true if address is part of the TrieTree.
   */
  public boolean includes(InetAddress address) {
    if (all) {
      return true;
    }
    if (address == null) {
      throw new IllegalArgumentException("address is null.");
    }
    // iterate through the ip ranges for inclusion
    if (node != null) {
      String binaryIp = ipToBinaryNumber(address.getHostAddress());
      if (node.search(binaryIp, null)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Accept an inet address and a user and
   * return true if address and user are in the TrieTree .
   *
   * @param address A address.
   * @param user    A user.
   * @return true if address is part of the TrieTree
   */
  public boolean includes(InetAddress address, String user) {
    if (all) {
      return true;
    }
    if (address == null) {
      throw new IllegalArgumentException("address is null.");
    }
    if (node != null) {
      String binaryIp = ipToBinaryNumber(address.getHostAddress());
      if (node.search(binaryIp, user)) {
        return true;
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

  final private static class TrieTree {
    private Node root;

    final private static class Node {
      private boolean netRange;
      private HashSet<String> userSet;
      private Node[] children;

      private Node() {
        netRange = false;
        children = new Node[2];
        Arrays.fill(children, null);
      }
    }

    private TrieTree() {
      root = new Node();
    }

    private void insert(String ipAddress, HashSet<String> user) {
      Node currNode = root;
      for (int i = 0; i < ipAddress.length(); i++) {
        int index = (int) ipAddress.charAt(i) - '0';
        if (currNode.children[index] == null) {
          currNode.children[index] = new Node();
        }
        currNode = currNode.children[index];
      }
      currNode.netRange = true;
      currNode.userSet = user;
    }

    private boolean isEmpty() {
      return root.children[0] == null &&
          root.children[1] == null;
    }

    private boolean search(String binaryIP, String user) {
      Node currNode = root;
      for (int i = 0; !currNode.netRange; i++) {
        int index = (int) binaryIP.charAt(i) - '0';
        if (currNode.children[index] == null) {
          return false;
        }
        Node nodeTmp = currNode.children[index];
        Set users = nodeTmp.userSet;
        if (nodeTmp.netRange) {
          if (user == null) {
            return true;
          } else if (users == null) {
            return false;
          } else if (users.contains(user) || users.contains(WILDCARD_VALUE)) {
            return true;
          }
        }
        currNode = currNode.children[index];
      }
      return false;
    }
  }

  /**
   * Get ip address binary mask.
   *
   * @param ipAddress A ip.
   * @return A binary mask.
   */
  private static String ipToBinaryNumber(String ipAddress) {
    String[] octetArray = ipAddress.split("\\.");
    StringBuilder binaryNumber = new StringBuilder();
    for (String str : octetArray) {
      int octet = Integer.parseInt(str, 10);
      StringBuilder binaryOctet =
          new StringBuilder(Integer.toBinaryString(octet));
      int length = binaryOctet.length();
      if (length < 8) {
        for (int i = 0; i < 8 - length; i++) {
          binaryOctet.insert(0, '0');
        }
      }
      binaryNumber.append(binaryOctet);
    }
    return binaryNumber.toString();
  }

  private String subtractBinaryNumber(String s, int size) {
    return s.substring(0, size);
  }

  /**
   * Accept an String to determine whether it is an ip address.
   *
   * @param ip A ip
   * @return if the given string is a Ipv4 false otherwise.
   */
  private static boolean isValidIPv4(String ip) {
    if (ip.length() < 7) {
      return false;
    }
    if (ip.charAt(0) == '.') {
      return false;
    }
    if (ip.charAt(ip.length() - 1) == '.') {
      return false;
    }
    String[] tokens = ip.split("\\.");
    if (tokens.length != 4) {
      return false;
    }
    for (String token : tokens) {
      if (!isValidIPv4Segment(token)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isValidIPv4Segment(String token) {
    if (token.startsWith("0") && token.length() > 1) {
      return false;
    }
    try {
      int parsedInt = Integer.parseInt(token);
      if (parsedInt < 0 || parsedInt > 255) {
        return false;
      }
      if (parsedInt == 0 && token.charAt(0) != '0') {
        return false;
      }
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }
}
