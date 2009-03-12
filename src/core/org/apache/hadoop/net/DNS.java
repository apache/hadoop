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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.util.Enumeration;
import java.util.Vector;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

/**
 * 
 * A class that provides direct and reverse lookup functionalities, allowing
 * the querying of specific network interfaces or nameservers.
 * 
 * 
 */
public class DNS {
  private static final String LOCALHOST = "localhost";
  private static final Log LOG = LogFactory.getLog(DNS.class);

  /**
   * Returns the hostname associated with the specified IP address by the
   * provided nameserver.
   *
   * @param address The address to reverse lookup
   * @param nameserver The host name of a reachable DNS server; can be null
   * @return The host name associated with the provided IP
   * @throws NamingException If a NamingException is encountered
   * @throws IllegalArgumentException if the protocol is unsupported
   */
  public static String reverseDns(InetAddress address, String nameserver)
          throws NamingException {
    if (address instanceof Inet4Address) {
      return reverseDns((Inet4Address) address, nameserver);
    } else if (address instanceof Inet6Address) {
      return reverseDns((Inet6Address) address, nameserver);
    } else {
      throw new IllegalArgumentException("Unsupported Protocol " + address);
    }
  }

  /**
   * Returns the hostname associated with the specified IP address by the
   * provided nameserver.
   * 
   * @param hostIp
   *            The address to reverse lookup
   * @param nameserver
   *            The host name of a reachable DNS server; can be null
   * @return The host name associated with the provided IP
   * @throws NamingException
   *             If a NamingException is encountered
   */
  private static String reverseDns(Inet4Address hostIp, String nameserver)
    throws NamingException {
    //
    // Builds the reverse IP lookup form
    // This is formed by reversing the IP numbers and appending in-addr.arpa
    //
    byte[] address32 = hostIp.getAddress();
    String reverseIP;
    reverseIP =    ""+(address32[3]&0xff) + '.'
                    + (address32[2] & 0xff) + '.'
                    + (address32[1] & 0xff) + '.'
                    + (address32[0] & 0xff) + '.'
                    + "in-addr.arpa";
/*
    String hostAddress = hostIp.getHostAddress();
    String[] parts = hostAddress.split("\\.");
    if (parts.length < 4) {
      throw new IllegalArgumentException("Unable to determine IPv4 address of "
              + hostAddress);
    }
    reverseIP = parts[3] + "." + parts[2] + "." + parts[1] + "."
      + parts[0] + ".in-addr.arpa";
*/

    return retrievePTRRecord(nameserver, reverseIP);
  }

  /**
   * Retrieve the PTR record from a DNS entry; if given the right
   * reverse IP this will resolve both IPv4 and IPv6 addresses
   * @param nameserver name server to use; can be null
   * @param reverseIP reverse IP address to look up
   * @return the PTR record of the host
   * @throws NamingException if the record can not be found.
   */
  private static String retrievePTRRecord(String nameserver, String reverseIP)
          throws NamingException {
    DirContext ictx = new InitialDirContext();
    try {
      // Use "dns:///" if the default nameserver is to be used
      Attributes attribute = ictx.getAttributes("dns://"
                         + ((nameserver == null) ? "" : nameserver) +
                         "/" + reverseIP, new String[] { "PTR" });
      return attribute.get("PTR").get().toString();
    } finally {
      ictx.close();
    }
  }

  /**
   * Returns the hostname associated with the specified IP address by the
   * provided nameserver.
   *
   * @param address
   *            The address to reverse lookup
   * @param nameserver
   *            The host name of a reachable DNS server; can be null
   * @return The host name associated with the provided IP
   * @throws NamingException
   *             If a NamingException is encountered
   */
  private static String reverseDns(Inet6Address address, String nameserver)
          throws NamingException {
    throw new IllegalArgumentException("Reverse DNS lookup of IPv6 is currently unsupported: " + address);
  }

  /**
   * Returns all the IPs associated with the provided interface, if any, in
   * textual form.
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0)
   * @return A string vector of all the IPs associated with the provided
   *         interface
   * @throws UnknownHostException
   *             If an UnknownHostException is encountered in querying the
   *             default interface
   * 
   */
  public static String[] getIPs(String strInterface)
    throws UnknownHostException {
    try {
      NetworkInterface netIF = NetworkInterface.getByName(strInterface);
      if (netIF == null)
        return new String[] {getLocalHostIPAddress()};
      else {
        Vector<String> ips = new Vector<String>();
        Enumeration<InetAddress> e = netIF.getInetAddresses();

        while (e.hasMoreElements()) {
          ips.add((e.nextElement()).getHostAddress());
        }
        return ips.toArray(new String[ips.size()]);
      }
    } catch (SocketException e) {
      return new String[] {getLocalHostIPAddress()};
    }
  }


    /**
   * Returns the first available IP address associated with the provided
   * network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0)
   * @return The IP address in text form
   * @throws UnknownHostException
   *             If one is encountered in querying the default interface
   */
  public static String getDefaultIP(String strInterface)
    throws UnknownHostException {
    String[] ips = getIPs(strInterface);
    return ips[0];
  }

  /**
   * Returns all the host names associated by the provided nameserver with the
   * address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0)
   * @param nameserver
   *            The DNS host name
   * @return A string vector of all host names associated with the IPs tied to
   *         the specified interface
   * @throws UnknownHostException if the hostname cannot be determined
   */
  public static String[] getHosts(String strInterface, String nameserver)
    throws UnknownHostException {
    String[] ips = getIPs(strInterface);
    Vector<String> hosts = new Vector<String>();
    for (String ip : ips) {
      InetAddress ipAddr = null;
      try {
        ipAddr = InetAddress.getByName(ip);
        hosts.add(reverseDns(ipAddr, nameserver));
      } catch (UnknownHostException e) {
        if (LOG.isDebugEnabled())
          LOG.debug("Unable to resolve hostname of " + ip + ": " + e, e);
      } catch (NamingException e) {
        if(LOG.isDebugEnabled())
          LOG.debug("Unable to reverse DNS of host" + ip
                +  (ipAddr != null ? (" and address "+ipAddr): "")
                + " : " + e, e);
      } catch (IllegalArgumentException e) {
        if (LOG.isDebugEnabled())
          LOG.debug("Unable to resolve IP address of " + ip
                  + (ipAddr != null ? (" and address " + ipAddr) : "")
                  + " : " + e, e);
      }
    }

    if (hosts.isEmpty()) {
      return new String[] { getLocalHostname() };
    } else {
      return hosts.toArray(new String[hosts.size()]);
    }
  }

  /**
   * The cached hostname -initially null.
   */

  private static volatile String cachedHostname;

  /**
   * The cached address hostname -initially null.
   */
  private static volatile String cachedHostAddress;

    /**
     * Determine the local hostname; retrieving it from cache if it is known
     * If we cannot determine our host name, return "localhost"
     * This code is not synchronized; if more than one thread calls it while
     * it is determining the address, the last one to exit the loop wins.
     * However, as both threads are determining and caching the same value, it
     * should be moot.
     * @return the local hostname or "localhost"
     */
  private static String getLocalHostname() {
    if(cachedHostname == null) {
      try {
        cachedHostname = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e) {
        LOG.info("Unable to determine local hostname "
                + "-falling back to \""+LOCALHOST+"\"", e);
        cachedHostname = LOCALHOST;
      }
    }
    return cachedHostname;
  }

  /**
   * Get the IPAddress of the local host as a string. This may be a loop back
   * value.
   * This code is not synchronized; if more than one thread calls it while
   * it is determining the address, the last one to exit the loop wins.
   * However, as both threads are determining and caching the same value, it
   * should be moot.
   * @return the IPAddress of the localhost
   * @throws UnknownHostException if not even "localhost" resolves.
   */
  private static String getLocalHostIPAddress() throws UnknownHostException {
    if (cachedHostAddress == null) {
      try {
        InetAddress localHost = InetAddress.getLocalHost();
        cachedHostAddress = localHost.getHostAddress();
      } catch (UnknownHostException e) {
        LOG.info("Unable to determine local IP Address "
                + "-falling back to loopback address", e);
        cachedHostAddress = InetAddress.getByName(LOCALHOST).getHostAddress();
      }
    }
    return cachedHostAddress;
  }

    /**
   * Returns all the host names associated by the default nameserver with the
   * address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0)
   * @return The list of host names associated with IPs bound to the network
   *         interface
   * @throws UnknownHostException
   *             If one is encountered while querying the default interface
   * 
   */
  public static String[] getHosts(String strInterface)
    throws UnknownHostException {
    return getHosts(strInterface, null);
  }

  /**
   * Returns the default (first) host name associated by the provided
   * nameserver with the address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0)
   * @param nameserver
   *            The DNS host name
   * @return The default host names associated with IPs bound to the network
   *         interface
   * @throws UnknownHostException
   *             If one is encountered while querying the default interface
   */
  public static String getDefaultHost(String strInterface, String nameserver)
    throws UnknownHostException {
    if ("default".equals(strInterface)) {
      return getLocalHostname();
    }

    if ("default".equals(nameserver)) {
      return getDefaultHost(strInterface);
    }

    String[] hosts = getHosts(strInterface, nameserver);
    return hosts[0];
  }

  /**
   * Returns the default (first) host name associated by the default
   * nameserver with the address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0).
   *            Must not be null.
   * @return The default host name associated with IPs bound to the network
   *         interface
   * @throws UnknownHostException
   *             If one is encountered while querying the default interface
   */
  public static String getDefaultHost(String strInterface)
    throws UnknownHostException {
    return getDefaultHost(strInterface, null);
  }

}
