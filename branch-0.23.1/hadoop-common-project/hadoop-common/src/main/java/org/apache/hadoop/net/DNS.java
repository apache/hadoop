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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
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
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class DNS {

  private static final Log LOG = LogFactory.getLog(DNS.class);

  /**
   * The cached hostname -initially null.
   */

  private static final String cachedHostname = resolveLocalHostname();
  private static final String cachedHostAddress = resolveLocalHostIPAddress();
  private static final String LOCALHOST = "localhost";

  /**
   * Returns the hostname associated with the specified IP address by the
   * provided nameserver.
   *
   * Loopback addresses 
   * @param hostIp The address to reverse lookup
   * @param ns The host name of a reachable DNS server
   * @return The host name associated with the provided IP
   * @throws NamingException If a NamingException is encountered
   */
  public static String reverseDns(InetAddress hostIp, String ns)
    throws NamingException {
    //
    // Builds the reverse IP lookup form
    // This is formed by reversing the IP numbers and appending in-addr.arpa
    //
    String[] parts = hostIp.getHostAddress().split("\\.");
    String reverseIP = parts[3] + "." + parts[2] + "." + parts[1] + "."
      + parts[0] + ".in-addr.arpa";

    DirContext ictx = new InitialDirContext();
    Attributes attribute;
    try {
      attribute = ictx.getAttributes("dns://"               // Use "dns:///" if the default
                         + ((ns == null) ? "" : ns) +
                         // nameserver is to be used
                         "/" + reverseIP, new String[] { "PTR" });
    } finally {
      ictx.close();
    }

    return attribute.get("PTR").get().toString();
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
      if (netIF == null) {
        return new String[] { cachedHostAddress };
      } else {
        Vector<String> ips = new Vector<String>();
        Enumeration e = netIF.getInetAddresses();
        while (e.hasMoreElements()) {
          ips.add(((InetAddress) e.nextElement()).getHostAddress());
        }
        return ips.toArray(new String[] {});
      }
    } catch (SocketException e) {
      return new String[]  { cachedHostAddress };
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
    for (int ctr = 0; ctr < ips.length; ctr++)
      try {
        hosts.add(reverseDns(InetAddress.getByName(ips[ctr]),
                             nameserver));
      } catch (UnknownHostException ignored) {
      } catch (NamingException ignored) {
      }

    if (hosts.isEmpty()) {
      return new String[] { cachedHostname };
    } else {
      return hosts.toArray(new String[hosts.size()]);
    }
  }


  /**
   * Determine the local hostname; retrieving it from cache if it is known
   * If we cannot determine our host name, return "localhost"
   * @return the local hostname or "localhost"
   */
  private static String resolveLocalHostname() {
    String localhost;
    try {
      localhost = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      LOG.info("Unable to determine local hostname "
              + "-falling back to \"" + LOCALHOST + "\"", e);
      localhost = LOCALHOST;
    }
    return localhost;
  }


  /**
   * Get the IPAddress of the local host as a string.
   * This will be a loop back value if the local host address cannot be
   * determined.
   * If the loopback address of "localhost" does not resolve, then the system's
   * network is in such a state that nothing is going to work. A message is
   * logged at the error level and a null pointer returned, a pointer
   * which will trigger failures later on the application
   * @return the IPAddress of the local host or null for a serious problem.
   */
  private static String resolveLocalHostIPAddress() {
    String address;
      try {
        address = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        LOG.info("Unable to determine address of the host"
                + "-falling back to \"" + LOCALHOST + "\" address", e);
        try {
          address = InetAddress.getByName(LOCALHOST).getHostAddress();
        } catch (UnknownHostException noLocalHostAddressException) {
          //at this point, deep trouble
          LOG.error("Unable to determine local loopback address "
                  + "of \"" + LOCALHOST + "\" " +
                  "-this system's network configuration is unsupported", e);
          address = null;
        }
      }
    return address;
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
      return cachedHostname;
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
