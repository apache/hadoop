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

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map.Entry;
import java.util.*;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.util.ReflectionUtils;

public class NetUtils {
  private static final Log LOG = LogFactory.getLog(NetUtils.class);
  
  private static Map<String, String> hostToResolved = 
                                     new HashMap<String, String>();

  /**
   * Get the socket factory for the given class according to its
   * configuration parameter
   * <tt>hadoop.rpc.socket.factory.class.&lt;ClassName&gt;</tt>. When no
   * such parameter exists then fall back on the default socket factory as
   * configured by <tt>hadoop.rpc.socket.factory.class.default</tt>. If
   * this default socket factory is not configured, then fall back on the JVM
   * default socket factory.
   * 
   * @param conf the configuration
   * @param clazz the class (usually a {@link VersionedProtocol})
   * @return a socket factory
   */
  public static SocketFactory getSocketFactory(Configuration conf,
      Class<?> clazz) {

    SocketFactory factory = null;

    String propValue =
        conf.get("hadoop.rpc.socket.factory.class." + clazz.getSimpleName());
    if ((propValue != null) && (propValue.length() > 0))
      factory = getSocketFactoryFromProperty(conf, propValue);

    if (factory == null)
      factory = getDefaultSocketFactory(conf);

    return factory;
  }

  /**
   * Get the default socket factory as specified by the configuration
   * parameter <tt>hadoop.rpc.socket.factory.default</tt>
   * 
   * @param conf the configuration
   * @return the default socket factory as specified in the configuration or
   *         the JVM default socket factory if the configuration does not
   *         contain a default socket factory property.
   */
  public static SocketFactory getDefaultSocketFactory(Configuration conf) {

    String propValue = conf.get("hadoop.rpc.socket.factory.class.default");
    if ((propValue == null) || (propValue.length() == 0))
      return SocketFactory.getDefault();

    return getSocketFactoryFromProperty(conf, propValue);
  }

  /**
   * Get the socket factory corresponding to the given proxy URI. If the
   * given proxy URI corresponds to an absence of configuration parameter,
   * returns null. If the URI is malformed raises an exception.
   * 
   * @param propValue the property which is the class name of the
   *        SocketFactory to instantiate; assumed non null and non empty.
   * @return a socket factory as defined in the property value.
   */
  public static SocketFactory getSocketFactoryFromProperty(
      Configuration conf, String propValue) {

    try {
      Class<?> theClass = conf.getClassByName(propValue);
      return (SocketFactory) ReflectionUtils.newInstance(theClass, conf);

    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Socket Factory class not found: " + cnfe);
    }
  }

  /**
   * Util method to build socket addr from either:
   *   <host>:<post>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target) {
    int colonIndex = target.indexOf(':');
    if (colonIndex < 0) {
      throw new RuntimeException("Not a host:port pair: " + target);
    }
    String hostname;
    int port;
    if (!target.contains("/")) {
      // must be the old style <host>:<port>
      hostname = target.substring(0, colonIndex);
      port = Integer.parseInt(target.substring(colonIndex + 1));
    } else {
      // a new uri
      URI addr = new Path(target).toUri();
      hostname = addr.getHost();
      port = addr.getPort();
    }
  
    if (getStaticResolution(hostname) != null) {
      hostname = getStaticResolution(hostname);
    }
    return new InetSocketAddress(hostname, port);
  }

  /**
   * Handle the transition from pairs of attributes specifying a host and port
   * to a single colon separated one.
   * @param conf the configuration to check
   * @param oldBindAddressName the old address attribute name
   * @param oldPortName the old port attribute name
   * @param newBindAddressName the new combined name
   * @return the complete address from the configuration
   */
  @Deprecated
  public static String getServerAddress(Configuration conf,
                                        String oldBindAddressName,
                                        String oldPortName,
                                        String newBindAddressName) {
    String oldAddr = conf.get(oldBindAddressName);
    String oldPort = conf.get(oldPortName);
    String newAddrPort = conf.get(newBindAddressName);
    if (oldAddr == null && oldPort == null) {
      return newAddrPort;
    }
    String[] newAddrPortParts = newAddrPort.split(":",2);
    if (newAddrPortParts.length != 2) {
      throw new IllegalArgumentException("Invalid address/port: " + 
                                         newAddrPort);
    }
    if (oldAddr == null) {
      oldAddr = newAddrPortParts[0];
    } else {
      LOG.warn("Configuration parameter " + oldBindAddressName +
               " is deprecated. Use " + newBindAddressName + " instead.");
    }
    if (oldPort == null) {
      oldPort = newAddrPortParts[1];
    } else {
      LOG.warn("Configuration parameter " + oldPortName +
               " is deprecated. Use " + newBindAddressName + " instead.");      
    }
    return oldAddr + ":" + oldPort;
  }
  
  /**
   * Adds a static resolution for host. This can be used for setting up
   * hostnames with names that are fake to point to a well known host. For e.g.
   * in some testcases we require to have daemons with different hostnames
   * running on the same machine. In order to create connections to these
   * daemons, one can set up mappings from those hostnames to "localhost".
   * {@link NetUtils#getStaticResolution(String)} can be used to query for
   * the actual hostname. 
   * @param host
   * @param resolvedName
   */
  public static void addStaticResolution(String host, String resolvedName) {
    synchronized (hostToResolved) {
      hostToResolved.put(host, resolvedName);
    }
  }
  
  /**
   * Retrieves the resolved name for the passed host. The resolved name must
   * have been set earlier using 
   * {@link NetUtils#addStaticResolution(String, String)}
   * @param host
   * @return the resolution
   */
  public static String getStaticResolution(String host) {
    synchronized (hostToResolved) {
      return hostToResolved.get(host);
    }
  }
  
  /**
   * This is used to get all the resolutions that were added using
   * {@link NetUtils#addStaticResolution(String, String)}. The return
   * value is a List each element of which contains an array of String 
   * of the form String[0]=hostname, String[1]=resolved-hostname
   * @return the list of resolutions
   */
  public static List <String[]> getAllStaticResolutions() {
    synchronized (hostToResolved) {
      Set <Entry <String, String>>entries = hostToResolved.entrySet();
      if (entries.size() == 0) {
        return null;
      }
      List <String[]> l = new ArrayList<String[]>(entries.size());
      for (Entry<String, String> e : entries) {
        l.add(new String[] {e.getKey(), e.getValue()});
      }
    return l;
    }
  }
}
