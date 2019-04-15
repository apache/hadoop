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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.NoRouteToHostException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.SocketFactory;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);
  
  private static Map<String, String> hostToResolved = 
                                     new HashMap<String, String>();
  /** text to point users elsewhere: {@value} */
  private static final String FOR_MORE_DETAILS_SEE
      = " For more details see:  ";
  /** text included in wrapped exceptions if the host is null: {@value} */
  public static final String UNKNOWN_HOST = "(unknown)";
  /** Base URL of the Hadoop Wiki: {@value} */
  public static final String HADOOP_WIKI = "http://wiki.apache.org/hadoop/";

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

    String propValue = conf.get(
        CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
        CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT);
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
   *   {@literal <host>:<port>}
   *   {@literal <fs>://<host>:<port>/<path>}
   */
  public static InetSocketAddress createSocketAddr(String target) {
    return createSocketAddr(target, -1);
  }

  /**
   * Util method to build socket addr from either:
   *   {@literal <host>}
   *   {@literal <host>:<port>}
   *   {@literal <fs>://<host>:<port>/<path>}
   */
  public static InetSocketAddress createSocketAddr(String target,
                                                   int defaultPort) {
    return createSocketAddr(target, defaultPort, null);
  }

  /**
   * Create an InetSocketAddress from the given target string and
   * default port. If the string cannot be parsed correctly, the
   * <code>configName</code> parameter is used as part of the
   * exception message, allowing the user to better diagnose
   * the misconfiguration.
   *
   * @param target a string of either "host" or "host:port"
   * @param defaultPort the default port if <code>target</code> does not
   *                    include a port number
   * @param configName the name of the configuration from which
   *                   <code>target</code> was loaded. This is used in the
   *                   exception message in the case that parsing fails. 
   */
  public static InetSocketAddress createSocketAddr(String target,
                                                   int defaultPort,
                                                   String configName) {
    String helpText = "";
    if (configName != null) {
      helpText = " (configuration property '" + configName + "')";
    }
    if (target == null) {
      throw new IllegalArgumentException("Target address cannot be null." +
          helpText);
    }
    target = target.trim();
    boolean hasScheme = target.contains("://");    
    URI uri = null;
    try {
      uri = hasScheme ? URI.create(target) : URI.create("dummyscheme://"+target);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Does not contain a valid host:port authority: " + target + helpText
      );
    }

    String host = uri.getHost();
    int port = uri.getPort();
    if (port == -1) {
      port = defaultPort;
    }
    String path = uri.getPath();
    
    if ((host == null) || (port < 0) ||
        (!hasScheme && path != null && !path.isEmpty()))
    {
      throw new IllegalArgumentException(
          "Does not contain a valid host:port authority: " + target + helpText
      );
    }
    return createSocketAddrForHost(host, port);
  }

  /**
   * Create a socket address with the given host and port.  The hostname
   * might be replaced with another host that was set via
   * {@link #addStaticResolution(String, String)}.  The value of
   * hadoop.security.token.service.use_ip will determine whether the
   * standard java host resolver is used, or if the fully qualified resolver
   * is used.
   * @param host the hostname or IP use to instantiate the object
   * @param port the port number
   * @return InetSocketAddress
   */
  public static InetSocketAddress createSocketAddrForHost(String host, int port) {
    String staticHost = getStaticResolution(host);
    String resolveHost = (staticHost != null) ? staticHost : host;
    
    InetSocketAddress addr;
    try {
      InetAddress iaddr = SecurityUtil.getByName(resolveHost);
      // if there is a static entry for the host, make the returned
      // address look like the original given host
      if (staticHost != null) {
        iaddr = InetAddress.getByAddress(host, iaddr.getAddress());
      }
      addr = new InetSocketAddress(iaddr, port);
    } catch (UnknownHostException e) {
      addr = InetSocketAddress.createUnresolved(host, port);
    }
    return addr;
  }
  
  /**
   * Resolve the uri's hostname and add the default port if not in the uri
   * @param uri to resolve
   * @param defaultPort if none is given
   * @return URI
   */
  public static URI getCanonicalUri(URI uri, int defaultPort) {
    // skip if there is no authority, ie. "file" scheme or relative uri
    String host = uri.getHost();
    if (host == null) {
      return uri;
    }
    String fqHost = canonicalizeHost(host);
    int port = uri.getPort();
    // short out if already canonical with a port
    if (host.equals(fqHost) && port != -1) {
      return uri;
    }
    // reconstruct the uri with the canonical host and port
    try {
      uri = new URI(uri.getScheme(), uri.getUserInfo(),
          fqHost, (port == -1) ? defaultPort : port,
          uri.getPath(), uri.getQuery(), uri.getFragment());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    return uri;
  }  

  // cache the canonicalized hostnames;  the cache currently isn't expired,
  // but the canonicals will only change if the host's resolver configuration
  // changes
  private static final ConcurrentHashMap<String, String> canonicalizedHostCache =
      new ConcurrentHashMap<String, String>();

  private static String canonicalizeHost(String host) {
    // check if the host has already been canonicalized
    String fqHost = canonicalizedHostCache.get(host);
    if (fqHost == null) {
      try {
        fqHost = SecurityUtil.getByName(host).getHostName();
        canonicalizedHostCache.putIfAbsent(host, fqHost);
        // ensures that we won't return a canonicalized stale (non-cached)
        // host name for a given host
        fqHost = canonicalizedHostCache.get(host);
      } catch (UnknownHostException e) {
        fqHost = host;
      }
    }
    return fqHost;
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
  
  /**
   * Returns InetSocketAddress that a client can use to 
   * connect to the server. Server.getListenerAddress() is not correct when
   * the server binds to "0.0.0.0". This returns "hostname:port" of the server,
   * or "127.0.0.1:port" when the getListenerAddress() returns "0.0.0.0:port".
   * 
   * @param server
   * @return socket address that a client can use to connect to the server.
   */
  public static InetSocketAddress getConnectAddress(Server server) {
    return getConnectAddress(server.getListenerAddress());
  }
  
  /**
   * Returns an InetSocketAddress that a client can use to connect to the
   * given listening address.
   * 
   * @param addr of a listener
   * @return socket address that a client can use to connect to the server.
   */
  public static InetSocketAddress getConnectAddress(InetSocketAddress addr) {
    if (!addr.isUnresolved() && addr.getAddress().isAnyLocalAddress()) {
      try {
        addr = new InetSocketAddress(InetAddress.getLocalHost(), addr.getPort());
      } catch (UnknownHostException uhe) {
        // shouldn't get here unless the host doesn't have a loopback iface
        addr = createSocketAddrForHost("127.0.0.1", addr.getPort());
      }
    }
    return addr;
  }
  
  /**
   * Same as <code>getInputStream(socket, socket.getSoTimeout()).</code>
   * <br><br>
   * 
   * @see #getInputStream(Socket, long)
   */
  public static SocketInputWrapper getInputStream(Socket socket) 
                                           throws IOException {
    return getInputStream(socket, socket.getSoTimeout());
  }

  /**
   * Return a {@link SocketInputWrapper} for the socket and set the given
   * timeout. If the socket does not have an associated channel, then its socket
   * timeout will be set to the specified value. Otherwise, a
   * {@link SocketInputStream} will be created which reads with the configured
   * timeout.
   * 
   * Any socket created using socket factories returned by {@link #NetUtils},
   * must use this interface instead of {@link Socket#getInputStream()}.
   * 
   * In general, this should be called only once on each socket: see the note
   * in {@link SocketInputWrapper#setTimeout(long)} for more information.
   *
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. zero for waiting as
   *                long as necessary.
   * @return SocketInputWrapper for reading from the socket.
   * @throws IOException
   */
  public static SocketInputWrapper getInputStream(Socket socket, long timeout) 
                                           throws IOException {
    InputStream stm = (socket.getChannel() == null) ? 
          socket.getInputStream() : new SocketInputStream(socket);
    SocketInputWrapper w = new SocketInputWrapper(socket, stm);
    w.setTimeout(timeout);
    return w;
  }
  
  /**
   * Same as getOutputStream(socket, 0). Timeout of zero implies write will
   * wait until data is available.<br><br>
   * 
   * From documentation for {@link #getOutputStream(Socket, long)} : <br>
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see #getOutputStream(Socket, long)
   * 
   * @param socket
   * @return OutputStream for writing to the socket.
   * @throws IOException
   */  
  public static OutputStream getOutputStream(Socket socket) 
                                             throws IOException {
    return getOutputStream(socket, 0);
  }
  
  /**
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. This may not always apply. zero
   *        for waiting as long as necessary.
   * @return OutputStream for writing to the socket.
   * @throws IOException   
   */
  public static OutputStream getOutputStream(Socket socket, long timeout) 
                                             throws IOException {
    return (socket.getChannel() == null) ? 
            socket.getOutputStream() : new SocketOutputStream(socket, timeout);            
  }
  
  /**
   * This is a drop-in replacement for 
   * {@link Socket#connect(SocketAddress, int)}.
   * In the case of normal sockets that don't have associated channels, this 
   * just invokes <code>socket.connect(endpoint, timeout)</code>. If 
   * <code>socket.getChannel()</code> returns a non-null channel,
   * connect is implemented using Hadoop's selectors. This is done mainly
   * to avoid Sun's connect implementation from creating thread-local 
   * selectors, since Hadoop does not have control on when these are closed
   * and could end up taking all the available file descriptors.
   * 
   * @see java.net.Socket#connect(java.net.SocketAddress, int)
   * 
   * @param socket
   * @param address the remote address
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket,
      SocketAddress address,
      int timeout) throws IOException {
    connect(socket, address, null, timeout);
  }

  /**
   * Like {@link NetUtils#connect(Socket, SocketAddress, int)} but
   * also takes a local address and port to bind the socket to. 
   * 
   * @param socket
   * @param endpoint the remote address
   * @param localAddr the local address to bind the socket to
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket, 
                             SocketAddress endpoint,
                             SocketAddress localAddr,
                             int timeout) throws IOException {
    if (socket == null || endpoint == null || timeout < 0) {
      throw new IllegalArgumentException("Illegal argument for connect()");
    }
    
    SocketChannel ch = socket.getChannel();
    
    if (localAddr != null) {
      Class localClass = localAddr.getClass();
      Class remoteClass = endpoint.getClass();
      Preconditions.checkArgument(localClass.equals(remoteClass),
          "Local address %s must be of same family as remote address %s.",
          localAddr, endpoint);
      socket.bind(localAddr);
    }

    try {
      if (ch == null) {
        // let the default implementation handle it.
        socket.connect(endpoint, timeout);
      } else {
        SocketIOWithTimeout.connect(ch, endpoint, timeout);
      }
    } catch (SocketTimeoutException ste) {
      throw new ConnectTimeoutException(ste.getMessage());
    }

    // There is a very rare case allowed by the TCP specification, such that
    // if we are trying to connect to an endpoint on the local machine,
    // and we end up choosing an ephemeral port equal to the destination port,
    // we will actually end up getting connected to ourself (ie any data we
    // send just comes right back). This is only possible if the target
    // daemon is down, so we'll treat it like connection refused.
    if (socket.getLocalPort() == socket.getPort() &&
        socket.getLocalAddress().equals(socket.getInetAddress())) {
      LOG.info("Detected a loopback TCP socket, disconnecting it");
      socket.close();
      throw new ConnectException(
        "Localhost targeted connection resulted in a loopback. " +
        "No daemon is listening on the target port.");
    }
  }
  
  /** 
   * Given a string representation of a host, return its ip address
   * in textual presentation.
   * 
   * @param name a string representation of a host:
   *             either a textual representation its IP address or its host name
   * @return its IP address in the string format
   */
  public static String normalizeHostName(String name) {
    try {
      return InetAddress.getByName(name).getHostAddress();
    } catch (UnknownHostException e) {
      return name;
    }    
  }
  
  /** 
   * Given a collection of string representation of hosts, return a list of
   * corresponding IP addresses in the textual representation.
   * 
   * @param names a collection of string representations of hosts
   * @return a list of corresponding IP addresses in the string format
   * @see #normalizeHostName(String)
   */
  public static List<String> normalizeHostNames(Collection<String> names) {
    List<String> hostNames = new ArrayList<String>(names.size());
    for (String name : names) {
      hostNames.add(normalizeHostName(name));
    }
    return hostNames;
  }

  /**
   * Performs a sanity check on the list of hostnames/IPs to verify they at least
   * appear to be valid.
   * @param names - List of hostnames/IPs
   * @throws UnknownHostException
   */
  public static void verifyHostnames(String[] names) throws UnknownHostException {
    for (String name: names) {
      if (name == null) {
        throw new UnknownHostException("null hostname found");
      }
      // The first check supports URL formats (e.g. hdfs://, etc.). 
      // java.net.URI requires a schema, so we add a dummy one if it doesn't
      // have one already.
      URI uri = null;
      try {
        uri = new URI(name);
        if (uri.getHost() == null) {
          uri = new URI("http://" + name);
        }
      } catch (URISyntaxException e) {
        uri = null;
      }
      if (uri == null || uri.getHost() == null) {
        throw new UnknownHostException(name + " is not a valid Inet address");
      }
    }
  }

  private static final Pattern ipPortPattern = // Pattern for matching ip[:port]
    Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(:\\d+)?");
  
  /**
   * Attempt to obtain the host name of the given string which contains
   * an IP address and an optional port.
   * 
   * @param ipPort string of form ip[:port]
   * @return Host name or null if the name can not be determined
   */
  public static String getHostNameOfIP(String ipPort) {
    if (null == ipPort || !ipPortPattern.matcher(ipPort).matches()) {
      return null;
    }
    
    try {
      int colonIdx = ipPort.indexOf(':');
      String ip = (-1 == colonIdx) ? ipPort
          : ipPort.substring(0, ipPort.indexOf(':'));
      return InetAddress.getByName(ip).getHostName();
    } catch (UnknownHostException e) {
      return null;
    }
  }

  /**
   * Return hostname without throwing exception.
   * The returned hostname String format is "hostname".
   * @return hostname
   */
  public static String getLocalHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch(UnknownHostException uhe) {
      return "" + uhe;
    }
  }

  /**
   * Return hostname without throwing exception.
   * The returned hostname String format is "hostname/ip address".
   * @return hostname
   */
  public static String getHostname() {
    try {return "" + InetAddress.getLocalHost();}
    catch(UnknownHostException uhe) {return "" + uhe;}
  }

  /**
   * Compose a "host:port" string from the address.
   */
  public static String getHostPortString(InetSocketAddress addr) {
    return addr.getHostName() + ":" + addr.getPort();
  }
  
  /**
   * Checks if {@code host} is a local host name and return {@link InetAddress}
   * corresponding to that address.
   * 
   * @param host the specified host
   * @return a valid local {@link InetAddress} or null
   * @throws SocketException if an I/O error occurs
   */
  public static InetAddress getLocalInetAddress(String host)
      throws SocketException {
    if (host == null) {
      return null;
    }
    InetAddress addr = null;
    try {
      addr = SecurityUtil.getByName(host);
      if (NetworkInterface.getByInetAddress(addr) == null) {
        addr = null; // Not a local address
      }
    } catch (UnknownHostException ignore) { }
    return addr;
  }
  
  /**
   * Given an InetAddress, checks to see if the address is a local address, by
   * comparing the address with all the interfaces on the node.
   * @param addr address to check if it is local node's address
   * @return true if the address corresponds to the local node
   */
  public static boolean isLocalAddress(InetAddress addr) {
    // Check if the address is any local or loop back
    boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

    // Check if the address is defined on any interface
    if (!local) {
      try {
        local = NetworkInterface.getByInetAddress(addr) != null;
      } catch (SocketException e) {
        local = false;
      }
    }
    return local;
  }

  /**
   * Take an IOException , the local host port and remote host port details and
   * return an IOException with the input exception as the cause and also
   * include the host details. The new exception provides the stack trace of the
   * place where the exception is thrown and some extra diagnostics information.
   * If the exception is of type BindException, ConnectException,
   * UnknownHostException, SocketTimeoutException or has a String constructor,
   * return a new one of the same type; Otherwise return an IOException.
   *
   * @param destHost target host (nullable)
   * @param destPort target port
   * @param localHost local host (nullable)
   * @param localPort local port
   * @param exception the caught exception.
   * @return an exception to throw
   */
  public static IOException wrapException(final String destHost,
                                          final int destPort,
                                          final String localHost,
                                          final int localPort,
                                          final IOException exception) {
    try {
      if (exception instanceof BindException) {
        return wrapWithMessage(exception,
            "Problem binding to ["
                + localHost
                + ":"
                + localPort
                + "] "
                + exception
                + ";"
                + see("BindException"));
      } else if (exception instanceof ConnectException) {
        // Check if client was trying to connect to an unspecified IPv4 address
        // (0.0.0.0) or IPv6 address(0:0:0:0:0:0:0:0 or ::)
        if ((destHost != null && (destHost.equals("0.0.0.0") ||
            destHost.equals("0:0:0:0:0:0:0:0") || destHost.equals("::")))
            || destPort == 0) {
          return wrapWithMessage(exception, "Your endpoint configuration" +
              " is wrong;" + see("UnsetHostnameOrPort"));
        } else {
          // connection refused; include the host:port in the error
          return wrapWithMessage(exception,
              "Call From "
                  + localHost
                  + " to "
                  + destHost
                  + ":"
                  + destPort
                  + " failed on connection exception: "
                  + exception
                  + ";"
                  + see("ConnectionRefused"));
        }
      } else if (exception instanceof UnknownHostException) {
        return wrapWithMessage(exception,
            "Invalid host name: "
                + getHostDetailsAsString(destHost, destPort, localHost)
                + exception
                + ";"
                + see("UnknownHost"));
      } else if (exception instanceof SocketTimeoutException) {
        return wrapWithMessage(exception,
            "Call From "
                + localHost + " to " + destHost + ":" + destPort
                + " failed on socket timeout exception: " + exception
                + ";"
                + see("SocketTimeout"));
      } else if (exception instanceof NoRouteToHostException) {
        return wrapWithMessage(exception,
            "No Route to Host from  "
                + localHost + " to " + destHost + ":" + destPort
                + " failed on socket timeout exception: " + exception
                + ";"
                + see("NoRouteToHost"));
      } else if (exception instanceof EOFException) {
        return wrapWithMessage(exception,
            "End of File Exception between "
                + getHostDetailsAsString(destHost, destPort, localHost)
                + ": " + exception
                + ";"
                + see("EOFException"));
      } else if (exception instanceof SocketException) {
        // Many of the predecessor exceptions are subclasses of SocketException,
        // so must be handled before this
        return wrapWithMessage(exception,
            "Call From "
                + localHost + " to " + destHost + ":" + destPort
                + " failed on socket exception: " + exception
                + ";"
                + see("SocketException"));
      } else {
        // Return instance of same type if Exception has a String constructor
        return wrapWithMessage(exception,
            "DestHost:destPort " + destHost + ":" + destPort
                + " , LocalHost:localPort " + localHost
                + ":" + localPort + ". Failed on local exception: " +
                exception);

      }
    } catch (IOException ex) {
      return (IOException) new IOException("Failed on local exception: "
          + exception + "; Host Details : "
          + getHostDetailsAsString(destHost, destPort, localHost))
          .initCause(exception);
    }
  }

  private static String see(final String entry) {
    return FOR_MORE_DETAILS_SEE + HADOOP_WIKI + entry;
  }
  
  @SuppressWarnings("unchecked")
  private static <T extends IOException> T wrapWithMessage(
      T exception, String msg) throws T {
    Class<? extends Throwable> clazz = exception.getClass();
    try {
      Constructor<? extends Throwable> ctor = clazz.getConstructor(String.class);
      Throwable t = ctor.newInstance(msg);
      return (T)(t.initCause(exception));
    } catch (Throwable e) {
      LOG.trace("Unable to wrap exception of type {}: it has no (String) "
          + "constructor", clazz, e);
      throw exception;
    }
  }

  /**
   * Get the host details as a string
   * @param destHost destinatioon host (nullable)
   * @param destPort destination port
   * @param localHost local host (nullable)
   * @return a string describing the destination host:port and the local host
   */
  private static String getHostDetailsAsString(final String destHost,
                                               final int destPort,
                                               final String localHost) {
    StringBuilder hostDetails = new StringBuilder(27);
    hostDetails.append("local host is: ")
        .append(quoteHost(localHost))
        .append("; ")
        .append("destination host is: ").append(quoteHost(destHost))
        .append(":")
        .append(destPort).append("; ");
    return hostDetails.toString();
  }

  /**
   * Quote a hostname if it is not null
   * @param hostname the hostname; nullable
   * @return a quoted hostname or {@link #UNKNOWN_HOST} if the hostname is null
   */
  private static String quoteHost(final String hostname) {
    return (hostname != null) ?
        ("\"" + hostname + "\"")
        : UNKNOWN_HOST;
  }

  /**
   * @return true if the given string is a subnet specified
   *     using CIDR notation, false otherwise
   */
  public static boolean isValidSubnet(String subnet) {
    try {
      new SubnetUtils(subnet);
      return true;
    } catch (IllegalArgumentException iae) {
      return false;
    }
  }

  /**
   * Add all addresses associated with the given nif in the
   * given subnet to the given list.
   */
  private static void addMatchingAddrs(NetworkInterface nif,
      SubnetInfo subnetInfo, List<InetAddress> addrs) {
    Enumeration<InetAddress> ifAddrs = nif.getInetAddresses();
    while (ifAddrs.hasMoreElements()) {
      InetAddress ifAddr = ifAddrs.nextElement();
      if (subnetInfo.isInRange(ifAddr.getHostAddress())) {
        addrs.add(ifAddr);
      }
    }
  }

  /**
   * Return an InetAddress for each interface that matches the
   * given subnet specified using CIDR notation.
   *
   * @param subnet subnet specified using CIDR notation
   * @param returnSubinterfaces
   *            whether to return IPs associated with subinterfaces
   * @throws IllegalArgumentException if subnet is invalid
   */
  public static List<InetAddress> getIPs(String subnet,
      boolean returnSubinterfaces) {
    List<InetAddress> addrs = new ArrayList<InetAddress>();
    SubnetInfo subnetInfo = new SubnetUtils(subnet).getInfo();
    Enumeration<NetworkInterface> nifs;

    try {
      nifs = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      LOG.error("Unable to get host interfaces", e);
      return addrs;
    }

    while (nifs.hasMoreElements()) {
      NetworkInterface nif = nifs.nextElement();
      // NB: adding addresses even if the nif is not up
      addMatchingAddrs(nif, subnetInfo, addrs);

      if (!returnSubinterfaces) {
        continue;
      }
      Enumeration<NetworkInterface> subNifs = nif.getSubInterfaces();
      while (subNifs.hasMoreElements()) {
        addMatchingAddrs(subNifs.nextElement(), subnetInfo, addrs);
      }
    }
    return addrs;
  }

  /**
   * Return a free port number. There is no guarantee it will remain free, so
   * it should be used immediately.
   *
   * @return A free port for binding a local socket
   */
  public static int getFreeSocketPort() {
    int port = 0;
    try {
      ServerSocket s = new ServerSocket(0);
      port = s.getLocalPort();
      s.close();
      return port;
    } catch (IOException e) {
      // Could not get a free port. Return default port 0.
    }
    return port;
  }

  /**
   * Return an @{@link InetAddress} to bind to. If bindWildCardAddress is true
   * than returns null.
   *
   * @param localAddr
   * @param bindWildCardAddress
   * @return InetAddress
   */
  public static InetAddress bindToLocalAddress(InetAddress localAddr, boolean
      bindWildCardAddress) {
    if (!bindWildCardAddress) {
      return localAddr;
    }
    return null;
  }
}
