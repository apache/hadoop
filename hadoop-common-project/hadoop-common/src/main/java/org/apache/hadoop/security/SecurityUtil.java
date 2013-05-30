/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;

import com.google.common.annotations.VisibleForTesting;

//this will need to be replaced someday when there is a suitable replacement
import sun.net.dns.ResolverConfiguration;
import sun.net.util.IPAddressUtil;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SecurityUtil {
  public static final Log LOG = LogFactory.getLog(SecurityUtil.class);
  public static final String HOSTNAME_PATTERN = "_HOST";

  // controls whether buildTokenService will use an ip or host/ip as given
  // by the user
  @VisibleForTesting
  static boolean useIpForTokenService;
  @VisibleForTesting
  static HostResolver hostResolver;

  private static SSLFactory sslFactory;

  static {
    Configuration conf = new Configuration();
    boolean useIp = conf.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP,
      CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT);
    setTokenServiceUseIp(useIp);
    if (HttpConfig.isSecure()) {
      sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
      try {
        sslFactory.init();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }
  
  /**
   * For use only by tests and initialization
   */
  @InterfaceAudience.Private
  static void setTokenServiceUseIp(boolean flag) {
    useIpForTokenService = flag;
    hostResolver = !useIpForTokenService
        ? new QualifiedHostResolver()
        : new StandardHostResolver();
  }
  
  /**
   * Find the original TGT within the current subject's credentials. Cross-realm
   * TGT's of the form "krbtgt/TWO.COM@ONE.COM" may be present.
   * 
   * @return The TGT from the current subject
   * @throws IOException
   *           if TGT can't be found
   */
  private static KerberosTicket getTgtFromSubject() throws IOException {
    Subject current = Subject.getSubject(AccessController.getContext());
    if (current == null) {
      throw new IOException(
          "Can't get TGT from current Subject, because it is null");
    }
    Set<KerberosTicket> tickets = current
        .getPrivateCredentials(KerberosTicket.class);
    for (KerberosTicket t : tickets) {
      if (isOriginalTGT(t))
        return t;
    }
    throw new IOException("Failed to find TGT from current Subject:"+current);
  }
  
  /**
   * TGS must have the server principal of the form "krbtgt/FOO@FOO".
   * @param principal
   * @return true or false
   */
  static boolean 
  isTGSPrincipal(KerberosPrincipal principal) {
    if (principal == null)
      return false;
    if (principal.getName().equals("krbtgt/" + principal.getRealm() + 
        "@" + principal.getRealm())) {
      return true;
    }
    return false;
  }
  
  /**
   * Check whether the server principal is the TGS's principal
   * @param ticket the original TGT (the ticket that is obtained when a 
   * kinit is done)
   * @return true or false
   */
  protected static boolean isOriginalTGT(KerberosTicket ticket) {
    return isTGSPrincipal(ticket.getServer());
  }

  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal
   * names. It replaces hostname pattern with hostname, which should be
   * fully-qualified domain name. If hostname is null or "0.0.0.0", it uses
   * dynamically looked-up fqdn of the current host instead.
   * 
   * @param principalConfig
   *          the Kerberos principal name conf value to convert
   * @param hostname
   *          the fully-qualified domain name used for substitution
   * @return converted Kerberos principal name
   * @throws IOException if the client address cannot be determined
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static String getServerPrincipal(String principalConfig,
      String hostname) throws IOException {
    String[] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      return replacePattern(components, hostname);
    }
  }
  
  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal names.
   * This method is similar to {@link #getServerPrincipal(String, String)},
   * except 1) the reverse DNS lookup from addr to hostname is done only when
   * necessary, 2) param addr can't be null (no default behavior of using local
   * hostname when addr is null).
   * 
   * @param principalConfig
   *          Kerberos principal name pattern to convert
   * @param addr
   *          InetAddress of the host used for substitution
   * @return converted Kerberos principal name
   * @throws IOException if the client address cannot be determined
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static String getServerPrincipal(String principalConfig,
      InetAddress addr) throws IOException {
    String[] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      if (addr == null) {
        throw new IOException("Can't replace " + HOSTNAME_PATTERN
            + " pattern since client address is null");
      }
      return replacePattern(components, addr.getCanonicalHostName());
    }
  }
  
  private static String[] getComponents(String principalConfig) {
    if (principalConfig == null)
      return null;
    return principalConfig.split("[/@]");
  }
  
  private static String replacePattern(String[] components, String hostname)
      throws IOException {
    String fqdn = hostname;
    if (fqdn == null || fqdn.equals("") || fqdn.equals("0.0.0.0")) {
      fqdn = getLocalHostName();
    }
    return components[0] + "/" + fqdn.toLowerCase(Locale.US) + "@" + components[2];
  }
  
  static String getLocalHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  /**
   * Login as a principal specified in config. Substitute $host in
   * user's Kerberos principal name with a dynamically looked-up fully-qualified
   * domain name of the current host.
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void login(final Configuration conf,
      final String keytabFileKey, final String userNameKey) throws IOException {
    login(conf, keytabFileKey, userNameKey, getLocalHostName());
  }

  /**
   * Login as a principal specified in config. Substitute $host in user's Kerberos principal 
   * name with hostname. If non-secure mode - return. If no keytab available -
   * bail out with an exception
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @param hostname
   *          hostname to use for substitution
   * @throws IOException if the config doesn't specify a keytab
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void login(final Configuration conf,
      final String keytabFileKey, final String userNameKey, String hostname)
      throws IOException {
    
    if(! UserGroupInformation.isSecurityEnabled()) 
      return;
    
    String keytabFilename = conf.get(keytabFileKey);
    if (keytabFilename == null || keytabFilename.length() == 0) {
      throw new IOException("Running in secure mode, but config doesn't have a keytab");
    }

    String principalConfig = conf.get(userNameKey, System
        .getProperty("user.name"));
    String principalName = SecurityUtil.getServerPrincipal(principalConfig,
        hostname);
    UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename);
  }

  /**
   * create the service name for a Delegation token
   * @param uri of the service
   * @param defPort is used if the uri lacks a port
   * @return the token service, or null if no authority
   * @see #buildTokenService(InetSocketAddress)
   */
  public static String buildDTServiceName(URI uri, int defPort) {
    String authority = uri.getAuthority();
    if (authority == null) {
      return null;
    }
    InetSocketAddress addr = NetUtils.createSocketAddr(authority, defPort);
    return buildTokenService(addr).toString();
   }
  
  /**
   * Get the host name from the principal name of format <service>/host@realm.
   * @param principalName principal name of format as described above
   * @return host name if the the string conforms to the above format, else null
   */
  public static String getHostFromPrincipal(String principalName) {
    return new HadoopKerberosName(principalName).getHostName();
  }

  private static ServiceLoader<SecurityInfo> securityInfoProviders = 
    ServiceLoader.load(SecurityInfo.class);
  private static SecurityInfo[] testProviders = new SecurityInfo[0];

  /**
   * Test setup method to register additional providers.
   * @param providers a list of high priority providers to use
   */
  @InterfaceAudience.Private
  public static void setSecurityInfoProviders(SecurityInfo... providers) {
    testProviders = providers;
  }
  
  /**
   * Look up the KerberosInfo for a given protocol. It searches all known
   * SecurityInfo providers.
   * @param protocol the protocol class to get the information for
   * @param conf configuration object
   * @return the KerberosInfo or null if it has no KerberosInfo defined
   */
  public static KerberosInfo 
  getKerberosInfo(Class<?> protocol, Configuration conf) {
    synchronized (testProviders) {
      for(SecurityInfo provider: testProviders) {
        KerberosInfo result = provider.getKerberosInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      }
    }
    
    synchronized (securityInfoProviders) {
      for(SecurityInfo provider: securityInfoProviders) {
        KerberosInfo result = provider.getKerberosInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      }
    }
    return null;
  }
 
  /**
   * Look up the TokenInfo for a given protocol. It searches all known
   * SecurityInfo providers.
   * @param protocol The protocol class to get the information for.
   * @param conf Configuration object
   * @return the TokenInfo or null if it has no KerberosInfo defined
   */
  public static TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    synchronized (testProviders) {
      for(SecurityInfo provider: testProviders) {
        TokenInfo result = provider.getTokenInfo(protocol, conf);
        if (result != null) {
          return result;
        }      
      }
    }
    
    synchronized (securityInfoProviders) {
      for(SecurityInfo provider: securityInfoProviders) {
        TokenInfo result = provider.getTokenInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      } 
    }
    
    return null;
  }

  /**
   * Decode the given token's service field into an InetAddress
   * @param token from which to obtain the service
   * @return InetAddress for the service
   */
  public static InetSocketAddress getTokenServiceAddr(Token<?> token) {
    return NetUtils.createSocketAddr(token.getService().toString());
  }

  /**
   * Set the given token's service to the format expected by the RPC client 
   * @param token a delegation token
   * @param addr the socket for the rpc connection
   */
  public static void setTokenService(Token<?> token, InetSocketAddress addr) {
    Text service = buildTokenService(addr);
    if (token != null) {
      token.setService(service);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Acquired token "+token);  // Token#toString() prints service
      }
    } else {
      LOG.warn("Failed to get token for service "+service);
    }
  }
  
  /**
   * Construct the service key for a token
   * @param addr InetSocketAddress of remote connection with a token
   * @return "ip:port" or "host:port" depending on the value of
   *          hadoop.security.token.service.use_ip
   */
  public static Text buildTokenService(InetSocketAddress addr) {
    String host = null;
    if (useIpForTokenService) {
      if (addr.isUnresolved()) { // host has no ip address
        throw new IllegalArgumentException(
            new UnknownHostException(addr.getHostName())
        );
      }
      host = addr.getAddress().getHostAddress();
    } else {
      host = addr.getHostName().toLowerCase();
    }
    return new Text(host + ":" + addr.getPort());
  }

  /**
   * Construct the service key for a token
   * @param uri of remote connection with a token
   * @return "ip:port" or "host:port" depending on the value of
   *          hadoop.security.token.service.use_ip
   */
  public static Text buildTokenService(URI uri) {
    return buildTokenService(NetUtils.createSocketAddr(uri.getAuthority()));
  }
  
  /**
   * Perform the given action as the daemon's login user. If the login
   * user cannot be determined, this will log a FATAL error and exit
   * the whole JVM.
   */
  public static <T> T doAsLoginUserOrFatal(PrivilegedAction<T> action) { 
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation ugi = null;
      try { 
        ugi = UserGroupInformation.getLoginUser();
      } catch (IOException e) {
        LOG.fatal("Exception while getting login user", e);
        e.printStackTrace();
        Runtime.getRuntime().exit(-1);
      }
      return ugi.doAs(action);
    } else {
      return action.run();
    }
  }
  
  /**
   * Perform the given action as the daemon's login user. If an
   * InterruptedException is thrown, it is converted to an IOException.
   *
   * @param action the action to perform
   * @return the result of the action
   * @throws IOException in the event of error
   */
  public static <T> T doAsLoginUser(PrivilegedExceptionAction<T> action)
      throws IOException {
    return doAsUser(UserGroupInformation.getLoginUser(), action);
  }

  /**
   * Perform the given action as the daemon's current user. If an
   * InterruptedException is thrown, it is converted to an IOException.
   *
   * @param action the action to perform
   * @return the result of the action
   * @throws IOException in the event of error
   */
  public static <T> T doAsCurrentUser(PrivilegedExceptionAction<T> action)
      throws IOException {
    return doAsUser(UserGroupInformation.getCurrentUser(), action);
  }

  private static <T> T doAsUser(UserGroupInformation ugi,
      PrivilegedExceptionAction<T> action) throws IOException {
    try {
      return ugi.doAs(action);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Open a (if need be) secure connection to a URL in a secure environment
   * that is using SPNEGO to authenticate its URLs. All Namenode and Secondary
   * Namenode URLs that are protected via SPNEGO should be accessed via this
   * method.
   *
   * @param url to authenticate via SPNEGO.
   * @return A connection that has been authenticated via SPNEGO
   * @throws IOException If unable to authenticate via SPNEGO
   */
  public static URLConnection openSecureHttpConnection(URL url) throws IOException {
    if (!HttpConfig.isSecure() && !UserGroupInformation.isSecurityEnabled()) {
      return url.openConnection();
    }

    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    try {
      return new AuthenticatedURL(null, sslFactory).openConnection(url, token);
    } catch (AuthenticationException e) {
      throw new IOException("Exception trying to open authenticated connection to "
              + url, e);
    }
  }

  /**
   * Resolves a host subject to the security requirements determined by
   * hadoop.security.token.service.use_ip.
   * 
   * @param hostname host or ip to resolve
   * @return a resolved host
   * @throws UnknownHostException if the host doesn't exist
   */
  @InterfaceAudience.Private
  public static
  InetAddress getByName(String hostname) throws UnknownHostException {
    return hostResolver.getByName(hostname);
  }
  
  interface HostResolver {
    InetAddress getByName(String host) throws UnknownHostException;    
  }
  
  /**
   * Uses standard java host resolution
   */
  static class StandardHostResolver implements HostResolver {
    @Override
    public InetAddress getByName(String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }
  }
  
  /**
   * This an alternate resolver with important properties that the standard
   * java resolver lacks:
   * 1) The hostname is fully qualified.  This avoids security issues if not
   *    all hosts in the cluster do not share the same search domains.  It
   *    also prevents other hosts from performing unnecessary dns searches.
   *    In contrast, InetAddress simply returns the host as given.
   * 2) The InetAddress is instantiated with an exact host and IP to prevent
   *    further unnecessary lookups.  InetAddress may perform an unnecessary
   *    reverse lookup for an IP.
   * 3) A call to getHostName() will always return the qualified hostname, or
   *    more importantly, the IP if instantiated with an IP.  This avoids
   *    unnecessary dns timeouts if the host is not resolvable.
   * 4) Point 3 also ensures that if the host is re-resolved, ex. during a
   *    connection re-attempt, that a reverse lookup to host and forward
   *    lookup to IP is not performed since the reverse/forward mappings may
   *    not always return the same IP.  If the client initiated a connection
   *    with an IP, then that IP is all that should ever be contacted.
   *    
   * NOTE: this resolver is only used if:
   *       hadoop.security.token.service.use_ip=false 
   */
  protected static class QualifiedHostResolver implements HostResolver {
    @SuppressWarnings("unchecked")
    private List<String> searchDomains =
        ResolverConfiguration.open().searchlist();
    
    /**
     * Create an InetAddress with a fully qualified hostname of the given
     * hostname.  InetAddress does not qualify an incomplete hostname that
     * is resolved via the domain search list.
     * {@link InetAddress#getCanonicalHostName()} will fully qualify the
     * hostname, but it always return the A record whereas the given hostname
     * may be a CNAME.
     * 
     * @param host a hostname or ip address
     * @return InetAddress with the fully qualified hostname or ip
     * @throws UnknownHostException if host does not exist
     */
    @Override
    public InetAddress getByName(String host) throws UnknownHostException {
      InetAddress addr = null;

      if (IPAddressUtil.isIPv4LiteralAddress(host)) {
        // use ipv4 address as-is
        byte[] ip = IPAddressUtil.textToNumericFormatV4(host);
        addr = InetAddress.getByAddress(host, ip);
      } else if (IPAddressUtil.isIPv6LiteralAddress(host)) {
        // use ipv6 address as-is
        byte[] ip = IPAddressUtil.textToNumericFormatV6(host);
        addr = InetAddress.getByAddress(host, ip);
      } else if (host.endsWith(".")) {
        // a rooted host ends with a dot, ex. "host."
        // rooted hosts never use the search path, so only try an exact lookup
        addr = getByExactName(host);
      } else if (host.contains(".")) {
        // the host contains a dot (domain), ex. "host.domain"
        // try an exact host lookup, then fallback to search list
        addr = getByExactName(host);
        if (addr == null) {
          addr = getByNameWithSearch(host);
        }
      } else {
        // it's a simple host with no dots, ex. "host"
        // try the search list, then fallback to exact host
        InetAddress loopback = InetAddress.getByName(null);
        if (host.equalsIgnoreCase(loopback.getHostName())) {
          addr = InetAddress.getByAddress(host, loopback.getAddress());
        } else {
          addr = getByNameWithSearch(host);
          if (addr == null) {
            addr = getByExactName(host);
          }
        }
      }
      // unresolvable!
      if (addr == null) {
        throw new UnknownHostException(host);
      }
      return addr;
    }

    InetAddress getByExactName(String host) {
      InetAddress addr = null;
      // InetAddress will use the search list unless the host is rooted
      // with a trailing dot.  The trailing dot will disable any use of the
      // search path in a lower level resolver.  See RFC 1535.
      String fqHost = host;
      if (!fqHost.endsWith(".")) fqHost += ".";
      try {
        addr = getInetAddressByName(fqHost);
        // can't leave the hostname as rooted or other parts of the system
        // malfunction, ex. kerberos principals are lacking proper host
        // equivalence for rooted/non-rooted hostnames
        addr = InetAddress.getByAddress(host, addr.getAddress());
      } catch (UnknownHostException e) {
        // ignore, caller will throw if necessary
      }
      return addr;
    }

    InetAddress getByNameWithSearch(String host) {
      InetAddress addr = null;
      if (host.endsWith(".")) { // already qualified?
        addr = getByExactName(host); 
      } else {
        for (String domain : searchDomains) {
          String dot = !domain.startsWith(".") ? "." : "";
          addr = getByExactName(host + dot + domain);
          if (addr != null) break;
        }
      }
      return addr;
    }

    // implemented as a separate method to facilitate unit testing
    InetAddress getInetAddressByName(String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }

    void setSearchDomains(String ... domains) {
      searchDomains = Arrays.asList(domains);
    }
  }

  public static AuthenticationMethod getAuthenticationMethod(Configuration conf) {
    String value = conf.get(HADOOP_SECURITY_AUTHENTICATION, "simple");
    try {
      return Enum.valueOf(AuthenticationMethod.class, value.toUpperCase());
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException("Invalid attribute value for " +
          HADOOP_SECURITY_AUTHENTICATION + " of " + value);
    }
  }

  public static void setAuthenticationMethod(
      AuthenticationMethod authenticationMethod, Configuration conf) {
    if (authenticationMethod == null) {
      authenticationMethod = AuthenticationMethod.SIMPLE;
    }
    conf.set(HADOOP_SECURITY_AUTHENTICATION,
             authenticationMethod.toString().toLowerCase());
  }
}
