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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;

//this will need to be replaced someday when there is a suitable replacement
import sun.net.dns.ResolverConfiguration;
import sun.net.util.IPAddressUtil;
import sun.security.jgss.krb5.Krb5Util;
import sun.security.krb5.Credentials;
import sun.security.krb5.PrincipalName;

public class SecurityUtil {
  public static final Log LOG = LogFactory.getLog(SecurityUtil.class);
  public static final String HOSTNAME_PATTERN = "_HOST";

  // controls whether buildTokenService will use an ip or host/ip as given
  // by the user; visible for testing
  static boolean useIpForTokenService;
  static HostResolver hostResolver;
  
  static {
    boolean useIp = new Configuration().getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP,
      CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT);
    setTokenServiceUseIp(useIp);
  }
  
  /**
   * For use only by tests!
   */
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
      if (isOriginalTGT(t.getServer().getName()))
        return t;
    }
    throw new IOException("Failed to find TGT from current Subject:"+current);
  }
  
  // Original TGT must be of form "krbtgt/FOO@FOO". Verify this
  protected static boolean isOriginalTGT(String name) {
    if(name == null) return false;
    
    String [] components = name.split("[/@]");

    return components.length == 3 &&
           "krbtgt".equals(components[0]) &&
           components[1].equals(components[2]);
  }

  /**
   * Explicitly pull the service ticket for the specified host.  This solves a
   * problem with Java's Kerberos SSL problem where the client cannot 
   * authenticate against a cross-realm service.  It is necessary for clients
   * making kerberized https requests to call this method on the target URL
   * to ensure that in a cross-realm environment the remote host will be 
   * successfully authenticated.  
   * 
   * This method is internal to Hadoop and should not be used by other 
   * applications.  This method should not be considered stable or open: 
   * it will be removed when the Java behavior is changed.
   * 
   * @param remoteHost Target URL the krb-https client will access
   * @throws IOException if a service ticket is not available
   */
  public static void fetchServiceTicket(URL remoteHost) throws IOException {
    if(!UserGroupInformation.isSecurityEnabled())
      return;
    
    String serviceName = "host/" + remoteHost.getHost();
    if (LOG.isDebugEnabled())
      LOG.debug("Fetching service ticket for host at: " + serviceName);
    Credentials serviceCred = null;
    try {
      PrincipalName principal = new PrincipalName(serviceName,
          PrincipalName.KRB_NT_SRV_HST);
      serviceCred = Credentials.acquireServiceCreds(principal
          .toString(), Krb5Util.ticketToCreds(getTgtFromSubject()));
    } catch (Exception e) {
      throw new IOException("Can't get service ticket for: "
          + serviceName, e);
    }
    if (serviceCred == null) {
      throw new IOException("Can't get service ticket for " + serviceName);
    }
    Subject.getSubject(AccessController.getContext()).getPrivateCredentials()
        .add(Krb5Util.credsToTicket(serviceCred));
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
   * @throws IOException if the service ticket cannot be retrieved
   */
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
    return components[0] + "/" + fqdn.toLowerCase() + "@" + components[2];
  }
  
  static String getLocalHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  /**
   * If a keytab has been provided, login as that user. Substitute $host in
   * user's Kerberos principal name with a dynamically looked-up fully-qualified
   * domain name of the current host.
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @throws IOException if the client address cannot be determined
   */
  public static void login(final Configuration conf,
      final String keytabFileKey, final String userNameKey) throws IOException {
    login(conf, keytabFileKey, userNameKey, getLocalHostName());
  }

  /**
   * If a keytab has been provided, login as that user. Substitute $host in
   * user's Kerberos principal name with hostname.
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @param hostname
   *          hostname to use for substitution
   * @throws IOException if login fails
   */
  public static void login(final Configuration conf,
      final String keytabFileKey, final String userNameKey, String hostname)
      throws IOException {
    String keytabFilename = conf.get(keytabFileKey);
    if (keytabFilename == null)
      return;

    String principalConfig = conf.get(userNameKey, System
        .getProperty("user.name"));
    String principalName = SecurityUtil.getServerPrincipal(principalConfig,
        hostname);
    UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename);
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
    token.setService(buildTokenService(addr));
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
   * create the service name for a Delegation token
   * @param uri of the service
   * @param defPort is used if the uri lacks a port
   * @return the token service, or null if no authority
   * @see #buildTokenService(InetSocketAddress)
   */
  public static String buildDTServiceName(URI uri, int defPort) {
    String authority = uri.getAuthority();
    if (authority == null || authority.isEmpty()) {
      return null;
    }
    InetSocketAddress addr = NetUtils.createSocketAddr(authority, defPort);
    return buildTokenService(addr).toString();
   }
  
  /**
   * Get the ACL object representing the cluster administrators
   * The user who starts the daemon is automatically added as an admin
   * @param conf
   * @param configKey the key that holds the ACL string in its value
   * @return AccessControlList instance
   */
  public static AccessControlList getAdminAcls(Configuration conf, 
      String configKey) {
    try {
      AccessControlList adminAcl = 
        new AccessControlList(conf.get(configKey, " "));
      adminAcl.addUser(UserGroupInformation.getCurrentUser().
                       getShortUserName());
      return adminAcl;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  /**
   * Get the host name from the principal name of format <service>/host@realm.
   * @param principalName principal name of format as described above
   * @return host name if the the string conforms to the above format, else null
   */
  public static String getHostFromPrincipal(String principalName) {
    return new KerberosName(principalName).getHostName();
  }
  
  /**
   * Resolves a host subject to the security requirements determined by
   * hadoop.security.token.service.use_ip.
   * 
   * @param hostname host or ip to resolve
   * @return a resolved host
   * @throws UnknownHostException if the host doesn't exist
   */
  //@InterfaceAudience.Private
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
}
