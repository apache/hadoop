package org.apache.hadoop.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import java.net.InetAddress;

public final class InetAddressUtils {

  private static final Logger LOG = LoggerFactory.getLogger(InetAddressUtils.class);

  private InetAddressUtils() {
  }

  /**
   * Gets the fully qualified domain name for this IP address.  It uses the internally cached
   * <code>canonicalHostName</code> when it is available, but will fall back to attempting a reverse
   * DNS lookup when needed.  This is useful when an FQDN is required, and you are running in a
   * managed environment where IP addresses can change.
   *
   * @param addr the target address
   * @return the fully qualified domain name for this IP address, or the textual representation of
   *        the IP address.
   */
  public static String getCanonicalHostName(InetAddress addr) {
    String canonicalHostName = addr.getCanonicalHostName();
    if (canonicalHostName.equals(addr.getHostAddress())) {
      try {
        return DNS.reverseDns(addr, null);
      } catch (NamingException lookupFailure) {
        LOG.warn("Failed to perform reverse lookup: {}", addr, lookupFailure);
      }
    }
    return canonicalHostName;
  }

}
