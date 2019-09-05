package org.apache.hadoop.yarn.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.DefaultFailoverProxyProvider;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An implementation of {@link RMFailoverProxyProvider} which does nothing in the
 * event of failover, and always returns the same proxy object.
 * This is the default non-HA RM Failover proxy provider. It is used to replace
 * {@Link DefaultFailoveProxyProvider} which was used as Yarn default non-HA.
 */
public class DefaultNoHARMFailoverProxyProvider<T>
    implements RMFailoverProxyProvider<T> {
  private static final Logger LOG =
    LoggerFactory.getLogger(DefaultNoHARMFailoverProxyProvider.class);
  protected T proxy;
  protected Class<T> protocol;

  /**
   * Initialize internal data structures, invoked right after instantiation.
   *
   * @param conf     Configuration to use
   * @param proxy    The {@link RMProxy} instance to use
   * @param protocol The communication protocol to use
   */
  @Override
  public void init(Configuration conf, RMProxy<T> proxy,
      Class<T> protocol) {
    this.protocol = protocol;
    try {
      InetSocketAddress rmAddress =
          proxy.getRMAddress((YarnConfiguration) conf, protocol);
      LOG.info("Connecting to ResourceManager at " + rmAddress);
      this.proxy = proxy.getProxy(conf, protocol, rmAddress);
    } catch (IOException ioe) {
      LOG.error("Unable to create proxy to the ResourceManager ", ioe);
    }
  }

  /**
   * Get the protocol.
   * @return
   */
  @Override
  public Class<T> getInterface() {
    return protocol;
  }

  /**
   * Get the Proxy.
   * @return
   */
  @Override
  public ProxyInfo<T> getProxy() {
    return new ProxyInfo<T>(proxy, null);
  }

  /**
   * PerformFailover does nothing in this class.
   * @param currentProxy
   */
  @Override
  public void performFailover(T currentProxy) {
    // Nothing to do.
  }

  /**
   * Close the current proxy.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    RPC.stopProxy(proxy);
  }
}
