package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;

/**
 * This interface provides the abstraction for node membership (host config) management.
 * <p>
 * Implementations of this interface can manage node membership with different config sources, e.g.
 * local file, database or ZooKeeper, etc. The implementation class chosen is specified by the
 * property <code>yarn.resourcemanager.hosts.provider.classname</code>.
 */
public interface HostsConfigManager {

  /**
   * Refreshes the node membership config.
   * <p>
   * This method is called by the framework when initializing the cluster or when the external
   * config is changed. Subsequent calls of <code>getIncludedHosts</code> and
   * <code>getExludedHosts</code> should reflect the latest config.
   */
  void refresh(ConfigurationProvider configProvider, Configuration yarnConfig);

  /**
   * Gets the hostnames of the included nodes.
   */
  Set<String> getIncludedHosts();

  /**
   * Gets the hostnames of the excluded nodes.
   */
  Set<String> getExcludedHosts();

  /**
   * Gets the map of excluded nodes.
   * <p>
   * The key of the map is hostname, the value is optional timeout, value being null indicates
   * default timeout.
   */
  Map<String, Integer> getExcludedHostsMap();
}
