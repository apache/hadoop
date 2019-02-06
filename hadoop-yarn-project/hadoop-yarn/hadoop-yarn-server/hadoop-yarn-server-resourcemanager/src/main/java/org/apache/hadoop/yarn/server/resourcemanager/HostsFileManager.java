package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

public class HostsFileManager implements HostsConfigManager {

  private static final Log LOG = LogFactory.getLog(HostsFileManager.class);

  private HostsFileReader hostsFileReader;

  @Override
  public void refresh(ConfigurationProvider configProvider, Configuration yarnConfig) {
    try {
      String includesFile = yarnConfig.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
          YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH);
      String excludesFile = yarnConfig.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
          YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
      this.hostsFileReader =
          createHostsFileReader(configProvider, yarnConfig, includesFile, excludesFile);
    } catch (YarnException | IOException ex) {
      disableHostsFileReader(configProvider, yarnConfig, ex);
    }
  }

  @Override
  public Set<String> getIncludedHosts() {
    return hostsFileReader.getHosts();
  }

  @Override
  public Set<String> getExcludedHosts() {
    return hostsFileReader.getExcludedHosts();
  }

  @Override
  public Map<String, Integer> getExcludedHostsMap() {
    return hostsFileReader.getHostDetails().getExcludedMap();
  }

  private HostsFileReader createHostsFileReader(ConfigurationProvider configProvider,
      Configuration yarnConf, String includesFile,
      String excludesFile) throws IOException, YarnException {
    HostsFileReader hostsReader =
        new HostsFileReader(includesFile,
            (includesFile == null || includesFile.isEmpty()) ? null
                : configProvider.getConfigurationInputStream(yarnConf, includesFile),
            excludesFile,
            (excludesFile == null || excludesFile.isEmpty()) ? null
                : configProvider.getConfigurationInputStream(yarnConf, excludesFile));
    return hostsReader;
  }

  private void disableHostsFileReader(ConfigurationProvider configProvider,
      Configuration yarnConfig, Exception ex) {
    LOG.warn("Failed to init hostsReader, disabling", ex);
    try {
      String includesFile =
          yarnConfig.get(YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH);
      String excludesFile =
          yarnConfig.get(YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
      hostsFileReader =
          createHostsFileReader(configProvider, yarnConfig, includesFile, excludesFile);
    } catch (IOException | YarnException e) {
      // Should *never* happen
      hostsFileReader = null;
      throw new YarnRuntimeException(e);
    }
  }
}
