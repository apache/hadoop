package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.service.AbstractService;

public class NodesListManager extends AbstractService{

  private static final Log LOG = LogFactory.getLog(NodesListManager.class);

  private HostsFileReader hostsReader;
  private Configuration conf;

  public NodesListManager() {
    super(NodesListManager.class.getName());
  }

  @Override
  public void init(Configuration conf) {

    this.conf = conf;

    // Read the hosts/exclude files to restrict access to the RM
    try {
      this.hostsReader = 
        new HostsFileReader(
            conf.get(RMConfig.RM_NODES_INCLUDE_FILE, 
                RMConfig.DEFAULT_RM_NODES_INCLUDE_FILE),
            conf.get(RMConfig.RM_NODES_EXCLUDE_FILE, 
                RMConfig.DEFAULT_RM_NODES_EXCLUDE_FILE)
                );
      printConfiguredHosts();
    } catch (IOException ioe) {
      LOG.warn("Failed to init hostsReader, disabling", ioe);
      try {
        this.hostsReader = 
          new HostsFileReader(RMConfig.DEFAULT_RM_NODES_INCLUDE_FILE, 
              RMConfig.DEFAULT_RM_NODES_EXCLUDE_FILE);
      } catch (IOException ioe2) {
        // Should *never* happen
        this.hostsReader = null;
        throw new YarnException(ioe2);
      }
    }
    super.init(conf);
  }

  private void printConfiguredHosts() {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    
    LOG.debug("hostsReader: in=" + conf.get(RMConfig.RM_NODES_INCLUDE_FILE, 
        RMConfig.DEFAULT_RM_NODES_INCLUDE_FILE) + " out=" +
        conf.get(RMConfig.RM_NODES_EXCLUDE_FILE, 
            RMConfig.DEFAULT_RM_NODES_EXCLUDE_FILE));
    for (String include : hostsReader.getHosts()) {
      LOG.debug("include: " + include);
    }
    for (String exclude : hostsReader.getExcludedHosts()) {
      LOG.debug("exclude: " + exclude);
    }
  }

  public void refreshNodes() throws IOException {
    synchronized (hostsReader) {
      hostsReader.refresh();
      printConfiguredHosts();
    }
  }

  public boolean isValidNode(String hostName) {
    synchronized (hostsReader) {
      Set<String> hostsList = hostsReader.getHosts();
      Set<String> excludeList = hostsReader.getExcludedHosts();
      return ((hostsList.isEmpty() || hostsList.contains(hostName)) && 
          !excludeList.contains(hostName));
    }
  }
}
