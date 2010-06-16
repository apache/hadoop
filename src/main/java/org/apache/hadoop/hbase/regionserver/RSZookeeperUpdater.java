package org.apache.hadoop.hbase.regionserver;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.executor.HBaseEventHandler;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * This is a helper class for region servers to update various states in 
 * Zookeeper. The various updates are abstracted out here. 
 * 
 * The "startRegionXXX" methods are to be called first, followed by the 
 * "finishRegionXXX" methods. Supports updating zookeeper periodically as a 
 * part of the "startRegionXXX". Currently handles the following state updates:
 *   - Close region
 *   - Open region
 */
// TODO: make this thread local, in which case it is re-usable per thread
public class RSZookeeperUpdater {
  private static final Log LOG = LogFactory.getLog(RSZookeeperUpdater.class);
  private final String regionServerName;
  private String regionName = null;
  private String regionZNode = null;
  private ZooKeeperWrapper zkWrapper = null;
  private int zkVersion = 0;
  HBaseEventType lastUpdatedState;

  public RSZookeeperUpdater(Configuration conf,
                            String regionServerName, String regionName) {
    this(conf, regionServerName, regionName, 0);
  }
  
  public RSZookeeperUpdater(Configuration conf, String regionServerName,
                            String regionName, int zkVersion) {
    this.zkWrapper = ZooKeeperWrapper.getInstance(conf, regionServerName);
    this.regionServerName = regionServerName;
    this.regionName = regionName;
    // get the region ZNode we have to create
    this.regionZNode = zkWrapper.getZNode(zkWrapper.getRegionInTransitionZNode(), regionName);
    this.zkVersion = zkVersion;
  }
  
  /**
   * This method updates the various states in ZK to inform the master that the 
   * region server has started closing the region.
   * @param updatePeriodically - if true, periodically updates the state in ZK
   */
  public void startRegionCloseEvent(HMsg hmsg, boolean updatePeriodically) throws IOException {
    // if this ZNode already exists, something is wrong
    if(zkWrapper.exists(regionZNode, true)) {
      String msg = "ZNode " + regionZNode + " already exists in ZooKeeper, will NOT close region.";
      LOG.error(msg);
      throw new IOException(msg);
    }
    
    // create the region node in the unassigned directory first
    zkWrapper.createZNodeIfNotExists(regionZNode, null, CreateMode.PERSISTENT, true);

    // update the data for "regionName" ZNode in unassigned to CLOSING
    updateZKWithEventData(HBaseEventType.RS2ZK_REGION_CLOSING, hmsg);
    
    // TODO: implement the updatePeriodically logic here
  }

  /**
   * This method updates the states in ZK to signal that the region has been 
   * closed. This will stop the periodic updater thread if one was started.
   * @throws IOException
   */
  public void finishRegionCloseEvent(HMsg hmsg) throws IOException {    
    // TODO: stop the updatePeriodically here

    // update the data for "regionName" ZNode in unassigned to CLOSED
    updateZKWithEventData(HBaseEventType.RS2ZK_REGION_CLOSED, hmsg);
  }
  
  /**
   * This method updates the various states in ZK to inform the master that the 
   * region server has started opening the region.
   * @param updatePeriodically - if true, periodically updates the state in ZK
   */
  public void startRegionOpenEvent(HMsg hmsg, boolean updatePeriodically) throws IOException {
    Stat stat = new Stat();
    byte[] data = zkWrapper.readZNode(regionZNode, stat);
    // if there is no ZNode for this region, something is wrong
    if(data == null) {
      String msg = "ZNode " + regionZNode + " does not exist in ZooKeeper, will NOT open region.";
      LOG.error(msg);
      throw new IOException(msg);
    }
    // if the ZNode is not in the closed state, something is wrong
    HBaseEventType rsEvent = HBaseEventType.fromByte(data[0]);
    if(rsEvent != HBaseEventType.RS2ZK_REGION_CLOSED && rsEvent != HBaseEventType.M2ZK_REGION_OFFLINE) {
      String msg = "ZNode " + regionZNode + " is not in CLOSED/OFFLINE state (state = " + rsEvent + "), will NOT open region.";
      LOG.error(msg);
      throw new IOException(msg);
    }

    // get the version to update from ZK
    zkVersion = stat.getVersion();

    // update the data for "regionName" ZNode in unassigned to CLOSING
    updateZKWithEventData(HBaseEventType.RS2ZK_REGION_OPENING, hmsg);
    
    // TODO: implement the updatePeriodically logic here
  }
  
  /**
   * This method updates the states in ZK to signal that the region has been 
   * opened. This will stop the periodic updater thread if one was started.
   * @throws IOException
   */
  public void finishRegionOpenEvent(HMsg hmsg) throws IOException {
    // TODO: stop the updatePeriodically here

    // update the data for "regionName" ZNode in unassigned to CLOSED
    updateZKWithEventData(HBaseEventType.RS2ZK_REGION_OPENED, hmsg);
  }
  
  public boolean isClosingRegion() {
    return (lastUpdatedState == HBaseEventType.RS2ZK_REGION_CLOSING);
  }

  public boolean isOpeningRegion() {
    return (lastUpdatedState == HBaseEventType.RS2ZK_REGION_OPENING);
  }

  public void abortOpenRegion(HMsg hmsg) throws IOException {
    LOG.error("Aborting open of region " + regionName);

    // TODO: stop the updatePeriodically for start open region here

    // update the data for "regionName" ZNode in unassigned to CLOSED
    updateZKWithEventData(HBaseEventType.RS2ZK_REGION_CLOSED, hmsg);
  }

  private void updateZKWithEventData(HBaseEventType hbEventType, HMsg hmsg) throws IOException {
    // update the data for "regionName" ZNode in unassigned to "hbEventType"
    byte[] data = null;
    try {
      data = Writables.getBytes(new RegionTransitionEventData(hbEventType, regionServerName, hmsg));
    } catch (IOException e) {
      LOG.error("Error creating event data for " + hbEventType, e);
    }
    LOG.debug("Updating ZNode " + regionZNode + 
              " with [" + hbEventType + "]" +
              " expected version = " + zkVersion);
    lastUpdatedState = hbEventType;
    zkWrapper.writeZNode(regionZNode, data, zkVersion, true);
    zkVersion++;
  }
}
