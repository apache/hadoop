package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

public class NMLivelinessMonitor extends AbstractLivelinessMonitor<NodeId> {

  private EventHandler dispatcher;
  
  public NMLivelinessMonitor(Dispatcher d) {
    super("NMLivelinessMonitor", new SystemClock());
    this.dispatcher = d.getEventHandler();
  }

  public void init(Configuration conf) {
    super.init(conf);
    setExpireInterval(conf.getInt(RMConfig.NM_EXPIRY_INTERVAL,
        RMConfig.DEFAULT_NM_EXPIRY_INTERVAL));
    setMonitorInterval(conf.getInt(
        RMConfig.NMLIVELINESS_MONITORING_INTERVAL,
        RMConfig.DEFAULT_NMLIVELINESS_MONITORING_INTERVAL));
  }

  @Override
  protected void expire(NodeId id) {
    dispatcher.handle(
        new RMNodeEvent(id, RMNodeEventType.EXPIRE)); 
  }
}